#!/usr/bin/env python3
"""Build a shareable Garden Friends release from source and static artwork.

The source application and artwork tree are read-only inputs. Runtime settings,
credentials, camera snapshots and old backup trees are never copied.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import html
import json
import os
from pathlib import Path
import re
import shutil
import stat
import tempfile
from urllib.parse import quote
import xml.etree.ElementTree as ET
import zipfile

VERSION = '1.0.0'
MIB = 1024 * 1024
IMAGE_SUFFIXES = {'.png', '.webp', '.jpg', '.jpeg', '.gif', '.svg', '.ico'}
METADATA_NAMES = {
    'masks.json', 'ratios.json', 'art_ratios.json', 'artwork_ratios.json',
    'species_ratios.json', 'scale_overrides.json', 'species_sizes.json',
    'dimensions.json', 'art_manifest.json', 'artwork_manifest.json',
    'webp_manifest.json', 'manifest.json',
}
SOURCE_NAMES = {
    'main.py', 'garden_friends_settings.py', 'server.py', 'requirements.txt', 'requirements-dev.txt',
    'Dockerfile', 'compose.yaml', '.env.example', '.dockerignore', '.gitignore',
    'README.md', 'CHANGELOG.md', 'CONTRIBUTING.md', 'SECURITY.md', 'NOTICE.md',
    'LICENSE', 'LICENSE.md', 'LICENSE.txt',
}
SOURCE_DIRS = {'static', 'docs', 'tests', 'tools', '.github'}
PRIVATE_NAMES = {'admin.json', 'settings.json', 'setup-code.txt', 'restart.request'}
PRIVATE_DIR = re.compile(r'(^|[._-])(backup|backups|snapshot|snapshots|capture|captures|events|private|logs|secrets)([._-]|$)', re.I)
RUNTIME_IMAGE = re.compile(r'^(latest[_-](visual|snapshot|camera)|snapshot|camera[_-]capture)([._-]|$)', re.I)
WINDOWS_RESERVED = re.compile(r'^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\.|$)', re.I)
GALLERY_START = '<!-- GARDEN_FRIENDS_ARTWORK_GALLERY_START -->'
GALLERY_END = '<!-- GARDEN_FRIENDS_ARTWORK_GALLERY_END -->'


def fail(message):
    raise ValueError(message)


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(MIB), b''):
            value.update(chunk)
    return value.hexdigest()


def portable_path(relative):
    for part in relative.parts:
        if (not part or part in {'.', '..'} or re.search(r'[<>:"\\|?*\x00-\x1f]', part)
                or part.endswith((' ', '.')) or WINDOWS_RESERVED.match(part)):
            fail('An artwork filename is not portable to Windows: ' + str(relative))


def file_identity(path):
    info = path.stat()
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns


def safe_svg(path):
    raw = path.read_bytes()
    if len(raw) > 8 * MIB or b'<!DOCTYPE' in raw.upper() or b'<!ENTITY' in raw.upper():
        fail('Unsupported SVG artwork: ' + path.name)
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        fail('Invalid SVG artwork: ' + path.name)
    for element in root.iter():
        tag = element.tag.rsplit('}', 1)[-1].lower()
        if tag in {'script', 'foreignobject', 'iframe', 'object', 'embed'}:
            fail('SVG artwork contains active content: ' + path.name)
        for key, value in element.attrib.items():
            key = key.rsplit('}', 1)[-1].lower()
            if key.startswith('on') or (key in {'href', 'src'} and not value.startswith('#')):
                fail('SVG artwork contains active or external content: ' + path.name)
            for reference in re.findall(r'url\(([^)]*)\)', value, re.I):
                if not reference.strip(' \t\r\n\"\'').startswith('#'):
                    fail('SVG artwork contains an external reference: ' + path.name)
        if tag == 'style' and element.text:
            if '@import' in element.text.lower():
                fail('SVG artwork contains an external style import: ' + path.name)
            for reference in re.findall(r'url\(([^)]*)\)', element.text, re.I):
                if not reference.strip(' \t\r\n\"\'').startswith('#'):
                    fail('SVG artwork contains an external reference: ' + path.name)


def static_metadata(path):
    if path.stat().st_size > 64 * MIB:
        fail('Artwork metadata is unexpectedly large: ' + path.name)
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
    except (UnicodeError, ValueError):
        fail('Artwork metadata is not valid JSON: ' + path.name)
    if not isinstance(value, (dict, list)):
        fail('Artwork metadata has an unexpected structure: ' + path.name)
    blocked = re.compile(r'(password|passwd|secret|token|authorization|credential|api.?key)', re.I)

    def inspect(item):
        if isinstance(item, dict):
            for key, child in item.items():
                if blocked.search(str(key)):
                    fail('Artwork metadata contains a private configuration field: ' + path.name)
                inspect(child)
        elif isinstance(item, list):
            for child in item:
                inspect(child)
        elif isinstance(item, str):
            if re.search(r'(https?://|/mnt/|/home/|[A-Za-z]:\\)', item):
                fail('Artwork metadata contains a service URL or private filesystem path: ' + path.name)

    inspect(value)


def collect_artwork(root):
    root = Path(root)
    if not root.is_dir() or root.is_symlink():
        fail('The artwork input must be a real directory.')
    resolved_root = root.resolve()
    files, excluded = [], Counter()
    for directory, dirs, names in os.walk(root, followlinks=False):
        parent = Path(directory)
        for name in list(dirs):
            path = parent / name
            if path.is_symlink():
                fail('Artwork directory symlinks need review: ' + str(path.relative_to(root)))
            if name in {'.git', '.venv', 'data'} or PRIVATE_DIR.search(name):
                dirs.remove(name)
                excluded['private_or_runtime_directories'] += 1
        for name in sorted(names):
            path = parent / name
            relative = path.relative_to(root)
            if (name.startswith('.env') or name in PRIVATE_NAMES or RUNTIME_IMAGE.match(name)
                    or name.lower().endswith(('.bak', '.log', '.db', '.sqlite', '.sqlite3', '.pem', '.key'))):
                excluded['private_or_runtime_files'] += 1
                continue
            suffix = path.suffix.lower()
            if suffix not in IMAGE_SUFFIXES and name not in METADATA_NAMES:
                excluded['non_artwork_files'] += 1
                continue
            portable_path(relative)
            target = path.resolve()
            try:
                target.relative_to(resolved_root)
            except ValueError:
                fail('Artwork symlink leaves the artwork directory: ' + str(relative))
            if not target.is_file() or not stat.S_ISREG(target.stat().st_mode):
                fail('Artwork is not a regular file: ' + str(relative))
            if suffix == '.svg':
                safe_svg(target)
            if name in METADATA_NAMES:
                static_metadata(target)
            files.append((relative, target, file_identity(target)))
    if not any(relative.suffix.lower() in IMAGE_SUFFIXES and not relative.name.lower().startswith('favicon')
               for relative, _, _ in files):
        fail('No species artwork was found; no incomplete release was created.')
    return sorted(files, key=lambda item: item[0].as_posix()), dict(excluded)


def copy_checked(source, target, expected=None):
    source_stat = source.stat()
    before = (source_stat.st_dev, source_stat.st_ino, source_stat.st_size, source_stat.st_mtime_ns)
    if expected is not None and before != expected:
        fail('An input file changed during preparation: ' + source.name)
    target.parent.mkdir(parents=True, exist_ok=True)
    checksum = hashlib.sha256()
    with source.open('rb') as incoming, target.open('xb') as outgoing:
        for chunk in iter(lambda: incoming.read(MIB), b''):
            checksum.update(chunk)
            outgoing.write(chunk)
    target.chmod(0o644)
    if file_identity(source) != before:
        fail('An input file changed while being copied: ' + source.name)
    # The application compares PNG/WebP mtimes when choosing a fresh derivative.
    # Preserve these rather than stamping every exported asset with build time.
    os.utime(target, ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns))
    return {'sha256': checksum.hexdigest(), 'bytes': target.stat().st_size,
            'mtime_ns': before[3]}


def copy_source(project, target):
    if not project.is_dir():
        fail('Garden Friends source directory was not found.')
    required = {'main.py', 'garden_friends_settings.py', 'server.py', 'requirements.txt', 'README.md', 'compose.yaml', 'Dockerfile'}
    if not all((project / name).is_file() for name in required):
        fail('The embedded Garden Friends source is incomplete.')
    for source in sorted(project.rglob('*')):
        relative = source.relative_to(project)
        if relative.parts[0] not in SOURCE_DIRS and relative.as_posix() not in SOURCE_NAMES:
            continue
        if any(part in {'__pycache__', '.pytest_cache', '.git', '.venv', 'data'} for part in relative.parts):
            continue
        if source.is_symlink():
            fail('Source package contains a symlink: ' + relative.as_posix())
        if not source.is_file():
            continue
        if source.name in PRIVATE_NAMES or (source.name.startswith('.env') and source.name != '.env.example'):
            fail('Private configuration found in the source package: ' + relative.as_posix())
        if source.suffix.lower() in {'.pyc', '.log', '.db', '.sqlite', '.sqlite3', '.pem', '.key'}:
            continue
        portable_path(relative)
        copy_checked(source, target / relative)


def gallery(records):
    preferred = ('goldfinch', 'robin', 'blue_tit', 'great_tit', 'blackbird', 'pipistrelle', 'hedgehog', 'wren')
    candidates = [(name, meta) for name, meta in records.items()
                  if Path(name).suffix.lower() in {'.png', '.webp', '.jpg', '.jpeg'}
                  and not any(word in Path(name).name.lower() for word in ('favicon', 'mask', 'flight'))]

    def rank(pair):
        name = Path(pair[0]).stem.lower()
        order = next((i for i, word in enumerate(preferred) if word in name), len(preferred))
        return order, len(Path(pair[0]).parts), Path(pair[0]).suffix != '.webp', pair[0]

    chosen, seen_hashes, seen_species = [], set(), set()
    for name, meta in sorted(candidates, key=rank):
        stem = Path(name).stem.lower()
        species = next((p for p in preferred if p in stem), stem)
        if meta['sha256'] in seen_hashes or species in seen_species:
            continue
        seen_hashes.add(meta['sha256'])
        seen_species.add(species)
        chosen.append(name)
        if len(chosen) == 6:
            break
    cells = []
    for name in chosen:
        label = Path(name).stem.replace('_', ' ').replace('-', ' ').title()
        cells.append('<td align="center" width="16%"><img src="species_art/' + quote(name, safe='/')
                     + '" height="112" alt="' + html.escape(label, quote=True) + '"><br><sub>'
                     + html.escape(label) + '</sub></td>')
    return '<table><tr>\n' + '\n'.join(cells) + '\n</tr></table>'


def manifest_for(root):
    return {path.relative_to(root).as_posix(): {'sha256': digest(path), 'bytes': path.stat().st_size}
            for path in sorted(root.rglob('*')) if path.is_file() and path.name != 'RELEASE_MANIFEST.json'}


def export_release(project_dir, artwork_dir, output_dir, release_name='garden-friends', progress=None):
    """Return a new folder containing a complete repository, ZIP and checksums."""
    if release_name != 'garden-friends':
        fail('The distribution folder must be named garden-friends.')
    project = Path(project_dir).resolve()
    artwork = Path(artwork_dir)
    destination = Path(output_dir)
    if destination.exists() or destination.is_symlink():
        fail('The output already exists; choose a new release folder.')
    parent = destination.parent.resolve()
    parent.mkdir(parents=True, exist_ok=True)
    destination = parent / destination.name
    if destination.is_relative_to(project) or destination.is_relative_to(artwork.resolve()):
        fail('The output must be outside the source and artwork directories.')
    inputs, excluded = collect_artwork(artwork)
    staging = Path(tempfile.mkdtemp(prefix='.garden-friends-build-', dir=parent))
    try:
        repository = staging / release_name
        repository.mkdir()
        copy_source(project, repository)
        records = {}
        for number, (relative, source, expected) in enumerate(inputs, 1):
            records[relative.as_posix()] = copy_checked(source, repository / 'species_art' / relative, expected)
            if progress and (number == 1 or number % 50 == 0 or number == len(inputs)):
                progress('Copying artwork', number, len(inputs))
        after, after_excluded = collect_artwork(artwork)
        if [(str(p), identity) for p, _, identity in after] != [(str(p), identity) for p, _, identity in inputs]:
            fail('Artwork changed during the export; rerun after the artwork update finishes.')
        image_count = sum(Path(name).suffix.lower() in IMAGE_SUFFIXES for name in records)
        total_bytes = sum(value['bytes'] for value in records.values())
        art_manifest = {'version': VERSION, 'image_count': image_count, 'file_count': len(records),
                        'total_bytes': total_bytes, 'files': records}
        (repository / 'ARTWORK_MANIFEST.json').write_text(json.dumps(art_manifest, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
        readme = repository / 'README.md'
        content = readme.read_text(encoding='utf-8')
        if content.count(GALLERY_START) == content.count(GALLERY_END) == 1:
            before, remainder = content.split(GALLERY_START)
            _, following = remainder.split(GALLERY_END)
            content = before + GALLERY_START + '\n\n' + gallery(records) + '\n\n' + GALLERY_END + following
            readme.write_text(content, encoding='utf-8')
        source_manifest = manifest_for(repository)
        large = [name for name, meta in source_manifest.items() if meta['bytes'] > 100 * MIB]
        attributes = ['* text=auto eol=lf', '*.png binary', '*.webp binary', '*.jpg binary', '*.jpeg binary', '*.gif binary', '*.ico binary']
        attributes += [json.dumps(name, ensure_ascii=False) + ' filter=lfs diff=lfs merge=lfs -text' for name in large]
        (repository / '.gitattributes').write_text('\n'.join(attributes) + '\n', encoding='utf-8')
        report = {'version': VERSION, 'artwork_images': image_count, 'artwork_files': len(records),
                  'artwork_bytes': total_bytes, 'excluded_input_counts': excluded,
                  'git_lfs_required': bool(large), 'git_lfs_paths': large,
                  'files_over_browser_upload_limit': sum(meta['bytes'] > 25 * MIB for meta in source_manifest.values()),
                  'publication': 'Review NOTICE.md before publishing; credentials and runtime data are not included.'}
        (repository / 'BUILD_REPORT.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        release_manifest = {'version': VERSION, 'files': manifest_for(repository)}
        (repository / 'RELEASE_MANIFEST.json').write_text(json.dumps(release_manifest, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
        # Also set nested/generated modes before ZIP creation: sudo may inherit
        # umask 077, but these are public distribution files intended for SMB.
        repository.chmod(0o755)
        for path in repository.rglob('*'):
            path.chmod(0o755 if path.is_dir() else 0o644)
        archive = staging / (release_name + '.zip')
        if progress:
            progress('Creating and checking the ZIP archive', 0, 0)
        with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=1, allowZip64=True) as output:
            for path in sorted(repository.rglob('*')):
                if path.is_file():
                    output.write(path, path.relative_to(staging).as_posix())
        with zipfile.ZipFile(archive) as check:
            if check.testzip() is not None:
                fail('The release archive failed its integrity check.')
        checksum = digest(archive)
        (staging / (archive.name + '.sha256')).write_text(checksum + '  ' + archive.name + '\n', encoding='ascii')
        # The public release must remain readable through SMB when built with sudo.
        archive.chmod(0o644)
        (staging / (archive.name + '.sha256')).chmod(0o644)
        staging.chmod(0o755)
        os.rename(staging, destination)
        return {'repository': str(destination / release_name), 'archive': str(destination / archive.name),
                'sha256': checksum, **report}
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--artwork', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True, help='New output directory; existing folders are never overwritten.')
    args = parser.parse_args(argv)
    print(json.dumps(export_release(args.project, args.artwork, args.output), indent=2))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, zipfile.BadZipFile) as exc:
        raise SystemExit('STOPPED: ' + str(exc))
