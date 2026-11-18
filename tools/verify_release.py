#!/usr/bin/env python3
"""Verify a Garden Friends release against its recorded source/artwork hashes."""
import argparse
import hashlib
import json
from pathlib import Path


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def verify(root):
    root = Path(root).resolve()
    manifest = json.loads((root / 'RELEASE_MANIFEST.json').read_text(encoding='utf-8'))
    records = manifest['files']
    for relative, expected in records.items():
        path = root / relative
        if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(root):
            raise ValueError('Missing or unsafe release file: ' + relative)
        if path.stat().st_size != expected['bytes'] or digest(path) != expected['sha256']:
            raise ValueError('Changed release file: ' + relative)
    actual = {p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file()
              and p.name != 'RELEASE_MANIFEST.json' and '.git' not in p.relative_to(root).parts}
    extra = actual - set(records)
    if extra:
        raise ValueError('Unrecorded files in release: ' + ', '.join(sorted(extra)[:8]))
    return len(records)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('folder', type=Path, nargs='?', default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    try:
        print('Verified ' + str(verify(args.folder)) + ' release files.')
    except (OSError, ValueError, KeyError) as exc:
        raise SystemExit('VERIFICATION FAILED: ' + str(exc))
