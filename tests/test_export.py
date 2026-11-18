import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


exporter = load('gf_export', ROOT / 'tools/export_release.py')
verifier = load('gf_verify', ROOT / 'tools/verify_release.py')
PNG = base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y9ZkQAAAABJRU5ErkJggg==')


class ExportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='garden-export-test-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.project = self.root / 'source'
        self.project.mkdir()
        for name in ('main.py', 'garden_friends_settings.py', 'server.py', 'requirements.txt', 'compose.yaml', 'Dockerfile'):
            (self.project / name).write_text('# Synthetic packaging fixture.\n')
        (self.project / 'README.md').write_text('# Garden Friends\n' + exporter.GALLERY_START + '\n' + exporter.GALLERY_END)
        self.art = self.root / 'art'
        self.art.mkdir()
        (self.art / 'robin.png').write_bytes(PNG)
        self.out = self.root / 'release'

    def export(self):
        return exporter.export_release(self.project, self.art, self.out)

    def test_complete_art_exact_bytes_and_valid_zip(self):
        hidden = self.art / '.optimized_webp'
        hidden.mkdir()
        webp = b'RIFF\x0c\x00\x00\x00WEBPVP8 test'
        (hidden / 'robin.webp').write_bytes(webp)
        (self.art / 'robin.webp').write_bytes(webp)
        (self.art / 'masks.json').write_text('{"robin":{"w":1,"h":1,"aspect":1,"bitmap":["1"]}}')
        (self.art / 'latest_visual.jpg').write_bytes(b'private snapshot')
        (self.art / 'latest_visual.json').write_text('{"camera":"private"}')
        (self.project / '.env').write_text('INFLUX_TOKEN=private-secret')
        (self.art / '.env').write_text('MQTT_PASSWORD=secret')
        before = {p.relative_to(self.art).as_posix(): p.read_bytes() for p in self.art.rglob('*') if p.is_file()}
        result = self.export()
        repo = Path(result['repository'])
        self.assertEqual(result['artwork_images'], 3)
        self.assertEqual(result['artwork_files'], 4)
        self.assertEqual((repo / 'species_art/.optimized_webp/robin.webp').read_bytes(), webp)
        self.assertFalse((repo / 'species_art/latest_visual.jpg').exists())
        self.assertFalse((repo / '.env').exists())
        self.assertEqual(before, {p.relative_to(self.art).as_posix(): p.read_bytes() for p in self.art.rglob('*') if p.is_file()})
        self.assertGreater(verifier.verify(repo), 8)
        with zipfile.ZipFile(result['archive']) as z:
            self.assertIsNone(z.testzip())
            self.assertEqual(z.read('garden-friends/species_art/robin.png'), PNG)
        self.assertEqual(hashlib.sha256(Path(result['archive']).read_bytes()).hexdigest(), result['sha256'])
        self.assertIn('species_art/robin.webp', (repo / 'README.md').read_text())

    def test_existing_output_and_empty_artwork_are_rejected(self):
        self.out.mkdir()
        (self.out / 'keep.txt').write_text('keep')
        with self.assertRaisesRegex(ValueError, 'already exists'):
            self.export()
        self.assertEqual((self.out / 'keep.txt').read_text(), 'keep')
        (self.art / 'robin.png').unlink()
        with self.assertRaisesRegex(ValueError, 'No species artwork'):
            exporter.export_release(self.project, self.art, self.root / 'empty-release')

    def test_original_artwork_timestamps_are_preserved_in_folder_and_zip(self):
        png = self.art / 'robin.png'
        webp = self.art / 'robin.webp'
        webp.write_bytes(b'RIFF\x0c\x00\x00\x00WEBPVP8 test')
        png_ns = 1767323046123456789
        webp_ns = png_ns + 2_000_000_000
        os.utime(png, ns=(png_ns, png_ns))
        os.utime(webp, ns=(webp_ns, webp_ns))
        result = self.export()
        repository = Path(result['repository']) / 'species_art'
        self.assertEqual((repository / 'robin.png').stat().st_mtime_ns, png_ns)
        self.assertEqual((repository / 'robin.webp').stat().st_mtime_ns, webp_ns)
        with zipfile.ZipFile(result['archive']) as archive:
            for name, timestamp in [('robin.png', png_ns), ('robin.webp', webp_ns)]:
                local = list(time.localtime(timestamp / 1_000_000_000)[:6])
                local[5] -= local[5] % 2  # DOS/ZIP stores seconds in two-second steps.
                self.assertEqual(archive.getinfo('garden-friends/species_art/' + name).date_time, tuple(local))

    def test_aliases_inside_artwork_are_copied_external_links_are_rejected(self):
        (self.art / 'european_robin.png').symlink_to('robin.png')
        result = self.export()
        alias = Path(result['repository']) / 'species_art/european_robin.png'
        self.assertFalse(alias.is_symlink())
        self.assertEqual(alias.read_bytes(), PNG)
        external = self.root / 'external.png'
        external.write_bytes(PNG)
        (self.art / 'outside.png').symlink_to(external)
        with self.assertRaisesRegex(ValueError, 'leaves the artwork'):
            exporter.export_release(self.project, self.art, self.root / 'external-release')

    def test_private_metadata_and_active_svg_are_rejected(self):
        (self.art / 'manifest.json').write_text('{"api_token":"private"}')
        with self.assertRaisesRegex(ValueError, 'private configuration'):
            self.export()
        (self.art / 'manifest.json').unlink()
        (self.art / 'bird.svg').write_text('<svg xmlns="http://www.w3.org/2000/svg"><script>bad()</script></svg>')
        with self.assertRaisesRegex(ValueError, 'active content'):
            self.export()

    def test_tampering_is_found_by_release_verifier(self):
        result = self.export()
        repo = Path(result['repository'])
        (repo / 'species_art/robin.png').write_bytes(PNG + b'changed')
        with self.assertRaisesRegex(ValueError, 'Changed release file'):
            verifier.verify(repo)

    def test_large_files_get_exact_lfs_rules(self):
        (self.art / 'large bird.png').write_bytes(PNG + b'x' * (101 * 1024))
        with patch.object(exporter, 'MIB', 1024):
            result = self.export()
        self.assertTrue(result['git_lfs_required'])
        attrs = (Path(result['repository']) / '.gitattributes').read_text()
        self.assertIn('"species_art/large bird.png" filter=lfs diff=lfs merge=lfs -text', attrs)

    def test_source_or_art_change_cannot_silently_finish(self):
        original = exporter.copy_checked
        changed = False

        def mutate(source, target, expected=None):
            nonlocal changed
            result = original(source, target, expected)
            if source == self.art / 'robin.png' and not changed:
                changed = True
                source.write_bytes(PNG + b'updated')
            return result

        with patch.object(exporter, 'copy_checked', side_effect=mutate):
            with self.assertRaisesRegex(ValueError, 'Artwork changed'):
                self.export()
        self.assertFalse(self.out.exists())


if __name__ == '__main__':
    unittest.main()
