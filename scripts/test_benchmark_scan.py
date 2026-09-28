"""Correctness checks for the standalone filesystem scanner; no Git fixture."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from benchmark_scan import scan, find_scan


class ScanTest(unittest.TestCase):
    def test_find_rejects_matches_and_git_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'sub').mkdir()
            (root / 'sub' / '.hidden').write_text('scanned')
            self.assertGreater(find_scan(root)['seconds'], 0)
            (root / 'sub' / 'DOES_NOT_EXIST').touch()
            with self.assertRaisesRegex(ValueError, 'no matches'):
                find_scan(root)
            (root / '.git').mkdir()
            with self.assertRaisesRegex(ValueError, 'without root .git'):
                find_scan(root)

    def test_find_verifies_tree_after_timing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            a, b = root / 'a', root / 'b'
            a.mkdir()
            b.mkdir()
            for tree in (a, b):
                (tree / '.hidden').write_text('same')
            command = [sys.executable, str(Path(__file__).with_name('benchmark_scan.py')),
                       '--mount', str(a), '--checkout', str(b), '--mode', 'find',
                       '--runs', '2', '--max-ratio', '1000000']
            result = subprocess.run(command, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            (b / '.hidden').write_text('different size')
            result = subprocess.run(command, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b'traversal differs', result.stderr)

    def test_complete_walk_and_stat(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / '.git').mkdir()
            (root / '.git' / 'excluded').write_text('metadata')
            (root / '.hidden').write_bytes(b'abc')
            (root / 'sub').mkdir()
            (root / 'sub' / 'file').write_bytes(b'data')
            (root / 'sub' / '.git').mkdir()
            (root / 'loop').symlink_to(root, target_is_directory=True)
            before = scan(root, False)
            stat_before = scan(root, True)
            self.assertEqual(before['entries'], 5)
            self.assertEqual(before['directories'], 2)
            self.assertEqual(stat_before['entries'], 5)
            (root / '.git' / 'excluded').write_text('different metadata')
            self.assertEqual(scan(root, True)['digest'], stat_before['digest'])
            (root / 'sub' / 'file').write_bytes(b'longer content')
            self.assertEqual(scan(root, False)['digest'], before['digest'])
            self.assertNotEqual(scan(root, True)['digest'], stat_before['digest'])

    def test_cli_needs_no_git_and_rejects_different_trees(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            a, b = root / 'a', root / 'b'
            a.mkdir()
            b.mkdir()
            for tree in (a, b):
                (tree / 'file').write_text('same')
            command = [sys.executable, str(Path(__file__).with_name('benchmark_scan.py')),
                       '--mount', str(a), '--checkout', str(b), '--mode', 'stat',
                       '--runs', '2', '--max-ratio', '1000000', '--output', str(root / 'result.json')]
            env = dict(os.environ, PATH='')
            result = subprocess.run(command, env=env, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            measured = json.loads((root / 'result.json').read_text())
            self.assertEqual(list(measured['modes']), ['stat'])
            (b / 'extra').write_text('untracked files must be scanned')
            result = subprocess.run(command, env=env, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b'traversal differs', result.stderr)


if __name__ == '__main__':
    unittest.main()
