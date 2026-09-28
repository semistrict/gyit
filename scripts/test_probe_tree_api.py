"""Trees API splitting and verification against this repository's own Git trees; no network."""
from pathlib import Path
import subprocess
import unittest

from probe_tree_api import acquire, group, tree_id

ROOT = Path(__file__).resolve().parent.parent


def git(*args):
    return subprocess.run(["git", "-C", str(ROOT), *args], check=True, capture_output=True, text=True).stdout


def ls_tree(sha, recursive):
    rows = []
    for record in git("ls-tree", "-l", "-z", *(["-r", "-t"] if recursive else []), sha).split("\0"):
        if not record:
            continue
        meta, path = record.split("\t", 1)
        mode, kind, oid, size = meta.split()
        rows.append({"path": path, "mode": mode, "type": kind, "sha": oid} | ({"size": int(size)} if kind == "blob" else {}))
    return rows


class LocalTrees:
    """Serves `git ls-tree` like GitHub's trees endpoint, truncating recursive responses."""

    def __init__(self, limit, reverse=False, tamper=False):
        self.limit = limit
        self.reverse = reverse
        self.tamper = tamper
        self.calls = []

    def __call__(self, sha, recursive):
        self.calls.append((sha, recursive))
        rows = ls_tree(sha, recursive)
        if self.reverse:
            rows.reverse()
        if self.tamper:
            blob = next(r for r in rows if r["type"] == "blob")
            blob["sha"] = "0" * 40
        truncated = recursive and len(rows) > self.limit
        return {"sha": sha, "tree": rows[: self.limit] if truncated else rows, "truncated": truncated}


class AcquireTest(unittest.TestCase):
    def setUp(self):
        self.root = git("rev-parse", "HEAD^{tree}").strip()
        self.expected = {e["path"]: e for e in ls_tree(self.root, True)}

    def test_tree_id_matches_git(self):
        self.assertEqual(tree_id(group(ls_tree(self.root, False))[""]), self.root)

    def test_untruncated_snapshot_costs_one_request(self):
        fetch = LocalTrees(limit=10**9)
        self.assertEqual(acquire(fetch, self.root, 4), self.expected)
        self.assertEqual(fetch.calls, [(self.root, True)])

    def test_truncated_snapshot_is_split_and_verified(self):
        for reverse in (False, True):
            with self.subTest(reverse=reverse):
                fetch = LocalTrees(limit=40, reverse=reverse)
                self.assertEqual(acquire(fetch, self.root, 4), self.expected)
                self.assertEqual(fetch.calls[0], (self.root, True))
                self.assertIn((self.root, False), fetch.calls)

    def test_tampered_listing_is_rejected(self):
        with self.assertRaisesRegex(ValueError, f"does not hash to root tree {self.root}"):
            acquire(LocalTrees(limit=10**9, tamper=True), self.root, 4)


if __name__ == "__main__":
    unittest.main()
