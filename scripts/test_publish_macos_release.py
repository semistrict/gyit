import hashlib
import json
import pathlib
import tempfile
import unittest

from publish_macos_release import release_assets


class ReleaseAssetsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.dist = pathlib.Path(self.temp.name)
        self.commit = "a" * 40
        (self.dist / "SOURCE_COMMIT").write_text(self.commit + "\n")
        (self.dist / "NOTARIZED.json").write_text(json.dumps({
            "status": "Accepted", "id": "submission-id",
        }))
        names = ("gyit_0.1.14_arm64.dmg", "gyit_0.1.14_arm64.zip")
        for name in names:
            (self.dist / name).write_bytes(name.encode())
        (self.dist / "SHA256SUMS").write_text("".join(
            f"{hashlib.sha256(name.encode()).hexdigest()}  {name}\n"
            for name in names
        ))

    def test_accepts_matching_notarized_artifacts(self):
        version, assets = release_assets(self.dist, self.commit)
        self.assertEqual(version, "0.1.14")
        self.assertEqual([path.name for path in assets], [
            "gyit_0.1.14_arm64.dmg", "gyit_0.1.14_arm64.zip",
            "SHA256SUMS", "NOTARIZED.json",
        ])

    def test_rejects_artifact_modified_after_checksumming(self):
        (self.dist / "gyit_0.1.14_arm64.dmg").write_bytes(b"changed")
        with self.assertRaisesRegex(RuntimeError, "checksums do not match"):
            release_assets(self.dist, self.commit)

    def test_rejects_artifacts_from_another_commit(self):
        with self.assertRaisesRegex(RuntimeError, "do not belong"):
            release_assets(self.dist, "b" * 40)

    def test_rejects_unaccepted_notarization(self):
        (self.dist / "NOTARIZED.json").write_text(json.dumps({
            "status": "Invalid", "id": "submission-id",
        }))
        with self.assertRaisesRegex(RuntimeError, "accepted notarization"):
            release_assets(self.dist, self.commit)


if __name__ == "__main__":
    unittest.main()
