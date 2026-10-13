import importlib.util
import tomllib
import unittest
from pathlib import Path


CHECKER_PATH = Path(__file__).resolve().parents[1] / "check_registry_consumer_manifest.py"
SPEC = importlib.util.spec_from_file_location("check_registry_consumer_manifest", CHECKER_PATH)
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)


class RegistryConsumerManifestTests(unittest.TestCase):
    def test_rejects_compact_inline_path_dependency(self):
        manifest = """
[dependencies]
example={version="1",path="../example"}
"""
        parsed = tomllib.loads(manifest)
        self.assertTrue(any("path dependency" in issue for issue in CHECKER.manifest_issues(parsed)))

    def test_accepts_registry_fixture_manifest(self):
        fixture_path = Path(__file__).resolve().parents[2] / "tests/fixtures/registry_consumer/Cargo.toml"
        fixture = tomllib.loads(fixture_path.read_text())
        self.assertEqual(CHECKER.manifest_issues(fixture), [])

    def test_rejects_patch_table(self):
        manifest = """
[patch.crates-io]
example={git="https://example.invalid/repo"}
"""
        parsed = tomllib.loads(manifest)
        self.assertTrue(any("patch" in issue for issue in CHECKER.manifest_issues(parsed)))


if __name__ == "__main__":
    unittest.main()
