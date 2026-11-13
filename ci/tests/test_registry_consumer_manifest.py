import importlib.util
import tempfile
import tomllib
import unittest
from pathlib import Path
from types import SimpleNamespace


CHECKER_PATH = Path(__file__).resolve().parents[1] / "check_registry_consumer_manifest.py"
SPEC = importlib.util.spec_from_file_location("check_registry_consumer_manifest", CHECKER_PATH)
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)
TARGET_PATH = Path(__file__).resolve().parents[1] / "registry_consumer_target.py"
TARGET_SPEC = importlib.util.spec_from_file_location("registry_consumer_target", TARGET_PATH)
TARGET = importlib.util.module_from_spec(TARGET_SPEC)
TARGET_SPEC.loader.exec_module(TARGET)


class RegistryConsumerManifestTests(unittest.TestCase):
    def test_rejects_compact_inline_path_dependency(self):
        manifest = """
[dependencies]
example={version="1",path="../example"}
"""
        parsed = tomllib.loads(manifest)
        self.assertTrue(any("path dependency" in issue for issue in CHECKER.manifest_issues(parsed)))

    def test_prepared_registry_fixture_has_no_local_dependency(self):
        fixture = Path(__file__).resolve().parents[2] / "tests/fixtures/registry_consumer"
        with tempfile.TemporaryDirectory() as output:
            TARGET.prepare(SimpleNamespace(
                provider_version="0.7.3",
                core_version="0.21.0",
                spi_version="0.14.0",
                mode="source-candidate",
                source_manifest=str(fixture.parents[2] / "Cargo.toml"),
                fixture=str(fixture),
                output=output,
            ))
            manifest = TARGET.load_manifest(Path(output) / "Cargo.toml")
        self.assertEqual(CHECKER.manifest_issues(manifest), [])

    def test_rejects_patch_table(self):
        manifest = """
[patch.crates-io]
example={git="https://example.invalid/repo"}
"""
        parsed = tomllib.loads(manifest)
        self.assertTrue(any("patch" in issue for issue in CHECKER.manifest_issues(parsed)))

    def test_rejects_git_dependency(self):
        manifest = '''
[dependencies]
example = { version = "1.2.3", git = "https://example.invalid/repo" }
'''
        parsed = tomllib.loads(manifest)
        self.assertTrue(any("git dependency" in issue for issue in CHECKER.manifest_issues(parsed)))

    def test_rejects_nested_target_git_dependency(self):
        manifest = '''
[target.'cfg(unix)'.dependencies]
example = { git = "https://example.invalid/repo" }
'''
        parsed = tomllib.loads(manifest)
        self.assertTrue(any("git dependency" in issue for issue in CHECKER.manifest_issues(parsed)))


if __name__ == "__main__":
    unittest.main()
