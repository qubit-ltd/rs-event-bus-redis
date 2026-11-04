import importlib.util
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "check-doc-versions.py"
SPEC = importlib.util.spec_from_file_location("check_doc_versions", SCRIPT)
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


class DocumentationVersionTests(unittest.TestCase):
    def write_inputs(self, root, english, chinese, manifest=None):
        (root / "Cargo.toml").write_text(
            manifest
            or '[package]\nname = "qubit-event-bus-redis"\nversion = "0.7.0"\n'
            '[dependencies]\nqubit-event-bus = { version = "0.21", path = "../rs-event-bus" }\n'
            'qubit-spi = "0.14"\n',
            encoding="utf-8",
        )
        (root / "README.md").write_text(english, encoding="utf-8")
        (root / "README.zh_CN.md").write_text(chinese, encoding="utf-8")
        guide = root / "doc"
        guide.mkdir(exist_ok=True)
        (guide / "user_guide.md").write_text(english, encoding="utf-8")
        (guide / "user_guide.zh_CN.md").write_text(chinese, encoding="utf-8")

    @staticmethod
    def source_block(core="0.21", provider="0.7.0", spi="0.14"):
        return (
            "<!-- current-source-dependencies -->\n"
            "```toml\n[dependencies]\n"
            f'qubit-event-bus = {{ version = "{core}", path = "../rs-event-bus" }}\n'
            f'qubit-event-bus-redis = {{ version = "{provider}", path = "../rs-event-bus-redis" }}\n'
            f'qubit-spi = "{spi}"\n```.\n'
        )

    def test_matching_bilingual_source_versions_pass_and_ignore_history(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            block = self.source_block()
            self.write_inputs(root, "Current source:\n" + block + "Historical core 0.20.1.\n", "当前源码：\n" + block + "历史版本 0.20.1。\n")
            self.assertEqual(module.check(root), [])

    def test_core_minor_mismatch_is_reported(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.write_inputs(root, self.source_block(core="0.20"), self.source_block(core="0.20"))
            self.assertTrue(any("qubit-event-bus" in issue for issue in module.check(root)))

    def test_provider_version_mismatch_is_reported(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.write_inputs(root, self.source_block(provider="0.6"), self.source_block(provider="0.6"))
            self.assertTrue(any("qubit-event-bus-redis" in issue for issue in module.check(root)))

    def test_missing_marker_is_reported(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.write_inputs(root, "```toml\nqubit-event-bus = \"0.21\"\n```\n", self.source_block())
            self.assertTrue(any("current-source-dependencies" in issue for issue in module.check(root)))

    def test_bilingual_version_difference_is_reported(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.write_inputs(root, self.source_block(), self.source_block(spi="0.13"))
            self.assertTrue(any("README" in issue for issue in module.check(root)))

    def test_cargo_multiline_inline_dependency_tables_are_parsed(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = (
                '[package]\nname = "qubit-event-bus-redis"\nversion = "0.7.0"\n'
                '[dependencies]\nqubit-event-bus = { version = "0.21", path = "../rs-event-bus" }\n'
                'qubit-spi = "0.14"\nredis = {\n  version = "=2.0.0-rc.1",\n  git = "https://example.invalid/redis",\n}\n'
            )
            block = self.source_block()
            self.write_inputs(root, block, block, manifest=manifest)
            self.assertEqual(module.check(root), [])


if __name__ == "__main__":
    unittest.main()
