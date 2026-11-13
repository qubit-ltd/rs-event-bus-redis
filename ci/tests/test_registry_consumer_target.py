import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "registry_consumer_target.py"
SPEC = importlib.util.spec_from_file_location("registry_consumer_target", SCRIPT)
TARGET = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(TARGET)
ROOT = SCRIPT.parents[1]
VERSIONS = {"provider": "0.7.3", "core": "0.21.0", "spi": "0.14.0"}


def args(**overrides):
    values = dict(provider_version=VERSIONS["provider"], core_version=VERSIONS["core"],
                  spi_version=VERSIONS["spi"], mode="source-candidate")
    values.update(overrides)
    return SimpleNamespace(**values)


def metadata(double_core=False, bad_edge=False, git_target=None):
    core_id = "qubit-event-bus 0.21.0 (registry+https://github.com/rust-lang/crates.io-index)"
    provider_id = f"qubit-event-bus-redis {VERSIONS['provider']} (registry+https://github.com/rust-lang/crates.io-index)"
    spi_id = "qubit-spi 0.14.0 (registry+https://github.com/rust-lang/crates.io-index)"
    root_id = "registry-consumer 0.1.0 (path+file:///tmp/consumer)"
    other_core_id = "qubit-event-bus 0.20.1 (registry+https://github.com/rust-lang/crates.io-index)"
    packages = [
        {"id": root_id, "name": "registry-consumer", "version": "0.1.0", "source": None},
        {"id": core_id, "name": "qubit-event-bus", "version": "0.21.0", "source": TARGET.REGISTRY},
        {"id": provider_id, "name": "qubit-event-bus-redis", "version": VERSIONS["provider"], "source": TARGET.REGISTRY},
        {"id": spi_id, "name": "qubit-spi", "version": "0.14.0", "source": TARGET.REGISTRY},
    ]
    if double_core:
        packages.append({"id": other_core_id, "name": "qubit-event-bus", "version": "0.20.1", "source": TARGET.REGISTRY})
    target_id = {"core": core_id, "provider": provider_id, "spi": spi_id}.get(git_target, core_id)
    if git_target == "provider":
        packages[2]["source"] = "git+https://example.invalid/repo"
    elif git_target == "core":
        packages[1]["source"] = None
    provider_core = other_core_id if bad_edge else core_id
    resolve_nodes = [
        {"id": root_id, "deps": [
            {"name": "qubit-event-bus", "pkg": core_id},
            {"name": "qubit-event-bus-redis", "pkg": provider_id},
            {"name": "qubit-spi", "pkg": spi_id},
        ]},
        {"id": provider_id, "deps": [{"name": "qubit-event-bus", "pkg": provider_core}]},
    ]
    if bad_edge and not double_core:
        packages.append({"id": other_core_id, "name": "qubit-event-bus", "version": "0.20.1", "source": TARGET.REGISTRY})
    return {"packages": packages, "resolve": {"root": root_id, "nodes": resolve_nodes}}


class RegistryConsumerTargetTests(unittest.TestCase):
    def test_cli_requires_all_version_arguments(self):
        with patch("sys.argv", [str(SCRIPT), "prepare"]), self.assertRaises(SystemExit) as raised:
            TARGET.main()
        self.assertEqual(raised.exception.code, 2)

    def test_rejects_invalid_stability_or_precision(self):
        for version in ("0.7", "=0.7.0", "0.7.0-rc.1", "v0.7.0"):
            with self.subTest(version=version), self.assertRaises(TARGET.TargetError):
                TARGET.parse_version(version, "provider")

    def test_rejects_core_minor_mismatch_and_provider_source_mismatch(self):
        with self.assertRaisesRegex(TARGET.TargetError, "does not match"):
            TARGET.check_source_versions(ROOT / "Cargo.toml", TARGET.arguments_versions(args(core_version="0.20.1")))
        with self.assertRaisesRegex(TARGET.TargetError, "does not equal"):
            TARGET.check_source_versions(ROOT / "Cargo.toml", TARGET.arguments_versions(args(provider_version="0.8.0")))

    def test_published_prepare_accepts_historical_versions_without_source_constraint(self):
        fixture = ROOT / "tests/fixtures/registry_consumer"
        historical = args(core_version="0.20.1", mode="published")
        with tempfile.TemporaryDirectory() as directory:
            TARGET.prepare(SimpleNamespace(source_manifest=str(ROOT / "Cargo.toml"), fixture=str(fixture),
                                          output=directory, **vars(historical)))
            manifest = (Path(directory) / "Cargo.toml").read_text()
            self.assertIn(f'version = "={VERSIONS["provider"]}"', manifest)
            self.assertIn('version = "=0.20.1"', manifest)

    def test_source_candidate_prepare_rejects_historical_core_combination(self):
        fixture = ROOT / "tests/fixtures/registry_consumer"
        with tempfile.TemporaryDirectory() as directory, self.assertRaisesRegex(TARGET.TargetError, "does not match"):
            TARGET.prepare(SimpleNamespace(source_manifest=str(ROOT / "Cargo.toml"), fixture=str(fixture),
                                          output=directory, **vars(args(core_version="0.20.1"))))

    def test_prepare_rejects_unknown_mode(self):
        fixture = ROOT / "tests/fixtures/registry_consumer"
        with tempfile.TemporaryDirectory() as directory, self.assertRaisesRegex(TARGET.TargetError, "mode"):
            TARGET.prepare(SimpleNamespace(source_manifest=str(ROOT / "Cargo.toml"), fixture=str(fixture),
                                          output=directory, **vars(args(mode="typo"))))

    def test_prepare_defaults_to_source_candidate_mode(self):
        fixture = ROOT / "tests/fixtures/registry_consumer"
        with tempfile.TemporaryDirectory() as directory:
            values = vars(args())
            values.pop("mode")
            TARGET.prepare(SimpleNamespace(source_manifest=str(ROOT / "Cargo.toml"), fixture=str(fixture),
                                          output=directory, **values))

    def test_prepare_writes_exact_versions_and_preserves_fixture_files(self):
        fixture = ROOT / "tests/fixtures/registry_consumer"
        with tempfile.TemporaryDirectory() as directory:
            versions = TARGET.arguments_versions(args())
            TARGET.prepare(SimpleNamespace(source_manifest=str(ROOT / "Cargo.toml"), fixture=str(fixture),
                                          output=directory, **{f"{key}_version": value for key, value in versions.items()}))
            manifest = (Path(directory) / "Cargo.toml").read_text()
            self.assertIn(f'version = "={VERSIONS["provider"]}"', manifest)
            self.assertIn('version = "=0.21.0"', manifest)
            self.assertIn('qubit-spi = "=0.14.0"', manifest)
            self.assertEqual((Path(directory) / "src/main.rs").read_bytes(), (fixture / "src/main.rs").read_bytes())

    def assert_metadata_rejected(self, payload, pattern):
        with patch("sys.stdin") as stdin:
            stdin.read.return_value = json.dumps(payload)
            with self.assertRaisesRegex(TARGET.TargetError, pattern):
                TARGET.verify(args())

    def test_accepts_unique_exact_registry_combination(self):
        with patch("sys.stdin") as stdin, patch("builtins.print") as output:
            stdin.read.return_value = json.dumps(metadata())
            TARGET.verify(args())
            output.assert_called_once()

    def test_rejects_double_core_identity_and_wrong_provider_edge(self):
        self.assert_metadata_rejected(metadata(double_core=True), "multiple core package identities")
        self.assert_metadata_rejected(metadata(bad_edge=True), "core dependency edge")

    def test_rejects_missing_root_and_wrong_requested_version(self):
        payload = metadata()
        payload["resolve"]["root"] = "missing root"
        self.assert_metadata_rejected(payload, "root package is missing")
        with patch("sys.stdin") as stdin:
            stdin.read.return_value = json.dumps(metadata())
            with self.assertRaisesRegex(TARGET.TargetError, "resolved version"):
                TARGET.verify(args(core_version="0.20.1", mode="published"))

    def test_rejects_target_path_or_git_source(self):
        self.assert_metadata_rejected(metadata(git_target="provider"), "expected registry source")
        self.assert_metadata_rejected(metadata(git_target="core"), "expected registry source")


if __name__ == "__main__":
    unittest.main()
