import importlib.util
import json
import subprocess
import os
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "package_candidate.py"
SPEC = importlib.util.spec_from_file_location("package_candidate", SCRIPT)
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


class PackageCandidateTests(unittest.TestCase):
    def test_registry_only_production_dependencies_pass(self):
        manifest = {"dependencies": {"serde": "1", "redis": {"version": "2"}}}
        self.assertEqual(module.manifest_issues(manifest), [])

    def test_development_path_dependency_is_not_a_production_failure(self):
        manifest = {"dependencies": {"serde": "1"}, "dev-dependencies": {"fixture": {"path": "tests/fixtures"}}}
        self.assertEqual(module.manifest_issues(manifest), [])

    def test_production_path_git_patch_and_duplicate_core_are_rejected(self):
        manifest = {
            "dependencies": {
                "redis": {"git": "https://example.invalid/redis"},
                "qubit-event-bus": {"path": "../core"},
            },
            "patch": {"crates-io": {"qubit-event-bus": {"path": "../core"}}},
        }
        issues = module.manifest_issues(manifest)
        self.assertTrue(any("git" in issue for issue in issues))
        self.assertTrue(any("path" in issue for issue in issues))
        self.assertTrue(any("patch" in issue for issue in issues))

    def test_nonproduction_fixture_manifest_is_not_scanned(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "Cargo.toml").write_text('[package]\nname="candidate"\n')
            fixture = root / "tests" / "fixtures" / "consumer"
            fixture.mkdir(parents=True)
            (fixture / "Cargo.toml").write_text('[dependencies]\nlocal = { path = "../../.." }\n')
            self.assertEqual(module.manifest_issues(module.load_manifest(root / "Cargo.toml")), [])

    def test_metadata_rejects_local_production_source(self):
        metadata = {
            "packages": [
                {"id": "root", "name": "candidate", "version": "1.0.0", "source": None},
                {"id": "local", "name": "serde", "version": "1.0.0", "source": None},
            ],
            "resolve": {"root": "root", "nodes": [
                {"id": "root", "deps": [{"pkg": "local"}]},
                {"id": "local", "deps": []},
            ]},
        }
        with self.assertRaisesRegex(module.CandidateError, "non-registry source"):
            module.validate_metadata(metadata, "candidate")

    def test_metadata_rejects_missing_core_identity(self):
        metadata = {
            "packages": [{"id": "root", "name": "candidate", "version": "1.0.0", "source": None}],
            "resolve": {"root": "root", "nodes": [{"id": "root", "deps": []}]},
        }
        with self.assertRaisesRegex(module.CandidateError, "expected one reachable qubit-event-bus identity"):
            module.validate_metadata(metadata, "candidate")

    def test_metadata_rejects_duplicate_core_identity(self):
        metadata = {
            "packages": [
                {"id": "root", "name": "candidate", "version": "1.0.0", "source": None},
                {"id": "core-a", "name": "qubit-event-bus", "version": "0.21.0", "source": module.REGISTRY_PREFIX + "https://github.com/rust-lang/crates.io-index"},
                {"id": "core-b", "name": "qubit-event-bus", "version": "0.21.0", "source": module.REGISTRY_PREFIX + "https://github.com/rust-lang/crates.io-index"},
            ],
            "resolve": {"root": "root", "nodes": [{"id": "root", "deps": [{"pkg": "core-a"}, {"pkg": "core-b"}]}]},
        }
        with self.assertRaises(module.CandidateError):
            module.validate_metadata(metadata, "root")

    def test_failed_child_process_does_not_become_success(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fake_bin = root / "bin"
            fake_bin.mkdir()
            fake_cargo = fake_bin / "cargo"
            fake_cargo.write_text("#!/bin/sh\nexit 42\n")
            fake_cargo.chmod(0o755)
            env = os.environ.copy()
            env["PATH"] = f"{fake_bin}:/usr/bin:/bin"
            env["PACKAGE_CANDIDATE_LOG_DIR"] = str(root / "logs")
            result = subprocess.run(["bash", str(SCRIPT.parent / "check-package-candidate.sh")], cwd=Path(__file__).resolve().parents[2], env=env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("phase=package", result.stderr)


if __name__ == "__main__":
    unittest.main()
