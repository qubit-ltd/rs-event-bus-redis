import json
import os
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ci/check-registry-consumer.sh"
VERSIONS = ("0.7.0", "0.21.0", "0.14.0")


def metadata():
    core = "qubit-event-bus 0.21.0 (registry+https://github.com/rust-lang/crates.io-index)"
    provider = "qubit-event-bus-redis 0.7.0 (registry+https://github.com/rust-lang/crates.io-index)"
    spi = "qubit-spi 0.14.0 (registry+https://github.com/rust-lang/crates.io-index)"
    root = "registry-consumer 0.1.0 (path+file:///tmp/consumer)"
    return {
        "packages": [
            {"id": root, "name": "registry-consumer", "version": "0.1.0", "source": None},
            {"id": core, "name": "qubit-event-bus", "version": "0.21.0", "source": "registry+https://github.com/rust-lang/crates.io-index"},
            {"id": provider, "name": "qubit-event-bus-redis", "version": "0.7.0", "source": "registry+https://github.com/rust-lang/crates.io-index"},
            {"id": spi, "name": "qubit-spi", "version": "0.14.0", "source": "registry+https://github.com/rust-lang/crates.io-index"},
        ],
        "resolve": {
            "root": root,
            "nodes": [
                {"id": root, "deps": [
                    {"name": "qubit-event-bus", "pkg": core},
                    {"name": "qubit-event-bus-redis", "pkg": provider},
                    {"name": "qubit-spi", "pkg": spi},
                ]},
                {"id": provider, "deps": [{"name": "qubit-event-bus", "pkg": core}]},
            ],
        },
    }


class RegistryConsumerExecutionTests(unittest.TestCase):
    def run_script(self, fail_run=None):
        with tempfile.TemporaryDirectory() as temporary:
            temp = Path(temporary)
            bindir = temp / "bin"
            bindir.mkdir()
            log = temp / "cargo.log"
            cargo = bindir / "cargo"
            cargo.write_text(textwrap.dedent(f"""\
                #!{os.sys.executable}
                import json, os, sys
                args = sys.argv[1:]
                with open(os.environ["CARGO_LOG"], "a") as log:
                    log.write(" ".join(args) + "\\n")
                if args[-1] == "--locked" and "metadata" in args:
                    print({json.dumps(metadata())!r})
                if "run" in args and os.environ.get("FAIL_RUN") == " ".join(args):
                    sys.exit(23)
            """))
            cargo.chmod(0o755)
            environment = os.environ.copy()
            environment["PATH"] = f"{bindir}{os.pathsep}{environment['PATH']}"
            environment["CARGO_LOG"] = str(log)
            if fail_run:
                environment["FAIL_RUN"] = fail_run
            result = subprocess.run(
                ["bash", str(SCRIPT), "--mode", "source-candidate", "--provider-version", VERSIONS[0],
                 "--core-version", VERSIONS[1], "--spi-version", VERSIONS[2]],
                cwd=ROOT, env=environment, text=True, capture_output=True,
            )
            calls = log.read_text().splitlines()
            return result, calls

    def test_runs_all_checks_metadata_and_program_variants(self):
        result, calls = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("registry consumer checks passed", result.stdout)
        self.assertIn("+1.94.0 generate-lockfile", calls)
        self.assertIn("+1.94.0 check --locked --no-default-features --features sync,discovery", calls)
        self.assertIn("+1.94.0 check --locked --no-default-features --features async,discovery", calls)
        self.assertIn("+1.94.0 check --locked", calls)
        self.assertIn("+1.94.0 metadata --format-version 1 --locked", calls)
        self.assertIn("+1.94.0 run --locked --no-default-features --features sync,discovery", calls)
        self.assertIn("+1.94.0 run --locked --no-default-features --features async,discovery", calls)
        self.assertIn("+1.94.0 run --locked", calls)

    def test_each_run_stage_failure_propagates_without_success_message(self):
        stages = (
            ("+1.94.0 run --locked --no-default-features --features sync,discovery", "run-sync,discovery"),
            ("+1.94.0 run --locked --no-default-features --features async,discovery", "run-async,discovery"),
            ("+1.94.0 run --locked", "run-default-features"),
        )
        for command, stage in stages:
            with self.subTest(stage=stage):
                result, calls = self.run_script(fail_run=command)
                self.assertEqual(result.returncode, 23, result.stderr)
                self.assertIn(f"failed during {stage}", result.stderr)
                self.assertNotIn("registry consumer checks passed", result.stdout)
                self.assertIn(command, calls)
