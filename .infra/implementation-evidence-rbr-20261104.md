# Implementation evidence: Redis provider review remediation

Date: 2026-11-04 (UTC)
Project: `/data/working/qubit/rust-common/rs-event-bus-redis`
Plan: `doc/implementation-plan-20261104T103301158Z-15171d7a.md`
Design: `doc/design-20261104T103301158Z-15171d7a.md`

## Initial repository state

| Repository | Original branch | Original HEAD | Initial modifications |
| --- | --- | --- | --- |
| `/data/working/qubit/rust-common/rs-event-bus-redis` | `dev-starfish` | `b8c2c51d7d70564fc6bc850f123db268aef41a1c` | Two user-provided untracked design/plan files; retained and excluded from commits |
| `/data/working/qubit/rust-common/rs-task` | `dev-starfish` | `a29f3b93c2de884eaddf5fcd1bcb050874cf74ce` | Clean |

Implementation worktrees use branch `codex/rbr-20261104`. No original checkout was edited during implementation. The user later authorized using `sudo` to run Docker as the existing `starfish` user in group `docker`; no system permissions or group membership were changed.

## RBR-001 — Registry package candidate (blocked)

`cargo +1.94.0 info qubit-event-bus@0.21.0 --registry crates-io` exited 101: crates.io has no matching `0.21.0`; Cargo reported available versions including `0.20.1` and `0.20.0`.

`cargo +1.94.0 info redis@2.0.0-rc.1 --registry crates-io` and the corresponding `rc.2` command both exited 0. Inspection of both cached registry source trees found no `set_max_response_bytes`, `max_response_bytes`, or `response_too_large` identifiers. The provider fork is necessary for the current full-response-frame cap API.

```text
bash ci/check-package-candidate.sh
exit 1 — FIRST_CHECK phase=package; Cargo could not resolve qubit-event-bus ^0.21.
Log: target/package-candidate-logs/20261104T103857Z-1019315
```

No archive was produced by this isolated candidate check. A separate full CI package suite did create a normalized archive, then failed while compiling it against registry Redis `2.0.0-rc.1`:

```text
error[E0432]: unresolved import `redis::ConnectionConfig`
error[E0599]: no method named `set_max_response_bytes` found for `AsyncConnectionConfig`
error: failed to verify package tarball
```

The exact full CI log is `/tmp/rbr-ci-check-20261104-rerun.log`; the command exited 1. No package was published. RBR-001 remains externally blocked on registry releases of core `0.21` and a Redis client with equivalent response-frame budgeting APIs. The full CI script stopped at package before its configured coverage, audit, and loom tasks.

## RBR-002 — Single Redis identity in rs-task

`rs-task/Cargo.toml` now pins the same Git URL and exact Redis revision as the provider. Cargo regenerated the lockfile; unrelated patch-level updates from lockfile regeneration were reverted using precise Cargo package updates. The final lockfile change removes the second Redis source identity and updates the dependency edges.

```text
cargo +1.94.0 generate-lockfile
exit 0
cargo +1.94.0 update -p async-executor --precise 1.14.0
exit 0
cargo +1.94.0 update -p toml_edit --precise '0.25.17+spec-1.1.0'
exit 0
cargo metadata --format-version 1 --locked (provider and task)
exit 0 — both resolve git+https://github.com/qubit-ltd/redis-rs.git?rev=a668891e9fcfed32f8905c2fd729fa05c1732aa5#redis@2.0.0-rc.1
cargo +1.94.0 tree -d --locked (task)
exit 0 — only one Redis source identity
cargo +1.94.0 tree --manifest-path tests/fixtures/task_notifications/Cargo.toml -d --locked (provider)
exit 0 — only one Redis source identity across the fixture graph
cargo +1.94.0 test --test redis_event_bus_integration_tests --features event-bus --no-run --locked
exit 0 — integration target compiled
sudo -n -u starfish -g docker cargo +1.94.0 test --test redis_event_bus_integration_tests --features event-bus --locked -- --test-threads=1
exit 0 — 6 passed, 0 failed
```

The two repository graphs share one Redis package ID at the pinned fork revision.

## RBR-003 — Coverage history and current measurement

Both `doc/coverage-review.md` and `doc/coverage-review.zh_CN.md` identify `516/545 (94.68%)` as a historical, independently recorded capture. They retain its line/region results, date, report path, SHA256, and gate failure; they state the raw report is unavailable and has no source seal, so it cannot be attributed to a HEAD or used to infer current coverage.

The standalone `sudo -n -u starfish -g docker ... ./.infra/bin/coverage.sh` command exited 0 on 2026-11-04, from 13:01:00 to 13:05:51 UTC, with rustc/cargo 1.94.0, LLVM 21.1.8, cargo-llvm-cov 0.9.1. Results:

| Metric | Result | Gate |
| --- | ---: | ---: |
| Functions | 530/556 (95.32%) | at least 95% |
| Lines | 5,165/5,374 (96.11%) | above 90% |
| Regions | 8,014/8,435 (95.01%) | above 85% |

The 197-file tracked Rust source inventory, reports, and tested-input metadata are retained at `.infra/coverage/evidence-rbr-20261104/`. Canonical path/content inventory SHA256: `4de83bcdfcef5ff0e71e3f4f1d146a2e1e8a1892d375f51a2bad5f8c3d53e334`. `tested-inputs.json` records provider/core/task HEADs, dirty-status path digests, and hashes for provider/core/task manifests and lockfiles plus CI, coverage, and package validation configuration. Its SHA256 is `782b73a65c6c513332c1cc673c4fc6a042ce35d5777f52e4fdad689e2b9e7013`; input manifest SHA256 is `6c48a652e10e23490ba85118ddfe4418a6a30e0fde45169ea0b0a01656ceba8c`; repository-state manifest SHA256 is `7f203e8920e76de73c09d25d8b47bce9340f6528b3893036fa66b4ba0e339398`. Core contained two unrelated untracked design/plan files, which were preserved. Dirty status was captured during evidence finalization after coverage; the tracked Rust source and tested-input hashes independently bind the measured code/config. The exact report invocation was:

```text
set -o pipefail
sudo -n -u starfish -g docker /usr/bin/env PATH=/home/starfish/.cargo/bin:/usr/local/bin:/usr/bin:/bin RUSTUP_HOME=/home/starfish/.rustup CARGO_HOME=/home/starfish/.cargo /bin/bash ./.infra/bin/coverage.sh 2>&1 | tee /tmp/rbr-coverage-20261104.log
exit 0
```

| Artifact | SHA256 |
| --- | --- |
| `source-files.json` | `6bd13e838a2dcf689ab02654552b404a185a4a3d4b58869e7ddea0290a103ff0` |
| `coverage.json` | `7f2c7b5af121accd9d5d80e0aaa55a0cb4be0521bebf770ee50ebf4a28af8d2d` |
| `coverage.txt` | `3ac7037f735114f5f4131b6af33b2575b6c6f164ce4eb9f17060c453a67cf822` |
| `lcov.info` | `f823072cd775b0cfa7944977fc243241965e891f165dd2b971e53c7534a53b53` |
| `cobertura.xml` | `6f33ea4dcc1ffcc47b6e46f57407569e7a5ee40e6978af941ca371b557553343` |

This measurement is a standalone coverage run, not the coverage task inside full CI: full CI stopped at package before reaching that task. It measures the provider worktree source after the history wording correction and before adding this evidence narrative/archive; Rust source was unchanged during measurement. The package manifest/lock and CI settings were hashed separately during evidence capture. The full CI package blocker remains unresolved.

## Verification record

| Command | Result |
| --- | --- |
| `bash .infra/bin/align-ci.sh` | exit 0 — Rust style alignment completed |
| `python3 ci/tests/test_doc_versions.py` | exit 0 — 6 tests passed |
| `python3 ci/check-doc-versions.py <provider-worktree>` | exit 0 |
| `git diff --check` | exit 0 before final evidence text; rerun after final edits |
| Provider `client_tests --all-features --locked -- --test-threads=1` with Docker group | exit 0 — 46 passed |
| Provider `wire_limit_tests --all-features --locked -- --test-threads=1` with Docker group | exit 0 — 16 passed |
| Provider `downstream_tests --all-features --locked -- --test-threads=1` with Docker group | exit 0 — 4 passed |
| Task `redis_event_bus_integration_tests --features event-bus --locked -- --test-threads=1` with Docker group | exit 0 — 6 passed |
| `sudo ... .infra/bin/ci-check.sh` (first attempt without required sudo PATH) | script failed to find `rustc`; pipeline status was not used as success evidence |
| `.infra/bin/ci-check.sh` with explicit Rust PATH and `pipefail` | exit 1 — style, Clippy, regular tests/doctests, release build, feature matrix, fuzz smoke, and project hook passed; package suite failed on missing registry Redis fork APIs; coverage/audit/loom did not run |
| `.infra/bin/coverage.sh` with Docker group | exit 0 — coverage thresholds passed as recorded above |
| `rs-infra-ci --project <provider-worktree> --only audit,loom check` with invocation tool directory in PATH | exit 0 — audit succeeded with one allowed yanked-package warning; loom was skipped because the project does not configure a loom suite |

The full CI failure was diagnosed from the complete log and reproduced at the normalized-package layer: registry Redis `2.0.0-rc.1` lacks APIs required by this provider. No source/test workaround was applied because it would remove the required response cap or weaken package validation. The configured audit and loom tasks were then selected independently using the repository CI runner; audit succeeded with one repository-allowed yanked-package warning, and loom succeeded with a skip because no loom suite is configured for this project.

## Plan status

- RBR-001: **blocked / incomplete** pending the two compatible registry packages; no publishing was attempted.
- RBR-002: dependency identity unified; metadata, duplicate-tree, compilation, provider downstream, and live task Redis notification checks passed.
- RBR-003: historical claim corrected in both languages; current standalone coverage passed its configured gates and reports/source inventory are persisted.
- T4: full validation and coverage evidence are recorded. Full CI is **not green** because its package suite is blocked by RBR-001; standalone coverage and audit passed, while loom is not configured and was skipped.

The design and implementation-plan source files remain outside commits and preserved in the original provider checkout.
