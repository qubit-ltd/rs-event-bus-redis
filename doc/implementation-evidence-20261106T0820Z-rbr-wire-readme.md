# Implementation evidence: wire contract and README event correlation

## Scope and starting state

- Accepted items: RBR-001 (document and test the public `WireFields`/provider decoder boundary) and RBR-002 (correlate README success with the current `EventId`, including an older pending event).
- Target repository: `/data/working/qubit/rust-common/rs-event-bus-redis`.
- Target checkout at start: branch `dev-starfish`, commit `c0b2e786cf25b6b9598820aceefb22521037fb00`, tracking `origin/dev-starfish`; no tracked modifications. The only initial untracked files were the user-provided design and implementation-plan documents. They were left in the original checkout and excluded from every staging operation.
- Related repository checked: `/data/working/qubit/rust-common/rs-task`, branch `dev-starfish`, commit `2dceec21a99bc59e9dd2d205c734a3f410b54589`; it remained clean and unmodified.
- Implementation worktree: `/data/working/qubit/rust-common/rs-event-bus-redis-implement-rbr-20261106`, branch `codex/rbr-20261106-2`, created from the target starting commit. It was moved from the nested default worktree location after the alignment script encountered a pre-existing broken `rs-task` symlink there. The new location lets the repository's declared sibling dependency paths resolve to the existing workspace repositories.

## Changes

- `src/wire_fields.rs` now documents that `WireFields` is a concrete version 1 layout whose derived deserializer does not perform provider version-first classification or configured size checks. It explains that `into_parts` rejects an unknown version only after a compatible object has been constructed.
- `tests/wire/wire_fields_tests.rs` contrasts derived deserialization of a future layout with successful deserialization of a complete version 1-shaped object carrying an unknown version, which `into_parts` rejects.
- `src/tests/bounded_wire_decoder_tests.rs` verifies that the private bounded decoder classifies the same future-layout JSON as `UnsupportedWireVersion` before version 1 field validation.
- Both README quick starts send `(EventId, payload)` through the handler channel, preserve the published request's expected ID, apply one five-second total deadline, log earlier recovered IDs, and report success only for the current ID. The existing shutdown checks remain.
- `tests/documentation/markdown_examples_tests.rs` retains the empty-stream check and adds an old pending record with the same `order-42` payload but a different EventId. It observes the recovery log, checks that the reported success ID matches the new stream wire record, confirms both records remain, and confirms the PEL is empty for both README languages.
- `tests/fixtures/task_notifications/Cargo.lock` received a minimal Cargo-generated source disambiguation. The first full CI run showed the fixture's `cargo tree --locked` needed this update even at the original target HEAD; the same failure was reproduced against the original checkout. The task fixture is required by the existing full test and coverage suites, so its lockfile was regenerated offline without changing the downstream repository or weakening checks.
- No production API, wire format, dependencies, or Redis settlement behavior changed.

## Verification log

All repository commands below ran with the explicit implementation worktree path. Docker-backed commands used the previously documented temporary process identity `sudo -n -u starfish -g docker`; no system group or socket ACL was changed.

| Command | Result |
| --- | --- |
| Initial `cargo test --test wire_tests --all-features` without Docker group | Exit 101; 5 Docker-backed cases could not access `/var/run/docker.sock`. The focused test was rerun with the documented Docker-group identity. |
| `sudo -n -u starfish -g docker sh -lc 'cd /data/working/qubit/rust-common/rs-event-bus-redis-implement-rbr-20261106 && export QUBIT_EVENT_BUS_PATH=/data/working/qubit/rust-common/rs-event-bus && cargo test --test wire_tests --all-features'` | Exit 0; 20 passed. Repeated after alignment with the same result. |
| `sudo -n -u starfish -g docker sh -lc 'cd /data/working/qubit/rust-common/rs-event-bus-redis-implement-rbr-20261106 && cargo test --lib --all-features bounded_wire_decoder_tests'` | Exit 0; 9 passed. Repeated after alignment with the same result. |
| `sudo -n -u starfish -g docker sh -lc 'cd /data/working/qubit/rust-common/rs-event-bus-redis-implement-rbr-20261106 && export QUBIT_EVENT_BUS_PATH=/data/working/qubit/rust-common/rs-event-bus && cargo test --test documentation_tests --all-features documentation::markdown_examples_tests::test_markdown_fragments_and_programs_publish_settle_and_close -- --exact --test-threads=1'` | Exit 0; 1 passed, 3 filtered, 164.26 seconds after alignment. It exercised both README languages, their empty and seeded old-pending cases, and the existing guide examples. |
| Same exact documentation test with the original README files temporarily restored from `HEAD` | Expected exit 101. The empty-stream case passed, then the new recovery assertion failed because old README output was only `consumed order event: order-42` and contained no recovered EventId. Both README files were restored byte-for-byte afterward; the positive version passed in focused verification and full CI. |
| First `./.infra/bin/align-ci.sh` from the nested worktree location | Exit 128. The bootstrap tried to clone `rs-task` over a pre-existing broken symlink. The symlink was not removed or changed. After moving the worktree to the workspace root's sibling location, the repository alignment script exited 0. |
| `git diff --check` | Exit 0 after formatting and before evidence finalization. |
| `RUSTDOCFLAGS='-D warnings' cargo doc --all-features --no-deps` | Exit 0. |
| Initial `./.infra/bin/ci-check.sh` | Exit 1 at `downstream_tests`: both task-notifications fixture processes were stopped by `cargo tree --locked`, which reported it needed to update `tests/fixtures/task_notifications/Cargo.lock`. |
| `cargo tree --locked --edges features --manifest-path /data/working/qubit/rust-common/rs-event-bus-redis/tests/fixtures/task_notifications/Cargo.toml` against the original checkout | Exit 101 with the same lock-update error, confirming the failure at the starting commit. |
| `cargo tree --offline --edges features --manifest-path /data/working/qubit/rust-common/rs-event-bus-redis-implement-rbr-20261106/tests/fixtures/task_notifications/Cargo.toml` | Exit 0; Cargo regenerated only the fixture lockfile, adding source-qualified entries for the local and registry `qubit-redact 0.10.0` packages. |
| `cargo tree --locked --edges features --manifest-path /data/working/qubit/rust-common/rs-event-bus-redis-implement-rbr-20261106/tests/fixtures/task_notifications/Cargo.toml` | Exit 0. |
| `sudo -n -u starfish -g docker bash -lc 'cd /data/working/qubit/rust-common/rs-event-bus-redis-implement-rbr-20261106 && cargo test --test downstream_tests --all-features -- --test-threads=1'` | Exit 0; 4 passed, including both task-notifications fixture integrations. |
| `sudo -n -u starfish -g docker bash -lc 'cd /data/working/qubit/rust-common/rs-event-bus-redis-implement-rbr-20261106 && ./.infra/bin/ci-check.sh'` | Final exit 0. Style, Clippy, verify, strict docs, README, release build, configured feature matrix, fuzz smoke, package verification, coverage, and audit succeeded. `coverage-cfg-clippy` is not configured; loom is not configured and was skipped by the CI runner. The advisory audit loaded 1,409 advisories and found one allowed warning; the package step also reported the locked `async-executor 1.14.0` version is yanked. Neither warning failed CI. Coverage gates passed: functions 531/557 (95.33%, threshold >=95%), lines 5,191/5,400 (96.13%, threshold >90%), regions 8,038/8,462 (94.99%, threshold >85%). |

The complete CI logs were captured during execution in `/tmp/rs-event-bus-redis-ci-20261106.log` (first run) and `/tmp/rs-event-bus-redis-ci-20261106-rerun.log` (successful run). These temporary logs are not repository deliverables.

## Completion review

- RBR-001: complete; both JSON entry points are explicitly described and tested against the same future-layout input.
- RBR-002: complete; the bilingual README code is exercised against empty streams and a recoverable older pending event, with event identity, source retention, PEL settlement, and shutdown checked.
- Full required CI and the standalone strict Rustdoc check passed after the fixture lock repair. Coverage met all configured gates. No remaining verification blocker is known.
- The two input design/plan documents remain untouched in the original checkout and are not part of this evidence or implementation commits.
