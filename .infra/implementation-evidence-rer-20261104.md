# Implementation evidence: Redis provider review improvements

Date: 2026-11-04
Project: `/data/working/qubit/rust-common/rs-event-bus-redis`
Plan: `doc/implementation-plan-20261104T075516176Z-605b8622.md`
Design: `doc/design-20261104T075516176Z-605b8622.md`
Initial branch / HEAD: `dev-starfish` / `3bbfb43890f4d1dad0a61a71565870939fe2ac14`

The two supplied design and plan documents were pre-existing untracked input in the original checkout. They were not edited, staged, or included here; this evidence record is separate so the supplied input can remain untouched and excluded from commits.

## T1 — Current dependency and API evidence

| Query | Result |
| --- | --- |
| `cargo +1.94.0 info qubit-event-bus@0.21.0 --registry crates-io` (run outside the workspace to avoid the local Cargo patch) | Exit 1: crates.io has no `qubit-event-bus 0.21.0`; available candidates stop at 0.20.1. |
| `cargo +1.94.0 info qubit-event-bus-redis@0.7.0 --registry crates-io` | Exit 1: package/version not found. |
| `cargo +1.94.0 info qubit-spi@0.14.0 --registry crates-io` | Exit 0: version exists; latest was 0.14.1. |
| `cargo +1.94.0 info redis@2.0.0-rc.1 --registry crates-io` | Exit 0; registry source lacks `set_max_response_bytes` / `max_response_bytes`. |
| `cargo +1.94.0 info redis@2.0.0-rc.2 --registry crates-io` and `cargo +1.94.0 search redis --limit 1` | Exit 0; rc.2 was the latest registry candidate and its source also lacks that API. |

The source is pinned to Redis fork revision `a668891e9fcfed32f8905c2fd729fa05c1732aa5`; that checkout does implement the response-frame limit API. The source manifest versions are provider 0.7.0, core 0.21, and SPI requirement 0.14.

## RER-001 / T2 — Normalized package verification

First command:

```text
bash ci/check-package-candidate.sh
exit 1 — FIRST_CHECK phase=package
error: failed to read '../rs-event-bus/Cargo.toml' from the nested worktree layout
```

This first failure was caused by the worktree's missing sibling path. A temporary symlink to the existing core checkout was created in the task worktree parent, then the same script was rerun. The corrected run reached Cargo package resolution and failed for the external registry dependency:

```text
bash /data/working/qubit/rust-common/.worktrees/rs-event-bus-redis/codex-rer-20261104/ci/check-package-candidate.sh
exit 1 — FIRST_CHECK phase=package
error: failed to select a version for the requirement `qubit-event-bus = "^0.21"`
candidate versions found which didn't match: 0.20.1, 0.20.0, 0.14.0, ...
location searched: crates.io index
BLOCKED phase=package exit=1
```

The `.crate` archive was not produced. Normalized build, metadata, and receiver behavior were not run. `ci/package_candidate.py` already checks normalized production dependencies for registry sources and a unique core identity; no change to that script was justified. RER-001 is externally blocked until crates.io provides core 0.21 and a registry Redis client with equivalent response-frame budgeting. No release or upstream push was attempted.

## RER-003 / T3 — Cancellation resource experiment

The experiment used the pinned Redis fork and a no-reply controlled proxy. The agent worktree and experimental test changes were removed after the evidence was collected; no red test or production workaround remains in the implementation branch.

```text
cargo +1.94.0 test --manifest-path /data/working/qubit/rust-common/.worktrees/rs-event-bus-redis/codex-rer-20261104-t3/Cargo.toml --test client_tests --all-features repeated_cancel --locked -- --test-threads=1
exit 101 — two tests ran (Smol and Tokio); both reached and failed the intended finite-resource assertion.
```

With `total=2`, `general=1`, and one reserved settlement slot, the proxy confirmed 338 XADD requests had been forwarded without replies (16 + 64 + 256 cancellations, one request held through the 2-second command timeout, and one cancellation after timeout). Application in-flight gauges returned to zero after cancellation. The Redis driver's `PipelineSink::start_send` stores sent requests in `VecDeque<InFlight>` until their ordered replies arrive; its capacity-50 mpsc channel limits messages waiting to be processed, not sent requests awaiting replies. Provider `command_permit.rs` documents that cancellation releases admission but does not cancel Redis execution. Thus application admission is not a transport queue bound.

The pinned client exposes no finite transport admission or request-completion notification API that the provider can safely use. A provider-side permit that also releases on caller cancellation would reproduce the defect; retaining it forever would prevent recovery. T4 was therefore not implemented with an unsafe workaround. RER-003 is confirmed, but its required repair remains blocked on client capability. The existing ACK-reserved-lane regression passed:

```text
cargo +1.94.0 test --manifest-path /data/working/qubit/rust-common/.worktrees/rs-event-bus-redis/codex-rer-20261104-t3/Cargo.toml --lib test_reserved_settlement_lane_survives_general_saturation --locked
exit 0 — 1 passed
```

The agent's full `cargo fmt --check` on its temporary test edit returned exit 1 because of existing repository formatting differences; no such experimental edits were retained. The implementation worktree's prescribed `align-ci.sh` below completed successfully.

## RER-002 / T5 — Bilingual versions and offline gate

Updated `README.md`, `README.zh_CN.md`, `doc/user_guide.md`, `doc/user_guide.zh_CN.md`, `doc/migration.md`, and `doc/migration.zh_CN.md` to state the current source versions, label sibling path examples as local-source examples, and remove the unsupported provider 0.7/core 0.20 published pairing. Registry availability is dated 2026-11-04 and clearly distinguished from compatibility validation. Historical API migration context is retained.

Added `ci/check-doc-versions.py`, six focused unit cases, and `.infra/project-ci-check.local.sh`. The checker extracts only explicitly marked TOML blocks, compares their provider/core/SPI version requirements with the manifest, and checks the bilingual examples for parity. Historical version mentions outside the marked block do not affect the result.

```text
python3 ci/tests/test_doc_versions.py
exit 0 — 6 passed

python3 ci/check-doc-versions.py <absolute-worktree-path>
exit 0 — verified current-source dependency versions in bilingual documentation

.infra/project-ci-check.local.sh
exit 0 — checker passed and 6 tests passed

git diff --check
exit 0
```

## T6 — Formatting, CI, and downstream checks

```text
bash .infra/bin/align-ci.sh
exit 0 — Rust style operation completed successfully

bash .infra/bin/ci-check.sh
exit 1 — full configured CI ran. Rust style, workspace build, lock check, normal tests, doctests, feature-matrix suites, documentation checks, and fuzz smoke passed. The final package-tarball verification failed because Cargo resolved registry `redis 2.0.0-rc.1`, whose API does not match the pinned Redis fork required by the source.
```

The initial non-sudo full-CI attempt had Docker-backed test failures (`permission denied while trying to connect to the Docker API at unix:///var/run/docker.sock`). After the user authorized sudo access, CI was rerun using a temporary `sudo -n -u starfish -g docker` process identity without changing system group or socket permissions. That run passed the normal and feature-matrix Docker-backed suites before reaching the package verification failure.

The package verification diagnostic confirmed the registry/client API mismatch: `redis::ConnectionConfig` is absent; `AsyncConnectionConfig::set_max_response_bytes`, `Client::get_connection_with_timeout_and_config`, and `RedisError::is_response_too_large` are absent. Cargo ended with `failed to verify package tarball`. This is the normalized-package RER-001 blocker; the source checkout compiles and passes its configured feature matrix against the pinned fork.

Downstream repository baseline: `/data/working/qubit/rust-common/rs-task`, branch `dev-starfish`, initially had no task-related changes. It was not modified.

```text
cargo +1.94.0 test --features event-bus --test redis_event_bus_integration_tests --test redis_task_outbox_tests --locked -- --test-threads=1
exit 101 — 6 integration tests ran; 3 passed and 3 Docker-backed cases failed on the same socket permission error. The command stopped before the second target.

cargo +1.94.0 test --features event-bus --test redis_task_outbox_tests --locked -- --test-threads=1
exit 0 — 0 tests (the target requires both `sqlite` and `event-bus`).

cargo +1.94.0 test --features 'event-bus sqlite' --test redis_task_outbox_tests --locked -- --test-threads=1
exit 101 — 12 tests ran; 5 passed and 7 Docker-backed tests failed on the same socket permission error.
```

The `sqlite` feature was added only after inspecting the target's crate-level cfg. These failures are environment access failures, not test assertions. The full CI and downstream Redis-backed suites must be rerun after Docker access is available.

After sudo Docker access was authorized, both downstream checks were rerun:

```text
sudo -n -u starfish -g docker PATH=... cargo +1.94.0 test --features event-bus --test redis_event_bus_integration_tests --locked -- --test-threads=1
exit 0 — 6 passed

sudo -n -u starfish -g docker PATH=... cargo +1.94.0 test --features 'event-bus sqlite' --test redis_task_outbox_tests --locked -- --test-threads=1
exit 0 — 12 passed
```

The Docker validation block is resolved; the remaining CI failure is the registry package candidate incompatibility above.

Independent read-only review found no Critical or Important issue in the RER-002 documentation/checker changes. The reviewer independently reran the checker and its six tests successfully. It noted only optional extra negative-case tests for malformed TOML/manifest entries; current code already rejects those cases, so no scope expansion was made. The review agreed that RER-001, RER-003, and Docker-backed validation prevent the overall branch from being called complete or merge-ready.

## Final implementation status

- RER-001: not complete; normalized archive compilation fails because registry core 0.21 and an equivalent Redis response-budget API are unavailable. No archive or normalized consumer behavior could be validated.
- RER-002: implementation and offline checks complete.
- RER-003: defect confirmed; safe repair is blocked on finite transport admission or request-completion support from the Redis client.
- T6: all source CI checks, including Docker-backed normal and feature-matrix suites, passed; package-tarball verification remains failed for the RER-001 dependency blocker. Both downstream suites passed after sudo Docker access.
- No Rust behavior change was made because a provider-only RER-003 fix would not safely bound the Redis driver's sent-command queue.
- Commit, worktree integration/removal, branch synchronization/push, final checkout, and generated dependency-checkout cleanup are handled after final diff review.
