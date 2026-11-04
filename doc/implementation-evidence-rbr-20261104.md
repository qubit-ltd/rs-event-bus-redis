# RBR implementation evidence (2026-11-04)

## Scope and starting state

- Accepted scope: RBR-001 registry publication dependency closure while preserving the response-frame limit and package validation; RBR-002 correct unknown-outcome classification after an `XGROUP CREATE` reply is lost, with Redis regression coverage and recovery guidance.
- Provider repository started on `dev-starfish` at `7275445f64501fc3bf3646939e1df0d4fce921d6`. The only initial worktree changes were the user's untracked design and implementation-plan documents. They remain outside the implementation worktree and must not be committed.
- Downstream `rs-task` started on `dev-starfish` at `978795facc597744343c35f6bbc2e328b1479423`, clean. A temporary validation worktree pointed its provider path at this implementation worktree; the path edit was reverted and the validation worktree removed after its checks.

## RBR-001 registry/package gate

Evidence in [`implementation-evidence-rbr-t1-20261104.md`](implementation-evidence-rbr-t1-20261104.md): a clean Cargo home found no published `qubit-event-bus` 0.21; Redis `2.0.0-rc.1` and `rc.2` do not provide the current full-response-frame-limit API before `Value` construction. The current Git dependency and limit therefore remain intact. The independent package-candidate command `bash ci/check-package-candidate.sh` exited 1 because crates.io cannot resolve the required `qubit-event-bus = "^0.21"`; this gate is blocked externally and RBR-001 is not complete.

## RBR-002 changes and regression evidence

- Sync and async adapters retain whether the first `XGROUP CREATE` result was an I/O/timeout unknown. If the retry reports `BUSYGROUP` and the existing-group policy rejects an explicit cursor, the adapter returns `subscribe/outcome_unknown` with `retryable=Some(false)`. An initial explicit `BUSYGROUP` preserves the existing position-conflict classification. Retry count and accepted `New`/explicit-resume behavior are unchanged.
- Scripted sync/async regressions cover retry `BUSYGROUP` and initial `BUSYGROUP`.
- Real Redis regression tests execute the command, gate and discard its reply, and confirm the retry outcome. They cover `Earliest`, `At`, `New`, and explicit resume in sync and async modes. A real pending entry is left in the PEL during the lost-reply window; the group cursor and pending ID are checked after retry to guard against cursor changes, ACK, or group deletion.
- English and Chinese README, user guide, and design documents explain the distinct initial conflict and unknown retry cases, inspection via `XINFO GROUPS`/`XPENDING`, and safe recovery choices without automatic `SETID`, ACK, or group deletion.

## Verification log

| Command | Result |
| --- | --- |
| `./.infra/bin/align-ci.sh` | Passed after adding the required Rust source header. |
| `CARGO_TARGET_DIR=/tmp/rbr-final-target cargo +1.94.0 test --locked --test redis_fault_tests subscribe` | Passed, 6 tests. |
| `CARGO_TARGET_DIR=/tmp/rbr-final-target cargo +1.94.0 test --locked --test subscribe_uncertainty_tests` | Passed, 4 tests before PEL strengthening; final focused rerun is recorded below. |
| `CARGO_TARGET_DIR=/tmp/rbr-final-target cargo +1.94.0 test --locked --no-default-features --features sync --test subscribe_uncertainty_tests` | Passed, 3 tests before PEL strengthening; full CI feature matrix re-ran the strengthened sync test successfully. |
| `CARGO_TARGET_DIR=/tmp/rbr-final-target cargo +1.94.0 test --locked --no-default-features --features async --test subscribe_uncertainty_tests` | Passed, 3 tests before PEL strengthening; full CI feature matrix re-ran the strengthened async test successfully. |
| `CARGO_TARGET_DIR=/tmp/rbr-final-target cargo +1.94.0 test --locked --no-default-features --features sync --test redis_fault_tests subscribe` | Passed, 3 tests. |
| `CARGO_TARGET_DIR=/tmp/rbr-final-target cargo +1.94.0 test --locked --no-default-features --features async --test redis_fault_tests subscribe` | Passed, 3 tests. |
| `CARGO_TARGET_DIR=/tmp/rbr-final-target cargo +1.94.0 test --locked --test wire_limit_tests scripted_oversized` | Passed, 2 tests. |
| `python3 ci/check-doc-versions.py /data/working/qubit/rust-common/rs-event-bus-redis-impl` | Passed. |
| `python3 ci/tests/test_doc_versions.py` | Passed, 6 tests. |
| `cargo +1.94.0 test --locked --features sqlite,event-bus --test redis_event_bus_integration_tests -- --test-threads=1` (temporary downstream validation worktree) | Passed, 6 tests against the implementation worktree. |
| `cargo +1.94.0 test --locked --features sqlite,event-bus --test redis_task_outbox_tests -- --test-threads=1` (temporary downstream validation worktree) | Passed, 12 tests against the implementation worktree. |
| `cargo +1.94.0 test --locked --test subscribe_uncertainty_tests -- --test-threads=1` | First PEL-strengthened run failed in both sync/async paths because the fixture parsed summary-form `XPENDING key group` as a detailed reply. After changing to `XPENDING key group - + 10`, passed 4/4 against real Redis. |
| `./.infra/bin/align-ci.sh` | Passed after the final test change. |
| `./.infra/bin/ci-check.sh` | Exit 1 at the package verification stage. Style, build, 111 library unit tests, integration tests, docs, all feature-matrix suites, Redis 6.2/7 cases, wire-limit regressions, and fuzz suite passed. Cargo's package verification cannot resolve the required published core `qubit-event-bus ^0.21`; the isolated package check reports available versions only through 0.20.1. The packaged Redis registry dependency is `2.0.0-rc.1`, which lacks fork APIs used by this project, including the response-size limit and Redis error variants. |
| `bash ci/check-package-candidate.sh` | Exit 1 at `FIRST_CHECK phase=package`: crates.io has no `qubit-event-bus ^0.21` candidate (available candidates include 0.20.1). Confirms the package gate is blocked by the unavailable registry release. |
| `cargo +1.94.0 check --locked --manifest-path target/package/qubit-event-bus-redis-0.7.0/Cargo.toml` | Exit 101 for the same missing published `qubit-event-bus ^0.21` dependency. |

Docker socket access was granted temporarily by the user for verification. Revoke the temporary ACL after all Docker tests finish and verify it is absent.

## Remaining status

- RBR-002: implementation and regressions are complete. The full test and feature matrix passed after the PEL fixture correction.
- RBR-001: blocked because the required published core and an equivalent registry Redis response-frame-limit implementation are unavailable. The package-candidate gate has not passed.
