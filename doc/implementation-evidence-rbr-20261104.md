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

## Follow-up implementation (2026-11-04)

The implementation worktree for the newly accepted scope was created from target `dev-starfish` commit `c06577b37beb33e5390eadad74e0322302861e09`. At task start the target repository had only the user's untracked design and plan documents. Related repositories at start: `rs-event-bus` `dev-starfish` at `f8ae7f1e1f3a350774fcdcc1e96d1d0af9b55f9f`, and `rs-task` `dev-starfish` at `978795facc597744343c35f6bbc2e328b1479423`. Existing sibling-repository untracked documents were preserved. Rust tests used toolchain 1.94.0.

The two user-provided planning documents remain untracked and are excluded from all staging and commits:

- `/data/working/qubit/rust-common/rs-event-bus-redis/doc/design-20261104T160908541Z-85e4ecfb.md`
- `/data/working/qubit/rust-common/rs-event-bus-redis/doc/implementation-plan-20261104T160908541Z-85e4ecfb.md`

### RBR-001 — still blocked by external registry releases

`cargo search --registry crates-io qubit-event-bus --limit 10` exited 0 and listed 0.20.1 as latest. `cargo info --registry crates-io qubit-event-bus@0.21` exited 101 because that version is unavailable. Registry source for `redis 2.0.0-rc.1` and `rc.2` was checked; neither includes the fork APIs required by the current response-frame guard: sync `ConnectionConfig`, `set_max_response_bytes`, `get_connection_with_timeout_and_config`, `ErrorKind::ResponseTooLarge`, or `RedisError::is_response_too_large`. No compatible candidate was found, so no dependency downgrade or guard removal was made.

- `bash ci/check-package-candidate.sh` — exit 1 at package-candidate phase; crates.io does not satisfy `qubit-event-bus = ^0.21`.
- Full `.infra/bin/ci-check.sh` — exit 1 at `run --suite package`: archive creation succeeded, but normalized `cargo package --verify` resolved registry `redis 2.0.0-rc.1` and failed with six compiler diagnostics for the absent APIs listed above.

Resolving this item requires an authorized compatible external release (core and Redis fork API) followed by a rerun of package-candidate and registry-consumer checks. No source-candidate or published registry consumer result is claimed as passed.

### RBR-003 / RBR-004 — durable recovery example and guide boundaries

Both guides' existing-group resume snippet now explicitly selects `SubscriptionDurability::Durable`, retains Earliest and the resume policy, and matches the real extraction test. The test creates and acknowledges a first record, closes the subscription, resumes the same group, receives and acknowledges the second record, then verifies both stream records remain and the PEL is empty. The bilingual guides now describe quarantine Lua actions as ordered XADD then XACK without promising rollback after a partial operation. Resource documentation separates the full RESP frame cap from per-field decoder allocation bounds, and no longer implies an RSS limit or that other connections use the receiver cap.

- `cargo +1.94.0 test --test documentation_tests --all-features test_markdown_fragments_and_programs_publish_settle_and_close -- --nocapture --test-threads=1` — final exit 0; 1 focused integration test passed for both languages. Negative sensitivity checks without Durable and without the resume option each failed at the expected capability/policy assertion; final contents were restored and the focused test passed again.
- `python3 ci/check-doc-versions.py .` — exit 0.

### RBR-005 — TLS Sentinel receive recovery

The sync and async TLS Sentinel tests retain one durable receiver through failover; assert business event and Redis stream IDs; gate failover on a same-connection WAIT write barrier plus replicated stream/group/PEL ownership; then accept the recovered record and verify the promoted master's PEL is empty. No production runtime fix was required. Retry releases the test receiver's active slot without XACK so the record remains eligible for reclaim. TLS observer read timeout is seven seconds to cover the five-second replication WAIT.

- `cargo +1.94.0 test --test tls_sentinel_tests --no-default-features --features sync test_sync_tls_sentinel_rediscovers_tls_master_after_failover -- --exact --nocapture --test-threads=1` — exit 0, 1 passed.
- `cargo +1.94.0 test --test tls_sentinel_tests --all-features -- --nocapture --test-threads=1` — exit 0, 6 passed.
- `cargo +1.94.0 test --test tls_sentinel_tests --no-default-features --features sync -- --nocapture --test-threads=1` — exit 0, 4 passed.
- `cargo +1.94.0 test --test tls_sentinel_tests --no-default-features --features async -- --nocapture --test-threads=1` — exit 0, 4 passed.

### RBR-006 — CI hook

The existing `.infra/project-ci-check.local.sh` now runs the package-candidate, registry-consumer manifest, registry-consumer target, and preflight regression scripts under existing `set -euo pipefail`. Existing CI configuration calls this hook; no workflow change was needed.

- `bash -n .infra/project-ci-check.local.sh` — exit 0.
- `bash .infra/project-ci-check.local.sh` — exit 0: docs (6 tests), package candidate (8), manifest (5), target (12), and preflight passed.
- A temporary copied hook with one regression command replaced by `exit 42` returned 42 and stopped; the real hook was not mutated by this check.

### Integrated checks and test harness correction

An initial complete test-suite run found missing same-level path dependencies in the task worktree fixture (`rs-metadata`, `rs-redact`, `rs-value`). Temporary symlinks to the existing sibling repositories fixed its fixture layout. The next full run exposed parallel fixture invocations overwriting shared JSON evidence files. `tests/support/fixture_consumer.rs` now writes logs into a PID and atomic-sequence-specific evidence directory while retaining the shared Cargo target cache. The default-parallel focused downstream test then passed 4/4.

- `.infra/bin/align-ci.sh` — final run exit 0; Rust style operation passed.
- `.infra/bin/ci-check.sh` — final run exit 1 at package verification. Main tests, feature matrix, Redis 6.2/7, TLS/Sentinel, bilingual docs, downstream tests, fuzz smoke, and CI hook passed. The package verification failure is the RBR-001 registry gap above; the wrapper result is a failure, not a pass.
- `.infra/bin/coverage.sh` — exit 0: functions 95.32% (530/556; threshold >=95%), lines 96.12% (5179/5388; threshold >90%), regions 95.02% (8028/8449; threshold >85%). All configured coverage gates passed.
- Independent final read-only review of all seven source diffs — Ready, no Critical, Important, or Minor findings.
- `git diff --check` — exit 0.

### Item status after this follow-up

| Item | Status |
| --- | --- |
| RBR-001 | Blocked on a compatible external registry release; package-candidate and normalized package verification have not passed. |
| RBR-002 | Previously fixed; not reimplemented in this follow-up. |
| RBR-003 | Implemented and verified. |
| RBR-004 | Implemented and verified. |
| RBR-005 | Implemented and verified in sync, async, and all-feature modes. |
| RBR-006 | Implemented and verified with a negative exit-propagation experiment. |

The overall plan is not fully complete while RBR-001 remains externally blocked.
