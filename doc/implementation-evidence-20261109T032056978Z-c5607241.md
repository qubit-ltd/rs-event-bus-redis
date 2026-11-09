# Redis subscription creation implementation evidence

## Scope and baseline

- Project: `rs-event-bus-redis` at `/data/working/qubit/rust-common/rs-event-bus-redis`.
- Design: `/data/working/qubit/rust-common/rs-event-bus-redis/doc/design-20261109T032056978Z-c5607241.md`.
- Plan: `/data/working/qubit/rust-common/rs-event-bus-redis/doc/implementation-plan-20261109T032056978Z-c5607241.md`.
- Starting branch and revision: `dev-starfish`, `de762856caed069a898fbd61eb25abed6e30dc51`.
- Implementation branch/worktree: `codex/rer-20261109` at `/data/working/qubit/rust-common/.worktree-rer-20261109`.
- The supplied design and plan documents were pre-existing untracked user files in the original checkout; they were left untouched and excluded from commits.
- The downstream `/data/working/qubit/rust-common/rs-task` checkout had a pre-existing modified `Cargo.lock`; it was not changed. Downstream verification used a temporary copy with path dependencies pointing to the candidate provider worktree.

## Implemented plan goals

1. **RER-001:** Sync and async subscription creation send raw `XGROUP CREATE` and accept only RESP `+OK` (`Value::Okay`) as a successful creation reply. Top-level Redis errors retain their rejection classification; malformed, unexpected, or unparsable replies after send become `subscribe/outcome_unknown` and invalidate/discard the connection. The behavior is covered by scripted response matrices.
2. **RER-002:** A sent first attempt with an unknown result keeps that uncertainty across the one existing transport retry. A retry connection failure, explicit rejection, or second unknown result cannot overwrite the first uncertainty. A confirmed `+OK`, or a policy-accepted `BUSYGROUP` for `New`/resume, remains a valid success path; an explicit start-position rejection after an unknown attempt remains unknown.
3. Sync and async recovery tests verify pre-send failures keep the original transport category, malformed replies discard/invalidate the connection, receiver capacity is released, and a subsequent healthy subscribe succeeds. Real Redis reply-loss tests and fault assertions verify the durable group/cursor behavior.
4. Rustdoc, English and Chinese user guides, design documents, and affected README descriptions were updated. No public API, dependency, wire format, group name, or cursor semantics were changed.

The regression tests were first run against the pre-fix implementation. The non-`+OK` RESP cases incorrectly returned successful subscriptions, and the retry-rejection cases returned the second attempt's Redis error instead of preserving `outcome_unknown`. After the adapter changes, those same assertions passed in sync-only, async-only, and default/all-feature runs.

## Validation record

Redis-backed commands used the project-supported `RER_REDIS_6_2_SERVER=/tmp/codex-rer-20261109-redis-6.2.17/src/redis-server` and `RER_REDIS_7_SERVER=/usr/bin/redis-server` local server fallback because Docker socket access is unavailable in this environment. Redis 6.2.17 was built from the pre-existing `/tmp/redis-6.2.17.tar.gz` archive into the task-owned `/tmp/codex-rer-20261109-redis-6.2.17` directory. Redis 7 was `/usr/bin/redis-server` 7.0.15.

| Command | Result |
| --- | --- |
| `./.infra/bin/align-ci.sh` | Exit 0, run before and after final source/test edits; formatting/alignment completed. |
| `cargo test --locked --test subscribe_uncertainty_tests test_subscribe_create_ -- --nocapture` | Exit 0 after implementation; focused sync/async behavior passed. An initial compile attempt exposed test-only `Debug` bounds from `expect_err`; assertions were rewritten with explicit `match`, then passed. |
| `cargo test --locked --no-default-features --features sync --test subscribe_uncertainty_tests test_subscribe_create_` | Exit 0; sync-only focused cases passed. |
| `cargo test --locked --no-default-features --features async --test subscribe_uncertainty_tests test_subscribe_create_` | Exit 0; async-only focused cases passed. |
| `cargo test --locked --test subscribe_uncertainty_tests` | Exit 0; 14 passed. |
| `cargo test --locked --all-features --test redis_fault_tests -- --test-threads=1` | Exit 0; 20 passed. It includes updated sync/async expectations that uncertainty remains `outcome_unknown` after a retry connection refusal. |
| `RER_REDIS_6_2_SERVER=/tmp/codex-rer-20261109-redis-6.2.17/src/redis-server RER_REDIS_7_SERVER=/usr/bin/redis-server cargo test --locked --all-features --test recovery_regression_tests -- --test-threads=1` | Exit 0; 45 passed against Redis 6.2.17/7.0.15. |
| `RER_REDIS_6_2_SERVER=/tmp/codex-rer-20261109-redis-6.2.17/src/redis-server RER_REDIS_7_SERVER=/usr/bin/redis-server ./.infra/bin/ci-check.sh` | Final run exit 0. Style, Clippy, lock check, build, workspace tests/doctests, documentation examples, downstream project tests, all configured feature-matrix combinations, package validation, fuzz smoke, coverage, and configured audit passed. Loom was reported by repository configuration as not configured and skipped. `coverage-cfg-clippy` was also not configured and skipped. |
| Full CI diagnostic reruns | The first full run failed because Docker socket access was denied in Redis 6.2/7 integration fixtures; local Redis fallback was then built/configured. A subsequent run had one unrelated existing async settlement test fail to connect; its exact isolated rerun passed. Another run had one existing sync recovery test time out waiting for local Redis readiness; its isolated rerun passed. No validation threshold or test was disabled, and the final full CI run passed. |
| `RER_REDIS_6_2_SERVER=/tmp/codex-rer-20261109-redis-6.2.17/src/redis-server RER_REDIS_7_SERVER=/usr/bin/redis-server ./.infra/bin/coverage.sh` | Exit 0. Measured coverage: functions 553/580 = 95.34% (gate 95%); lines 5509/5732 = 96.11% (gate 90%); regions 8518/8967 = 94.99% (gate 85%). Coverage config is `.infra/coverage/coverage.json`. Generated raw reports/build outputs were retained temporarily for evidence extraction and are removed during final task cleanup. |
| `RER_REDIS_6_2_SERVER=/tmp/codex-rer-20261109-redis-6.2.17/src/redis-server RER_REDIS_7_SERVER=/usr/bin/redis-server cargo test --locked --features sqlite,event-bus --test redis_task_outbox_tests -- --test-threads=1` | Exit 0 in `/tmp/codex-rer-20261109-task-validation/rs-task`; 12 passed. The copy used the candidate provider at `/data/working/qubit/rust-common/.worktree-rer-20261109`. Its disposable Redis fixture was adapted only inside the temporary copy to launch the supported local server while preserving the test API. The actual `rs-task` checkout and its pre-existing lockfile edit were not touched. |
| `git diff --check` | Exit 0; no whitespace errors. |

The first `ci-check.sh` attempt before supplying local Redis variables failed only because container fixtures could not access `/var/run/docker.sock`; it was not treated as a pass. The final full run and coverage run both passed using the supported local Redis fallback. The CI hook itself cleaned its transient feature-matrix/coverage build products after each run.

## Review and final diff

An independent review of the final implementation and tests found no substantive issues. It confirmed that pre-send connection failures preserve `transport`, malformed replies invalidate/discard connections and release receiver capacity, and the retry path preserves unknown outcomes. The final source/documentation change set is limited to the two adapters, two regression-test files, English/Chinese user guides and design files, and English/Chinese README descriptions.
