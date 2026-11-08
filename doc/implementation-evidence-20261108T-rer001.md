# RER-001 implementation evidence

Date: 2026-11-08
Branch: `codex/rer-001-settlement-owner`
Base: `7dbf3ad91cecdbaf1dbceb0923d6c7429f6bac9b`
Worktree: `/data/working/qubit/rust-common/.worktrees/rs-event-bus-redis/codex-rer-001-settlement-owner`

## Implemented behavior

Settlement now uses a shared private Lua command that checks the Redis stream key, consumer group, message id, and pending owner atomically before acknowledging. Sync and async adapters share reply classification. A stale owner receives a sanitized, non-retryable error and cannot acknowledge the new owner's pending entry. Unknown outcomes retain the original settlement intent; repeating that same intent remains idempotent, while a conflicting disposition is rejected. Public APIs, stream keys, and message wire format are unchanged.

Regression coverage includes sync and async Accept/Reject owner transfers, unknown applied/unapplied replies, retry/conflicting disposition, facade stop, malformed replies, ACL denial for EVAL/XPENDING/XACK, and Redis PEL preservation. English and Chinese README, user guide, design, and migration docs describe the compatibility and ACL requirements.

The TDD ownership regression was run against the pre-implementation adapters with the test retained. It failed as intended: the stale token's settlement returned success (`()`), demonstrating the old behavior could settle after ownership transfer. The adapter implementation was then applied and the same regression passed on real Redis.

## Validation environment

The target worktree's parent contains a pre-existing dangling `rs-task` symlink (mtime Nov 4); it was preserved. CI ran in an isolated mirror at `/data/working/qubit/rust-common/.worktrees/rer001-validation/project`, with clean dependency siblings linked from their existing worktrees. The mirror's source inputs were synchronized from this branch. After the final cfg-only edits, `src/redis_provider_error.rs` and `tests/settlement_policy_tests.rs` were recopied and byte-compared; a recursive comparison excluding `.git`, `target`, fuzz artifacts/corpus, and Python cache showed no source-input differences.

Redis binaries used:

- Redis 6.2.17: `/tmp/redis-6.2.17/src/redis-server`
- Redis 7.2.6: `/tmp/redis-7.2.6/src/redis-server`
- Redis 7.0.15 with TLS: `/usr/bin/redis-server`

The CI environment preferred Redis 7.2.6 for the plain fixture, selected 6.2.17 and 7.2.6 by explicit version env vars, and selected the TLS-capable 7.0.15 binary for TLS fixtures. Docker access was denied by the environment, so fixtures retained Docker preference and used isolated local-process fallbacks. The Redis test fallbacks use temporary data directories and reap their child processes.

## Commands and results

- TDD red: focused `settlement_policy_tests` ownership regression with only the pre-implementation sync/async adapters restored; **failed as expected** because stale ownership could settle.
- Focused settlement, scripted Redis fault, ACL, standalone, cancellation, sync/async Sentinel, TLS transport, TLS Sentinel, and recovery regression tests: **passed**. The settlement policy suite passed 14/14; Redis fault suite passed 20/20. Redis 6.2.17 and 7.2.6 ownership regressions passed in both modes. Recovery tests for deleted-pending entries and tombstones passed on both configured Redis versions.
- Final async-only feature check: `cargo check --locked --no-default-features --features async`; **exit 0**, log `/tmp/rer001-check-async-only-final.log`.
- Final sync-only feature check: `cargo check --locked --no-default-features --features sync`; **exit 0**, log `/tmp/rer001-check-sync-only-final.log`.
- Final formatter/alignment: `.infra/bin/align-ci.sh` in the mirror with the configured rs-infra tool and `RS_INFRA_PROJECT_ROOT` pointing to the mirror; **exit 0**, log `/tmp/rer001-align-ci-final-cfg.log`.
- Full `.infra/bin/ci-check.sh` in the mirror with Redis env above; **exit 0**, complete log `/tmp/rer001-ci-check-final.log`. Configured suites passed: style, Clippy, lock/verification, build, test, docs, README, release build, feature matrix, 10-second `bounded_wire` fuzz smoke, package, coverage, audit, and loom handling. `coverage-cfg-clippy` was explicitly not configured; loom was explicitly skipped as not configured. Audit reported one allowed warning for yanked `async-executor v1.14.0`.
- Package gate: `Cargo package verification passed for 1 workspace package(s)`.
- Coverage gate: thresholds passed with functions **95.52% (555/581)**, lines **96.07% (5454/5677)**, regions **94.98% (8440/8886)**. Per-file report and thresholds are in the CI log. CI cleaned its transient coverage target after completion; no raw coverage artifact remains.
- Downstream named target: `cargo test --locked --features sqlite,event-bus --test redis_task_outbox_tests -- --test-threads=1` against a validation copy of `rs-task` whose provider path resolves to this final mirror; **12/12 passed**. The copy's test-only Redis fixture was temporarily adapted to prefer Docker and fall back to the configured local Redis 7.2.6 process, preserving its outage/restart behavior and assertions. The actual `/data/working/qubit/rust-common/rs-task` checkout was not modified.

## Environment limitations and diagnostics

The first broad downstream `cargo test --locked --features sqlite,event-bus` attempt in the validation copy exited 101: process-crash child fixtures attempted to update their copied lock file while using `--locked`. An offline retry reached those tests but the nested fixture lock update still failed. The plan's `redis_task_outbox_tests` target was then run directly and passed after the temporary test-fixture fallback above.

A separate direct `redis_event_bus_integration_tests` run had 3 pure/local tests pass and 3 Redis-backed tests fail before assertions because that test file hard-codes Docker and Docker socket access is denied. It is outside the plan's named downstream outbox target. Provider CI's own downstream suite, including `rediss`, passed 4/4 through the provider-side local fixtures.

The CI mirror's source formatter changes were copied back where needed. The final alignment run made no further source-input changes. No commit was created. The two plan/design source documents named in the task were not modified or staged.

## Cross-version settlement ACL follow-up

After review requested an explicit Redis 6.2 ACL run, the six settlement-denial cases were rerun on both Redis 6.2.17 and Redis 7.2.6. Each server ran locally on loopback with a unique temporary data directory, `--save ""`, AOF disabled, and `RER_ACL_URL` set to its isolated URL. The focused command was:

```bash
cargo test --locked --all-features --test acl_tests settlement -- --test-threads=1
```

Redis 6.2.17 (`127.0.0.1:6399`): **6 passed, 0 failed, 14 filtered out**, exit 0; log `/tmp/rer001-acl-6.2-settlement.log`.
Redis 7.2.6 (`127.0.0.1:6400`): **6 passed, 0 failed, 14 filtered out**, exit 0; log `/tmp/rer001-acl-7.2-settlement.log`.

The cases cover sync and async settlement denial for EVAL, XPENDING, and XACK. Both local Redis processes were shut down after their runs.
