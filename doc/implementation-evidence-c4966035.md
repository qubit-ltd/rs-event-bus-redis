# RER-001 / RER-002 implementation evidence

Date: 2026-11-10
Target: `rs-event-bus-redis`

## Baseline and protected state

- Target checkout at task start: branch `dev-starfish`, HEAD `1d02f157ba5bc3ba3acd2f0fbaf4cfba12a4ce1a`. Implementation used the Git worktree `/data/working/qubit/rust-common/rs-event-bus-redis-rer-20261110`, initially based on that HEAD.
- The original target checkout had the two user-supplied design/plan documents as untracked files. They remained on disk and were excluded from staging. Absolute paths: `/data/working/qubit/rust-common/rs-event-bus-redis/doc/design-20261110T180852436Z-c4966035.md` and `/data/working/qubit/rust-common/rs-event-bus-redis/doc/implementation-plan-20261110T180852436Z-c4966035.md`.
- Related `rs-event-bus` checkout: `dev-starfish`, HEAD `43ee17c8477ccb9b51bd8a28ecb199ecc3164d13`, already three commits ahead of its origin at baseline; it was not modified.
- Related `rs-task` checkout at baseline: `dev-starfish`, HEAD `924fdf206c5b5d79aff09850def0fe4cdbd3163f`, with four pre-existing deleted design/plan documents and two untracked design/plan documents. No `rs-task` file was changed by this implementation.
- During implementation, another workspace operation advanced the target repository's shared `dev`, `main`, and `dev-starfish` refs from the base through `48f2cd7`, `bfbacb2`, and `0c13da6`, and advanced `rs-task` through its own dependency-preparation commits to `015712a`. The target changes were reviewed: they add task-notification reconciliation guidance and local dependency mappings; these are compatible and preserved. The task work branch was fast-forwarded to target `0c13da6` before final verification. `rs-task` pre-existing dirty documents remained untouched; current downstream integration checks pass against its then-current checkout. These independently committed changes were not duplicated.

## RER-001: acquire admission before bounded wire encoding

The sync and async publish paths now check payload type and raw payload length first, acquire one general `CommandPermit`, then encode and carry that permit through connection setup and XADD. Encoding-stage header/wire failures therefore keep `resource_limit` precedence when general capacity is full; type and raw-payload errors retain their earlier precedence. The permit releases on encoding/setup failure, cancellation, and normal lease drop. No public API was added. Test-only encoding counters and an encoding gate exercise the actual publish paths, saturated-cap behavior, bounded concurrent encoding, unpolled futures, error precedence, permit release, and one encode/one XADD lifecycle. Existing reserved settlement capacity remains available.

The deterministic red regression was the saturated publish assertion: before moving admission ahead of encoding, the encoding counter advanced while all general permits were held; after the fix it remains zero and the publish returns `resource_limit` with `NotAccepted`.

## RER-002: verify and isolate canceled Sentinel queries

The locked `qubit-redis 2.0.0-rc.2` multiplexed request path queues requests FIFO and delivers replies through per-request oneshot receivers. Dropping a query future drops its receiver while a late reply is still ahead of subsequent replies on that connection. The controlled full-duplex Redis proxy held the real Sentinel reply after observing the request; a second query on the same connection could not complete before its deadline. In the baseline probe both requests used connection ID 1; the second reached the proxy after 6.62 ms and missed its 2.0067 s deadline while the old reply was held. Releasing that reply allowed completion about 42.7 ms later. This established the cancellation impact.

The async resolver now uses a local generation-bound guard. It invalidates only after polling has started and before a reply completes, and `invalidate_if_current` prevents a stale guard from clearing a replacement generation. Unpolled queries and completed responses do not invalidate. The green real-Sentinel probe observes a new connection for the subsequent query and successful completion before releasing the old reply. The change does not claim the old server operation stopped or that its effects were rolled back.

## Verification record

Commands ran in the target worktree unless an absolute repository path is shown. Exit codes below are shell results; test counts are from Cargo output.

| Command / action | Result |
| --- | --- |
| `cargo check --lib --no-default-features` | exit 0 |
| `cargo test --lib --all-features` | exit 0; 150 passed |
| `cargo test --test client_tests --all-features` | exit 0; 62 passed |
| `cargo test --test wire_limit_tests --all-features` | exit 0; 21 passed |
| `cargo test --test publish_uncertainty_tests --all-features` | exit 0; 21 passed |
| `cargo test --test async_sentinel_tests --all-features -- --test-threads=1` | exit 0; 8 passed, including cancellation recovery |
| `cargo test --test controlled_redis_tests --all-features -- --test-threads=1` | exit 0; 8 passed |
| `cargo test --test downstream_tests --all-features -- --test-threads=1` | exit 0; 7 passed against the existing shared `rs-task` checkout; its HEAD was not changed by this task |
| `cargo test --test documentation_examples_tests --all-features -- --test-threads=1` | exit 0; 8 passed |
| `cargo test --test acl_tests --all-features test_sync_sentinel_acl_workflow -- --exact --nocapture --test-threads=1` | exit 0; isolated ACL/Sentinel case passed |
| `cargo test --test acl_tests --all-features -- --test-threads=1` | exit 0; 19 passed, 4 parent-launched child tests ignored |
| `RER_REDIS_6_2_SERVER=/tmp/rer-redis-6.2.17-20261110/src/redis-server RER_REDIS_7_SERVER=/usr/bin/redis-server cargo test --test acl_tests --all-features test_concurrent_sentinel_fixture_starts_use_distinct_ports -- --exact --nocapture --test-threads=1` | exit 0; concurrent fixtures started with disjoint ports |
| `RER_REDIS_6_2_SERVER=/tmp/rer-redis-6.2.17-20261110/src/redis-server RER_REDIS_7_SERVER=/usr/bin/redis-server cargo test --test acl_tests --all-features` | exit 0; 20 passed, 4 parent-launched child tests ignored |
| `./.infra/bin/align-ci.sh` | final post-integration run exit 0; formatting/alignment and CI check succeeded |
| `./.infra/bin/style-check.sh` | exit 0; also rerun by final `ci-check.sh` style suite |
| `RER_REDIS_6_2_SERVER=/tmp/rer-redis-6.2.17-20261110/src/redis-server RER_REDIS_7_SERVER=/usr/bin/redis-server ./.infra/bin/ci-check.sh` | final post-integration run exit 0; complete configured suite passed, including build, test, docs, README, feature matrix, fuzz, package, coverage, dependency audit, and loom configuration check |
| `git diff --check` | exit 0 |

CI/environment diagnosis and handling:

1. The first alignment attempt ran from a nested `.worktrees` location and failed because the repository helper resolved a dependency destination to an existing non-empty directory. The worktree was moved with Git to the required sibling path shown above; subsequent alignment passed.
2. Initial integration tests could not use the restricted Docker socket for Redis 6.2. Following the repository fixture guide, the pre-existing `/tmp/redis-6.2.17.tar.gz` was built under `/tmp/rer-redis-6.2.17-20261110` with `make -j2 MALLOC=libc`; the binary reports Redis 6.2.17. The existing `/usr/bin/redis-server` reports Redis 7.0.15. CI and recovery regressions then ran with the two explicit environment variables above. The pre-existing tarball was retained.
3. The feature matrix first exposed a sync-only test import of async `SentinelQueryGuard`; that import was gated on `feature = "async"`. It also exposed the test encoding seam in a feature combination with neither provider; that seam was restricted to test builds with sync or async enabled.
4. A subsequent full CI run emitted a dead-code warning for `published_generation` in async-only tests. The test accessor is now compiled only for the sync+async test combination where the Sentinel resolver unit test uses it. Alignment and the complete CI were rerun afterward.
5. A full CI run failed in `test_sync_sentinel_acl_workflow`. The complete log showed a local Sentinel child could not bind port 38135 (`Address already in use`), leaving the fixture below quorum. The Sentinel fixture selected several free ports by opening then closing listeners before spawning Redis, and concurrent Sentinel fixture constructors could select the same ephemeral port in that gap. The fix serializes Sentinel fixture creation with a process-local mutex; a concurrent-start regression starts two fixtures on separate threads and asserts their ports are disjoint. Its first compile exposed an E0716 temporary-borrow error in the test; the test now owns the collected port strings. The focused regression passed, and the complete parallel ACL suite passed (20 passed, 4 parent-launched child cases ignored). The final complete `ci-check.sh` then passed under its normal parallelism across all feature configurations; no test was skipped or weakened.

Coverage from the final successful CI run met repository thresholds: functions 95.38% (661/693; threshold >=95%), lines 96.42% (6524/6766; threshold >90%), regions 95.60% (10331/10807; threshold >85%). The security audit scanned 153 locked dependencies and reported no vulnerability requiring an upgrade. The project does not configure loom, which the existing CI reports as skipped; no loom check was invented or added. CI's configured downstream suite ran; no downstream `--all-features` test was added.

The README files were reviewed and required no edits because their stated contract did not conflict. English/Chinese design and user-guide contracts now describe encoding admission, error precedence, concurrency limits, and the verified Sentinel cancellation behavior.
