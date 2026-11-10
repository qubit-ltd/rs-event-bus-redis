# RER-002 implementation evidence

## Finding

RER-002 was confirmed and repaired with generation retirement on cancellation of an incomplete standalone async command. The dependency driver retains each sent request in its in-flight `VecDeque` until a reply arrives or the driver ends. Dropping the caller future closes its oneshot receiver but does not remove that already-sent entry. Repeated post-send cancellation therefore retained request state on a cached connection independently of the provider's released command permit.

## Locked dependency trace

`Cargo.lock` resolves the production alias `redis` to `qubit-redis 2.0.0-rc.2` (lock entry around line 770). Source inspected from `/home/starfish/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/qubit-redis-2.0.0-rc.2/`:

- `src/aio/multiplexed_connection.rs`: `Pipeline::new` creates `mpsc::channel(buffer_size)`; default `Pipeline::DEFAULT_BUFFER_SIZE` is 50. This is the unsent channel capacity.
- In the same file, `PipelineSink::start_send` first writes the command into the sink and then pushes an `InFlight { output, response_aggregate }` into `in_flight: VecDeque`. The vector has no configured count limit. A dropped request receiver is skipped only if already closed when `start_send` runs; if the command has been sent, its entry remains until `send_result` consumes the response.
- `Pipeline::send_recv` waits on a oneshot receiver inside the response timeout. Dropping that request future drops the receiver. It does not send a cancellation message to `PipelineSink`.
- `MultiplexedConnection::send_packed_command` holds its optional concurrency semaphore permit in a local `_permit`. The provider does not set the dependency concurrency limit in `open_async`; even if configured, the local permit is released when the call future is dropped and is not stored in `InFlight`.
- `Client::get_multiplexed_async_connection_inner` starts the driver and stores the handle in the connection. `HandleContainer::drop` aborts the Tokio task or drops the Smol task. The dependency docs distinguish this canonical setup from manually spawned connections: dropping all clones of the canonical connection also drops the driver task. Thus removing the current cache clone and then dropping the incomplete lease closes the driver even if its response queue contains unanswered requests.

## Controlled blackhole experiment

The focused test `test_async_repeated_post_send_cancellation_resource_boundary` uses the existing `BlackholeRedis`, which answers setup and withholds every XADD response. For each request it waits until the server worker has parsed the XADD, checks the provider in-flight gauge, then drops the public publish future. Batches end at 1, 16, 128, and 1024 cancellations. After each cancellation, the test checks that the application gauge returns to zero and polls the fixture's active socket count until the retired connection's worker has exited, with a one-second watchdog. It also checks the next send requires a new connection generation. The server observed exactly 1024 commands: cancellation does not replay XADD.

The regression `test_async_cancelled_old_lease_preserves_replacement_generation` holds two unanswered leases on generation one, cancels one, and verifies a new request opens generation two. It then drops the remaining stale generation-one lease while a generation-two request is live, verifies the old socket closes, and verifies the stale drop does not evict generation two. This tests the generation comparison under overlapping live leases.

The resource bound is the configured finite command admission budget per client, not the test's 1024 sample count: cancellation retires that lease's generation; any other commands retaining that old generation are themselves bounded by the remaining live command permits. Once those leases finish or are dropped, their final multiplexed connection clones disappear and the canonical driver stops. No cancellation-specific background task is created. Sentinel command connections are not cached and are not affected by this retirement hook. General and settlement use distinct cache instances.

## Repair

`AsyncCommandConnection` tracks whether a request has started and returned. On drop, it synchronously invalidates its standalone cache generation only when the request started but did not return a response/error. Cache invalidation uses the existing generation equality check, so an old lease cannot clear a replacement. A normal completed operation keeps the cache and existing cache-hit behavior. Existing timeout/error paths still invalidate through their current error handling. Sentinel leases carry no cache invalidator. The repair neither retries nor replays XADD and starts no background work.

## Validation

Commands run in the implementation worktree:

- `cargo test --test client_tests test_async_repeated_post_send_cancellation_resource_boundary -- --nocapture` — passed (1 test; 1024 confirmed sends; retired sockets observed closed).
- `cargo test --test client_tests async_cancelled_old_lease_preserves_replacement_generation -- --nocapture` — passed (1 test; stale lease did not evict replacement).
- `cargo test --test client_tests test_async_post_send_timeout_retires_transport_generation -- --nocapture` — passed (1 test; two timed-out XADDs used two generations, produced exactly two server commands, and left no active fixture sockets).
- `cargo test --test client_tests test_async_command_cap_fails_before_second_xadd_and_cancel_releases -- --nocapture` — passed after updating the expected connection-attempt count to reflect retirement and replacement.
- `cargo test --test async_settlement_isolation_tests -- --nocapture` — passed (8 tests).
- `cargo test --lib client::internal::async_connection_cache -- --nocapture` — passed (4 cache lifecycle tests).
- `cargo test --test client_tests -- --nocapture` — first run: 60 passed and one timing-sensitive existing test, `test_async_block_adds_command_margin_and_next_poll_restores_short_wait`, failed at its assertion that the zero poll sent the short claim query. Rerunning that test alone passed. Final full rerun after adding the timeout case: 62 passed, 0 failed.
- `python3 -m unittest ci.tests.test_registry_consumer_manifest` — passed (5 tests) after correcting the baseline test to inspect the prepared fixture and fixing preparation to remove its local path dependency. The source candidate parser now accepts the repository's `^0.21` core requirement.
- `./.infra/bin/align-ci.sh` — passed after all source, test, and CI baseline repairs.
- `RUST_TEST_THREADS=2 RER_REDIS_6_2_SERVER=/tmp/codex-rer-20261110-redis-6.2.17/src/redis-server RER_REDIS_7_SERVER=/usr/bin/redis-server ./.infra/bin/ci-check.sh` — passed (exit 0), including feature matrix, package verification, coverage, bounded fuzz, registry-consumer checks, and security audit. Coverage was 95.22% functions (578/607), 96.16% lines (5760/5990), and 95.17% regions (8982/9438), above repository thresholds. `coverage-cfg-clippy` and `loom` reported not configured and were skipped by repository configuration.

## Coverage limits

The repeated-cancellation test covers the no-reply case and directly observes sent command count, local admission release, transport replacement, and socket-worker exit. A separate targeted test covers a still-polled publish timing out twice and confirms generation retirement without XADD replay. Recovery replies and server disconnect are not directly tested for a post-send canceled publish; existing disconnect/recovery tests exercise other operation paths. Timeout of a still-polled publish is handled by the existing error branch, which invalidates that generation; this change specifically covers external future drop before that branch can run. The experiment observes fixture socket-worker exit as transport closure and the source establishes canonical driver abort; it does not inspect the dependency's private `VecDeque` at runtime.
