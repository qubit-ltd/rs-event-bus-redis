# Implementation evidence: RER-001–003 (2026-11-09)

## Baseline and environment

- Provider baseline: `c9d7fab77bcd701d042a26f84e386caa13b61c11` (`dev-starfish`). Candidate source was based on the same commit.
- Rust: `rustc 1.94.0 (4a4ef493e 2026-03-02)`; Redis fixture: 7.0.15; Linux x86_64, kernel 6.8.0-147-generic.
- Host load varied during the runs (the last captured load average was `16.54 21.32 23.08`). Throughput and latency are therefore descriptive samples, not evidence of a stable latency improvement.
- Commands and raw output below use three Sentinel rounds, eight business samples per round, sync and async. They were run sequentially to avoid competing benchmark processes.

## RER-001 Sentinel connection reuse

The fixture inserts a transparent TCP proxy on the Sentinel endpoint and counts provider-created Sentinel sockets. Redis `INFO commandstats` counts `ROLE` probes on the master; each probe is one newly opened candidate-master connection in this implementation. Sentinel query and master probe behavior remain one per resolution/operation.

Reproduction (run in the baseline and candidate source trees):

```sh
REDIS_BENCH_SCENARIOS=sentinel REDIS_BENCH_MODES=sync,async \
REDIS_BENCH_ROUNDS=3 REDIS_BENCH_SAMPLES=8 \
REDIS_BENCH_OUTPUT=/tmp/redis-ee29363d-before-final \
REDIS_BENCH_LABEL=before cargo bench --locked --all-features --bench redis_workloads
```

For the candidate, use `/tmp/redis-ee29363d-after-final` and label `after`.

| Mode | Sentinel connections per round before → after | Master ROLE probes before → after | Normal publish attempts |
| --- | ---: | ---: | ---: |
| sync | 18 → 1 | 18 → 18 | 8/8 in each round |
| async | 18 → 1 | 18 → 18 | 8/8 in each round |

The proxy attributes the 18 baseline connections to the first Sentinel node; other endpoints receive no connections because the first node is healthy. The optimization retains one idle connection for that endpoint and mode, while every operation still queries Sentinel and opens a fresh master connection. This removes 17 Sentinel TCP handshakes per measured round (about 94.4%) without claiming reduced master connects. Recovery rounds each record the expected failover `error=1, unknown=1`, followed by 8/8 successful post-recovery publishes. The same expected recovery classification appears before and after.

Across normal publish rounds, measured throughput varied materially: sync was 283.97–418.97 events/s before and 481.02–843.97 after; async was 201.28–371.61 before and 457.96–734.75 after. These short, noisy samples and host load do not establish a stable throughput or p95 improvement; the measured connection reduction is the basis for retaining the optimization. Per-sample latency, summaries, transparent Sentinel connection counts, and Redis server snapshots are preserved in [`before`](connection-reuse-artifacts/rer-20261109/before/) and [`after`](connection-reuse-artifacts/rer-20261109/after/).

## RER-003 shared async transport investigation

The test uses a TCP proxy with independent request and response pumps so a gated command does not serialize other connections. Redis was local 7.0.15. A shared-provider control first reproduced head-of-line delay: XADD's response was held for 150 ms, EVAL reached the proxy, and settlement completed only after release (192.18 ms elapsed in the probe). An independently created provider/cache completed settlement before release (1.34 ms). This confirmed an instance-local async multiplexing interference worth testing against an internal role split.

The production change gives standalone async general commands and settlement commands independent, on-demand multiplexed connection caches, each with generation-scoped invalidation. Tests on the same provider now complete settlement before XADD response release (~4.5 ms against a 150 ms hold). A 16 MiB publish test gates the unread XADD request body; the settlement EVAL is forwarded and settlement returns in ~48 ms while that request remains gated. A control using Redis `CLIENT PAUSE 300 WRITE` delays settlement ~307 ms, confirming separate client connections cannot bypass a server-wide write pause. A cancellation test also confirms Redis applied XADD before its response was held; it does not claim cancellation retracts an in-flight command.

Commands:

```sh
cargo test --locked --all-features --test async_settlement_isolation_tests -- --test-threads=1 --nocapture
cargo test --locked --all-features --lib tests::client::cache_tests::test_general_and_settlement_cache_generations_are_isolated
```

Both passed after the role-cache change. The full isolation target has seven passing tests, including fixture tests. The role split is limited to standalone async short operations; Sentinel continues resolving and opening a fresh master connection per operation.

## RER-002 TLS trust-source documentation

The dependency's optional root selection is left unchanged when no custom CA material is configured. The crate's default feature graph uses built-in WebPKI roots and no client identity; downstream Cargo feature unification can enable additional root sources. Rustdoc and English/Chinese user guides now describe this without claiming that the provider itself loads system roots.
