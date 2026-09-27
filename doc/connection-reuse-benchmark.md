# Redis connection reuse benchmark

## Setup

The same ignored integration benchmark ran against two revisions using Docker's
`redis:7-alpine` image, ten synchronous idle subscriptions, and 1,000 sequential
publish/receive/accept round trips. Each run used a fresh Redis container and
`REDIS_BENCH_IDLE_SECONDS=30`.

Run it with:

```sh
REDIS_BENCH_IDLE_SECONDS=30 cargo test --test connection_reuse_benchmark_tests -- --ignored --nocapture
```

The before run used `b6be1e7b9b203b8391f705ef83c043646e79bfa2`, before the
Redis connection reuse refactor. The after run used
`87e6eb013d5a3065df7cb3c2f44eb2a44b43697f` plus the uncommitted receive
connection reuse changes in this worktree.

## Results

| Measurement | Before | After |
| --- | ---: | ---: |
| Idle consumers | 10 | 10 |
| Idle observation window | 30.033 s | 30.008 s |
| New Redis connections during idle receives | 2,960 | 10 |
| New connections per second | 98.56 | 0.33 |
| Sequential round trips | 1,000 | 1,000 |
| Round trip throughput | 1,039.52 msg/s | 2,068.25 msg/s |
| Round trip p50 | 942 µs | 454 µs |
| Round trip p95 | 1,070 µs | 653 µs |

The idle connection count fell by 99.66% in this run. The measured sequential
round-trip throughput was 1.99x higher, with p50 latency down 51.8% and p95
latency down 38.9%. These are single-run measurements, so the latency and
throughput figures indicate direction rather than a stable performance bound.
The idle connection count is the more direct result: it includes one initial
dedicated reader connection per subscription and no repeated reconnects for
timeouts.
