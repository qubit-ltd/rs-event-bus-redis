# Redis public API workload benchmark

## 2026-10-03 final-source fixed matrix (`108d0ad`)

This run measures the final provider source revision `108d0ad947f98e26a723adbc50878e58c9684cf9` with the same public-SPI harness. It ran on Rust 1.94.0 against an owned Redis 7.4.8 standalone fixture. The complete fixed matrix covers sync/async, raw payloads of 64/4,096/262,144 bytes, concurrency 1/8/32, default/limited admission, and idle receivers 10/100; every configuration has three rounds of 1,000 attempts. The process exited 0, emitted all 120 summary rows and 120,000 per-attempt samples. Exit 0 means the harness completed; it does not mean every attempt succeeded.

Reproduction command:

```sh
REDIS_BENCH_LABEL=redesign-final-20261003 \
REDIS_BENCH_OUTPUT=/tmp/redis-redesign-benchmark-final-20261003 \
REDIS_BENCH_ROUNDS=3 REDIS_BENCH_SAMPLES=1000 \
REDIS_BENCH_SCENARIOS=round_trip,idle \
cargo bench --locked --all-features --bench redis_workloads
```

Across 108,000 business attempts, 105,436 succeeded and 2,564 failed; 197 failures had an unknown outcome. The raw sample classifications were 1,243 `publish:resource_limit`, 950 `event_id_mismatch`, 174 transport errors, and 197 unknown outcomes. The mismatch samples are consistent with the harness cascade: an uncertain publish/settle can leave an older pending entry, while each subsequent attempt publishes once and does not recover the old token first. They are not evidence of Redis event corruption. The 12,000 idle polls all timed out without errors or unknown outcomes. Idle command deltas total 680 `XAUTOCLAIM` and 11,943 `XREADGROUP`.

### Business matrix

Each row aggregates three rounds (3,000 attempts). Throughput is the median of per-round events/second; p95/p99 are medians of per-round successful-sample percentiles. `unknown` is a subset of errors. Command counts are summed over the three rounds.

| Mode | Admission | Payload B | Concurrency | Success / attempts | Errors (unknown) | Median events/s | p95 / p99 ms | Σ XAUTOCLAIM | Σ XREADGROUP |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| async | default | 64 | 1 | 3000/3000 | 0 (0) | 6280.3 | 0.3/0.4 | 3 | 3003 |
| async | default | 64 | 8 | 3000/3000 | 0 (0) | 27853.9 | 0.4/0.8 | 24 | 3024 |
| async | default | 64 | 32 | 3000/3000 | 0 (0) | 28889.8 | 2.4/5.6 | 96 | 3096 |
| async | default | 4,096 | 1 | 3000/3000 | 0 (0) | 2073.4 | 0.6/1.0 | 3 | 3003 |
| async | default | 4,096 | 8 | 3000/3000 | 0 (0) | 5899.8 | 1.9/17.4 | 24 | 3024 |
| async | default | 4,096 | 32 | 3000/3000 | 0 (0) | 8094.7 | 4.9/49.6 | 96 | 3096 |
| async | default | 262,144 | 1 | 3000/3000 | 0 (0) | 30.6 | 70.2/192.9 | 93 | 3093 |
| async | default | 262,144 | 8 | 3000/3000 | 0 (0) | 39.1 | 830.0/1163.4 | 448 | 3448 |
| async | default | 262,144 | 32 | 2547/3000 | 453 (55) | 51.9 | 1866.9/2501.3 | 995 | 4646 |
| async | limited | 64 | 1 | 3000/3000 | 0 (0) | 1305.9 | 0.9/1.8 | 4 | 3004 |
| async | limited | 64 | 8 | 3000/3000 | 0 (0) | 7722.4 | 2.9/5.0 | 24 | 3024 |
| async | limited | 64 | 32 | 2538/3000 | 462 (0) | 11742.8 | 4.2/6.7 | 83 | 2621 |
| async | limited | 4,096 | 1 | 3000/3000 | 0 (0) | 1911.5 | 0.7/2.4 | 3 | 3003 |
| async | limited | 4,096 | 8 | 3000/3000 | 0 (0) | 6301.2 | 1.6/18.1 | 24 | 3024 |
| async | limited | 4,096 | 32 | 3000/3000 | 0 (0) | 8147.9 | 4.1/47.1 | 96 | 3096 |
| async | limited | 262,144 | 1 | 3000/3000 | 0 (0) | 29.7 | 47.6/690.4 | 87 | 3087 |
| async | limited | 262,144 | 8 | 2907/3000 | 93 (10) | 47.4 | 882.6/1711.1 | 340 | 3423 |
| async | limited | 262,144 | 32 | 2336/3000 | 664 (78) | 62.1 | 1172.2/1694.0 | 704 | 3382 |
| sync | default | 64 | 1 | 3000/3000 | 0 (0) | 7209.1 | 0.2/0.4 | 3 | 3003 |
| sync | default | 64 | 8 | 3000/3000 | 0 (0) | 8614.9 | 1.6/2.6 | 24 | 3024 |
| sync | default | 64 | 32 | 3000/3000 | 0 (0) | 11250.9 | 4.7/14.5 | 96 | 3096 |
| sync | default | 4,096 | 1 | 3000/3000 | 0 (0) | 2290.8 | 0.7/1.6 | 3 | 3003 |
| sync | default | 4,096 | 8 | 3000/3000 | 0 (0) | 3504.7 | 3.8/10.4 | 24 | 3024 |
| sync | default | 4,096 | 32 | 3000/3000 | 0 (0) | 6919.4 | 10.5/20.7 | 96 | 3096 |
| sync | default | 262,144 | 1 | 3000/3000 | 0 (0) | 34.0 | 65.8/125.4 | 85 | 3085 |
| sync | default | 262,144 | 8 | 3000/3000 | 0 (0) | 50.1 | 676.5/957.2 | 401 | 3401 |
| sync | default | 262,144 | 32 | 2736/3000 | 264 (35) | 48.4 | 1822.4/2390.9 | 991 | 4122 |
| sync | limited | 64 | 1 | 3000/3000 | 0 (0) | 7731.4 | 0.3/0.4 | 5 | 3005 |
| sync | limited | 64 | 8 | 3000/3000 | 0 (0) | 11348.9 | 1.3/3.0 | 24 | 3024 |
| sync | limited | 64 | 32 | 2795/3000 | 205 (0) | 10506.6 | 4.9/7.0 | 92 | 2887 |
| sync | limited | 4,096 | 1 | 3000/3000 | 0 (0) | 1988.2 | 1.0/2.3 | 3 | 3003 |
| sync | limited | 4,096 | 8 | 3000/3000 | 0 (0) | 1412.5 | 4.2/13.6 | 31 | 3031 |
| sync | limited | 4,096 | 32 | 2976/3000 | 24 (0) | 4569.7 | 21.4/28.6 | 96 | 3072 |
| sync | limited | 262,144 | 1 | 3000/3000 | 0 (0) | 22.1 | 94.0/800.3 | 113 | 3113 |
| sync | limited | 262,144 | 8 | 2910/3000 | 90 (4) | 55.5 | 897.9/1482.0 | 309 | 3406 |
| sync | limited | 262,144 | 32 | 2691/3000 | 309 (15) | 72.8 | 1687.0/2169.7 | 676 | 3573 |

### Idle receiver matrix

| Mode | Idle receivers | Timed out / attempts | Errors (unknown) | Median polls/s | p95 / p99 ms | Σ XAUTOCLAIM | Σ XREADGROUP |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| async | 10 | 3000/3000 | 0 (0) | 1808.3 | 7.0/9.0 | 40 | 3017 |
| async | 100 | 3000/3000 | 0 (0) | 16226.5 | 7.2/8.6 | 300 | 2757 |
| sync | 10 | 3000/3000 | 0 (0) | 1733.0 | 6.5/8.1 | 40 | 3038 |
| sync | 100 | 3000/3000 | 0 (0) | 15323.9 | 7.0/7.2 | 300 | 3131 |

### Fresh-fixture diagnostics

The original matrix CSV remains unchanged. A separate invocation created a fresh owned Redis fixture and reran sync c32, 64 B and 256 KiB, default and limited, three rounds each (12,000 attempts; exit 0). The default 256 KiB group had 2,977/3,000 successes, 23 errors and 2 unknown outcomes, versus 2,736/3,000 successes, 264 errors and 35 unknown outcomes in the original matrix; two of the three fresh rounds were clean. This variability points to transport/unknown pressure under the shared host rather than a repeatable provider-data failure. The limited 64 B group had 2,805/3,000 successes and 195 errors, all `publish:resource_limit`, both in the original matrix and fresh fixture (fresh per-round failures: 90, 61, 44). This repeats the admission limit behavior when 32 publishers contend for a 24-command general lane (32 total minus 8 reserved settlement commands). The separate limited 256 KiB group had 233 errors, all `publish:resource_limit`, with no unknown or mismatch; default 64 B had none. The default 256 KiB recheck had 7 mismatch samples following its transport/unknown outcomes, matching the same harness cascade described above.

All original and diagnostic samples are retained under `/tmp/redis-redesign-benchmark-final-20261003/` and `/tmp/redis-redesign-benchmark-final-recheck-20261003/`; no failed round was dropped or replaced. The executable SHA-256 was `1e65a56af9fd4f737ec8d81765060af29114cc65fdbf4739ab0733c80e274e4a`. Redis was 7.4.8. The host exposed 6 CPUs; the post-run load snapshot was 38.97/41.64/44.62 after the separate diagnostic run and is not per-round attribution. Treat throughput and transport failures as descriptive single-host observations, not stable capacity claims.


## 2026-10-03 pre-fix redesign run (`b61dd8b`)

This run uses the checked-in public SPI harness from source revision
`b61dd8b3549d175dcdd8f5b507c88bb9fa3cecc0`, Rust 1.94.0, and an owned Docker
Redis 7.4.8 standalone fixture. It covers synchronous and asynchronous modes,
64/4,096/262,144-byte raw payloads, concurrency 1/8/32, default and limited
admission, plus 10/100 idle receivers, with three 1,000-attempt rounds per
configuration. Limited mode sets 32 total commands and one retained idle
connection. This is a single-host workload sample, not a paired comparison to
the historical 2026-09-29 baseline.

Reproduction command:

```sh
REDIS_BENCH_LABEL=redesign-20261003 \
REDIS_BENCH_OUTPUT=/tmp/redis-redesign-benchmark-20261003 \
REDIS_BENCH_ROUNDS=3 REDIS_BENCH_SAMPLES=1000 \
REDIS_BENCH_SCENARIOS=round_trip,idle \
cargo bench --locked --all-features --bench redis_workloads
```

The first run completed with exit code 0 and produced 120 summary rows plus
120,000 attempt rows. A zero process exit means the harness finished, not that
all workloads passed: only 87/108 business rounds had 1,000 successes and zero
errors; 11/12 idle rounds had zero errors. `unknown` is a subset of `errors`.
The 108,000 business attempts recorded 104,337 successful publish/receive/Accept
round trips, 3,663 errors, and 446 unknown outcomes. The 12,000 idle polls
recorded 11,990 timeouts and 10 receive errors/unknowns. Outcome counts were
104,337 `ok`, 11,990 `timed_out`, 694 `publish:resource_limit`, 179 transport
errors, 456 unknown outcomes, and 2,344 `event_id_mismatch` samples. A lost or
uncertain publish/settle can leave an older PEL entry; because this harness
does not recover that old token before its next attempt, the resulting mismatch
is a measurement-harness cascade, not evidence of event data corruption.

### First-run business workload matrix

Each row aggregates the three rounds (3,000 attempts). Throughput is the median
of per-round accepted events/second. The latency columns are medians of the
per-round nearest-rank successful-sample p95/p99 values. Redis command counts
are sums over all three rounds. `Errors (unknown)` reports unknown outcomes as
a subset; the raw CSV also retains each error's own p50/p95/p99 latency.

| Mode | Admission | Payload B | Concurrency | Success / attempts | Errors (unknown) | Median events/s | p95 / p99 ms | Σ XAUTOCLAIM | Σ XREADGROUP |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| sync | default | 64 | 1 | 3000/3000 | 0 (0) | 1453.9 | 2.1/4.0 | 3 | 3003 |
| sync | default | 64 | 8 | 3000/3000 | 0 (0) | 8981.5 | 1.6/3.1 | 24 | 3024 |
| sync | default | 64 | 32 | 3000/3000 | 0 (0) | 5306.3 | 4.4/12.5 | 96 | 3096 |
| sync | default | 4096 | 1 | 3000/3000 | 0 (0) | 602.4 | 4.1/6.0 | 5 | 3005 |
| sync | default | 4096 | 8 | 3000/3000 | 0 (0) | 2637.1 | 4.7/9.5 | 24 | 3024 |
| sync | default | 4096 | 32 | 3000/3000 | 0 (0) | 2242.0 | 12.5/20.5 | 96 | 3096 |
| sync | default | 262144 | 1 | 3000/3000 | 0 (0) | 13.9 | 152.3/287.2 | 192 | 3192 |
| sync | default | 262144 | 8 | 3000/3000 | 0 (0) | 30.4 | 1098.0/1497.0 | 554 | 3554 |
| sync | default | 262144 | 32 | 2764/3000 | 236 (26) | 39.1 | 2037.6/2965.9 | 1038 | 4348 |
| sync | limited | 64 | 1 | 3000/3000 | 0 (0) | 1776.3 | 1.6/2.8 | 4 | 3004 |
| sync | limited | 64 | 8 | 3000/3000 | 0 (0) | 5960.8 | 2.4/3.8 | 24 | 3024 |
| sync | limited | 64 | 32 | 2969/3000 | 31 (0) | 7165.4 | 4.9/6.6 | 96 | 3065 |
| sync | limited | 4096 | 1 | 3000/3000 | 0 (0) | 383.2 | 5.5/10.0 | 10 | 3010 |
| sync | limited | 4096 | 8 | 3000/3000 | 0 (0) | 1691.3 | 6.5/11.5 | 24 | 3024 |
| sync | limited | 4096 | 32 | 2991/3000 | 9 (0) | 1992.4 | 17.3/32.4 | 96 | 3087 |
| sync | limited | 262144 | 1 | 3000/3000 | 0 (0) | 13.8 | 155.0/470.2 | 172 | 3172 |
| sync | limited | 262144 | 8 | 2933/3000 | 67 (1) | 38.6 | 838.8/1396.3 | 513 | 3601 |
| sync | limited | 262144 | 32 | 2543/3000 | 457 (35) | 45.9 | 1658.0/2108.1 | 951 | 3932 |
| async | default | 64 | 1 | 3000/3000 | 0 (0) | 814.6 | 3.0/3.8 | 5 | 3005 |
| async | default | 64 | 8 | 3000/3000 | 0 (0) | 4848.2 | 3.6/5.5 | 24 | 3024 |
| async | default | 64 | 32 | 3000/3000 | 0 (0) | 6087.0 | 8.8/11.2 | 96 | 3096 |
| async | default | 4096 | 1 | 3000/3000 | 0 (0) | 372.2 | 5.1/7.1 | 9 | 3009 |
| async | default | 4096 | 8 | 3000/3000 | 0 (0) | 1236.9 | 6.9/33.0 | 24 | 3024 |
| async | default | 4096 | 32 | 3000/3000 | 0 (0) | 2225.0 | 49.1/162.2 | 111 | 3111 |
| async | default | 262144 | 1 | 3000/3000 | 0 (0) | 19.4 | 107.7/181.6 | 155 | 3155 |
| async | default | 262144 | 8 | 3000/3000 | 0 (0) | 34.1 | 977.0/1245.0 | 496 | 3496 |
| async | default | 262144 | 32 | 1870/3000 | 1130 (248) | 25.9 | 1916.8/2695.4 | 1151 | 4954 |
| async | limited | 64 | 1 | 3000/3000 | 0 (0) | 953.8 | 0.7/2.4 | 5 | 3005 |
| async | limited | 64 | 8 | 3000/3000 | 0 (0) | 5404.0 | 3.2/5.4 | 24 | 3024 |
| async | limited | 64 | 32 | 2870/3000 | 130 (0) | 12019.3 | 4.5/7.0 | 96 | 2966 |
| async | limited | 4096 | 1 | 3000/3000 | 0 (0) | 1006.4 | 3.0/4.5 | 4 | 3004 |
| async | limited | 4096 | 8 | 3000/3000 | 0 (0) | 1165.9 | 5.8/65.0 | 32 | 3032 |
| async | limited | 4096 | 32 | 2955/3000 | 45 (0) | 3971.8 | 15.7/21.2 | 96 | 3051 |
| async | limited | 262144 | 1 | 3000/3000 | 0 (0) | 22.0 | 87.8/854.4 | 110 | 3110 |
| async | limited | 262144 | 8 | 2357/3000 | 643 (16) | 32.8 | 995.0/1316.9 | 1514 | 5111 |
| async | limited | 262144 | 32 | 2085/3000 | 915 (120) | 32.0 | 1971.9/2506.0 | 1081 | 4345 |

Idle receive rounds (1,000 polls per round) had these measurements:

| Mode | Idle receivers | Timed out / attempts | Errors (unknown) | Median polls/s | p95 / p99 ms | Σ XAUTOCLAIM | Σ XREADGROUP |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| sync | 10 | 2990/3000 | 10 (10) | 1555.3 | 8.0/9.4 | 51 | 3018 |
| sync | 100 | 3000/3000 | 0 (0) | 16267.7 | 7.4/9.0 | 740 | 2684 |
| async | 10 | 3000/3000 | 0 (0) | 1417.6 | 7.8/38.3 | 30 | 3021 |
| async | 100 | 3000/3000 | 0 (0) | 9253.9 | 6.1/7.9 | 2520 | 591 |

### Fresh-fixture repeats of error rounds

The original CSVs were preserved. Repeats below used the same public harness,
source and 1,000 attempts per round on newly owned Redis fixtures; they do not
replace or overwrite the first-run samples.

| Workload slice | First run success / attempts; errors (unknown) | Fresh repeats success / attempts; errors (unknown) |
| --- | ---: | ---: |
| default, sync, 256 KiB, c32 | 2764/3000; 236 (26) | 2845/3000; 155 (27) |
| default, async, 256 KiB, c32 | 1870/3000; 1130 (248) | 2370/3000; 630 (123) |
| limited, sync, 256 KiB, c8 | 2933/3000; 67 (1) | 3000/3000; 0 (0) |
| limited, async, 256 KiB, c8 | 2357/3000; 643 (16) | 3000/3000; 0 (0) |
| limited, sync, 256 KiB, c32 | 2543/3000; 457 (35) | 2976/3000; 24 (2) |
| limited, async, 256 KiB, c32 | 2085/3000; 915 (120) | 2931/3000; 69 (0) |
| limited, sync, 64 B, c32 | 2969/3000; 31 (0) | 3000/3000; 0 (0) |
| limited, sync, 4 KiB, c32 | 2991/3000; 9 (0) | 2928/3000; 72 (0) |
| limited, async, 64 B, c32 | 2870/3000; 130 (0) | 2734/3000; 266 (0) |
| limited, async, 4 KiB, c32 | 2955/3000; 45 (0) | 2810/3000; 190 (0) |
| idle, sync, 10 receivers | 2990/3000; 10 (10) | 3000/3000; 0 (0) |

All 528 errors in the fresh limited small-payload c32 repeats were
`publish:resource_limit`. With a total of 32 commands and the default 8
settlement reservations, only 24 slots serve general commands; 32 simultaneous
publish attempts can exceed that admission lane. The limited large-payload c32
repeat set had 70 `publish:resource_limit` outcomes. The default large-payload
c32 repeats had no `resource_limit`; their errors were transport/unknown
outcomes followed by the harness's old-entry `event_id_mismatch` cascade.
Counter pressure is therefore a repeatable configuration effect in the tested
limited c32 bursts; high-payload default transport behavior varied between
rounds and remains unresolved.

The raw files are retained at
`/tmp/redis-redesign-benchmark-20261003/` and
`/tmp/redis-redesign-benchmark-rerun-20261003/`. They include every attempt,
per-round successful and failed latency distributions, and command-counter
deltas. Redis server identity was 7.4.8. At the end of measurement the host had
6 CPUs visible to the benchmark and load averages 56.98/54.97/57.19; this is
only a post-run snapshot, not per-round load attribution. Because errors remain
in high-concurrency groups and host load was high, these throughput figures are
descriptive samples, not stable capacity claims or a performance comparison.

The executable SHA-256 was
`e9bfd5d724c297564e39b74da0f64d9c2f17aa3fee48aa93759a8127ad08f380`.
First-run command:

```sh
REDIS_BENCH_LABEL=redesign-20261003 \
REDIS_BENCH_OUTPUT=/tmp/redis-redesign-benchmark-20261003 \
REDIS_BENCH_ROUNDS=3 REDIS_BENCH_SAMPLES=1000 \
REDIS_BENCH_SCENARIOS=round_trip,idle \
cargo bench --locked --all-features --bench redis_workloads
```
Use a new label and output directory when repeating: CSV creation is exclusive.

The 2026-09-29 results below are historical baseline evidence. The redesign
runs above are not paired with the 2026-09-29 baseline and must not be
interpreted as a before/after result.

## Current measurement contract

The harness uses only public synchronous and asynchronous provider SPI APIs.
Both modes share one provider across independent worker threads. Each worker
owns one durable receiver and one topic, and completes publish, receive, and
Accept sequentially. Async workers drive runtime-neutral futures with
`futures_lite::future::block_on`; these figures describe this executor arrangement.
Messages, receivers, thread creation, Redis startup, and CSV writes are outside
the timed worker region. Each message uses a prebuilt shared raw payload of
exactly 64, 4,096, or 262,144 bytes; JSON wire metadata and byte-array expansion
are additional. A start barrier releases concurrency 1, 8, or 32 simultaneously.

Every default configuration and restricted command-admission configuration
has 1,000 attempts per round and three rounds. The restricted setting
changes `redis.max_concurrent_commands` from 64 to 32 and
`redis.max_idle_connections` from 8 to 1; all receivers fit within the default
256 active-receiver cap. With the default 8 reserved settlement slots, only 24
of the 32 total command slots admit general commands, so concurrency 32 can be
refused; the total limit is not a promise that all workers' commands are
admitted. A business round is accepted only with exactly 1,000 successes and
zero errors.
An invalid round retains its raw evidence and is investigated before the entire
round is repeated on a fresh fixture; individual publications are never retried
to fill a successful-sample quota.
A separate after-only command-limit case submits 1,000 attempts at concurrency
32 with just one command slot and one idle connection; rejected attempts remain in raw CSV and successes
and refusals are reported separately. A separate receiver-limit case holds the sole admitted receiver and
measures 1,000 rejected subscribe attempts in each of three rounds.
The baseline does not recognize the new admission options, so those cases have
no before comparison. Ten and 100 idle receivers perform 1-ms short polls;
Redis scheduling may make actual waits substantially longer.

Other after defaults remain: 2,000-ms connect and command budgets, 1,048,576
raw payload bytes, 8,388,608 complete wire bytes, a 30,000-ms minimum claim idle
time, 1,000-ms recovery interval, and 100 unsettled deliveries per receiver.
Approximate stream trimming is disabled. Workers use durable groups with
`StartPosition::New`, empty headers, and `application/octet-stream` content.
The baseline retains its own default behavior; it has no new command/receiver
admission limits or wire size budgets.

Throughput divides successful accepted events by elapsed time between the
start barrier and joining all workers. Idle throughput counts timed-out polls.
Latency uses `Instant`, spans one complete attempt, and has nearest-rank
p50/p95/p99 percentiles computed from successful samples. Failed-attempt
percentiles are recorded separately. Error and unknown counts use the SPI
`kind()` classification. The CSV contains every attempt, including errors.
Setup is excluded from latency and throughput but included in the connection
and command-counter deltas. One persistent INFO observer and one peak sampler
are connected before the counter baseline; their setup connections are excluded
from the connection delta. Active-client snapshots include these two observers;
the peak sampler waits 5 ms between INFO replies, so scheduling and command time
can lengthen its interval and shorter peaks may be missed. XAUTOCLAIM and
XREADGROUP are cumulative Redis INFO commandstats deltas, including zero counts.

Owned standalone fixtures use a local `redis-server` when installed, otherwise
Docker `redis:7-alpine`, loopback ports, AOF, and `appendfsync everysec`. They
flush only their own database after each completed scenario. An externally
provided URL is never flushed. Each Sentinel round owns a fresh master, replica,
and three-node quorum. The benchmark first measures 1,000 publications, observes
replication catch-up as a setup condition, abruptly kills the master, waits for
two Sentinel votes plus replica `INFO replication` master-role convergence, and retries through the same
provider/client instance and receiver. Recovery elapsed time includes master stop,
promotion, errors, retry delay, and the first accepted event; per-attempt recovery
latencies are separate. After one acknowledged publication, recovery retries
only receive for that same event ID, which is matched on delivery. Publication
errors (including unknown outcomes) and settlement errors stop probes without
republishing. Receive unknowns are counted and the receiver is polled again,
consistent with its recovery contract. It then measures 1,000 post-recovery events. No observer
WAIT is used as a provider write replication fence or durability guarantee.
Recovery successful samples measure receive/Accept after the acknowledged
publication; failed publication samples measure that publish only. Full failover
latency is the separately reported recovery elapsed time across the three rounds.
Recovery command and connection counters cover the promoted replica only;
pre-failure counters cover the original master. Sentinel counts include its
control-plane/replication clients. Fixture queries reuse observer connections
opened before the baseline snapshot through the pre-failure replication check.
Promotion intentionally closes normal clients ([Redis 7.4.8 implementation](https://github.com/redis/redis/blob/7.4.8/src/sentinel.c#L4501)).
After at least two Sentinel votes, an I/O failure causes only that fixture
observer to reopen the same promoted endpoint and verify its master role.
The peak sampler also restarts after promotion. Recovery connection deltas
include those two observer reopenings. The reported recovery peak is the maximum
of the actual samples from the two intervals; the promotion gap means this is
an observed lower bound, not the true maximum. Other scenarios keep one sampler.

## Reproduce

```sh
cargo bench --locked --all-features --bench redis_workloads --no-run
REDIS_BENCH_LABEL=after \
REDIS_BENCH_OUTPUT=/tmp/redis-workload-benchmark \
cargo bench --locked --all-features --bench redis_workloads
```

`REDIS_BENCH_SAMPLES` defaults to 1,000 and `REDIS_BENCH_ROUNDS` to 3;
use a fresh label or empty output directory because raw CSV files are never overwritten.
`REDIS_BENCH_FIRST_ROUND` defaults to 1 and identifies individually paired rounds. A dedicated
`REDIS_BENCH_URL` is optional. Diagnostic filters are `REDIS_BENCH_MODES`,
`REDIS_BENCH_SCENARIOS`, `REDIS_BENCH_LIMITS`, `REDIS_BENCH_PAYLOADS`, and
`REDIS_BENCH_CONCURRENCY`, each a comma-separated list. Filters or smaller samples
must be disclosed and do not satisfy the complete matrix. Scenarios are
`round_trip`, `idle`, `command_limit`, `receiver_limit`, and `sentinel`. A clean external driver
uses these same public API sources against the baseline without changing its
production behavior. Sentinel async access resolves the master for each connection;
this scenario validates recovery of the same provider/client instance rather than
invalidation of a cached Sentinel master socket. Standalone cache reuse is
measured separately. Both runs must be isolated from other Redis load. This is a shared workstation:
comparable default scenarios use paired before→after runs for each configuration
and round, with timestamps and load/CPU observations recorded. Main throughput
runs wait at most 30 seconds for observed background compilation to end;
timeouts and compiler/load observations remain attached to each run. Stable apparent regressions
are not inferred from this wait alone. The resumed bounded runner has a
five-minute compilation-wait budget; after that, runs retain environment flags and
continue without waiting. Apparent regressions are rechecked against the paired
load evidence and repeated when noise differs;
high-load diagnostic numbers are not used as final regression evidence. Follow-up
quiet-window waiting is limited to two minutes in total; if no quiet window is
available, the report leaves stable performance attribution unresolved.

## Historical results from 2026-09-29

The complete accepted matrix contains 234 summary rows: 84 before and 150 after.
The CSV validator matched every summary to its attempt samples and all three
rounds. The 186 business rows contain 186,000 successful events, zero errors,
and zero unknowns. Recovery adds 12 successes and six receive unknowns; each
after recovery round has one unknown receive followed by a successful matching
delivery, with no publication replay. Admission stress contains 12,000 attempts,
118 successes, 11,882 resource refusals, and zero unknowns. Idle cases add 24,000
poll attempts, with the refusals below. The accepted raw samples total 78,006
before and 144,012 after. These totals exclude the preserved invalid rounds.
After idle100 refusals total 856/6,000 = 14.27%; the two admission stress
cases refuse 11,882/12,000 = 99.02%. The original invalid business round
has 34/1,000 = 3.4% errors, including 2/1,000 = 0.2% unknowns. Zero errors
in the accepted business aggregate do not establish zero failures across all
attempts or overall reliability; preserved failures and pressure denominators
remain part of the evidence.

### Environment and identities

Measurements ran on Ubuntu 24.04.4 LTS, Linux 7.0.0-34-generic, Intel
i5-9600K at nominal 3.70 GHz (six physical cores/threads), 33,491,451,904 bytes
RAM and no swap. CPU affinity was 0–5; the powersave governor reported
800 MHz–4.6 GHz and frequency was not forced. Toolchains were rustc 1.94.0
(4a4ef493e, 2026-03-02), Cargo 1.94.0 (85eff7c80), and LLVM 21.1.8.
Owned Docker fixtures used Redis 7.4.8, jemalloc 5.3.0, build
3235b4981286ba61, image digest
`redis@sha256:8b81dd37ff027bec4e516d41acfbe9fe2460070dc6d4a4570a2ac5b9d59df065`.
No externally supplied Redis URL was used.

The clean production baseline is
`5e36aede24c5db5de2e282932546de2fe500e9c2`; the shared public facade is
`96e7422af3fc5c12ddc46ac623d3218b75a0c7f4` on both sides. The after source
was uncommitted on the baseline SHA during measurement. Its source plus
Cargo.toml/Cargo.lock SHA-256 manifest is retained separately from report prose:

| Evidence | SHA-256 |
| --- | --- |
| Before source/manifest | `7832b81ad2adffecc48eee3b2209c3e76edd96618beaf238e0179905b64f2e43` |
| After V1 source/manifest | `c6922c1314011b745f7b004cb83c7fcba23af9d4137ed6f6e38f0e66b9dcb87d` |
| After V2 source/manifest | `a9c071e808fbbf35c06592ff0acb0d3334d5355d6cce87fc2d401fd5b308b91b` |
| V2 tracked implementation diff, reports excluded | `2d8708295d6fcdbae3486ad7ba52772f3869600c9e9df251ad02557976aee7cf` |
| V2 before executable | `4acb97140478b07743c1eabe0694ec069a772e275c0c7add320d44dc0f50768b` |
| V2 after executable | `95b6632ce901a329e94a61efe64afb28f16d8f834ef5f33f9a63e9d342113eea` |

The first 66 accepted invocations (sync default and idle) used V1 on both
sides. V2 changed only fixture observer recovery and Sentinel peak sampling;
standalone workload/statistics bodies remained unchanged. Later pairs used V2
on both sides. At measurement freeze, all 14 baseline driver harness files
matched the respective after harness byte for byte. The external driver resolves the real baseline provider
and facade paths, preserving baseline production behavior. Package versions
and registry sources match the current lock; successful builds used `--locked`.
V1/V2 source copies, lock review, individual executable hashes, and per-run
version attribution are retained with the raw evidence. Authorized test-only
edits occurred during measurement; production, Cargo, and measured executables
stayed frozen.

After measurement, the final CI Clippy check prompted a syntax-equivalent
cleanup in `benches/support/redis_fixture/redis_node.rs`: nested connection/PING
conditions became a short-circuit let-chain. Startup calls, arguments, error
propagation, and waits remain the same. The final helper SHA-256 is
`7442c72922e0de879f8acf9721355657d8a27aee0bd36b2d3bf140b17311f8fb`,
while the measured helper SHA-256 is
`0c00aecb5c7d1643f0d7265f8dd0fdc3f065375c8ec8a341111a804ff1cbe92d`.
No performance measurement was repeated for this syntax cleanup. The results
remain bound to the measurement-time V1/V2 source manifests and actual
executable hashes above. The final source differs and is not claimed to be
byte-identical or to produce the same binary. Baseline driver sources and
measurement seals were retained.

A later production cleanup added the crate-internal `Client::command_timeout()`
getter and replaced eight short-command `response_timeout(None)` call sites
(four sync and four async). The getter returns the same configured policy
`Duration` directly; the old `None` branch always returned `Ok` of that duration,
with no fallible conversion or BLOCK-duration addition. Implementation review
approved the semantic equivalence of these replacements. Fallible socket
operations and BLOCK-aware timeout calculation retain their error paths.
This is a production source delta after the performance measurement, in
addition to the fixture helper cleanup above. No performance measurement was
repeated for this production cleanup. The reported results remain bound only
to the measurement-time source seals and actual executable SHA-256 identities,
not to the final production source or a newly built final executable.

### Observed behavior and interpretation

Connection reuse is already present in this baseline. At concurrency 1 both
revisions opened two measured standalone connections and sampled four active
clients for every default business round. Ten idle receivers opened 11
connections on both sides, including receiver setup. This experiment therefore
does not reproduce the historical 99.66% reduction against the older baseline.
At higher concurrency, connection churn and sampled peaks vary; full values
are retained below rather than treating an admission cap as a Redis-client cap.

At 100 idle receivers, after sync rounds have 929/860/835 timed-out polls and
71/140/165 `receive:resource_limit` refusals; async has 815/889/816 polls and
185/111/184 refusals. Before has 1,000 timed-out polls and zero errors each
round. The default 64 simultaneous command slots can reject concurrent receiver
recovery commands even though 100 receivers fit the separate 256 receiver cap.
These errors have no unknown outcome. After connection churn reaches 283 and
its sampled active peak reaches 184. Command admission is not a hard bound on
all physical Redis clients: readers, idle connections, observers, and control
clients have separate lifetimes. Idle throughput uses successful polls as its
numerator and elapsed time as its denominator; the numerator can be below
1,000 and is not an unconditional performance improvement.

With one command slot, sync succeeds 38/2/17 times and async 31/9/21 times out
of 1,000 attempts per round. Rejections occur at publish, receive, or settle,
with no unknowns and no automatic replay. The one-receiver case rejects all
1,000 subscribe attempts each round before issuing Redis commands (zero new
connections, XAUTOCLAIM, and XREADGROUP). The limited 32-command/one-idle main
matrix separately achieved 1,000 successes per configuration and round; it
does not show that the one-command configuration can complete all events.

For every Sentinel business phase, 1,000 events completed with zero errors.
Replication evidence samples the master's target offset after the business and
receiver-setup commands, then confirms replica offset at least that target.
All fresh successful rounds recorded at least two promotion votes and master
role convergence. Full recovery elapsed time is 2.325–2.639 seconds before and
2.354–2.687 seconds after. Each after recovery has one counted receive unknown
and subsequently accepts the original acknowledged event; before has none.
The recovery connection delta includes two fixture observer reopenings, and
the two-interval sampled peak is a lower bound because promotion creates a gap.
This demonstrates same-instance failover recovery under these conditions, not
a provider write replication guarantee or cached async Sentinel socket test.

### Preserved failures and shared-host limits

The initial before sync Sentinel round completed its 1,000 pre-failure events
and replication check, then the replica observer reached EOF during promotion.
An independent fresh-fixture reproduction showed the old normal-client
observer disconnected while a new observer reported master role. The V2 helper
reopens only that same endpoint after quorum votes and an I/O error, within the
original deadline, and keeps the role/offset checks. All before/after Sentinel
rounds were then run fresh. Failed raw files are excluded from accepted totals.

An after async limited 262,144-byte/concurrency-8 round had 966 successes and
34 errors: one settle unknown at 2.019563687 seconds, one publish unknown at
2.007222953 seconds, and 32 event-ID mismatches. All mismatches were worker 5
sequences 93–124 immediately after its unknown publish at sequence 92. This
supports a possible stream-cursor cascade, but actual returned IDs and post-error
Redis state were not captured: applied XADD is not established. The errors are
not ResourceLimit. The invocation had 38.55% host I/O-wait ticks and load
14.06→14.69 with external compilers visible at both endpoints. That snapshot
covers fixture startup/cleanup as well as business work; host contention is a
candidate cause, not an established root cause. One entire fresh-fixture repeat
used the same timeouts and configuration and completed 1,000 successes, zero
errors/unknowns. No individual uncertain publication was replayed. Both raw
rounds and their diagnosis are retained.

Other projects compiled on this shared host (separate Redis/task boundary
refactors, model metadata, reflection, and model documentation fixtures). No
external process was changed or terminated. Every invocation records UTC,
load, CPU ticks, and observed compiler processes at its endpoints; endpoint
snapshots can miss jobs inside a run. Early runs used an unbounded compiler
guard; the resumed runner imposed at most 30 seconds per case and five minutes
aggregate waiting, then continued with disturbance flags. An authorized
167.807298-second fixture-free pause for focused test checks consumed wall-clock
guard budget; the conservative active compilation-wait upper bound at resume
was 224.831843 seconds. The pause is outside all business timers. Its UTC
pause/resume events and raw guard accounting remain available. Background load
can affect throughput and tails, so the tables are observed distributions, not
fixed cross-machine performance thresholds.

### Finite follow-up and phase wall-time decomposition

The single follow-up guard expired after 120.032572 seconds without obtaining
an observed quiet window. All 16 follow-up invocations had compiler processes
visible at both endpoints. Eight fresh standalone workload rounds (8,000
successful events, zero errors/unknowns) rechecked sync concurrency 1: three
paired 4,096-byte rounds and one paired 262,144-byte round. Their ratios are
below. The primary sync 4,096-byte ratios were 0.739/0.838/0.798 (median 0.798);
follow-up ratios are 1.026/0.394/0.727 (median 0.727). The large-payload
follow-up ratio is 0.666. These observations retain an apparent performance
concern, but their variability and continuing shared load prevent attributing
a stable regression to the implementation.

A separate temporary driver uses the same public provider SPI against the
actual before/after paths, prebuilds 1,000 messages, and times publish, receive
(3-second receive budget), and Accept separately with `Instant`. Each error stops
the diagnostic without replay or filling a quota. All eight phase invocations
completed 1,000 successful events, zero errors/unknowns (8,000 total). The
before/after phase main source is identical, and lock review found no package
version/source differences. This diagnostic runs on the main thread without
INFO observers or the worker barrier; CSV writes occur between events, outside
each phase timer. Its absolute stage sums must not be substituted for the
primary benchmark's worker-region throughput. These are wall-time measurements
including network, scheduling, and execution, not CPU flamegraphs.

One before async 262,144-byte diagnostic stalled in Docker fixture startup for
over three minutes, before producing business samples. Other independent
Docker starts also waited. Only its confirmed owned Docker CLI received
SIGTERM at 11:56:35.758088 UTC. The pending start completed around that boundary,
and the original Rust process continued through all 1,000 events with no error
or retry. The after diagnostic also completed normally. No evidence shows the
signal repaired Docker. The fixture setup is excluded from stage timing; its
invocation-wide environment observations include that delay. The unique owned
mount had no remaining container at final inspection, and all own processes
exited before the computational window was released.

The publish mean rose in all four phase pairs; receive and settle changed in
both directions. Sync 4,096-byte receive, for example, fell from 294.3 to 254.4
µs, while sync 262,144-byte receive rose from 9,421.8 to 13,194.9 µs. These
measurements do not establish a particular serializer/parser as the cause.
Static candidates for a future controlled CPU profile are bounded writer
checks/capacity growth and the version probe, structural-depth check, and typed
wire decode traversals. Any proposed optimization must preserve version/error
precedence, ignored-field depth and duplicate-selector checks, inclusive size
budgets, and rejection before Redis I/O. No such production optimization was
implemented or claimed faster from this noisy experiment. Stable attribution
remains unresolved without a controlled host measurement.

| Payload bytes | Round | Before success/s | After success/s | Ratio | Before load start/end | After load start/end |
| ---: | ---: | ---: | ---: | ---: | --- | --- |
| 4096 | 1 | 1317.7 | 1352.2 | 1.026 | 16.31/19.80 | 19.80/20.15 |
| 4096 | 2 | 1650.7 | 650.2 | 0.394 | 20.15/20.79 | 20.79/18.85 |
| 4096 | 3 | 1037.8 | 754.9 | 0.727 | 18.85/18.85 | 18.85/19.26 |
| 262144 | 1 | 51.5 | 34.3 | 0.666 | 19.26/17.47 | 17.47/16.40 |

Sync 4,096-byte follow-up ratio median: 0.727.

| Mode | Bytes | Revision | Phase | Mean µs | p50 µs | p95 µs | p99 µs | Load start/end |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | --- |
| sync | 4096 | before | publish | 142.3 | 98.9 | 225.1 | 1603.3 | 16.40/16.05 |
| sync | 4096 | before | receive | 294.3 | 240.9 | 374.2 | 1570.8 | 16.40/16.05 |
| sync | 4096 | before | settle | 101.4 | 38.8 | 229.6 | 2209.3 | 16.40/16.05 |
| sync | 4096 | after | publish | 178.2 | 141.4 | 182.9 | 1871.2 | 16.05/15.65 |
| sync | 4096 | after | receive | 254.4 | 236.4 | 269.1 | 403.2 | 16.05/15.65 |
| sync | 4096 | after | settle | 83.9 | 38.0 | 55.8 | 2350.4 | 16.05/15.65 |
| sync | 262144 | before | publish | 5120.3 | 3213.2 | 9518.1 | 40827.8 | 15.65/16.12 |
| sync | 262144 | before | receive | 9421.8 | 8151.2 | 16115.5 | 23857.4 | 15.65/16.12 |
| sync | 262144 | before | settle | 1410.8 | 140.2 | 2447.5 | 27035.1 | 15.65/16.12 |
| sync | 262144 | after | publish | 10924.2 | 10078.2 | 19218.2 | 41321.4 | 16.12/15.94 |
| sync | 262144 | after | receive | 13194.9 | 12931.2 | 22997.0 | 31784.2 | 16.12/15.94 |
| sync | 262144 | after | settle | 2543.6 | 156.0 | 5240.7 | 42711.6 | 16.12/15.94 |
| async | 4096 | before | publish | 273.8 | 141.5 | 703.9 | 2213.1 | 15.94/16.18 |
| async | 4096 | before | receive | 709.0 | 340.1 | 1896.3 | 3372.4 | 15.94/16.18 |
| async | 4096 | before | settle | 203.8 | 70.8 | 654.5 | 1865.5 | 15.94/16.18 |
| async | 4096 | after | publish | 413.0 | 191.4 | 1480.5 | 2986.9 | 16.18/18.40 |
| async | 4096 | after | receive | 888.2 | 328.6 | 2813.0 | 6487.0 | 16.18/18.40 |
| async | 4096 | after | settle | 269.3 | 70.7 | 1031.8 | 2747.5 | 16.18/18.40 |
| async | 262144 | before | publish | 12481.8 | 5647.7 | 15226.6 | 118307.7 | 18.40/21.12 |
| async | 262144 | before | receive | 19185.7 | 16101.8 | 33567.7 | 51349.4 | 18.40/21.12 |
| async | 262144 | before | settle | 9194.9 | 560.3 | 8176.8 | 303931.6 | 18.40/21.12 |
| async | 262144 | after | publish | 16677.0 | 6739.6 | 23388.6 | 102362.8 | 21.12/19.80 |
| async | 262144 | after | receive | 13074.5 | 7663.3 | 17375.9 | 27572.1 | 21.12/19.80 |
| async | 262144 | after | settle | 3204.2 | 204.7 | 2745.9 | 23035.8 | 21.12/19.80 |



### Complete three-round measurements

Each slash-separated value is round 1/2/3, not a pooled percentile. Bytes are
raw payload lengths; c is worker/receiver concurrency. Success/s counts accepted
round trips, timed-out idle polls, or probe completions as appropriate.
`active_after` is an endpoint snapshot, `Sampled peak` is the observed maximum
(lower bound), and errors/unknowns are separately classified counts. The
Sentinel recovery throughput column divides its one success by full failover
elapsed time; its p50/p95/p99 columns describe the single successful receive/
Accept attempt. Rejected-attempt latency has a separate table. A dash means no
samples in that percentile population; raw CSV stores zero for that empty
population. All connection and command columns include setup deltas as defined
above.


### round_trip

| Mode | Limits | Bytes | c | Revision | Success/s r1/r2/r3 | p50 µs | p95 µs | p99 µs | New connections | Active after | Sampled peak | XAUTOCLAIM | XREADGROUP | Errors; unknown |
| --- | --- | ---: | ---: | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| sync | default | 64 | 1 | before | 5498.7/6173.5/5870.6 | 166.5/149.8/153.5 | 304.2/259.5/268.0 | 410.7/356.0/377.5 | 2/2/2 | 4/4/4 | 4/4/4 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | default | 64 | 1 | after | 5756.9/5782.6/4939.8 | 156.6/155.7/184.4 | 273.2/283.9/301.0 | 393.5/368.8/801.0 | 2/2/2 | 4/4/4 | 4/4/4 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | default | 64 | 8 | before | 16183.5/11023.3/9131.2 | 467.3/516.5/459.8 | 646.5/1376.0/2271.4 | 737.9/4274.5/5066.5 | 16/16/16 | 18/18/18 | 18/18/18 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | default | 64 | 8 | after | 10827.6/13046.6/15887.7 | 527.5/524.0/495.7 | 1607.8/1163.6/637.8 | 3122.8/1412.3/718.4 | 16/16/16 | 18/18/18 | 18/18/18 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | default | 64 | 32 | before | 13651.2/16187.9/14099.6 | 1783.1/1698.8/1824.4 | 4481.5/2460.3/3111.0 | 11339.7/8680.0/12711.9 | 130/114/137 | 42/42/42 | 59/56/57 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | default | 64 | 32 | after | 15842.4/16331.6/15245.3 | 1773.0/1723.2/1804.2 | 2709.2/2382.5/2611.1 | 7558.8/7508.4/9462.0 | 125/116/128 | 42/42/42 | 59/62/63 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | default | 4096 | 1 | before | 3009.0/2887.1/2933.5 | 306.7/316.7/312.9 | 459.3/497.1/479.2 | 561.4/612.2/568.2 | 2/2/2 | 4/4/4 | 4/4/4 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | default | 4096 | 1 | after | 2224.8/2419.7/2340.0 | 413.6/382.9/395.3 | 608.1/588.9/586.0 | 883.3/709.1/767.9 | 2/2/2 | 4/4/4 | 4/4/4 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | default | 4096 | 8 | before | 9162.1/7369.8/10770.3 | 739.7/738.7/711.5 | 1646.3/3555.8/964.0 | 3231.5/5646.1/1324.4 | 16/16/16 | 18/18/18 | 18/18/18 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | default | 4096 | 8 | after | 9319.9/9795.4/7783.8 | 822.5/769.7/732.2 | 1192.2/1123.2/2639.6 | 1395.7/1424.0/5256.6 | 16/16/16 | 18/18/18 | 18/18/18 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | default | 4096 | 32 | before | 9528.3/9571.0/9870.7 | 2775.7/2772.6/2837.4 | 5163.3/7187.2/4758.3 | 16191.2/13922.8/10464.6 | 150/127/160 | 42/42/42 | 58/58/61 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | default | 4096 | 32 | after | 9064.1/7572.7/9921.1 | 2849.1/2833.1/2684.9 | 4694.0/12890.6/5080.5 | 19263.7/23402.8/13932.0 | 196/188/186 | 42/42/42 | 65/58/61 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | default | 262144 | 1 | before | 95.7/59.8/63.0 | 9829.0/12579.7/11588.1 | 12121.3/31944.3/34869.9 | 25819.4/49922.1/58735.3 | 2/2/2 | 4/4/4 | 4/4/4 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | default | 262144 | 1 | after | 75.5/36.7/18.4 | 12658.0/24991.0/44997.2 | 14504.2/47866.1/115364.6 | 30750.2/87950.6/173102.1 | 2/2/2 | 4/4/4 | 4/4/4 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | default | 262144 | 8 | before | 91.9/98.0/92.1 | 73590.3/60005.6/56142.9 | 162248.1/136611.1/167544.0 | 247981.9/167512.3/608512.7 | 16/16/16 | 18/18/18 | 18/18/18 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | default | 262144 | 8 | after | 56.5/197.0/227.5 | 109264.7/24889.3/27136.8 | 371974.4/100497.6/61155.7 | 553222.1/135728.6/253186.7 | 16/16/16 | 18/18/18 | 18/18/18 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | default | 262144 | 32 | before | 207.7/215.6/89.5 | 125948.9/125563.8/194120.2 | 315577.7/275987.9/1138516.8 | 452783.6/340687.9/1482576.0 | 326/297/322 | 42/42/42 | 60/63/62 | 1000/1000/1004 | 2000/2000/2004 | 0/0/0;0/0/0 |
| sync | default | 262144 | 32 | after | 221.6/274.3/107.0 | 102989.5/99950.2/186903.9 | 216451.4/198770.5/1054421.5 | 1067223.0/240947.7/1365386.8 | 400/386/557 | 42/42/42 | 61/62/66 | 1000/1000/1004 | 2000/2000/2004 | 0/0/0;0/0/0 |
| sync | limited | 64 | 1 | after | 6073.4/5999.9/5909.5 | 152.6/153.1/154.6 | 258.7/264.2/274.2 | 341.1/378.4/378.8 | 2/2/2 | 4/4/4 | 4/4/4 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | limited | 64 | 8 | after | 10822.0/9888.1/10212.2 | 699.7/735.2/760.0 | 1008.8/1311.2/1078.5 | 1434.9/1620.8/1316.6 | 768/764/712 | 11/11/11 | 17/17/16 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | limited | 64 | 32 | after | 7701.0/7444.1/6485.9 | 2887.4/3479.7/3153.3 | 8860.7/7178.2/7049.4 | 30772.5/9962.7/13992.9 | 1001/788/807 | 35/35/35 | 58/57/59 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | limited | 4096 | 1 | after | 1911.8/1980.6/1571.1 | 414.2/411.6/425.3 | 820.1/665.3/1973.6 | 3053.1/3083.1/3428.6 | 2/2/2 | 4/4/4 | 4/4/4 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | limited | 4096 | 8 | after | 2415.1/2448.9/4084.2 | 2406.2/2201.1/1559.8 | 6419.8/6997.0/3639.8 | 8992.8/8675.0/5387.2 | 748/750/837 | 11/11/11 | 18/18/18 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | limited | 4096 | 32 | after | 4937.1/4526.9/4758.6 | 5327.1/4351.3/4792.1 | 12935.8/15737.2/11697.5 | 17634.3/33624.9/17823.8 | 962/953/937 | 35/35/35 | 64/56/58 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | limited | 262144 | 1 | after | 29.5/40.6/57.7 | 28525.5/19543.3/14412.1 | 63260.1/50603.1/28799.8 | 88375.9/84391.4/59257.5 | 2/2/2 | 4/4/4 | 4/4/4 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | limited | 262144 | 8 | after | 134.6/134.4/175.5 | 36580.2/38666.6/27008.9 | 123438.1/113697.0/94724.9 | 253228.1/733269.4/305471.4 | 896/1009/929 | 11/11/11 | 18/18/18 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | limited | 262144 | 32 | after | 292.5/232.3/177.4 | 101031.8/98674.4/137800.6 | 166474.9/222705.2/369078.8 | 194275.9/1023162.4/710492.7 | 1125/1146/1188 | 35/35/35 | 61/63/58 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | default | 64 | 1 | before | 1566.6/1723.7/1411.8 | 293.7/270.1/269.4 | 2628.8/2328.1/2553.4 | 5711.0/6717.5/5249.8 | 2/2/2 | 4/4/4 | 4/4/4 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | default | 64 | 1 | after | 440.9/546.8/4358.0 | 995.3/549.7/215.4 | 6982.5/6926.6/349.4 | 9988.5/10055.2/495.5 | 2/2/2 | 4/4/4 | 4/4/4 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | default | 64 | 8 | before | 12262.1/14138.8/10568.1 | 459.5/461.1/622.3 | 1451.7/910.1/1277.4 | 2944.0/1663.0/1856.5 | 9/9/9 | 11/11/11 | 11/11/11 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | default | 64 | 8 | after | 12516.8/15246.9/5952.9 | 533.1/483.6/691.7 | 1183.2/748.9/3995.6 | 1367.4/916.7/7006.1 | 9/9/9 | 11/11/11 | 11/11/11 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | default | 64 | 32 | before | 3106.5/2981.9/2578.9 | 9456.3/9404.1/9720.9 | 15396.8/15483.3/20620.7 | 24753.9/20034.0/64605.1 | 33/33/33 | 35/35/35 | 35/35/35 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | default | 64 | 32 | after | 3315.5/2633.8/3655.3 | 8531.7/9781.3/7038.7 | 16583.3/23171.7/17219.6 | 20306.7/36851.1/29580.4 | 33/33/33 | 35/35/35 | 35/35/35 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | default | 4096 | 1 | before | 1587.5/298.4/505.9 | 456.1/2853.2/1590.9 | 1681.5/7872.8/5102.1 | 4275.7/14489.4/7440.2 | 2/2/2 | 4/4/4 | 4/4/4 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | default | 4096 | 1 | after | 738.0/1141.5/759.3 | 544.4/528.4/704.5 | 3952.1/2886.9/3549.0 | 8226.4/4370.1/5773.3 | 2/2/2 | 4/4/4 | 4/4/4 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | default | 4096 | 8 | before | 1793.3/2533.5/1559.8 | 3924.6/2580.3/3924.7 | 8710.9/6111.3/10922.5 | 10445.9/8812.4/16631.6 | 9/9/9 | 11/11/11 | 11/11/11 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | default | 4096 | 8 | after | 1992.5/2493.8/1631.3 | 3228.8/2593.9/3953.4 | 7438.4/6551.2/10735.4 | 9733.4/9321.8/19348.9 | 9/9/9 | 11/11/11 | 11/11/11 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | default | 4096 | 32 | before | 1921.6/1765.3/2246.8 | 13551.2/15239.9/11213.2 | 28565.9/29971.0/27800.0 | 38832.2/40126.8/33693.8 | 33/33/33 | 35/35/35 | 35/35/35 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | default | 4096 | 32 | after | 2222.2/1891.3/1631.5 | 11801.7/15222.9/12178.4 | 27445.6/23996.7/41349.5 | 33205.6/27518.1/48920.8 | 33/33/33 | 35/35/35 | 35/35/35 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | default | 262144 | 1 | before | 35.7/79.9/44.8 | 23159.8/10334.3/17500.0 | 56433.5/16452.6/40968.5 | 78398.1/49174.5/88363.4 | 2/2/2 | 4/4/4 | 4/4/4 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | default | 262144 | 1 | after | 62.2/26.4/26.2 | 14266.1/26624.9/29965.6 | 22677.6/61113.3/76935.7 | 53215.8/417839.7/152270.9 | 2/2/2 | 4/4/4 | 4/4/4 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | default | 262144 | 8 | before | 68.1/91.4/80.3 | 88288.7/73725.1/85049.1 | 205147.5/163414.4/155506.3 | 740732.6/283096.7/329134.6 | 9/9/9 | 11/11/11 | 11/11/11 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | default | 262144 | 8 | after | 64.2/98.0/85.7 | 107355.9/64037.3/82937.9 | 207837.8/151741.8/160079.7 | 281989.4/320064.2/247246.2 | 9/9/9 | 11/11/11 | 11/11/11 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | default | 262144 | 32 | before | 76.8/92.2/103.0 | 289093.3/218785.0/198925.7 | 1397751.8/1063556.1/992048.7 | 1956343.4/1467855.6/1060551.9 | 33/33/33 | 35/35/35 | 35/35/35 | 1025/1011/1000 | 2025/2011/2000 | 0/0/0;0/0/0 |
| async | default | 262144 | 32 | after | 106.3/94.0/98.6 | 178271.0/228662.1/233783.4 | 1112559.2/1051286.8/1007143.9 | 1176735.7/1702438.3/1613007.6 | 33/33/33 | 35/35/35 | 35/35/35 | 1011/1000/1002 | 2011/2000/2002 | 0/0/0;0/0/0 |
| async | limited | 64 | 1 | after | 645.3/843.6/446.1 | 1243.9/480.4/2027.5 | 4124.2/4059.0/5009.1 | 6122.8/8764.5/6997.8 | 2/2/2 | 4/4/4 | 4/4/4 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | limited | 64 | 8 | after | 4148.1/10682.6/5575.8 | 1367.5/613.6/990.6 | 3329.1/1289.4/3743.2 | 6391.4/3113.4/7629.9 | 9/9/9 | 11/11/11 | 11/11/11 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | limited | 64 | 32 | after | 5910.1/3785.7/5582.8 | 4800.5/4912.1/5715.0 | 9328.0/20991.6/10692.3 | 12794.8/27194.0/14239.6 | 33/33/33 | 35/35/35 | 35/35/35 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | limited | 4096 | 1 | after | 1781.6/1663.4/1460.7 | 471.2/471.6/471.9 | 803.6/870.0/1745.7 | 2941.6/3694.7/4143.6 | 2/2/2 | 4/4/4 | 4/4/4 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | limited | 4096 | 8 | after | 5753.9/7109.8/8274.6 | 866.1/810.7/855.4 | 5229.6/3784.2/1686.3 | 7985.4/4557.6/1917.1 | 9/9/9 | 11/11/11 | 11/11/11 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | limited | 4096 | 32 | after | 8718.7/8480.4/7004.8 | 2330.1/2506.1/2731.9 | 9672.8/7984.8/14327.0 | 20185.4/22104.7/26636.0 | 33/33/33 | 35/35/35 | 35/35/35 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | limited | 262144 | 1 | after | 65.7/50.8/41.3 | 12853.3/13622.0/14399.4 | 17979.3/33386.4/55342.5 | 54559.6/82916.0/140961.8 | 2/2/2 | 4/4/4 | 4/4/4 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | limited | 262144 | 8 | after | 55.8/72.1/83.9 | 130988.0/97559.7/85490.5 | 247570.0/203620.0/153756.4 | 297654.3/337434.5/277973.2 | 9/9/9 | 11/11/11 | 11/11/11 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | limited | 262144 | 32 | after | 85.7/120.3/128.8 | 333288.2/254016.2/236060.5 | 632763.2/361426.4/372571.6 | 847559.1/443645.4/417371.6 | 33/33/33 | 35/35/35 | 35/35/35 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |

### idle

| Mode | Limits | Bytes | c | Revision | Success/s r1/r2/r3 | p50 µs | p95 µs | p99 µs | New connections | Active after | Sampled peak | XAUTOCLAIM | XREADGROUP | Errors; unknown |
| --- | --- | ---: | ---: | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| sync | default | 0 | 10 | before | 2070.6/1807.7/2752.0 | 5169.4/5631.9/2826.3 | 7970.6/8498.6/6606.0 | 11865.7/12625.9/7687.1 | 11/11/11 | 13/13/13 | 13/13/13 | 999/1000/997 | 1867/1933/1737 | 0/0/0;0/0/0 |
| sync | default | 0 | 10 | after | 2417.2/2522.2/1970.5 | 3090.9/3070.2/5453.8 | 7044.1/6713.7/7014.3 | 8495.1/8228.8/9508.9 | 11/11/11 | 13/13/13 | 13/13/13 | 1000/1000/1000 | 1713/1858/1918 | 0/0/0;0/0/0 |
| sync | default | 0 | 100 | before | 12134.1/16467.2/11010.6 | 1768.6/2790.2/2301.8 | 7714.0/10916.4/12736.8 | 22761.2/18612.3/30424.3 | 101/101/101 | 103/103/103 | 103/103/103 | 920/911/928 | 767/58/844 | 0/0/0;0/0/0 |
| sync | default | 0 | 100 | after | 17450.7/12660.1/11494.8 | 1995.2/2036.3/1880.1 | 4374.5/5940.4/6235.2 | 6144.4/29578.3/18022.0 | 167/238/251 | 98/100/88 | 103/104/103 | 865/737/685 | 817/969/537 | 71/140/165;0/0/0 |
| async | default | 0 | 10 | before | 1867.4/1897.7/2060.3 | 5458.0/5437.6/5415.7 | 5677.8/5637.9/6636.9 | 6810.8/6966.9/8053.4 | 11/11/11 | 13/13/13 | 13/13/13 | 1000/1000/1000 | 1995/1986/1683 | 0/0/0;0/0/0 |
| async | default | 0 | 10 | after | 2530.5/2137.9/2523.3 | 2974.9/5400.3/3355.3 | 6848.3/6836.4/7233.8 | 9102.1/8205.8/10583.1 | 11/11/11 | 13/13/13 | 13/13/13 | 1000/1000/1000 | 1517/1829/1531 | 0/0/0;0/0/0 |
| async | default | 0 | 100 | before | 18102.3/12529.1/12257.7 | 3574.8/4601.7/7253.6 | 7955.9/9167.0/12982.8 | 10223.4/10137.1/15877.5 | 101/101/101 | 103/103/103 | 103/103/103 | 936/919/900 | 63/70/16 | 0/0/0;0/0/0 |
| async | default | 0 | 100 | after | 15745.0/12683.8/21989.4 | 2916.5/3477.9/2587.5 | 12922.1/11321.4/6642.5 | 14261.1/20685.9/7854.1 | 283/212/280 | 100/103/98 | 184/120/117 | 637/779/639 | 44/150/82 | 185/111/184;0/0/0 |

### sentinel_publish

| Mode | Limits | Bytes | c | Revision | Success/s r1/r2/r3 | p50 µs | p95 µs | p99 µs | New connections | Active after | Sampled peak | XAUTOCLAIM | XREADGROUP | Errors; unknown |
| --- | --- | ---: | ---: | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| sync | default | 64 | 1 | before | 564.7/1033.5/463.4 | 1357.3/877.6/1308.1 | 3819.0/1646.3/5712.7 | 4821.2/3199.0/14232.1 | 4004/4004/4004 | 9/9/9 | 10/10/10 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | default | 64 | 1 | after | 1440.1/642.9/723.0 | 636.7/1189.8/995.4 | 993.0/3477.5/3482.5 | 1509.9/4577.6/5257.9 | 2002/2002/2002 | 9/9/9 | 10/10/10 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | default | 64 | 1 | before | 361.1/572.9/1104.1 | 1725.7/1255.8/820.1 | 8675.3/5136.6/1315.1 | 13486.5/7717.1/1703.9 | 4004/4004/4004 | 9/9/9 | 11/11/10 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | default | 64 | 1 | after | 690.4/206.5/1130.3 | 1295.6/3040.1/770.7 | 2435.1/15002.0/1175.8 | 4129.1/20658.2/2347.5 | 2002/2002/2002 | 9/9/9 | 10/10/10 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |

### sentinel_recovery

| Mode | Limits | Bytes | c | Revision | Success/s r1/r2/r3 | p50 µs | p95 µs | p99 µs | New connections | Active after | Sampled peak | XAUTOCLAIM | XREADGROUP | Errors; unknown |
| --- | --- | ---: | ---: | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| sync | default | 64 | 1 | before | 0.4/0.4/0.4 | 929.6/770.2/752.2 | 929.6/770.2/752.2 | 929.6/770.2/752.2 | 13/12/13 | 3/3/3 | 9/9/9 | 1/1/1 | 2/2/2 | 0/0/0;0/0/0 |
| sync | default | 64 | 1 | after | 0.4/0.4/0.4 | 1103.4/1096.4/1528.5 | 1103.4/1096.4/1528.5 | 1103.4/1096.4/1528.5 | 10/10/10 | 3/3/3 | 9/9/9 | 1/1/1 | 2/2/2 | 1/1/1;1/1/1 |
| async | default | 64 | 1 | before | 0.4/0.4/0.4 | 7512.7/9397.9/7876.8 | 7512.7/9397.9/7876.8 | 7512.7/9397.9/7876.8 | 13/13/13 | 4/4/4 | 9/9/9 | 1/1/1 | 2/2/2 | 0/0/0;0/0/0 |
| async | default | 64 | 1 | after | 0.4/0.4/0.4 | 1934.9/4576.7/1450.8 | 1934.9/4576.7/1450.8 | 1934.9/4576.7/1450.8 | 10/10/10 | 3/4/4 | 9/9/9 | 1/1/1 | 2/2/2 | 1/1/1;1/1/1 |

### sentinel_post_recovery

| Mode | Limits | Bytes | c | Revision | Success/s r1/r2/r3 | p50 µs | p95 µs | p99 µs | New connections | Active after | Sampled peak | XAUTOCLAIM | XREADGROUP | Errors; unknown |
| --- | --- | ---: | ---: | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| sync | default | 64 | 1 | before | 772.7/1042.5/1410.9 | 893.2/740.2/631.9 | 2973.1/2450.4/982.4 | 6544.2/4173.5/1884.0 | 4010/4010/4004 | 10/10/4 | 11/11/5 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| sync | default | 64 | 1 | after | 714.5/786.8/374.3 | 879.0/843.7/1938.0 | 4133.2/3503.4/7138.0 | 6554.1/7396.0/10329.7 | 2008/2008/2008 | 10/10/10 | 11/11/11 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | default | 64 | 1 | before | 238.3/189.8/883.1 | 1501.6/2819.7/811.0 | 14006.4/18481.4/2874.0 | 20367.5/26196.7/3604.0 | 4010/4010/4010 | 10/11/10 | 12/12/11 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |
| async | default | 64 | 1 | after | 752.0/572.2/1280.7 | 974.6/1325.2/728.1 | 3204.4/3996.2/996.4 | 6007.3/6656.4/1614.0 | 2008/2008/2006 | 10/10/8 | 11/11/9 | 1000/1000/1000 | 2000/2000/2000 | 0/0/0;0/0/0 |

### command_limit

| Mode | Limits | Bytes | c | Revision | Success/s r1/r2/r3 | p50 µs | p95 µs | p99 µs | New connections | Active after | Sampled peak | XAUTOCLAIM | XREADGROUP | Errors; unknown |
| --- | --- | ---: | ---: | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| sync | command_1 | 64 | 32 | after | 1933.0/577.2/1848.5 | 210.9/404.6/167.2 | 2682.8/412.1/4328.5 | 2934.6/412.1/4328.5 | 33/33/33 | 34/31/31 | 2/31/31 | 41/4/17 | 82/4/34 | 962/998/983;0/0/0 |
| async | command_1 | 64 | 32 | after | 1322.7/1121.0/1568.0 | 282.9/283.5/258.1 | 3498.9/2179.0/451.0 | 7419.9/2179.0/3942.7 | 33/34/33 | 35/26/29 | 35/35/31 | 31/9/21 | 62/18/42 | 969/991/979;0/0/0 |

### receiver_limit

| Mode | Limits | Bytes | c | Revision | Success/s r1/r2/r3 | p50 µs | p95 µs | p99 µs | New connections | Active after | Sampled peak | XAUTOCLAIM | XREADGROUP | Errors; unknown |
| --- | --- | ---: | ---: | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| sync | receiver_1 | 0 | 1 | after | 0.0/0.0/0.0 | —/—/— | —/—/— | —/—/— | 0/0/0 | 4/4/4 | 4/4/4 | 0/0/0 | 0/0/0 | 1000/1000/1000;0/0/0 |
| async | receiver_1 | 0 | 1 | after | 0.0/0.0/0.0 | —/—/— | —/—/— | —/—/— | 0/0/0 | 4/4/4 | 4/4/4 | 0/0/0 | 0/0/0 | 1000/1000/1000;0/0/0 |

### Paired default throughput ratios (after / before)

| Mode | Bytes | c | Ratio r1/r2/r3 | Median |
| --- | ---: | ---: | --- | ---: |
| sync | 64 | 1 | 1.047/0.937/0.841 | 0.937 |
| sync | 64 | 8 | 0.669/1.184/1.740 | 1.184 |
| sync | 64 | 32 | 1.161/1.009/1.081 | 1.081 |
| sync | 4096 | 1 | 0.739/0.838/0.798 | 0.798 |
| sync | 4096 | 8 | 1.017/1.329/0.723 | 1.017 |
| sync | 4096 | 32 | 0.951/0.791/1.005 | 0.951 |
| sync | 262144 | 1 | 0.790/0.615/0.293 | 0.615 |
| sync | 262144 | 8 | 0.615/2.011/2.472 | 2.011 |
| sync | 262144 | 32 | 1.067/1.272/1.196 | 1.196 |
| async | 64 | 1 | 0.281/0.317/3.087 | 0.317 |
| async | 64 | 8 | 1.021/1.078/0.563 | 1.021 |
| async | 64 | 32 | 1.067/0.883/1.417 | 1.067 |
| async | 4096 | 1 | 0.465/3.825/1.501 | 1.501 |
| async | 4096 | 8 | 1.111/0.984/1.046 | 1.046 |
| async | 4096 | 32 | 1.156/1.071/0.726 | 1.071 |
| async | 262144 | 1 | 1.741/0.330/0.585 | 0.585 |
| async | 262144 | 8 | 0.943/1.073/1.067 | 1.067 |
| async | 262144 | 32 | 1.385/1.020/0.958 | 1.020 |

### Full failover elapsed seconds

| Mode | Revision | r1/r2/r3 seconds |
| --- | --- | --- |
| sync | before | 2.431/2.325/2.407 |
| sync | after | 2.354/2.481/2.474 |
| async | before | 2.564/2.638/2.401 |
| async | after | 2.464/2.687/2.449 |

### Fail-fast rejected-attempt latency distributions (µs)

| Scenario | Mode | Successes r1/r2/r3 | Errors | Failed p50 µs | Failed p95 µs | Failed p99 µs |
| --- | --- | --- | --- | --- | --- | --- |
| command_limit | sync | 38/2/17 | 962/998/983 | 2.5/4.9/6.1 | 3.8/310.9/241.6 | 7.5/673.2/854.3 |
| command_limit | async | 31/9/21 | 969/991/979 | 2.9/8.3/6.2 | 35.6/164.1/197.8 | 944.2/443.4/604.3 |
| receiver_limit | sync | 0/0/0 | 1000/1000/1000 | 0.9/0.9/0.9 | 0.9/1.0/1.0 | 1.3/1.5/1.4 |
| receiver_limit | async | 0/0/0 | 1000/1000/1000 | 1.1/1.1/1.1 | 1.3/1.4/1.3 | 1.7/2.1/1.8 |

Raw CSV, logs, per-run environment observations, strict validation, source and
executable hashes are retained at
`/tmp/superpowers-redis-refactor-jh52708m/benchmark-data`. The full tables derive
from `before-summary-all.csv` and `after-summary-all.csv`; every attempt is in
`before-samples-all.csv` and `after-samples-all.csv`. `accepted-labels.json`
identifies the accepted runs. `validator.log`, `results-tables.md`,
`invalid-business-round-diagnosis.json`, and the source manifests provide audit
evidence. Failed/partial files remain alongside the accepted aggregate files.
The run and validation drivers are retained in the parent temporary directory.

## Historical single-run experiment

The following measurements are preserved as history. They are not the baseline
for the current matrix.

### Setup

The same ignored integration benchmark ran against two revisions using Docker's
`redis:7-alpine` image, ten synchronous idle subscriptions, and 1,000 sequential
publish/receive/accept round trips. Each run used a fresh Redis container and
`REDIS_BENCH_IDLE_SECONDS=30`.

The historical invocation was:

```sh
REDIS_BENCH_IDLE_SECONDS=30 cargo test --test connection_reuse_benchmark_tests -- --ignored --nocapture
```

The before run used `b6be1e7b9b203b8391f705ef83c043646e79bfa2`, before the
Redis connection reuse refactor. The after run used
`87e6eb013d5a3065df7cb3c2f44eb2a44b43697f` plus the uncommitted receive
connection reuse changes in this worktree.

### Results

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
