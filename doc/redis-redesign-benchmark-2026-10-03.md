# Redis Provider Redesign Benchmark (2026-10-03)


| Item | Value |
|---|---|
| Measured provider source | `f50d09da931d3e59a2ab3892adbe59c9b3c43858` |
| Date | 2026-10-03 |
| Redis | 7.4.8, standalone fixture |
| Workload | Public SPI round trip, 1,000 attempts per round; payloads 64 B, 4 KiB, 256 KiB; concurrency 1/8/32; idle receivers 10/100 |
| Repetitions | Three rounds per matrix cell |
| Limited configuration | `redis.max_concurrent_commands=32`, `redis.max_idle_connections=1` |
| Runner | `REDIS_BENCH_LABEL=redesign-final REDIS_BENCH_OUTPUT=/tmp/redis-event-bus-redesign-benchmark-20261003 REDIS_BENCH_SAMPLES=1000 REDIS_BENCH_ROUNDS=3 REDIS_BENCH_SCENARIOS=round_trip,idle cargo bench --locked --bench redis_workloads` |
| Raw evidence | `/tmp/redis-event-bus-redesign-benchmark-20261003/redesign-final-summary.csv`, `redesign-final-samples.csv`, and `redesign-final-redis-server.txt` |

The table reports the median of the three per-round throughput, p95, and p99 values. Counts sum across rounds. `XAUTOCLAIM` and `XREADGROUP` list raw per-round command counts. These measurements are a confirmation of the new source, not a before/after comparison. Results are host-sensitive; the limited c=32 rows include fast admission rejections.

All 860 rejected business attempts were `publish:resource_limit` in limited c=32 workloads; business workloads had zero unknown outcomes. For payloads up to 4 KiB, each receiver issued one `XAUTOCLAIM` per round (`c` commands for concurrency `c`). The 256 KiB workloads took longer and triggered periodic scans, but remained between 15 and 251 `XAUTOCLAIM` calls for 1,000 attempts per round. Idle workloads issued one initial scan per receiver. This confirms the hot path no longer scans the pending list for every message while preserving periodic recovery.

## Round-trip results

| Mode | Limits | Payload (bytes) | Concurrency | Success / errors / unknown (total) | Throughput median (msg/s) | p95 median (µs) | p99 median (µs) | XAUTOCLAIM (r1/r2/r3) | XREADGROUP (r1/r2/r3) |
|---|---|---:|---:|---:|---:|---:|---:|---|---|
| async | default | 64 | 1 | 3000 / 0 / 0 | 5506.9 | 323.2 | 447.1 | 1/1/1 | 1001/1001/1001 |
| async | default | 64 | 8 | 3000 / 0 / 0 | 28583.9 | 394.1 | 473.0 | 8/8/8 | 1008/1008/1008 |
| async | default | 64 | 32 | 3000 / 0 / 0 | 38315.7 | 1214.2 | 1574.0 | 32/32/32 | 1032/1032/1032 |
| async | default | 4096 | 1 | 3000 / 0 / 0 | 2321.0 | 605.4 | 716.4 | 1/1/1 | 1001/1001/1001 |
| async | default | 4096 | 8 | 3000 / 0 / 0 | 10923.8 | 1038.2 | 1274.8 | 8/8/8 | 1008/1008/1008 |
| async | default | 4096 | 32 | 3000 / 0 / 0 | 14382.6 | 3061.9 | 3881.6 | 32/32/32 | 1032/1032/1032 |
| async | default | 262144 | 1 | 3000 / 0 / 0 | 65.0 | 22741.3 | 39033.2 | 15/15/22 | 1015/1015/1022 |
| async | default | 262144 | 8 | 3000 / 0 / 0 | 52.1 | 274753.0 | 407720.8 | 136/124/139 | 1136/1124/1139 |
| async | default | 262144 | 32 | 3000 / 0 / 0 | 176.3 | 385447.0 | 461614.1 | 217/166/128 | 1217/1166/1128 |
| async | limited | 64 | 1 | 3000 / 0 / 0 | 6180.6 | 265.9 | 379.6 | 1/1/1 | 1001/1001/1001 |
| async | limited | 64 | 8 | 3000 / 0 / 0 | 22675.9 | 589.6 | 916.8 | 8/8/8 | 1008/1008/1008 |
| async | limited | 64 | 32 | 2769 / 231 / 0 | 27055.8 | 1697.4 | 2118.6 | 32/32/31 | 980/924/960 |
| async | limited | 4096 | 1 | 3000 / 0 / 0 | 2244.0 | 623.8 | 777.7 | 1/1/1 | 1001/1001/1001 |
| async | limited | 4096 | 8 | 3000 / 0 / 0 | 7412.1 | 1910.1 | 2701.3 | 8/8/8 | 1008/1008/1008 |
| async | limited | 4096 | 32 | 2987 / 13 / 0 | 10337.3 | 4421.8 | 5501.8 | 32/32/32 | 1032/1032/1019 |
| async | limited | 262144 | 1 | 3000 / 0 / 0 | 61.1 | 32230.4 | 61511.3 | 15/17/35 | 1015/1017/1035 |
| async | limited | 262144 | 8 | 3000 / 0 / 0 | 142.7 | 103629.7 | 137421.9 | 85/56/48 | 1085/1056/1048 |
| async | limited | 262144 | 32 | 2953 / 47 / 0 | 242.1 | 190535.2 | 213809.1 | 128/124/251 | 1128/1086/1242 |
| sync | default | 64 | 1 | 3000 / 0 / 0 | 8927.2 | 184.5 | 247.4 | 1/1/1 | 1001/1001/1001 |
| sync | default | 64 | 8 | 3000 / 0 / 0 | 25102.0 | 431.8 | 567.1 | 8/8/8 | 1008/1008/1008 |
| sync | default | 64 | 32 | 3000 / 0 / 0 | 19045.2 | 2287.7 | 5551.2 | 32/32/32 | 1032/1032/1032 |
| sync | default | 4096 | 1 | 3000 / 0 / 0 | 2899.7 | 455.8 | 553.4 | 1/1/1 | 1001/1001/1001 |
| sync | default | 4096 | 8 | 3000 / 0 / 0 | 9788.9 | 932.3 | 1391.2 | 8/8/8 | 1008/1008/1008 |
| sync | default | 4096 | 32 | 3000 / 0 / 0 | 14372.2 | 3512.3 | 4987.7 | 32/32/32 | 1032/1032/1032 |
| sync | default | 262144 | 1 | 3000 / 0 / 0 | 69.4 | 17880.4 | 35212.1 | 15/14/15 | 1015/1014/1015 |
| sync | default | 262144 | 8 | 3000 / 0 / 0 | 230.1 | 62059.9 | 81026.2 | 40/40/32 | 1040/1040/1032 |
| sync | default | 262144 | 32 | 3000 / 0 / 0 | 333.0 | 147319.1 | 182573.7 | 96/96/96 | 1096/1096/1096 |
| sync | limited | 64 | 1 | 3000 / 0 / 0 | 7712.2 | 186.7 | 307.9 | 1/1/1 | 1001/1001/1001 |
| sync | limited | 64 | 8 | 3000 / 0 / 0 | 13741.1 | 881.0 | 1043.4 | 8/8/8 | 1008/1008/1008 |
| sync | limited | 64 | 32 | 2480 / 520 / 0 | 14852.9 | 2814.5 | 3357.2 | 32/30/30 | 837/881/854 |
| sync | limited | 4096 | 1 | 3000 / 0 / 0 | 3048.6 | 432.3 | 527.0 | 1/1/1 | 1001/1001/1001 |
| sync | limited | 4096 | 8 | 3000 / 0 / 0 | 8776.8 | 1367.3 | 1671.1 | 8/8/8 | 1008/1008/1008 |
| sync | limited | 4096 | 32 | 2959 / 41 / 0 | 7762.8 | 6167.7 | 18037.2 | 32/32/32 | 1019/1010/1026 |
| sync | limited | 262144 | 1 | 3000 / 0 / 0 | 64.4 | 26672.7 | 36959.5 | 15/22/16 | 1015/1022/1016 |
| sync | limited | 262144 | 8 | 3000 / 0 / 0 | 294.4 | 42314.5 | 63798.3 | 40/31/32 | 1040/1031/1032 |
| sync | limited | 262144 | 32 | 2992 / 8 / 0 | 331.2 | 157277.1 | 196087.5 | 96/96/98 | 1096/1095/1091 |

## Idle-receiver results

| Mode | Limits | Payload (bytes) | Concurrency | Success / errors / unknown (total) | Throughput median (msg/s) | p95 median (µs) | p99 median (µs) | XAUTOCLAIM (r1/r2/r3) | XREADGROUP (r1/r2/r3) |
|---|---|---:|---:|---:|---:|---:|---:|---|---|
| async | default | 0 | 10 | 3000 / 0 / 0 | 1625.7 | 8941.7 | 11911.6 | 10/10/10 | 1001/992/997 |
| async | default | 0 | 100 | 3000 / 0 / 0 | 5971.3 | 13321.7 | 15974.7 | 100/100/100 | 913/911/947 |
| sync | default | 0 | 10 | 3000 / 0 / 0 | 1677.6 | 6714.8 | 7166.6 | 10/10/10 | 1010/1010/1010 |
| sync | default | 0 | 100 | 3000 / 0 / 0 | 16330.4 | 7276.4 | 7690.7 | 100/100/100 | 1075/1025/976 |
