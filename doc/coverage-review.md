# Redis coverage review

## Latest CI coverage report

| Metric | Covered | Total | Result | Required |
| --- | ---: | ---: | --- | ---: |
| Functions | 266 | 292 | 91.10% — fails | at least 95% |
| Lines | 2,614 | 2,790 | 93.69% — passes | above 90% |
| Regions | 4,043 | 4,340 | 93.16% — passes | above 85% |

The test suite exercises both sync and async Redis providers, Sentinel promotion,
strict conformance and durable recovery, connection reuse, malformed records,
Redis 6.2 tombstone recovery, quarantine failures, and settlement/retry behavior.
Additional unit tests validate malformed `XPENDING` shapes and the configured
claim idle threshold.

The remaining function gap is concentrated in sync and async subscription error
mappings and defensive branches. Several mappings depend on errors that cannot
be induced with the current Redis integration harness, including OS entropy
failure and JSON serialization failure for the fixed, string-and-byte-only wire
record. The configured 95% function threshold is still unmet; this report does
not claim that the CI coverage gate passes.
