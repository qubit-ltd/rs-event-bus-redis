# Redis coverage review

[简体中文](coverage-review.zh_CN.md)

## Verified coverage

The package coverage gate uses the metrics collected by `coverage.sh` and
`ci-check.sh`. The baseline and post-fix measurements are:

| Metric | Baseline | After targeted tests and the recovery fix | Required |
| --- | ---: | ---: | ---: |
| Functions | 266/292 (91.10%) | 289/293 (98.63%) | at least 95% |
| Lines | 2,614/2,790 (93.69%) | 2,706/2,797 (96.75%) | above 90% |
| Regions | 4,043/4,340 (93.16%) | 4,199/4,348 (96.57%) | above 85% |

All three coverage thresholds pass. Both subscription implementations now have
100% function coverage: sync 45/45 and async 54/54. Region counts can vary slightly
between runs because deadline and recovery interval tests exercise timed paths.

Verification on 2026-09-28 used `./align-ci.sh`, `./coverage.sh`, and the full
`./ci-check.sh` with the repository's pinned toolchains and default test
concurrency. All commands succeeded, including feature combinations, doctests,
package verification and dependency security checks.

## Identifying uncovered functions

The detailed LLVM JSON report is `target/infra/coverage/raw.json`. Its function
records include compiler-generated error-mapping closures and separate
compilation instances of the same source function. To find genuinely uncovered
functions, group records by source file and their first region's start/end
coordinates, and retain only groups whose instances all have zero execution
counts. Counting every zero-count instance individually overstates the gap.

This analysis identified 26 initially uncovered source functions/closures:

| Source and behavior | Initially uncovered | Covered by the follow-up |
| --- | ---: | ---: |
| Sync subscription: timeout overflow, invalid claim reply, XPENDING command/parser errors, XRANGE failure, tombstone acknowledgement failure, pending/nonblocking XREADGROUP failures | 8 | 8 |
| Async subscription: the same paths, deferred-claim filtering, blocking XREADGROUP failure, settlement reconnection failure | 11 | 11 |
| Sync/async bus: failed reconnection during XGROUP retry | 2 | 2 |
| Standalone client: poisoned pool lock | 1 | 1 |
| Sync/async bus: JSON serialization and consumer identity generation error mappings | 4 | 0 |

The recovery fix introduces one additional, exercised filtering closure, giving
the final function total of 293.

## Deterministic fault tests

`tests/redis_fault_tests.rs` tests the public SPI through actual Redis client TCP
connections. `tests/support/scripted_redis.rs` binds an ephemeral local socket,
checks the ordered application commands, and returns explicit RESP2 responses
or disconnects before replying. Client setup commands are handled separately.
The fixture verifies that its entire script was consumed, records unexpected
commands, bounds socket I/O, and shuts down and joins all connection workers.

The tests check exact SPI operation, resource, retry policy and sanitized error
category. Raw Redis diagnostics and nested error sources must not escape. After
each recoverable receive fault, a healthy receive must succeed on the same
subscription. Separate scripts cover transport retry with identical group
creation arguments, failed reconnection, and failed settlement followed by a
Retry disposition on the still-unapplied token. Timeout overflow must fail
before issuing recovery commands.

The existing pool-poisoning test originally constructed a client without a
standalone endpoint. That client took the missing-Sentinel path, so the assertion
passed without checking the poisoned pool. It now constructs a standalone client
and asserts the pool-specific error.

Docker-backed Redis 6.2/7 tests remain responsible for real Redis behavior,
durability, Sentinel promotion, quarantine scripts and ownership semantics.
Their read-error regressions also verify recovery after a server restart and
consumer-group reconstruction.

## Recovery defect discovered by the tests

A Redis 7 claim reply can contain a live claimed record together with deleted
pending IDs. The receiver reports a gap first and retains the live record for
the next receive. The sync implementation held a temporary recovery mutex guard
across a chained condition and attempted to acquire the same mutex again when
delivering that retained record. This deadlocked the second receive.

The regression failed with a bounded completion timeout before the fix. The
sync implementation now takes and filters the deferred record under one scoped
guard, then releases the guard before decoding and updating delivery state.
Sync and async regressions both verify that the retained record is delivered
after the gap without another Redis read.

## Remaining four uncovered mappings

The remaining zero-count groups are in `publish` and `subscribe` in both bus
implementations:

- The JSON serialization failure mapping for the fixed `WireFields` record. Its
  strings, integers, optional strings and byte arrays serialize successfully in
  the tested wire format; Redis protocol fault injection cannot induce this
  local serialization error.
- The consumer identity failure mapping for an unavailable operating-system
  entropy source. Normal UUID generation is covered; operating-system entropy
  failure is not induced by these tests.

These mappings remain as defensive error handling. They account for 4/293
functions, while the unchanged 95% function gate passes. No production injection
interface is needed to exercise the Redis transport and protocol failures.
