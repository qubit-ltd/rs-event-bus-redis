# Redis coverage review

[简体中文](coverage-review.zh_CN.md)

## 2026-10-09 implementation checkout

The current task checkout ran the full instrumented test suite successfully, but its coverage gate did not pass: functions were 516/545 (94.68%) against the 95% minimum; lines were 5,039/5,253 (95.93%) and regions 7,747/8,176 (94.75%), both above their gates. The raw LLVM report is `/tmp/redis-provider-improvements-coverage.json` (SHA256 `0ca405be9fec70b8b6381fbab02e9dc7a85d52e6f080e4737255ec89236e84e6`). This is a separate, current implementation capture and does not replace or revise the historical sealed-run evidence below. It also does not establish a passing coverage gate or release readiness.

## Recorded CI evidence (2026-10-09)

The current manifest version is `0.7.0`. The measurements in this section belong to the sealed, uncommitted implementation snapshot identified below; they do not measure the current HEAD. Root-level coverage artifacts from a different capture (including the 489/512 function summary) likewise describe only their own source snapshot and must not be presented as current HEAD coverage.

The full CI run recorded for 2026-10-09 (Asia/Shanghai) exited with status 0. Its clean package coverage measurement passed the then-current gates:

| Metric | Recorded snapshot result | Gate |
| --- | ---: | ---: |
| Functions | 508/534 (95.13%) | at least 95% |
| Lines | 4,865/5,063 (96.09%) | above 90% |
| Regions | 7,566/7,976 (94.86%) | above 85% |

Coverage runs through `.infra/bin/ci-check.sh` → `project-hook` → `project-ci-check.sh` → `.infra/bin/coverage.sh`, selecting `qubit-event-bus-redis` with `--locked --all-features -- --test-threads=1`. The package-level totals include the source files selected by the coverage configuration; inline private tests and helpers in those files participate in LLVM summaries, so these metrics are not coverage of production declarations alone. Their denominator differs from the manual Rustdoc declaration count. The generated `coverage.json` and `ci-summary.json` agree on function, line and region totals. CI cleanup removed the raw profile report after successful collection.

`.infra/bin/coverage.sh` cleans profile data before building the instrumented examples and measuring coverage. The measurement used the uncommitted implementation on branch `codex/redis-provider-improvements-20261009`, based on baseline HEAD `214cca830b6e3c8ff03d28dd3545387e88057f55`. Its reproducible source seal is SHA256 `6b86ea82115554016bc3601eeff59881fdfb9f6665e01761fc5e8ffe5b95fd4b` over 202 sorted relative paths and file contents: Rust files under `src`, `tests`, `examples`, `benches` and `fuzz/fuzz_targets`; root `Cargo.lock`, `fuzz/Cargo.lock` and all fixture `Cargo.lock` files; root `Cargo.toml`; and `.infra/bin/coverage.sh` and `.infra/bin/ci-check.sh`. The local `qubit-event-bus` and `qubit-task` dependencies were pinned to commits `387f16df9a1b380946dd559ad7632c66b8c19fa1` and `87148632e07ce4136ca1db5eef78db3365f2c27a`. The archived manifest contents are unavailable for independent inspection, so the package version used for this historical run is unreviewed; the current manifest is `0.7.0`. These changes were unreleased at measurement time.

The coverage hook and full test suite passed, including the TLS transport, TLS Sentinel failover and downstream outbox TLS regressions. All nine configured feature variants passed: default, no default features, sync, async, sync+discovery, async+discovery, sync+conformance, async+conformance and all features. Full CI also passed style/Clippy/Rustdoc, README checks, release build, package verification and fuzz smoke checks. The dependency audit scanned 180 dependencies using the cached 1,295-advisory database; refreshing that database from GitHub failed due to a network error, so the audit did not use freshly fetched advisory data.

The run used `x86_64-unknown-linux-gnu`, rustc `1.94.0` (`4a4ef493e`, LLVM `21.1.8`), cargo-llvm-cov `0.8.6`, and the pinned style toolchain `nightly-2026-06-05`. Cargo resolved the pinned local event-bus checkout; this run does not establish registry availability.

| Artifact | SHA256 |
| --- | --- |
| `coverage.json` | `d920a0de2d6738a9cb5949f1e12c0271ecf9b86fbaadfc09aaf2737cd5c3bd32` |
| `ci-summary.json` | `5635b7354c2d561950e8d22002c1f9eb1da864b3b7be6fcf3780e46b8d7736f1` |

The reports were written to the repository root as `coverage.json` and `ci-summary.json`; the detailed `target/infra/coverage/raw.json` report was removed by CI artifact cleanup after successful collection.

## Earlier CI6 gaps in file summaries

Only these seven files have uncovered functions in the CI6 LLVM file summaries:

| Source file | Covered/total functions | Uncovered |
| --- | ---: | ---: |
| `src/async/async_redis_event_bus.rs` | 21/22 | 1 |
| `src/async/async_redis_event_bus_provider.rs` | 5/6 | 1 |
| `src/async/subscription.rs` | 56/58 | 2 |
| `src/sync/redis_event_bus.rs` | 19/20 | 1 |
| `src/sync/redis_event_bus_provider.rs` | 3/4 | 1 |
| `src/sync/subscription.rs` | 48/58 | 10 |
| `src/sync/subscription/internal/receive_command.rs` | 6/8 | 2 |

These counts sum to 18 uncovered functions in the 384/402 metric. File summaries can include closures and inline private tests/helpers; this is not a list of 18 production declarations. Raw zero-count compilation instances are not independent functions and must not replace these summary counts. The historical 4/293 source-group diagnosis below belongs to a separate snapshot.

The [guide](user_guide.md) and [design](design.md) explain the current error contract: after an unknown XACK, only the original settlement intent may be retried. Markdown acceptance extracts all ten Rust blocks from both languages and builds/runs isolated Cargo projects against real Redis. The [workload benchmark](connection-reuse-benchmark.md) records separate measured workload results; the ignored legacy manual benchmark is not their acceptance run.

## Pre-final diagnostic attempts

CI3 passed 285 coverage-hook tests with one ignored benchmark, then failed the function gate: 335/383 (87.47%); lines were 3,112/3,356 (92.73%) and regions 4,651/5,098 (91.23%). It exited with status 1 before processed reports and dependency audit. CI4 completed with status 0, but its coverage included 26 old and 26 current profiles; those metrics do not establish fresh coverage. CI5 exited with status 1 during admission-test connection setup, before coverage. Increasing both admission fixtures' setup budget to 3,000 ms was followed by two passing focused regressions and the successful clean CI6 run above.

## Historical report provenance

The 306/312, 2,910/3,038 and 4,591/4,812 measurements below were recorded in commit `0dc7a551f0c91d57d612d78644c8258671b0ca56`, using a local patch of the unpublished upstream `qubit-event-bus` 0.15.0 snapshot. The separate 293-function analysis was recorded in commit `1f833e944a53cdbeb7c8ffd571ca6dea6390cf5f`. Their original raw reports were not independently revalidated during this refactor; their denominators describe different source snapshots. The historical Retry-after-failure description cannot establish safety after an unknown XACK.

## Historical coverage: candidate snapshot (package version unreviewed)

The current manifest version is `0.7.0`; the original manifest for this historical candidate measurement has not been recovered or verified. Its package version is therefore unreviewed, and the table records only the historical metrics:

| Metric | Historical candidate snapshot | Required |
| --- | ---: | ---: |
| Functions | 306/312 (98.08%) | at least 95% |
| Lines | 2,910/3,038 (95.79%) | above 90% |
| Regions | 4,591/4,812 (95.41%) | above 85% |

The historical report records that all three coverage thresholds passed. Function/line/region totals came from
the then-current `.infra/bin/ci-check.sh` coverage run. Region counts can vary slightly between runs
because deadline and recovery interval tests exercise timed paths.

Verification on 2026-09-29 used `./.infra/bin/align-ci.sh` followed by the full
`./.infra/bin/ci-check.sh`, with the repository's pinned toolchains and default test
concurrency. The run covered the default/all-feature test suite, the feature
matrix, strict Clippy/Rustdoc, package verification, coverage, and dependency
security checks. The report records that all thresholds passed. That run used the isolated local Cargo
patch for the unpublished `qubit-event-bus` 0.15.0 snapshot; it does not establish
registry availability. At the time of that measurement, the manual connection-reuse benchmark was not rerun;
the benchmark data cited by the historical report belonged to an earlier snapshot.

## Historical follow-up: the 293-function snapshot

That measurement’s detailed LLVM JSON report path was `target/infra/coverage/raw.json`. Its function
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
that snapshot’s function total of 293.

## Historical fault tests

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

## Historical recovery defect discovered by the tests

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

## Historical four uncovered mappings

The remaining zero-count groups are in `publish` and `subscribe` in both bus
implementations:

- The JSON serialization failure mapping for the fixed `WireFields` record. Its
  strings, integers, optional strings and byte arrays serialize successfully in
  the historical wire format; Redis protocol fault injection cannot induce this
  local serialization error.
- The consumer identity failure mapping for an unavailable operating-system
  entropy source. Normal UUID generation is covered; operating-system entropy
  failure is not induced by these tests.

These mappings remain as defensive error handling. They account for 4/293
functions, while the unchanged 95% function gate passed for that historical snapshot. No production injection
interface is needed to exercise the Redis transport and protocol failures.
