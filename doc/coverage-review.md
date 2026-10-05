# Redis coverage review

[简体中文](coverage-review.zh_CN.md)

## Current refactor acceptance

The sixth full CI attempt on 2026-09-29 exited with status 0. Its clean package coverage measurement passes the unchanged gates:

| Metric | CI6 measured result | Gate |
| --- | ---: | ---: |
| Functions | 384/402 (95.52%) | at least 95% |
| Lines | 3,755/3,913 (95.96%) | above 90% |
| Regions | 5,821/6,160 (94.50%) | above 85% |

Coverage runs through `.infra/bin/ci-check.sh` → `project-hook` → `project-ci-check.sh` → `.infra/bin/coverage.sh`, selecting `qubit-event-bus-redis` with `--locked --all-features -- --test-threads=1`. The file summaries cover 39 provider `src` files, excluding external `tests`, `src/tests`, `examples` and upstream package paths. Inline private tests and helpers in the included source files participate in the LLVM summaries, so these metrics are not coverage of production declarations alone. Their denominator differs from the manual Rustdoc declaration count. The raw and processed totals, `ci-summary.json`, and sums of the 39 file summaries agree for functions, lines and regions.

`.infra/bin/coverage.sh` cleans profile data before building the instrumented examples and measuring coverage. All 26 profiles in CI6 were created after its start at 2026-09-29 13:54:33 UTC; none predates this attempt. The measured source seal contains 154 Rust files and `.infra/bin/coverage.sh` (155 entries), SHA256 `02510d2bb99983c7a6b6977bebdec34bf99b83faaeaefc0a567bfab1eb7a9861`. Baseline HEAD was `5e36aede24c5db5de2e282932546de2fe500e9c2`; the measured changes were still uncommitted, so that HEAD identifies the baseline, not the complete measured source. Cargo remains at `0.4.0`, and these changes are unreleased.

The coverage hook ran 23 suites: 302 passed, zero failed and one ignored legacy manual benchmark. The earlier verification phase passed 310 tests including eight doctests, with one ignored benchmark. These counts describe separate executions and exclude feature-matrix repetitions. The configured nine variants passed: default, no default features, sync, async, sync+discovery, async+discovery, sync+conformance, async+conformance and all features. Full CI also passed strict style/Clippy/Rustdoc, README checks, release build, package verification and the dependency audit of 148 dependencies. Five additional locked minimal/discovery checks also passed in separate runs; their test executions are not added to the 302 coverage-hook tests.

CI6 ran `RS_INFRA_ARTIFACT_CLEANUP=0 ./.infra/bin/ci-check.sh` on `x86_64-unknown-linux-gnu`, using rustc `1.94.0` (`4a4ef493e`, LLVM `21.1.8`), cargo-llvm-cov `0.8.6`, and the pinned style toolchain `nightly-2026-06-05`. The parent process supplied no overrides for `CARGO_INCREMENTAL`, `RUSTFLAGS`, `RUSTDOCFLAGS`, `LLVM_PROFILE_FILE` or `RUST_TEST_THREADS`; coverage tools derive their instrumentation environment, and the test invocation explicitly uses one test thread. These parent-environment facts do not imply that the instrumented child variables are unset. The measured dependency was the local `qubit-event-bus` `0.16.0`; this run does not establish registry availability.

| Artifact | SHA256 |
| --- | --- |
| `target/infra/coverage/raw.json` and `coverage.json` (identical bytes) | `2f557b11291b3e306b64bacd89733f05d77137ada777e6bb132c243e9b52d025` |
| `ci-summary.json` | `de63bce0b50cf2fb759a88750acfacefdfaad8f0491cf6783c762958e4f21f88` |

## Current gaps in file summaries

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

## Historical coverage: 0.4.0 candidate snapshots

The package coverage gate uses the metrics collected by `.infra/bin/coverage.sh` and
`.infra/bin/ci-check.sh`. The 0.4.0 release-candidate measurement was:

| Metric | 0.4.0 candidate | Required |
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
