# Implementation Evidence: REV-001 and REV-002

**Project:** `rs-event-bus-redis`
**Project path:** `/data/working/qubit/rust-common/rs-event-bus-redis`
**Worktree:** `/data/working/qubit/.worktrees/rs-event-bus-redis/implement-20261106`
**Base commit:** `e5b860171f9fda443e0a4b6c109f9f8222d08ab8`
**Implementation branch:** `codex/redis-provider-state-receive-20261106`

This record covers the accepted REV-001 and REV-002 scope. The separate design and implementation-plan documents are retained in the original checkout and excluded from commits.

## Implemented targets

- **REV-001:** Updated the bilingual README, user guide, and design status sections to distinguish local source integration, source-candidate package validation, and formal published-install acceptance. Preserved the historical registry query as historical evidence and retained the exact Redis package/version statement required by the documentation CI check.
- **REV-002:** Centralized pure decode-failure to SPI-error mapping and poison-outcome to receive-outcome mapping. Sync and async adapters continue to own quarantine I/O and diagnostics. The sync adapter records delivery only after the XPENDING lookup and message assembly. Existing settlement, attempt-count, cancellation, recovery, unknown-version, size-limit, ownership, and quarantine-count behavior remains covered.
- Added table-driven mapping tests and extended both sync and async XPENDING-failure tests to prove delivered messages remain settleable.
- No public API, wire format, key, dependency version, or settlement policy changed.

Code/test snapshot at final full-CI run: SHA-256 `e0cb56fcd45445e98024e3dac9ae7e45ef60f348c53f297ffd9bedefd2530aa1` for the implementation and test diff paths (`src/async/subscription.rs`, `src/internal/decode_failure.rs`, `src/internal/poison_outcome.rs`, `src/sync/subscription.rs`, `src/tests/internal/mod.rs`, `src/tests/internal/receive_decision_tests.rs`, and `tests/redis_fault_tests.rs`).

## Verification record

Commands were run from the worktree unless noted.

| Command | Result | Notes |
| --- | --- | --- |
| `./.infra/bin/align-ci.sh` | Exit 1 initially; later exit 0 | The first run found Rustdoc/style issues in the new method and test; these were corrected. The final run after the settlement regression passed and cleaned transient build artifacts. |
| `cargo test --locked --lib --all-features receive_decision` | Exit 0 | Mapping table tests passed (2 tests). |
| `cargo test --locked --no-default-features` | Exit 0 | Existing `redis_error_code_class` dead-code warning remains in the no-default-features build. |
| `cargo test --locked --all-features --test sync_tests` | Exit 0 | Sync SPI suite passed (18 tests). |
| `cargo test --locked --all-features --test async_tests` | Exit 0 | Async SPI suite passed (19 tests). |
| `cargo test --locked --all-features --test redis_fault_tests` | Exit 0 | Fault suite passed; the final two XPENDING-failure cases assert settlement succeeds. |
| `sudo -n -u starfish -g docker env HOME=/home/starfish PATH=/home/starfish/.cargo/bin:/home/starfish/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin cargo test --locked --all-features --test redis_fault_tests attempt_query_failure_delivers_unknown_and_discards_connection -- --test-threads=1` | Exit 0 | Final focused sync and async cases passed (2/2). |
| `./.infra/bin/project-ci-check.sh` | Exit 0 | Bilingual dependency-version checks and project preflight fixtures passed. An earlier run correctly rejected the missing verbatim package/version claim; the claim was restored in the affected docs before this passing run. |
| `sudo -n -u starfish -g docker env HOME=/home/starfish PATH=/home/starfish/.cargo/bin:/home/starfish/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/sbin:/bin ./.infra/bin/ci-check.sh` | Exit 0 | Final complete CI run on the snapshot including the new settlement assertions. |
| `bash ci/check-package-candidate.sh` | Exit 0 | Printed `GREEN phase=metadata`; archive, manifest, no-default, sync/discovery, async/discovery, default-feature, and metadata checks passed. Generated logs and package artifacts were removed after recording the result. This validates the source candidate, not a published release. |
| `git diff --check` | Exit 0 | No whitespace errors. |

The final `ci-check.sh` run passed the configured build, test, documentation, README, feature-matrix, package, coverage, fuzz-smoke, dependency-audit, and loom steps. The coverage gate passed; the CI transcript retained here does not include a separately captured percentage. The CI reported one allowed advisory warning for yanked `async-executor 1.14.0`; loom was skipped because it is not configured in this project. The script removed its transient build directories after completion.

## Diagnostic attempts and resolutions

- Initial `./.infra/bin/align-ci.sh` exited 1 for missing Rustdoc sections and test naming/header style; after those corrections, it passed.
- Initial `./.infra/bin/ci-check.sh` as the default identity failed Docker-backed tests because the shell identity lacked Docker socket access (2 tests failed; 112/114 library tests passed). `sudo -n -u starfish -g docker ./.infra/bin/ci-check.sh` then exited 127 because that invocation did not receive Cargo's `PATH`. The corrected Docker-group invocation with explicit `HOME` and `PATH` passed without changing system permissions.
- An intermediate CI run found Clippy's `collapsible_if` lint in the adapter diagnostic increment; a later run then reached downstream fixtures but could not find sibling repositories adjacent to the worktree. After creating temporary symlinks to the existing `rs-metadata`, `rs-redact`, and `rs-value` checkouts, downstream tests passed.
- A later feature-matrix run had one transient async TLS Sentinel failover failure (`WAIT barrier reached only 0 replicas`); its isolated rerun passed. One full parallel test run also transiently failed `test_sync_retained_message_does_not_hold_receiver_permit_after_close` with `outcome_unknown` during publish; the isolated correctly filtered rerun passed 1/1, and subsequent full runs passed.
- An intermediate project CI check rejected removal of the exact bilingual `qubit-redis` version/source statement required by the existing documentation checker. The exact claims were restored in the affected documents, after which `./.infra/bin/project-ci-check.sh` passed.
- Final `./.infra/bin/align-ci.sh`, `./.infra/bin/project-ci-check.sh`, the focused settlement tests, the full `ci-check.sh`, and `bash ci/check-package-candidate.sh` all passed on the final implementation snapshot. The temporary sibling symlinks must be removed during task cleanup.
