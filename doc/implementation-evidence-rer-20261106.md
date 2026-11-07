# RER Implementation Evidence — 2026-11-06

## Baseline and environment

- Project: `/data/working/qubit/rust-common/rs-event-bus-redis-impl`, a Git worktree for `/data/working/qubit/rust-common/rs-event-bus-redis`.
- Base commit: `5586b3635b623544dc5e4b99fa4aece567e8d01a` on `dev-starfish`.
- Initial task worktree was clean. The original project checkout already contained the two untracked design/plan documents specified by the user; both remain excluded from this worktree and all commits.
- Initial dependent checkout `rs-task`: `dev-starfish` at `2dceec21a99bc59e9dd2d205c734a3f410b54589`, clean.
- `rs-event-bus` was later observed cleanly untouched by this task but already had two untracked documents: `doc/design-20261106T193503136Z-462f29ed.md` and `doc/implementation-plan-20261106T193503136Z-462f29ed.md`. They were not created or modified by this task and remain untouched.
- Rust: `rustc 1.94.0 (4a4ef493e 2026-03-02)`; Cargo: `1.94.0 (85eff7c80 2026-01-15)`; Linux x86_64, 56 logical CPUs, 125 GiB memory.
- Docker client: `29.9.2`; Docker daemon endpoint: `unix:///var/run/docker.sock`. Current user is `starfish` in group `developer`, not group `docker`; the socket is `root:docker` mode `0660`. Daemon requests fail with `permission denied`. Local `redis-server` is `7.0.15`; no separate `redis-sentinel` executable is installed.
- Before this evidence file was added, the uncommitted implementation diff contained 9 files and had SHA-256 `d5c0dae8918de5bb77ccf70b17d8e2aa257e58929c8219e412892356fcc8a3c1` (binary `git diff` stream). This digest predates subsequent evidence-file and any further changes.

## RER-001 — response-frame budget

Implemented a private 1 MiB discovery budget on both Sentinel sockets and ordinary candidate-master sockets. Candidate receiver sockets keep their supplied response budget. Synchronous and asynchronous TCP fixture tests send complete oversized RESP Sentinel replies and structurally valid ROLE master replies with oversized replica-list data. They assert discovery fails before a business command; another fixture confirms the next Sentinel can discover a healthy master and publish successfully.

- `cargo test --locked --all-features --test client_tests sentinel_response_budget -- --test-threads=1` — exit 0; 6 passed, 0 failed (50 filtered out). Repeated after formatting and after the CI failure; both runs passed.
- `cargo test --locked --all-features --test wire_limit_tests sentinel_receiver_budget -- --test-threads=1` with the authorized temporary sudo Docker wrapper — exit 0; sync and async real-Sentinel tests passed, publishing and receiving payloads larger than 1 MiB under the configured receiver budget, then settling them.
- `python3 ci/check-doc-versions.py` — exit 0; bilingual dependency-version check passed.
- `git diff --check` — exit 0.

## RER-002 — bilingual history navigation

Both coverage review documents now link the latest implementation evidence and installation-validation status section, state that the 2026-11-06 candidate has a full CI/coverage-gate pass record without a separately stated percentage, and keep candidate evidence distinct from published-install acceptance. The 2026-11-04 failures, table, and source hash remain historical and are explicitly limited to that snapshot.

- `python3 ci/check-doc-versions.py` — exit 0.
- `git diff --check -- doc/coverage-review.md doc/coverage-review.zh_CN.md` — exit 0 (reported by the T2 implementer).
- T2 link and anchor existence check — exit 0 (reported by the T2 implementer; both linked targets exist).
- Independent review found that the initial wording could associate the older candidate's CI pass with this implementation. Both languages now identify the earlier snapshot, link this RER evidence, distinguish the initial Docker-blocked attempts from the final CI exit 0, and note that no independent current percentage was retained. The final evidence and translation wording were independently re-reviewed; no further findings remain.
- Human review of English/Chinese status meaning — completed; current candidate evidence and published-install acceptance remain separate.

## RER-V01 — diagnostic runs and status

The first attempts below failed before behavior execution because Docker access was unavailable. After the user explicitly authorized `sudo`, `sudo -n docker --host unix:///var/run/docker.sock info` succeeded. A temporary Docker wrapper in `/tmp/rer-sudo-docker.s1OPdV` invoked Docker through `sudo -n`; no system ACL or group membership was changed. Repetitions then ran sequentially as separate Cargo test processes, with each command's stdout/stderr retained in a temporary log directory until this evidence was updated.

- `cargo test --locked --all-features --test tls_sentinel_tests test_async_tls_sentinel_rediscovers_tls_master_after_failover -- --test-threads=1` — exit 101; 0/1 test completed; failed starting the TLS master container due Docker socket permission denied. Test runner duration: 1.09 s.
- `cargo test --locked --all-features --test client_tests test_sync_retained_message_does_not_hold_receiver_permit_after_close -- --test-threads=1` — exit 101; 0/1 test completed; failed starting isolated Redis due Docker socket permission denied. Test runner duration: 0.03 s.
- Docker daemon diagnostic — client responds, server request returns `permission denied while trying to connect to the docker API at unix:///var/run/docker.sock`.

The behavior runs used the specified fixed toolchain and Docker image. Environment at start: Docker Engine `29.9.2`; Redis image `redis:7-alpine`, image digest `sha256:c5d34a98396a4775e223c74e929162c35c5a2182901ba26b50ade939941ce14b` (created `2026-11-05T19:11:20.765020619Z`); 56 logical CPUs; 135,037,526,016 bytes memory; load average `9.26, 5.41, 2.73`; host was running other workspace CI workloads, so this is a recorded shared-host environment. The run window was `2026-11-07T05:18:12Z` through `2026-11-07T05:30:25Z`. Per-process UTC start/end, exit code, and test summary were recorded in `runs.csv`; every full command log is available for inspection during this task.

- Async TLS Sentinel failover focused command, 20 independent serial processes: **20/20 exit 0**, one test passed per process.
- Sync retained-message receiver-close focused command, 20 independent serial processes: **20/20 exit 0**, one test passed per process.
- Complete `tls_sentinel_tests` binary at default test concurrency, 10 processes: **10/10 exit 0**, six tests passed in each process (60 executions total).
- Complete `client_tests` binary at default test concurrency, 10 processes: **10/10 exit 0**, 56 tests passed in each process (560 executions total).

The TLS failover test passed its existing barriers and assertions: WAIT was issued on the same verified TLS master connection as the barrier write; the test then required replica group cursor and PEL ID/owner to match; after promotion it required the same stream ID and business event to be recovered, then ACKed with an empty PEL. The close-lifecycle test passed its close/admission assertions before any subsequent action. The prescribed repeated runs did not reproduce either reported symptom. Later full-CI runs exposed two separate integration-fixture issues, described below; they do not establish that the originally reported lifecycle symptom is a product defect. **Result: the originally reported symptom was not reproduced in the prescribed run set; root cause remains unconfirmed.** This finite sample does not establish zero probability. No production behavior was weakened.

### CI-discovered integration-fixture issues

- After the user authorized `sudo`, the first full CI rerun reached feature-matrix `downstream_tests` and failed because the sibling `rs-redact` worktree already had uncommitted package version `0.10.1`, while `tests/fixtures/task_notifications/Cargo.lock` recorded the prior path-package version `0.10.0`. The user authorized updating this fixture lock. It now resolves the current local `0.10.1` path package; the sibling repository was not edited. `cargo +1.94.0 test --locked --workspace --all-features --test downstream_tests -- --test-threads=1` — exit 0; 4 passed.
- A later `async,conformance` matrix run failed `test_async_tls_sentinel_rediscovers_tls_master_after_failover`: its one-shot `WAIT 1 5000` returned 0 replicas. Inspection showed `TlsSentinel::wait_until_discovered` only waited for Sentinel to report a master, not for the replica connection, so the replication barrier could race replica startup. The fixture now retries `WAIT 1 1000` until a bounded 20-second deadline before checking the replica cursor/PEL. The same matrix-focused async test passed after the change; the final ordinary and coverage TLS Sentinel binaries each passed all 6 tests, including both failover tests.
- Another complete-CI attempt failed both sync and async command-margin tests during `subscribe`. Those tests set `redis.command_timeout_ms=200` before subscription setup, making the same 200 ms limit govern cold Redis setup while the whole suite ran concurrently. The test now uses a 1000 ms command timeout and an 1800 ms watchdog; the assertion still distinguishes the short poll from the prior `BLOCK` budget (over 2 seconds). Focused sync/async tests — exit 0; 2 passed. The complete ordinary test suite then passed.
- These fixture adjustments retain all assertions and CI checks. No test was ignored, removed, or excluded from coverage.

After those corrections, all T3 repetition counts were rerun serially in independent Cargo test processes. The run window was `2026-11-07T09:18:00Z`–`2026-11-07T09:24:02Z`; environment: rustc/Cargo 1.94.0, Docker 29.9.2, the same `redis:7-alpine` digest, 56 CPUs, 135,037,526,016 bytes memory, starting load average `1.80, 1.73, 6.89`. Per-process logs and UTC result rows were retained in `/tmp/rer-v01-postfix-logs-20261107` until this evidence update.

- Async TLS Sentinel failover focused command: **20/20 exit 0**, one test per process.
- Sync retained-message close focused command: **20/20 exit 0**, one test per process.
- Full `tls_sentinel_tests` binary at default concurrency: **10/10 exit 0**, six tests per process (60 executions).
- Full `client_tests` binary at default concurrency: **10/10 exit 0**, 56 tests per process (560 executions).

Thus the CI-discovered replica-readiness fixture failure was not reproduced in the post-fix repetition sample. The initial one-shot `WAIT` failure remains documented as a test-fixture timing issue; this repetition sample still cannot prove zero probability or identify a production root cause for the original RER-V01 report.

## CI and validation record

- `./.infra/bin/align-ci.sh` — first attempt from a deeply nested worktree failed after four dependency-preparation retries because the repository helper tried to clone over an existing `rs-task` symlink. The worktree was safely moved (preserving branch and edits) to `/data/working/qubit/rust-common/rs-event-bus-redis-impl`, beside the project’s existing downstream checkouts. Rerun: exit 0; formatting completed.
- `./.infra/bin/style-check.sh` — exit 0; “Rust mechanical style checks passed; semantic review remains separate.”
- Initial non-sudo `./.infra/bin/ci-check.sh` — exit 1 because Docker-backed tests could not access the daemon. After sudo authorization, subsequent CI attempts exposed and diagnosed the fixture issues above; those attempts also exited 1 and did not complete the configured task list.
- `cargo test --locked --all-features --test client_tests sentinel_response_budget -- --test-threads=1` after CI cleanup — exit 0; 6 passed, 0 failed.
- T2’s `python3 ci/check-doc-versions.py` — exit 0.
- `cargo clippy --locked --workspace --all-features --all-targets -- -D warnings` — exit 0.
- `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --all-features --no-deps` — exit 0.
- `bash ci/check-package-candidate.sh` — exit 0; normalized archive checks passed for no-default-features, sync-discovery, async-discovery, default features, and metadata.

- Final `./.infra/bin/align-ci.sh` after all source/test/lock changes — exit 0; formatting completed.
- Final `PATH=/tmp/rer-sudo-docker.s1OPdV:$PATH ./.infra/bin/ci-check.sh` — exit 0. Configured style, Clippy, verify (lock/build/test/doc), strict docs, README, release build, feature matrix, fuzz smoke, project hook, package candidate, coverage, and audit tasks completed successfully. The CI task for Loom exited successfully with `loom: skipped: not configured in /data/working/qubit/rust-common/rs-event-bus-redis-impl`, independently reconfirmed by `rs-infra-verify --project /data/working/qubit/rust-common/rs-event-bus-redis-impl run --suite loom` (exit 0). `coverage-cfg-clippy` is explicitly disabled by `coverage_cfg_clippy = false` in the repository CI configuration and was skipped. The CI bootstrap removed its temporary stdout log before it could be copied; the wrapper retained the final command exit code 0. No coverage percentage is claimed.

## Current plan status

- RER-001: complete; scripted sync/async RESP regressions and real-Sentinel sync/async receiver tests passed with sudo Docker access. Public wire/API/configuration behavior is unchanged.
- RER-002: complete.
- RER-V01: prescribed repeated focused and full-binary runs completed. The original symptom was not reproduced in those samples. Full CI exposed two separate fixture readiness/timeout issues; both were corrected in tests, and the final full CI passed. No product-level root cause for the original symptom is claimed.
- T4/full CI: complete; final configured CI exited 0. `coverage-cfg-clippy` remains intentionally unconfigured per repository settings. No numerical coverage percentage is inferred.
