# RER-106 Implementation Evidence

## Scope and changes

- Moved the ACL example start/end markers outside the fenced code blocks in `doc/user_guide.md` and `doc/user_guide.zh_CN.md`; the command and surrounding ACL guidance remain unchanged.
- Added strict extraction checks for the complete single `text` fence, unique and ordered markers, and marker placement outside any Markdown backtick or tilde fence. Added regression coverage for old, mixed, incomplete, duplicate, reordered, CRLF, and nested-fence layouts.
- Kept all implementation and regression code in test support; no public API, runtime behavior, ACL permissions, or dependency configuration changed.

## Verification record

Commands below ran from the project root or the isolated implementation worktree. `PASS` means exit code 0. The initial expected-red test is explicitly marked as an expected failure.

| Command | Result |
| --- | --- |
| `cargo test --locked --all-features --test acl_tests test_documented_acl_parameters_match_both_guides -- --exact --nocapture` before moving the markers | Expected failure: confirmed the strict parser rejected the original fenced-marker layout. |
| `cargo test --locked --all-features --test acl_tests test_documented_acl_parameters_match_both_guides -- --exact --nocapture` after the guide edits | PASS; both guides yielded the same validated ACL command and expected keys. |
| `cargo test --locked --all-features --test acl_tests test_acl_copy_block_ -- --nocapture` | PASS; all 6 parser tests ran, including the outer-fence regression. |
| `RER_REDIS_7_SERVER=/usr/bin/redis-server cargo test --locked --all-features --test acl_tests test_documented_acl_command_runs_in_isolated_redis -- --exact --nocapture` | PASS; actual guide command ran against isolated local Redis. |
| `RER_REDIS_7_SERVER=/usr/bin/redis-server cargo test --locked --all-features --test acl_tests -- --test-threads=1` | PASS; 30 passed, 4 ignored child-process tests. |
| `cargo metadata --manifest-path /data/working/qubit/.worktrees/rer106/rust-common/rs-event-bus-redis/Cargo.toml --locked --format-version 1` | PASS; local dependency paths resolved through linked worktrees. |
| `RER_REDIS_7_SERVER=/usr/bin/redis-server cargo test --locked --all-features --test downstream_tests test_task_notifications_preserve -- --nocapture --test-threads=1` | PASS; 2 downstream tests passed after adding the recursively required `rs-value` linked worktree. |
| `./.infra/bin/align-ci.sh` | PASS after fixing repository style findings: renamed the helper to end in `_tests.rs` and removed the disallowed `#[path]` module redirect. |
| `RUST_TEST_THREADS=1 RER_REDIS_6_2_SERVER=/tmp/rer-ef620e32-redis-6.2.17/redis-6.2.17/src/redis-server RER_REDIS_7_SERVER=/usr/bin/redis-server ./.infra/bin/ci-check.sh` | PASS; full test and feature matrix, docs/examples, package verification, fuzz smoke, coverage, and security audit completed. Final coverage: functions 95.40% (threshold ≥95%), lines 96.47% (>90%), regions 95.61% (>85%). Security audit scanned 153 locked crate dependencies; no vulnerability failure was reported. Loom was skipped by the existing CI because it is not configured for this repository. |
| `git diff --check` | PASS. |

## Diagnostic and repair notes

- The first full ACL suite without a configured local Redis 7 binary failed because the host Docker socket was unavailable and no local Redis 7 server was selected. Rerunning with `RER_REDIS_7_SERVER=/usr/bin/redis-server` passed; no test was skipped or weakened.
- The first full CI attempt exposed a missing recursive local path dependency (`rs-value`) in the worktree dependency layout. Added its exact-commit linked worktree from the existing source repository, verified `cargo metadata --locked`, reran the affected downstream tests, and then ran the complete CI successfully.
- `align-ci.sh` initially rejected the helper filename and then the `#[path]` module redirect. The test helper/module was adjusted to repository conventions, after which alignment and final CI passed.

## Git and retained files

- The design and implementation-plan files were preserved on disk and excluded from staging/commits.
- The final code changes are confined to the two ACL guides and ACL test-support/test files. The evidence file is part of this task's delivery record.
- Existing unrelated workspace files were left untouched.
