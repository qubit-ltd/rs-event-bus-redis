# Implementation evidence

Design: `doc/design-20261107T154148714Z-4b831ddf.md` (kept in the original checkout; not copied here).

Plan: `doc/implementation-plan-20261107T154148714Z-4b831ddf.md` (kept in the original checkout; not copied here).

The implementation worktree is `/data/working/qubit/rust-common/rs-event-bus-redis-rer-20261107`, branch `codex/redis-acl-docs-shared-rules`. No public API, configuration default, dependency version, key naming, wire v1 format, error contract, or delivery behavior was intentionally changed.

## Changes

- Added matching English and Simplified Chinese Redis ACL guidance with command/key permissions by publisher, subscriber/quarantine, Sentinel, master, and observer roles; clarified RESP2 authentication, optional `CLIENT SETINFO`, `SELECT`, key glob matching, and failure behavior.
- Added a crate-level Rustdoc landing page with feature-aware API links and provider-construction examples. Examples demonstrate lazy construction and do not claim to verify connectivity.
- Moved the pure stream-ID validator and fixed Redis capability set into shared internal modules, with boundary/contract unit tests and a sync/async capability integration test.
- Added isolated child-process ACL workflows for sync and async clients, including success, missing `EVAL`, missing quarantine-key access, and Sentinel discovery/master authentication and refusal paths.

The ACL helper is located at `tests/acl_support/mod.rs`, rather than the planned `tests/support/acl.rs`: repository style rules reject `#[path]` redirection and `include!`, and Cargo automatically treats a top-level `tests/support/acl.rs` as a separate test target. A normal nested module avoids both issues while retaining reusable support code.

## Focused ACL verification

Each command below ran in the implementation worktree with a temporary `docker` executable prepended to `PATH`; that wrapper forwarded to `sudo -n /usr/bin/docker` and was removed automatically with its temporary directory. No system group, daemon, or permission settings were changed. Sentinel ACL cases used Redis 7 with three Sentinel nodes. Each command exited 0; each reported six successful tests and two ignored child-process entry points (the parent test launches those helpers with isolated credentials).

1. `RER_ACL_REDIS_IMAGE=7-alpine cargo test --locked --no-default-features --features sync --test acl_tests -- --test-threads=1` — exit 0.
2. `RER_ACL_REDIS_IMAGE=6.2-alpine cargo test --locked --no-default-features --features sync --test acl_tests -- --test-threads=1` — exit 0.
3. `RER_ACL_REDIS_IMAGE=7-alpine cargo test --locked --no-default-features --features async --test acl_tests -- --test-threads=1` — exit 0.
4. `RER_ACL_REDIS_IMAGE=6.2-alpine cargo test --locked --no-default-features --features async --test acl_tests -- --test-threads=1` — exit 0.

The Redis 6.2 and Redis 7 standalone cases exercised the restricted user, expected denial outcomes, source pending state, quarantine behavior, and unrelated key scope. Sentinel success and explicit Sentinel-command/`ROLE` refusal cases ran with Redis 7 for both sync and async providers. Redis 6.2 does not support `CLIENT SETINFO`; denied metadata setup remains non-fatal. No ACL credential values are recorded here.

## Rust and documentation checks

- `cargo test --locked --no-default-features --features sync --lib valid_stream_id` — exit 0, 2/2 boundary tests.
- `cargo test --locked --no-default-features --features async --lib valid_stream_id` — exit 0, 2/2 boundary tests.
- `cargo test --locked --no-default-features --features sync --test capabilities_tests` — exit 0, 1/1 provider capability contract test.
- `cargo test --locked --no-default-features --features async --test capabilities_tests` — exit 0, 1/1 provider capability contract test.
- `cargo test --locked --all-features --test acl_tests --no-run` — exit 0 after final helper-module layout.
- `RUSTDOCFLAGS='-D warnings' cargo doc --locked --no-deps --no-default-features` — exit 0.
- `RUSTDOCFLAGS='-D warnings' cargo doc --locked --no-deps --no-default-features --features sync` — exit 0.
- `RUSTDOCFLAGS='-D warnings' cargo doc --locked --no-deps --no-default-features --features async` — exit 0 after correcting the async API link.
- `cargo test --locked --doc --no-default-features` — exit 0, 13/13 doctests.
- `cargo test --locked --doc --no-default-features --features sync` — exit 0, 14/14 doctests.
- `cargo test --locked --doc --no-default-features --features async` — exit 0, 14/14 doctests.
- `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` — exit 0.
- `.infra/bin/align-ci.sh` — final run exit 0. An earlier run at the original worktree path exited 128 because its expected adjacent `../rs-event-bus` path was occupied. After the authorized worktree move into `/data/working/qubit/rust-common/rs-event-bus-redis-rer-20261107`, the expected clean sibling checkouts resolved normally. Intermediate style failures from test naming and prohibited module redirection were fixed before the final successful alignment.
- `git diff --check` — exit 0.

The new crate doctest initially exposed an incorrect options getter and feature-gated examples without a `main`; both were corrected. The async-only strict rustdoc run initially found an unresolved `crate::r#async` link; it was changed to the public `async::AsyncRedisEventBusProvider` link and passed on rerun.

## Full CI

The complete command was run with a temporary Docker wrapper (same forwarding and automatic cleanup described above):

```sh
python3 - <<'PY'
import os, pathlib, subprocess, tempfile
with tempfile.TemporaryDirectory(prefix='rer-ci-docker-') as wrapper:
    docker = pathlib.Path(wrapper) / 'docker'
    docker.write_text('#!/bin/sh\nexec sudo -n /usr/bin/docker "$@"\n')
    docker.chmod(0o700)
    env = os.environ.copy()
    env['PATH'] = f'{wrapper}:{env["PATH"]}'
    run = subprocess.run(['.infra/bin/ci-check.sh'], cwd=pathlib.Path.cwd(), env=env)
    print(f'ci-check after ACL module fix exit={run.returncode}')
    raise SystemExit(run.returncode)
PY
```

Final exit: **0**. The command completed the style/clippy checks, unit and integration test suites, strict docs and README checks, feature matrix (default/all-features, no-default, sync-only, async-only, discovery combinations, and conformance/latest dependency builds), package verification, fuzz target, coverage, advisory scan, and configured project checks. All test and docs suites completed successfully. The two Markdown example test runs took 163.21s and 142.48s respectively; each built an isolated temporary downstream example and then passed 4/4 documentation tests. These were progressing child Cargo builds, not timeouts.

Coverage thresholds were satisfied:

- Functions: 95.48% (549/575), threshold >=95.00%.
- Lines: 96.01% (5372/5595), threshold >90.00%.
- Regions: 94.94% (8336/8780), threshold >85.00%.

The package suite verified one workspace package successfully. The security advisory scan loaded 1,422 advisories and scanned 152 locked dependencies; it reported one allowed warning that `async-executor 1.14.0` is yanked. Loom was skipped because no loom configuration exists in this project. The final CI cleanup removed only transient build artifacts under this worktree (`target/{debug,release,llvm-cov-target,infra-feature-matrix,infra/coverage,tmp}` and `fuzz/target`).

## Failed attempts and corrections

- A plain Docker-backed ACL run initially failed because the current user lacked access to `/var/run/docker.sock`. Using a temporary per-command wrapper around the already-authorized `sudo -n /usr/bin/docker` resolved this without changing system permissions.
- The first sync Redis 7 ACL workflow run exited nonzero because the test expected the poison reason `invalid_wire_field`, while the actual malformed JSON classification is `invalid_json`. The assertion was corrected to the observed stable contract; all four Redis 6.2/7 × sync/async ACL suite commands then passed.
- A full CI attempt exited 1 when the ACL helper was imported into shared test support and clippy found `no_effect_replace`. The helper was isolated to the ACL test module and the ineffective replacement was removed.
- A later CI/style attempt exited 1 because `#[path]` redirection is disallowed. A `include!` workaround was also rejected by style rules; the helper was moved to the normal nested `tests/acl_support/mod.rs` module.
- The final full CI command after those fixes exited 0. No coverage threshold was lowered and no unresolved test failure remains.

