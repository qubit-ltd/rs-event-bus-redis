# Implementation evidence: coverage entry and Sentinel costs

Date: 2026-11-11

## RER-001: current coverage evidence

The standalone command `./.infra/bin/coverage.sh` exited 0 in the implementation
worktree. Its package totals passed the configured gates:

| Metric | Result | Gate |
| --- | ---: | ---: |
| Functions | 664/696 (95.40%) | at least 95% |
| Lines | 6,587/6,827 (96.48%) | above 90% |
| Regions | 10,464/10,941 (95.64%) | above 85% |

This captured the dirty implementation worktree based on HEAD
`5df03105b4c4feb4355dfd0b242010d85f9560b0`, not a clean committed HEAD or a
published install. `tested-inputs.json` records the baseline HEAD, 226-file Rust
source inventory, source inventory digest, selected manifest/lock/config
digests, and report digests. The reports and input metadata are retained under
`.infra/coverage/evidence-73af5c86/`. SHA256 values:

| Artifact | SHA256 |
| --- | --- |
| `coverage.json` | `235ab2f070fe651b35b448b6226818ad4e2b6e1c9e9feb2adc952fab2ee738fb` |
| `lcov.info` | `12484112d69bc6e4b965c8b4bae9060541133ebec461e52ca5f5554f3ad79784` |
| `coverage.txt` | `bbf19f1cbf4d50e998195dbad268cfa5b1a262014591b49f9c78f240eb79952b` |
| `cobertura.xml` | `d123123b30ee69afa6e80b5cb2e7e69c2a4cc4bc613125b51975c9961fb04df6` |
| `source-files.sha256` | `0e24b5410ecd0b64e2e1b095cae868bc44a7882d1bd5ff9597a13ad638e05583` |
| `tested-inputs.json` | `69c0914226a5667c26cd3dfbc861ed2cfaed77f45e475cd9608a6de3c7b0e3e9` |

The coverage script's `json` argument is unsupported and exited 2; the
documented no-argument invocation above is the successful report generation.
Coverage is collected from configured package sources and inline test/helper
code; it is not production-declarations-only coverage.

## RER-002: Sentinel connection-cost measurement

The ordinary focused command
`cargo test --locked --all-features --test sentinel_cost_tests -- --test-threads=1`
passed the statistical correctness cases; the long matrix remained ignored.
The 2-sample/1-round smoke matrix exited 0 across 24 dimension cells and
48/48 successful operations. The formal matrix command was:

```text
RER_COST_SAMPLES=1000 RER_COST_ROUNDS=5 RER_COST_OUTPUT=.infra/benchmark/evidence-73af5c86/final cargo test --locked --all-features --test sentinel_cost_tests -- --ignored --exact test_sentinel_cost_matrix --nocapture --test-threads=1
```

It exited 0 in 1,615.49 seconds: 120 cells, 120,000 attempts, 120,000
successes, zero errors and zero unknown outcomes; all PEL checks passed. It
covers sync/async, plaintext/TLS Redis master, concurrency 1/8, and publish,
settle, round-trip scenarios, with five rounds and 1,000 measured samples per
cell after 100 warmups. Redis was 7.0.15. The master used TLS for TLS cases;
Sentinel remained plaintext and a transparent proxy counted provider Sentinel
sockets without decrypting traffic.

The recommendation is limited prototype research only. TLS measurements show
potential cost worth isolating further, but no production SLO or valid A/B
comparison exists. TLS handshake cost was not separately measured. Provider
connection counts are logical provider open attempts, not physical TCP opens;
one attempt can query multiple Sentinel endpoints. Provider ROLE values are
attributable estimates only in zero-failure rows. Raw Redis `INFO ROLE`
commandstats are retained separately and mix provider, Sentinel and background
calls. Launch-time system load was not captured. These results do not claim a
reuse percentage, an SLO violation, or justification for a production cache.

Raw data, command, completion log and checksums are in
`.infra/benchmark/evidence-73af5c86/final/`. SHA256 values are recorded in that
directory's `SHA256SUMS`; the summary CSV hash is
`2c4bcf67698f4bf1360205cc5d7b973c2423d13520e5a1f78fdbceab831847d4`, samples
CSV hash is `4d836cba0ea05ccda112659257fb3d070cbb356a5d050e77307b979d56ea358f`,
and manifest hash is `28d13316debf6099202e70e5cd18ea7816e37b1dff6e31a7e320b41cf51e04c5`.

## Validation record

Commands and outcomes on the integrated implementation worktree:

| Command | Result |
| --- | --- |
| `./.infra/bin/align-ci.sh` | Exit 0; Rust formatting/alignment and configured Clippy checks passed. `coverage-cfg-clippy` and Loom are explicitly unconfigured. |
| `env PATH="/tmp/rer-sudo-docker-bin:$PATH" ./.infra/bin/ci-check.sh` | Exit 0. Full configured suite, feature matrix, docs/README checks, release build, package verification, fuzz smoke, coverage gate and dependency audit passed. Docker commands used `sudo -n docker` through the temporary wrapper. |
| `env PATH="/tmp/rer-sudo-docker-bin:$PATH" ./.infra/bin/coverage.sh` | Exit 0; standalone totals above. |
| Four `cargo tree --locked --manifest-path ...` fixture checks | Exit 0; local `qubit-event-bus` 0.21.0 resolves to the mapped exact source commit in all four fixtures; `qubit-task` 0.11.0 resolves correctly for task notifications. |
| `git diff --check` | Exit 0 after evidence and bilingual entry updates. |

The first Docker-backed CI attempt failed because the container user lacked
Docker access. After using `sudo -n docker`, an early full run exposed stale
fixture dependency worktree versions (event-bus 0.20 and task 0.10); these were
replaced with read-only worktrees at the required existing local commits and
the fixture Cargo trees were checked. A later run exposed a 2-second default
command timeout in a 16 MiB payload isolation test under proxy/CI load. The
test received a local 60-second command budget (without changing production
timeouts or assertions); its focused regression and final full CI passed.
No CI gate or coverage threshold was removed or weakened.

Repository-wide Rust formatting changes emitted by `align-ci.sh` are retained
as required and included in the successful final CI run; they were not discarded
as unrelated style edits. CI was run on the complete aligned source and test
changes after the final code repair. The only subsequent changes are this
evidence file and the bilingual coverage summary entry; they do not modify Rust
source, configuration or tested behavior. The design and implementation plan files were retained and excluded
from commits.
