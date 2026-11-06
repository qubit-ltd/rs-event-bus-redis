# Redis provider 实施验证记录

## 基线与工作区

- 目标项目：`/data/working/qubit/rust-common/rs-event-bus-redis`
- 原分支：`dev-starfish`，HEAD `7b13d775070883fc4e888950cb1b42c6178bb7cb`
- 实施 worktree：`/data/working/qubit/rust-common/.worktrees/rs-event-bus-redis/codex-rbr-20261106`，分支 `codex/rbr-20261106`，从原 HEAD 创建。
- 开始时目标仓库只有两个预存的未跟踪文件：本轮设计文档及实施计划；它们未复制进 worktree，未暂存、修改或删除。
- 相关下游 `rs-task` 开始时为 `dev-starfish` 且干净。测试时仅临时将 provider path 指向本 worktree，之后恢复其 `Cargo.toml`、`Cargo.lock`；最终状态仍干净。
- `rs-redact`、`rs-value` 在验证开始时有既存未提交修改，未修改、暂存或提交这些工作区；下游验证使用各自干净 HEAD 的 detached worktree。
- `rustc +1.94.0 --version`、`cargo +1.94.0 --version`、`docker --version` 均成功。经用户明确授权，已将 `starfish` 加入 `docker` 组；通过 `sg docker -c 'docker ps'` 验证 socket 可用。

## 条目结果

| 条目 | 结果与证据 |
| --- | --- |
| RBR-001 | 四份安装 TOML 已加入 workspace 根 `[patch.crates-io]`，直接 core path 与 patch path 相同。检查器拒绝缺少 patch 或错误 patch path；真实 README TOML 独立 consumer 的 metadata 只有一个 `qubit-event-bus` package identity，应用和 provider 依赖边指向同一 identity，自动发现 README 示例构建通过。 |
| RBR-002 | README、双语指南、双语设计文档统一说明导入名 `redis`、Cargo package `qubit-redis`、版本 `=2.0.0-rc.2`、registry 来源；2026-11-06 package candidate 阶段实测通过，文档注明这不代表已发布或运行时验收。 |
| RBR-003 | 双语文档与 Rustdoc 描述完整 wire-byte 限制、所有版本先解析完整 JSON 并受 Serde 递归保护、有效未知版本与深层 malformed/poison 的分类边界，以及 RESP frame 独立边界。移除了未引用的 `src/wire_fields/internal/json_depth.rs`。分类回归断言浅层未知版本、129 层未知布局、有效 v1 和 wire 超限行为。 |
| RBR-004 | 测试夹具使用有限 deadline 的 loopback RESP PING/PONG 探测；启动 5 秒、重启 10 秒，connect/read/write 与 sleep 均裁剪到剩余预算。黑洞、非 PONG 和 PONG 测试通过；完整 CI 中 Redis runtime 与 Redis 6.2 测试通过。 |
| RBR-V01 | 脚本端点确认两次 `XGROUP` 请求均已到达并断连；修复前下一次 publish 返回 `outcome_unknown` 且没有 `XADD`。现对第二次 XGROUP 的 I/O/timeout 结果按 generation 失效连接，并保留其他服务端错误语义；断连与延迟超时恢复回归通过。 |

## 命令、结果与失败诊断

| 命令 | 退出/结果 | 摘要 |
| --- | --- | --- |
| `python3 ci/check-doc-versions.py .` | 0 | 双语依赖版本、path 一致性与六份当前 Redis 来源声明通过。 |
| `python3 ci/tests/test_doc_versions.py` | 0 | 9/9 通过；涵盖缺少/错误 core patch、错误包名/版本/来源、双语差异及历史结论。 |
| `cargo +1.94.0 test --test documentation_tests --all-features --locked test_source_install_manifest_has_one_core_identity -- --nocapture` | 0 | 1/1 通过；负例在任何 Cargo metadata/build 命令前删除 patch 并由安装契约检查拒绝；正例 metadata/build 通过。 |
| `cargo +1.94.0 test --lib --all-features --locked bounded_wire_decoder_tests` | 0 | 8/8 通过。 |
| `cargo +1.94.0 test --lib --all-features --locked poison_reason_tests` | 0 | 2/2 通过，包括深层未来布局被分类为 `InvalidJson` poison。 |
| `cargo +1.94.0 test --lib --all-features --locked cache_tests` | 0 | 2/2 通过；已有 stale-generation 测试覆盖旧 generation 不清新缓存。 |
| `cargo +1.94.0 test --test subscribe_uncertainty_tests --all-features --locked -- --test-threads=1` | 0 | 6/6 通过，包括两次 disconnect 和 delayed timeout 后首次 publish 恢复。 |
| `cargo +1.94.0 test --test fixture_process_tests --all-features --locked readiness -- --test-threads=1` | 0 | 3/3 通过：blackhole、non-PONG、PONG。 |
| `cargo +1.94.0 doc --workspace --all-features --no-deps --locked` | 0 | Rustdoc 构建成功。 |
| `cargo +nightly-2026-06-05 clippy --test fixture_process_tests --all-features --locked -- -D warnings` | 0 | readiness 测试 Clippy 通过。 |
| `cargo +nightly-2026-06-05 clippy --test documentation_tests --all-features --locked -- -D warnings` | 0 | 安装验收测试 Clippy 通过。 |
| `bash ci/check-package-candidate.sh` | 0 | 归档、manifest、no-default-features、sync-discovery、async-discovery、default build 与 metadata identity 均通过。最后日志在 `target/package-candidate-logs/20261106T035628Z-2582376`。 |
| `rs-infra-tools latest --project <worktree> --tool rs-infra-style -- fix`，随后 `... -- check` | 0 | 仓库 style 格式化和检查通过；命令通过已安装的官方 rs-infra 工具直接运行。 |
| `./.infra/bin/align-ci.sh` | 128 | wrapper 依赖准备尝试创建 `../rs-task`，但该路径是实施前已存在、指向缺失 checkout 的悬空 symlink；未替换或删除该 symlink。官方 style 工具直接运行成功。 |
| `./.infra/bin/ci-check.sh` | 128 | 同一依赖准备错误；失败时脚本仅清理本 worktree 的 `target/debug` 与 `target/tmp` 临时产物。 |
| `rs-infra-tools latest --project <worktree> --tool rs-infra-ci -- --project <worktree> check`，置于 `sg docker` 下 | 0（最终重跑） | 完整 CI 通过：style、Clippy、build、lockfile、workspace tests/doctests、strict docs、README、release build、3 组 feature matrix、fuzz smoke、package、coverage、audit。coverage functions 95.33% (531/557)、lines 96.13% (5191/5400)、regions 94.99% (8038/8462)，均高于原门槛；audit 有 1 条仓库允许的 yanked `async-executor 1.14.0` 警告；loom 未配置并按规则跳过。 |
| CI 首次 Docker-enabled rerun | 1 | Docker tests 已运行；下游 fixtures 暴露验证布局缺少 `rs-model-metadata`、`rs-metadata`、`rs-redact`、`rs-value` 的 sibling 路径。按仓库清单补齐依赖布局后重跑成功。已有 `rs-task` dangling link 保持不变，其原目标处仅添加本轮 detached validation worktree。 |
| `sg docker -c 'cargo +1.94.0 test --manifest-path <worktree>/Cargo.toml --test downstream_tests --all-features --locked -- --test-threads=1'` | 0 | 4/4 通过，包括无 TLS 与 rediss/TLS 两个 task notification consumers。 |
| `sg docker -c 'cargo +1.94.0 test --manifest-path <worktree>/Cargo.toml --test documentation_examples_tests --all-features --locked -- --test-threads=1'` | 0（完整 CI 覆盖） | 5/5 通过，包括独立示例与 Sentinel 示例。 |
| `sg docker -c 'cargo +1.94.0 test --manifest-path <worktree>/Cargo.toml --test runtime_tests --all-features --locked -- --test-threads=1'` | 0（完整 CI 覆盖） | 9/9 通过，包括 Redis 6.2 stream PING 与 pending recovery。 |
| `sg docker -c /bin/bash /tmp/rbr-run-task-outbox-20261106.sh` | 0 | rs-task `redis_task_outbox_tests` 12/12 通过。测试期间临时将 provider path 指向当前 worktree；脚本退出 trap 恢复 Cargo.toml、Cargo.lock，rs-task 最终干净。 |
| `python3 ci/check-doc-versions.py .`; `python3 ci/tests/test_doc_versions.py`; `git diff --check` | 0 | 文档契约检查、9/9 Python tests 与 whitespace 检查通过。 |
| `git diff --check` | 0 | 当前 worktree 无 whitespace error。 |

## 实施状态

此前因 Docker socket 权限失败的检查已在用户授权授予访问后重跑；后续首次 CI rerun 又发现并补齐本地验证 checkout 布局，最终完整 CI、coverage 与下游测试通过。尚待最终差异复核、提交、快进整合、既有分支同步及 push。原目标仓库的两份设计/计划文档必须保留且从所有提交排除。
