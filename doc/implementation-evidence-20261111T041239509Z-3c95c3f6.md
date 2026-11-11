# XAUTOCLAIM 校验与双语 README 实施证据

目标仓库：`/data/working/qubit/rust-common/rs-event-bus-redis`
实施 worktree：`/data/working/qubit/rust-common/.worktrees/redis-rer-20261111/rs-event-bus-redis/project`
基线：`30cfd469734664ff5d4770604abda9fdd8674289`
任务分支：`codex/rer-20261111-xautoclaim`

## 目标与实现

| 目标 | 实现位置 | 证据 |
| --- | --- | --- |
| RER-001：拒绝当前非 JUSTID、COUNT 1 请求不可能合法的回复 | `src/stream_protocol.rs` | 裸 ID 不再被转换为空字段记录；第二条有效记录返回静态协议错误；nil tombstone 和 Redis 7 deleted IDs 保留。Rustdoc 说明请求形态与规则。 |
| RER-001：保持错误后的 cursor 与订阅恢复语义 | `src/tests/stream_protocol_tests.rs`、`tests/redis_fault_tests.rs` | parser 覆盖裸 ID、双 entry、`Value::Nil` 与 `[Value::Nil]` tombstone、deleted IDs、完整最大 ID；同步/异步 SPI 覆盖 cursor 保留、没有消息/Gap、没有额外 EVAL/XACK，并在同一 subscription 使用合法回复恢复。 |
| RER-002：压缩双语 README 重复契约 | `README.md`、`README.zh_CN.md` | 保留 Redis 版本、Durable、至少一次投递/幂等、XADD 与业务完成的区别、未知 publish 结果、retention、pending 超限、结算 ACL 摘要，并链接到对应的双语用户指南和迁移指南。 |
| 必要 CI 基线修复：避免受限文件描述符下测试 fixture 累积 socket | `tests/client/support/blackhole_redis.rs` | CI 用户的 `RLIMIT_NOFILE` 为 1,024；原 fixture 将每次接受连接的 socket 副本一直保留到 fixture 销毁。第 994 次连接失败并导致 listener 提前退出。现在 socket worker 结束时从活动 registry 移除副本；Drop 仍会 shutdown 所有活动连接。保留 1,024 次取消压力与原断言。 |
| 必要文档测试修复：避免 resume 示例在 ACK 完成前重建订阅 | `tests/documentation/markdown_examples_tests.rs` | 全量 CI 偶发在 handler 通知后立即 cancel 并重建订阅，导致 ACK 竞态：恢复测试看到仍 pending 的 `order-42`。生成的回归程序现在先优雅关闭 bus，再重建 bus 验证 group cursor；不改变生产 API 或 README 示例。 |

双语详细契约仍由现有用户指南及迁移指南承载，没有迁移调用方或公共 API；未修改 manifest、lockfile 或下游项目。独立只读复审未发现 Critical/Important 代码、兼容性或链接缺陷，复审结果指出应补此 evidence 文件。socket 生命周期修复在最终 CI 通过后纳入审查与提交。

## 先失败后通过与验证记录

工具链：`rustc 1.94.0 (4a4ef493e 2026-03-02)`，`cargo 1.94.0 (85eff7c80 2026-01-15)`。

| 命令 | 结果 |
| --- | --- |
| 新增 parser 回归在旧行为上的 RED 检查 | 失败符合预期：旧解析器接受裸合法 ID 和多条 claimed entry；早期一次编译失败是非穷尽枚举匹配问题，修正测试后确认行为失败。 |
| `cargo test --lib --all-features stream_protocol_tests --locked` | 通过，16 项。 |
| `cargo test --test redis_fault_tests --all-features --locked` | 通过，31 项。 |
| `cargo test --test redis_fault_tests --no-default-features --features sync --locked` | 通过，18 项。 |
| `cargo test --test redis_fault_tests --no-default-features --features async --locked` | 通过，18 项。 |
| `cargo test --test recovery_regression_tests --all-features --locked`（无 Docker 组权限） | 环境受阻，42/48 通过；6 项 Redis 6.2/7 Docker 场景因当前账号无 `/var/run/docker.sock` 权限失败。本地只有 Redis 7.0.15。之后依用户授权仅以 `starfish:docker` 运行同一测试并全部通过。 |
| `cargo test --test documentation_examples_tests --all-features --locked` | 通过，8 项；本地 Redis 7 fallback 可用。 |
| `cargo test --test downstream_tests test_task_notifications_preserve_monotonic_projection_and_business_state --all-features --locked -- --exact --test-threads=1` | 通过，1 项（6 项过滤）。 |
| `RS_INFRA_PROJECT_ROOT=<实施项目绝对路径> ./.infra/bin/align-ci.sh --dry-run` | 通过，解析到实施 worktree 与仓库 rustfmt 配置。 |
| `RS_INFRA_PROJECT_ROOT=<实施项目绝对路径> ./.infra/bin/align-ci.sh` | 通过；rs-infra-style 与 `rs-infra-ci --only clippy check` 成功。对齐中发现两项仓库格式修正：`tests/acl_tests.rs`、`tests/support/controlled_redis/proxy.rs`，已审查并纳入实施差异。 |
| `git diff --check` | 通过，无空白错误。 |
| `sudo -n -u starfish -g docker /home/starfish/.cargo/bin/cargo test --test recovery_regression_tests --all-features --locked` | 通过，48 项；包括 Redis 6.2/7 场景。按用户授权仅对验证命令使用该身份，未修改系统权限。 |
| `sudo -n -u starfish -g docker env PATH=/home/starfish/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin /home/starfish/.cargo/bin/cargo test --test client_tests --all-features --locked client::limit_tests::test_async_repeated_post_send_cancellation_resource_boundary -- --exact --test-threads=1` | 通过，1 项；受限文件描述符下仍完成 1,024 次取消。 |
| 最终 `./.infra/bin/align-ci.sh` | 通过；风格对齐与 `rs-infra-ci --only clippy check` 成功。 |
| 首次完整 `sudo -n -u starfish -g docker sh -c 'ulimit -n 1024; ... ./.infra/bin/ci-check.sh'` | 退出 1；feature matrix 的 `documentation::markdown_examples_tests::test_markdown_fragments_and_programs_publish_settle_and_close` 暴露 handler ACK 竞态。runner 的一次重试在中文 existing-group-resume 路径同样复现，输出为 `order-42` 而非 `order-43`。 |
| 诊断复现 `sudo -n -u starfish -g docker sh -c 'ulimit -n 1024; ... cargo test --test documentation_tests --all-features --locked documentation::markdown_examples_tests::test_markdown_fragments_and_programs_publish_settle_and_close -- --exact --nocapture'` | 单独运行时 1 项通过，说明问题为取消后立即重新订阅的调度竞态；据测试代码追踪确认 handler 发 channel 通知早于 SPI ACK。 |
| 修复后聚焦文档测试，同上 `cargo test` 命令 | 通过，1 项（6 项过滤），实际运行双语 README 与用户指南中全部标记示例。生成器先完成 graceful shutdown，再创建新 bus 并验证 cursor。 |
| 最终 `sudo -n -u starfish -g docker sh -c 'ulimit -n 1024; export PATH=/home/starfish/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin; ./.infra/bin/ci-check.sh'` | 通过，退出码 0；构建、测试、feature matrix、Rustdoc、Markdown 示例、fuzz、crate package、覆盖率和 security audit 完成。 |
| 最终 CI 覆盖率 | 满足阈值：functions 95.40%（664/696，门槛 ≥95%）；lines 96.48%（6587/6827，门槛 >90%）；regions 95.64%（10464/10941，门槛 >85%）。 |
| 最终 security audit | 扫描 153 个锁文件依赖，无漏洞告警；无需更新依赖或 lockfile。 |

CI 的 feature matrix 与覆盖收集重复执行了文档示例测试，最终均通过。最初的完整 CI 在 Docker 授权用户的默认软上限 1,024 下暴露文档测试的 ACK 调度竞态；按数据流追踪后修复生成器的关闭/重启顺序，聚焦测试与最终完整 CI 均通过。黑洞 fixture 的 socket 生命周期修复也保留在最终代码中。覆盖率满足阈值，153 个锁文件依赖的 security audit 无漏洞告警；无需更新依赖或 lockfile。

## 变更归属与剩余 Git 步骤

工作开始时原始 `dev-starfish` 位于基线提交，除本轮指定设计/计划文档外无未提交用户改动。实施期间原始 checkout 出现 128 个 Rust 文件的格式差异；独立复核将 HEAD 与这些文件分别用当前 rustfmt 格式化后，128/128 完全一致，未发现语义或文本变更。这些差异不是 RER 范围内容；实施 worktree 的仓库对齐生成 `tests/acl_tests.rs`、`tests/support/controlled_redis/proxy.rs` 两项 canonical 格式修正，并在最终代码上运行 CI。原始 checkout 未被覆盖或清理。实施 worktree 中不包含指定的设计/计划文档。

验证已通过。后续待按实施授权提交、将任务 worktree 快进整合至原 `dev-starfish`、依次同步既有 `dev`、`main`、`dev-starfish` 并推送，最后回到 `dev-starfish`。两份设计/计划文档须继续保留，直到用户明确授权删除。
