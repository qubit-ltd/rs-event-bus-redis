# RER-104 / RER-105 实施证据

实施目标：修正文档 ACL 示例，使实际双语参数可回归验证；统一“至少一次投递”的适用条件。目标项目 worktree：`/data/working/qubit/rust-common/.worktrees/rer-104-105-layout/rust-common/rs-event-bus-redis`。原始仓库：`/data/working/qubit/rust-common/rs-event-bus-redis`。

## RER-104：双语 ACL 示例

- `doc/user_guide.md` 和 `doc/user_guide.zh_CN.md` 使用相同的、单行 Redis 管理交互命令，并以唯一注释标记包围。两份指南明确说明命令应在已认证的 `redis-cli` 会话中执行，不是 shell 命令。
- 文档参数测试从两份实际指南提取命令，检查标记唯一性、单行格式、完整参数向量一致性，并使用公开命名 helper 验证 source、group 和 quarantine key。ACL 权限保留脚本所需的 `+eval`、`+xpending`、`+xack`，未扩大到 `+@all`。
- 隔离 Redis 测试直接用 `redis::cmd` 执行文档 ACL 参数，在该用户下验证认证、publish、receive 和 Accept；未调用 shell，也未连接外部 Redis。
- 设计样例的 quarantine key 长度与当前 `poison_key` helper 不一致：helper 对本示例生成 42 字符，因此双语说明和 ACL 参数使用 helper 结果 42，并由测试锁定。

## RER-105：条件化投递保证

- README 和双语用户指南开头说明：仍保留且可恢复的 Redis Stream entry 可以重复投递；`Accepted` / Redis 接受 `XADD` 不保证消费者端到端至少收到一次。
- 用户指南继续保留慢 handler、`redis.claim_min_idle_ms`、幂等、SQLite outbox、Redis 持久化、版本缺口对账及 `XADD MAXLEN ~` 有损保留边界。
- 交叉检查 `rs-task/src/service/task_event_publisher/internal/publisher_state.rs::publish`：仅在 publish 被接受后删除 outbox 行；未将此行为表述为消费端收到保证，也未修改下游代码。

## 变更与验证记录

| 命令/检查 | 结果 |
| --- | --- |
| `RER_REDIS_7_SERVER=/usr/bin/redis-server cargo test --locked --all-features --test acl_tests -- --test-threads=1` | 退出码 0；24 passed、4 ignored。 |
| `cargo test --locked --all-features --test acl_tests test_documented_acl_parameters_match_both_guides -- --exact --nocapture` | 退出码 0；文档参数测试通过。 |
| `RER_REDIS_7_SERVER=/usr/bin/redis-server cargo test --locked --all-features --test acl_tests test_documented_acl_command_runs_in_isolated_redis -- --exact --nocapture` | 退出码 0；隔离 Redis 上的实际 ACL 工作流通过。 |
| `rg -n 'at least once|at-least-once|至少一次' README.md README.zh_CN.md doc/user_guide.md doc/user_guide.zh_CN.md src` | 逐项核对剩余命中均是条件化投递说明或 outbox/持久化/版本缺口边界；`src` 无额外无条件承诺。 |
| `./.infra/bin/prepare-local-path-dependencies.sh` | 退出码 0；本地路径依赖准备成功。 |
| `cargo metadata --format-version 1 --locked --manifest-path tests/fixtures/<fixture>/Cargo.toml`（分别对 `business_consumer`、`async_smol_consumer`、`async_tokio_consumer`、`task_notifications` 执行） | 每个 fixture 的 metadata 解析成功，证明保留的相对路径依赖能解析。 |
| `./.infra/bin/align-ci.sh` | 最终退出码 0；Rust style 和 Clippy 检查通过。首次对齐因原布局已有悬空 `../rs-task` 链接失败；随后将项目移至隔离 worktree 布局，并从本地完整源仓库用 `git worktree add --detach` 建立精确提交依赖 worktree，未浅克隆、复制源码或改写依赖配置。 |
| `RUST_TEST_THREADS=1 RER_REDIS_6_2_SERVER=/tmp/rer-ef620e32-redis-6.2.17/redis-6.2.17/src/redis-server RER_REDIS_7_SERVER=/usr/bin/redis-server ./.infra/bin/ci-check.sh` | 最终退出码 0。完整测试、文档示例、feature matrix、coverage 和配置的依赖安全审计通过。Coverage：functions 95.40% (664/696，门槛 ≥95%)；lines 96.47% (6586/6827，门槛 >90%)；regions 95.61% (10461/10941，门槛 >85%)。Loom 按仓库配置为未配置并跳过。 |

## CI 基线诊断与修复

首次全量 CI 未提供 Redis 6.2 本地 fixture，且 Docker socket 权限不可用；按仓库已有 fallback 从本地 `/tmp/redis-6.2.17.tar.gz` 构建 Redis 6.2.17 并设置 `RER_REDIS_6_2_SERVER` 后重跑。随后 feature matrix 暴露既有 `test_async_block_adds_command_margin_and_next_poll_restores_short_wait` 的时序问题：测试使用 1 秒默认 recovery interval，却假定首次 1 秒超时后立即进入下一轮 XAUTOCLAIM；第二次立即调用可能仍在间隔内。该断言在两次全量运行中复现，单独运行及完整 async client 测试组通过。将这两个配对 timeout 测试显式配置为 50ms recovery interval，并在首次调用后等待 100ms；异步用例连续运行 5 次通过。修复只调整测试调度前提，不改生产行为或验证门槛。修复后重新运行 align 和完整 CI，最终均通过。

本轮的设计与实施计划文件保留在原始项目 `doc/` 中，不纳入提交；由循环清理阶段按用户授权处理。
