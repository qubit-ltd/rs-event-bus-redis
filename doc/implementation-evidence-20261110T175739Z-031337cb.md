# RER-001–RER-004 实施证据

日期：2026-11-10
目标项目：`rs-event-bus-redis`
实施仓库：`/data/working/qubit/rust-common/rs-event-bus-redis-impl`
实施分支：`codex/rer-20261110`，起始提交 `323e4ab348feda893bfa99e537cee6236c1533b8`

## 目标与结果

- **RER-001：通过。** 异步 Sentinel 发现链保留经过脱敏的稳定 Redis 错误类别；认证拒绝不再在缓存初始化边界降级成 I/O 错误。同步/异步 wrong-password 和 missing-auth 回归均断言 `authentication`、`retryable: false`，并检查用户名、密码和端点不会出现在错误文本中。公开 API 未改变。
- **RER-002：通过。** 英文和中文指南均描述 owner-check EVAL 的 `0`/`1`、`-1`、未知回复及 `AckPending`/`Applied` 意图规则，并说明停止订阅后的恢复路径、首次明确 Redis 拒绝和业务 fencing 边界。
- **RER-003：通过。** 双语指南区分 SQLite outbox 的发布重试和消费者收件保证，记录 `Accepted` 后删除 outbox、Redis 持久化前故障窗口、消费者端到端至少一次收件不作保证，并建议持久 checkpoint、版本检查和权威状态查询。
- **RER-004：通过。** Python 回归可由模块发现或直接执行，且已由 `.infra/project-ci-check.local.sh` 调用。直接执行报告 2 项测试通过；hook 其余 Python 测试和 preflight fixtures 也通过。
- **必要基线修复：** 完整 CI 暴露 `fixture_process_tests` 解析 libtest 输出的问题：`--nocapture` 输出会在 PID 标记前带 `test ... ...` 前缀。解析器现按标记提取 PID，并新增该格式的回归测试；测试语义和门槛未降低。

## 验证记录

以下命令均在目标实施 worktree 中执行。退出码 0 表示通过。

| 命令 | 结果 |
| --- | --- |
| `cargo +1.94.0 test --test acl_tests --all-features --locked sentinel_acl -- --test-threads=1`（实现前） | 退出码 0；原有 Sentinel ACL 用例通过。 |
| 同上（新增 wrong-password 测试、实现修复前） | 失败并复现问题：异步 Sentinel setup 认证拒绝被分类为 transport/retryable，而非 authentication/non-retryable。 |
| 同上（实现后） | 退出码 0；同步与异步 Sentinel ACL 成功、角色拒绝、权限拒绝、错误密码及缺少认证用例通过。 |
| `cargo +1.94.0 test --lib --all-features --locked client -- --test-threads=1` | 退出码 0；28/28 通过。 |
| `cargo +1.94.0 test --test client_tests --all-features --locked -- --test-threads=1` | 退出码 0；62/62 通过。 |
| `RER_REDIS_6_2_SERVER=/tmp/codex-rer-20261110T161412476Z-redis-6.2.17/src/redis-server RER_REDIS_7_SERVER=/usr/bin/redis-server cargo +1.94.0 test --locked --all-features --test recovery_regression_tests -- --test-threads=1` | 退出码 0；48/48 通过，覆盖 Redis 6.2 和 7。 |
| `RUST_TEST_THREADS=1 cargo +1.94.0 test --locked --all-features --test fixture_process_tests -- --test-threads=1` | 退出码 0；19/19 通过，含新的 PID 输出前缀回归。 |
| `python3 -B -m unittest discover -s ci/tests -p test_registry_consumer_execution.py -v` | 退出码 0；2/2 通过。 |
| `python3 -B ci/tests/test_registry_consumer_execution.py -v` | 退出码 0；2/2 通过，直接入口有效。 |
| `bash -n .infra/project-ci-check.local.sh` | 退出码 0。 |
| `git diff --check` | 退出码 0。 |
| `./.infra/bin/align-ci.sh` | 最终改动对齐通过，退出码 0；未报告待修的风格错误或警告。 |
| `RUST_TEST_THREADS=1 RER_REDIS_6_2_SERVER=/tmp/codex-rer-20261110T161412476Z-redis-6.2.17/src/redis-server RER_REDIS_7_SERVER=/usr/bin/redis-server ./.infra/bin/ci-check.sh` | 最终执行通过，退出码 0。完整测试、文档样例和 doctests、feature matrix、package verification、`bounded_wire` 10 秒 fuzz smoke、本地 hook、覆盖率及 security audit 均通过。覆盖率：functions 95.08%（580/610），lines 96.15%（5767/5998），regions 95.16%（8991/9448）；均超过仓库门槛。security audit 扫描 153 项锁定依赖，未报告漏洞。loom 按仓库配置显示未配置并跳过。 |
| `bash -n .infra/project-ci-check.local.sh && bash .infra/project-ci-check.local.sh` | 最终补跑退出码 0；文档版本 9/9、package candidate 8/8、registry manifest 5/5、registry target 12/12、registry execution 2/2 通过，preflight fixtures 通过。 |

完整 CI 诊断过程中，首次执行受 Docker socket 权限及缺少 Redis 6.2 本地 binary 阻塞；按仓库现有 fallback 环境变量，用已有 Redis 6.2.17 源码归档在任务临时目录构建本地 binary。第二次全量执行发现测试并发造成的 client timing 测试干扰，保持测试不变，使用单线程重跑。随后定位并修复上述 PID 标记解析基线问题。最终完整 CI、coverage、审计和 hook 均在修复后的完整代码上通过。

## Git 写入边界

本轮提交包括四项计划目标、必要的 fixture 测试基线修复，以及该证据文档。以下用户提供的原始文档必须留在磁盘且不加入本轮暂存/提交：

- `/data/working/qubit/rust-common/rs-event-bus-redis/doc/design-20261110T161412476Z-031337cb.md`
- `/data/working/qubit/rust-common/rs-event-bus-redis/doc/implementation-plan-20261110T161412476Z-031337cb.md`
