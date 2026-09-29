# Redis 覆盖率评估

[English](coverage-review.md)

## 当前重构的验收

2026-09-29 第六次完整 CI 以状态 0 退出。其干净包级覆盖率测量通过原有门槛：

| 指标 | CI6 实测结果 | 门槛 |
| --- | ---: | ---: |
| 函数 | 384/402（95.52%） | 至少 95% |
| 行 | 3,755/3,913（95.96%） | 高于 90% |
| 区域 | 5,821/6,160（94.50%） | 高于 85% |

覆盖率执行链为 `ci-check.sh` → `project-hook` → `project-ci-check.sh` → `coverage.sh`，选择 `qubit-event-bus-redis`，使用 `--locked --all-features -- --test-threads=1`。文件汇总覆盖 39 个 provider `src` 文件，排除外部 `tests`、`src/tests`、`examples` 和上游包路径。被纳入文件中的内联私有测试与 helper 也参与 LLVM 汇总，因此这些指标不等于仅生产声明的覆盖率，其分母与手工 Rustdoc 声明计数不同。原始及处理后的报告、`ci-summary.json` 和 39 个文件汇总的函数、行及区域计数完全一致。

`coverage.sh` 在构建插桩示例及测量前清理 profile 数据。CI6 的全部 26 份 profile 都产生于本次开始时间 2026-09-29 13:54:33 UTC 之后，没有早于本次运行的 profile。实测源码 seal 包含 154 个 Rust 文件及 `coverage.sh`，共 155 项，SHA256 为 `02510d2bb99983c7a6b6977bebdec34bf99b83faaeaefc0a567bfab1eb7a9861`。基线 HEAD 为 `5e36aede24c5db5de2e282932546de2fe500e9c2`；实测变更当时尚未提交，因此该 HEAD 标识基线，不能代表全部实测源码。Cargo 版本仍为 `0.4.0`，变更尚未发布。

覆盖率 hook 执行 23 个 suite：302 项通过、0 项失败、1 项旧手工基准忽略。此前 verify 阶段有 310 项通过，包括 8 项 doctest，另有 1 项基准忽略。两组计数对应不同执行，不累计 feature matrix 重复次数。配置中的九种组合均通过：默认、无默认 feature、sync、async、sync+discovery、async+discovery、sync+conformance、async+conformance 及 all-features。完整 CI 还通过严格 style/Clippy/Rustdoc、README 检查、release 构建、打包验证，以及覆盖 148 个依赖的安全检查。另有五项 locked 最小 feature/discovery 检查也在独立运行中通过，其测试执行不重复累计到覆盖率 hook 的 302 项。

CI6 在 `x86_64-unknown-linux-gnu` 上执行 `RS_INFRA_ARTIFACT_CLEANUP=0 ./ci-check.sh`，使用 rustc `1.94.0`（`4a4ef493e`，LLVM `21.1.8`）、cargo-llvm-cov `0.8.6` 及固定的 style toolchain `nightly-2026-06-05`。父进程未覆盖 `CARGO_INCREMENTAL`、`RUSTFLAGS`、`RUSTDOCFLAGS`、`LLVM_PROFILE_FILE` 或 `RUST_TEST_THREADS`；覆盖率工具会派生插桩环境，测试命令显式使用单测试线程。这些父进程环境事实不表示插桩子进程变量未设置。实测依赖为本地 `qubit-event-bus` `0.16.0`，此次运行不能证明 registry 已可用。

| 产物 | SHA256 |
| --- | --- |
| `target/infra/coverage/raw.json` 与 `coverage.json`（字节相同） | `2f557b11291b3e306b64bacd89733f05d77137ada777e6bb132c243e9b52d025` |
| `ci-summary.json` | `de63bce0b50cf2fb759a88750acfacefdfaad8f0491cf6783c762958e4f21f88` |

## 当前文件汇总中的缺口

CI6 的 LLVM 文件汇总中，仅以下七个文件存在未覆盖函数：

| 源码文件 | 已覆盖/总函数数 | 未覆盖 |
| --- | ---: | ---: |
| `src/async/async_redis_event_bus.rs` | 21/22 | 1 |
| `src/async/async_redis_event_bus_provider.rs` | 5/6 | 1 |
| `src/async/subscription.rs` | 56/58 | 2 |
| `src/sync/redis_event_bus.rs` | 19/20 | 1 |
| `src/sync/redis_event_bus_provider.rs` | 3/4 | 1 |
| `src/sync/subscription.rs` | 48/58 | 10 |
| `src/sync/subscription/internal/receive_command.rs` | 6/8 | 2 |

这些计数合计为 384/402 指标中的 18 个未覆盖函数。文件汇总可能包含闭包及内联私有测试/helper，并非 18 个生产声明的清单。原始零计数编译实例不代表独立函数，不能替代上述汇总计数。下方历史 4/293 的源码分组诊断属于另一快照。

[用户指南](user_guide.zh_CN.md)和[设计说明](design.zh_CN.md)解释当前错误合同：未知 XACK 后只允许重试原 settlement 意图。Markdown 验收直接提取双语全部十个 Rust 块，在独立 Cargo 项目中构建并对真实 Redis 执行。[工作负载基准](connection-reuse-benchmark.zh_CN.md)记录独立的工作负载实测结果，忽略的旧手工基准不是该报告的验收运行。

## 最终验收前的诊断尝试

CI3 的覆盖率 hook 有 285 项通过、1 项基准忽略，随后因函数门槛失败：335/383（87.47%）；行是 3,112/3,356（92.73%），区域是 4,651/5,098（91.23%）。它以状态 1 退出，未生成处理后的报告，也未进入依赖安全检查。CI4 以状态 0 完成，但覆盖率包含 26 份旧 profile 和 26 份本次 profile，不能证明干净覆盖率。CI5 在准入测试连接 setup 阶段以状态 1 退出，尚未进入覆盖率。两个准入 fixture 的 setup 预算增加至 3,000 ms 后，两个 focused 回归均通过，随后完成上述干净的 CI6。

## 历史报告来源

下方 306/312、2,910/3,038、4,591/4,812 来自 commit `0dc7a551f0c91d57d612d78644c8258671b0ca56` 的记录，使用尚未发布的上游 `qubit-event-bus` 0.15.0 快照本地 patch。另一个 293 函数分析来自 commit `1f833e944a53cdbeb7c8ffd571ca6dea6390cf5f`。本轮重构未独立重新核验其原始报告，两组分母描述不同源码快照。历史“失败后改用 Retry”的描述不能证明未知 XACK 后的安全性。

## 历史覆盖率：0.4.0 候选快照

项目使用 `coverage.sh` 和 `ci-check.sh` 收集的包级指标执行覆盖率门槛检查。
0.4.0 发布候选历史快照的结果如下：

| 指标 | 0.4.0 候选版本 | 要求 |
| --- | ---: | ---: |
| 函数 | 306/312（98.08%） | 至少 95% |
| 行 | 2,910/3,038（95.79%） | 高于 90% |
| 区域 | 4,591/4,812（95.41%） | 高于 85% |

历史报告记载该测量的三个门槛均通过。函数、行和区域总数来自当时完整 `ci-check.sh`
覆盖率运行。部分截止时间和恢复间隔测试依赖计时，区域计数在不同运行之间
可能略有变化。

2026-09-29 使用仓库固定的工具链及默认测试并发度，先运行 `./align-ci.sh`，
再运行完整 `./ci-check.sh`。默认及 all-features 测试、feature matrix、严格
Clippy/Rustdoc、打包验证、覆盖率和依赖安全检查均通过。当时使用了指向尚未
发布的 `qubit-event-bus` 0.15.0 本地快照的隔离 Cargo patch；该结果不能证明
上游 registry 已可用。当时未重新运行手工连接复用基准，历史报告引用的基准数据
属于更早的快照。

## 历史补充：293 个函数的快照

当时 LLVM 的详细 JSON 报告路径是 `target/infra/coverage/raw.json`。其中包含编译器
生成的错误映射闭包，以及同一源码函数的多个编译实例。应按源码文件和首个
区域的起止坐标合并记录，仅将所有实例执行次数均为零的组标记为未覆盖。
直接统计所有零计数实例会夸大缺口。

按此方法确认了最初的 26 个未覆盖源码函数或闭包：

| 源码及行为 | 最初未覆盖 | 历史补测覆盖 |
| --- | ---: | ---: |
| 同步 subscription：超时溢出、畸形 claim 响应、XPENDING 命令及解析错误、XRANGE 失败、墓碑确认失败、pending 及非阻塞 XREADGROUP 失败 | 8 | 8 |
| 异步 subscription：上述路径，以及保留 claim 的过滤、阻塞 XREADGROUP 失败、settlement 重连失败 | 11 | 11 |
| 同步及异步 bus：XGROUP 重试时重连失败 | 2 | 2 |
| standalone client：连接池锁中毒 | 1 | 1 |
| 同步及异步 bus：JSON 序列化和消费者身份生成的错误映射 | 4 | 0 |

恢复修复增加了一个被实际执行的过滤闭包，因此该历史快照的函数总数为 293。

## 历史故障测试

`tests/redis_fault_tests.rs` 通过真实 Redis 客户端的 TCP 连接测试公共 SPI。
`tests/support/scripted_redis.rs` 绑定本机临时端口，按顺序检查业务命令，
返回指定的 RESP2 响应，或在回复前断开连接；客户端初始化命令单独处理。
辅助服务检查脚本是否完整执行、记录意外命令、限制 socket I/O 等待时间，
并在退出时关闭连接和等待所有工作线程结束。

测试精确验证 SPI 操作、资源、重试策略和脱敏后的错误类别，确保 Redis 原始
诊断及嵌套错误源不会泄漏。每次可恢复的接收故障后，同一个 subscription
还必须能成功接收健康响应。独立脚本验证断线重试保持完全相同的组创建参数、
重连失败，以及 settlement 失败后尚未应用的 token 仍能接受 Retry。
超时溢出必须在发送恢复命令前被拒绝。

原连接池锁中毒测试构造了一个没有 standalone endpoint 的 client，实际执行
的是 Sentinel 缺失路径，因此断言通过却没有验证连接池。测试现在构造
standalone client，并精确检查连接池锁中毒错误。

基于 Docker 的 Redis 6.2/7 测试继续验证真实 Redis 行为、持久性、Sentinel
切换、隔离脚本和所有权语义。原有读取错误回归还补充了服务重启及重建消费者
组后恢复接收的验证。

## 历史测试发现的恢复缺陷

Redis 7 的 claim 响应可能同时包含可交付记录和已删除的 pending ID。订阅先
报告 gap，再保留可交付记录供下次 receive 使用。同步实现的链式条件保留了
临时恢复锁守卫，又在交付保留记录时尝试获取同一个锁，导致第二次 receive
死锁。

修复前，带完成超时保护的回归测试稳定失败。同步实现现在在单个独立作用域内
取出并过滤保留记录，在解码和更新交付状态前释放锁。同步和异步回归均验证
gap 后能交付保留记录，且无须额外 Redis 读取。

## 历史快照剩余四个未覆盖映射

剩余零计数组位于同步和异步 bus 的 `publish`、`subscribe` 中：

- 固定 `WireFields` 记录的 JSON 序列化错误映射。该历史 wire 格式中的字符串、
  整数、可选字符串和字节数组能够正常序列化，Redis 协议注入无法触发这一
  本地序列化错误。
- 操作系统熵源不可用时的消费者身份生成错误映射。正常 UUID 生成已覆盖，
  这些测试没有诱发操作系统熵源失败。

保留这些防御性错误处理后，未覆盖项占 4/293，该历史快照的原有 95% 函数门槛通过。
Redis 传输和协议故障的测试无须增加生产注入接口。
