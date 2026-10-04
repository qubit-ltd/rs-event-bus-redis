# 变更记录

## Unreleased（尚未发布）

当前工作树的包版本为 `0.7.0`；这些变更尚未发布，本文不声称已有发布标签。

### 迁移说明

- 新增公开的进程内 `RedisProviderDiagnostics::snapshots()`，可读取存活的同步/异步
  SPI 实例。快照包含实例 ID、模式、namespace、三个准入 gauge，以及覆盖拒绝、
  连接、发布、未知结果、认领、隔离和 Gap 的十一个饱和计数器。ID 与计数随进程
  或实例生命周期重置，多个字段并非原子快照；facade 指标和 Redis PEL/内存仍须
  分别采集。[运维指南](doc/user_guide.zh_CN.md#11-怎样维护消费组和处理下游通知)
  列出命令、告警示例与人工保留清单。

- 连接和命令现在默认使用有限等待预算。若默认 2,000 毫秒不适合部署环境，
  显式设置 `redis.connect_timeout_ms` 和 `redis.command_timeout_ms`，两者均接受
  1–60,000 毫秒。阻塞读取的响应预算为实际 BLOCK 时长加命令预算。
  receive deadline 约束调度；同步 DNS、多地址尝试及持续 socket 数据传输可能
  使整体调用超过墙钟预算。
- 按新增的每个 client 准入与尺寸限制检查现有工作负载：

  | 配置项 | 默认值 | 合法范围 |
  | --- | --- | --- |
  | `redis.max_concurrent_commands` | 64 | 2–4,096 |
  | `redis.max_active_receivers` | 256 | 1–4,096 |
  | `redis.max_payload_bytes` | 1,048,576 | 1–67,108,864 |
  | `redis.max_wire_bytes` | 8,388,608 | 1–268,435,456 |
  | `redis.max_idle_connections` | 8 | 1–64 |

  闲置连接上限不得超过命令准入上限，wire 上限不得小于 payload 上限。命令准入
  低于八时，也要下调闲置连接上限。payload 加上编码后的元数据仍须符合 wire
  限制。Sentinel 配置最多接受 16 个 endpoint。准入名额耗尽立即返回
  `resource_limit`，应用可以退避。历史超限记录会被隔离并 ACK，返回 Gap。
- 显式处理 `outcome_unknown`。发布的回复丢失时，消息仍可能已写入 Redis。
  这类错误标为不可重试，provider 不会透明重放。不要盲目重试；使用应用标识符，
  并明确选择可能产生重复消息时的处理策略。outbox 仍由应用负责。
- XACK 结果未知后，原 Accept 或 Reject 意图在该存活 receiver/token 内固定。
  只能重试相同意图，不能改为 Retry 或相反的终结意图；后续明确 Redis 错误也
  不能解除原先未知结果的约束。相同意图重试收到 XACK=0 时可幂等完成。
  首次尝试明确在 XACK 应用前被拒绝时，决策可以保持开放。
- 公开 provider 错误新增结果、资源及尺寸相关变体。更新穷举匹配，并结合上下文
  检查 SPI `kind()` 与 `retryable()`：未知 receive/settlement 允许恢复，但未知
  发布或隔离结果不表示可安全重放。取消释放本地准入名额，不保证 Redis 已停止
  在途命令。
- 公开合同测试现在镜像生产模块路径，crate 内部测试放在 `src/tests`。更新按旧
  测试位置筛选的脚本，并运行 sync/async conformance 及 all-features 配置。

### 保留的数据与生命周期合同

Redis key 命名、`wire` 字段、wire version 1 和 payload 字节数组编码保持不变，
已有数据无须格式迁移。在 wire 尺寸限制以内的未知版本仍保留 pending。
Close 和 Drop 不会自动 ACK，也不删除 consumer、group 或 stream。隔离脚本
排除执行过程中的交错操作，但不提供回滚或 exactly-once 隔离保证。
源 stream 默认无限保留；近似 `XADD MAXLEN ~` 必须同时配置
`redis.stream_maxlen_approx` 和 `redis.allow_lossy_retention=true`，可能丢失未读或
pending 历史。隔离流的保留须由运维单独决定；本次没有新增自动裁剪或指标 exporter。

配置、恢复和 consumer 运维见[用户指南](doc/user_guide.zh_CN.md)，状态及资源合同
见[设计说明](doc/design.zh_CN.md)。
