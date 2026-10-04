# Redis Streams Provider 设计说明

[English](design.md) · [用户指南](user_guide.zh_CN.md) · [README](../README.zh_CN.md)

本文记录当前未发布工作树的实现合同及边界，Cargo 版本为 `0.7.0`。测试与基准报告另行提供验收证据；本文不表示已经发布新版本，也不宣称未经测量的性能结果。

## 职责与支持范围

provider 实现同步和运行时中立的异步 event-bus SPI。facade 负责类型化 codec、handler 执行和应用策略；Redis Streams 负责保留记录、消费组游标和待处理条目。业务幂等、可靠通知事务、Redis 持久化/复制及保留策略由应用或部署负责。

同步、异步实现共用纯命令构造、协议归一、接收调度、wire 解码及结算决策，I/O、超时和取消仍由各自 adapter 处理。同步 API 不通过隐藏 executor 运行。支持 Redis 6.2+ standalone/Sentinel 与编码 payload；不提供 Cluster、TLS 配置、延迟投递、native payload、业务死信路由或恰好一次处理。

| 实现位置 | 职责 |
| --- | --- |
| `src/sync/subscription.rs`、`src/async/subscription.rs` | adapter I/O、取消边界与共享决策调用 |
| `src/internal/settlement_progress.rs`、`settlement_state.rs` | 允许的意图及本地账本原子提交 |
| `src/internal/receive_driver.rs`、`recovery_scan_budget.rs` | 每次 dispatch 一个动作、截止时间及扫描/维护配额 |
| `src/client.rs`、`src/client/internal/` | 受控传输、准入名额、standalone 连接池/cache 及 Sentinel 探测 |
| `src/internal/transport_policy.rs` | 连接/命令等待与实际 BLOCK 裕量 |
| `src/wire_fields.rs`、`src/wire_fields/internal/`、`src/internal/decode.rs` | 有界 provider 编码、版本/深度策略及 receiver 绑定 token 构造 |
| `src/poison.rs` | 在 Redis 内检查 owner、复制隔离记录并 ACK |

## Provider 诊断与保留边界

公开的 `diagnostics` 模块提供 `RedisProviderDiagnostics::snapshots() -> Vec<RedisProviderSnapshot>` 与 `RedisProviderMode::{Sync, Async}`。快照可通过只读 getter 获取 `instance_id`、`mode`、`namespace`，三个实时 gauge（`general_in_flight`、`settlement_in_flight`、`active_receivers`）及十一个饱和计数器（`command_rejections`、`receiver_rejections`、`connection_attempts`、`connection_failures`、`publish_accepted`、`publish_unknown`、`receive_unknown`、`settlement_unknown`、`recovery_claim_commands`、`quarantine_succeeded`、`delivery_gaps`）。普通名额包含从普通通道准入的结算，结算预留名额单独计算。连接尝试是 provider 实际打开连接的一次调用，不按 Sentinel 探测节点数或池命中次数计算；恢复计数针对命令，不针对认领条目；未知结果只统计有返回值的 SPI 调用。

SPI 成功创建时分配进程内唯一且不复用的 ID。client 强引用诊断状态，进程目录仅保存弱引用，实例释放后即从快照中消失。`snapshots()` 在短锁内获取存活实例，再于锁外读取原子值并按 ID 排序；多个字段并非同一时刻的事务快照。快照不包含 URL、凭据、payload 或原始 Redis 错误，ID 也不跨进程重启保持不变。facade delivery metrics 与 Redis PEL/内存须另外采集；采集和告警见[运维指南](user_guide.zh_CN.md)。

本次不引入 exporter、后台采集、自动 `XTRIM`、stream/group/consumer 删除、Redis 侧 EventId 索引或新 wire 格式。源 stream 默认无限保留。选择 `XADD MAXLEN ~` 时，必须同时配置 `redis.stream_maxlen_approx` 与 `redis.allow_lossy_retention=true`，并接受未读或 pending 历史可能丢失。运维须先确认所有 group、PEL、未读位置、未来回放和审计期限；隔离流单独增长。

## 结算意图与本地提交

token 保存 Redis 坐标、receiver 身份和共享进度。终结路径为 Open → AckPending(原终结意图) → Applied(原意图)；Retry 则从 Open 直接在本地进入 Applied(Retry)，不执行 ACK。再次请求已提交的相同意图会直接成功，冲突请求不执行 I/O。

新终结意图在取得连接和命令名额后才固定。XACK 可能发送前通过短临界区进入 AckPending，不持有标准 mutex 跨 await 或网络操作。结果未知或取消保留原意图，仅允许相同 Accept/Reject 重试。合法 XACK 整数回复 0 和 1 都允许完成同意图结算。本地提交先锁进度、再锁恢复状态，在没有 await 的临界区更新进度和活跃名额；锁失败不会虚报成功。

首个 XACK 尝试收到完整 Redis 顶层拒绝可以恢复 Open。此前已有未知结果时，后续拒绝不能证明前一次 ACK 未执行，因此不会重新开放 token。嵌套 RESP 错误、畸形回复也不构成明确未执行证据。close/drop 释放 receiver 资源，不伪造 Retry 或重建 PEL。意图只约束当前 token/receiver 生命周期，不跨重启持久化，也不对其他 consumer 提供 fencing。

## 接收调度与恢复

每次 dispatch 只取一次动作，仅在得到真实回复后提交归一反馈。每轮 claim 和本 consumer pending 扫描各最多 8 条命令；tombstone 维护最多执行一次 XPENDING、四次 XRANGE 和总计四次隔离 EVAL。共享游标及为 Gap 保留的记录跨 receive 调用延续，时钟预算属于单次调用。

恢复调度状态属于订阅，并跨 receive 调用共享。新订阅第一次 receive 会立即扫描 pending。完整恢复轮次结束后，下一轮在 `redis.recovery_interval_ms`（默认 1,000 毫秒）后到期；截止前的调用跳过 claim 与本 consumer pending 扫描，直接读取新记录。Retry、receive 失败或已开始执行的 async receive 被取消时，会标记下一次调用立即恢复。未 poll 的 future 被丢弃不会改变调度。未完成的恢复轮次不会推进截止时间。

零超时最多进行一次 claim、一次本 consumer pending 查询和一次新消息查询，均不带 BLOCK，可因 Message/Gap 提前返回；跳过 tombstone 扫描，允许隔离一条畸形记录。`Duration::MAX` 通过不超过一秒的有限 BLOCK 循环等待。有限截止时间停止新增恢复工作并限制所选 BLOCK，不能强制终止已发出的命令。恢复保留活跃去重、Gap 后延后交付的有效记录及 Redis 6.2 tombstone 修复。

## 连接策略与资源生命周期

| Provider option | 默认值 | 闭区间 / 关联限制 |
| --- | ---: | --- |
| `redis.connect_timeout_ms` | 2000 | 1–60,000 |
| `redis.command_timeout_ms` | 2000 | 1–60,000 |
| `redis.max_concurrent_commands` | 64 | 2–4,096 |
| `redis.reserved_settlement_commands` | 8 | 1–总命令数减 1 |
| `redis.max_active_receivers` | 256 | 1–4,096 |
| `redis.max_idle_connections` | 8 | 1–64，且不超过并发命令上限 |
| `redis.max_payload_bytes` | 1,048,576 | 1–67,108,864 |
| `redis.max_wire_bytes` | 8,388,608 | 1–268,435,456，且不小于 payload 限额 |
| `redis.sentinel.nodes` | 未设置 | 最多 16 个合法 host/port 地址 |

其余原有配置见[指南](user_guide.zh_CN.md)。新增有限配置拒绝 0、符号、溢出及非十进制文本。命令并发降至 8 以下时，空闲保留数也须降低。

standalone 同步短操作立即获取 RAII 命令名额，连接池锁不跨网络 I/O；每次 checkout 恢复命令 read/write 等待预算，I/O、协议错误或超时后丢弃连接。异步 standalone 短操作共享 multiplexed 连接；冷启动期间由异步 mutex 保证一次初始化，取消时空 cache 保持为空。单调递增代际使旧失败租约只能清除自身连接；溢出时拒绝，不循环复用。receiver 使用专用连接，响应预算为实际 BLOCK 加命令超时。

Sentinel 每次解析对配置节点各尝试一次，优先探测上次成功节点，查询 `SENTINEL get-master-addr-by-name`，校验 host/port，再验证候选 ROLE=master。Sentinel/master ACL 分开，setup、探测及目标命令均受等待策略约束。master socket 不进入 standalone 连接池/cache。ROLE 后仍可能切换，写入失败后不会透明重放 XADD。

短命令总额度分为普通与结算通道；普通命令耗尽时，保留名额仍允许 XACK 准入。receiver 连接不占用短命令名额。同一个已创建 SPI 实例及其 Arc clone 共用命令、receiver 预算，新的 `create_configured` 调用获得独立预算。registry 不会合并各实例预算，Redis 也没有全局 provider 准入上限；provider 不维护无界等待队列。receiver 在 setup 前取得名额，close/drop 释放，token 存活不会延长占用。失败、取消释放本地命令名额，multiplexed driver 或 Redis 仍可能随后完成在途请求。限额不能约束所有 server 任务/socket，也不是整个进程全部 client 的总上限。关闭资源不需要新增 receiver 名额。

同步超时只是每阶段、每次 I/O 等待的软限制。DNS、多地址尝试、setup 和持续小包都可能使整体调用超出预算。receive 截止时间约束调度，不是绝对墙钟期限。本设计不通过无法取消的辅助线程承诺同步硬截止时间。

## 结果确定性与错误合同

adapter 按实际操作阶段分类失败，不额外定义两状态 outcome enum。脱敏的 `RedisProviderError` 转换为稳定的 `SpiError::Operation` 类别。

| 结果 | kind | 重试提示及含义 |
| --- | --- | --- |
| 发布未知 | `outcome_unknown` | `Some(false)`，应用主动重发可能重复 |
| 接收未知 | `outcome_unknown` | `Some(true)`，通过 PEL/claim 恢复 |
| 结算未知 | `outcome_unknown` | `Some(true)`，相同 token 和原终结意图 |
| 隔离未知/可能部分执行 | `outcome_unknown` | `Some(false)`，先检查源记录/PEL/owner |
| 准入拒绝 | `resource_limit` | `Some(true)`，该操作没有发出命令 |
| 发布字节超限 | `payload_too_large` / `wire_too_large` | `Some(false)`，不发送 XADD |
| wire 限额内的未知版本 | `unsupported_wire_version` | `Some(false)`，保留 PEL |

配置和已知 Redis 拒绝仍使用脱敏分类。retryable 是上下文提示，不表示自动重试或无重复保证。业务命令发送前的失败不会固定结算意图。错误不包含 URL 密钥、Redis 原始文本或 payload。

## Wire v1 与隔离

wire 保持为 stream 的 `wire` 字段中的版本 1 JSON，保存 byte-array 编码 payload 和元数据。发布先检查原始 payload 长度，通过 Write sink 限制外层 wire 和中间 headers JSON，不先构造无限增长的最终 String。provider 编码借用调用方 event ID、content type、schema ID、ordering key 和 payload，不复制任意长度的元数据；只分配受限的 headers JSON 和完整 wire String。公开的 `WireFields::from_outbound` 便捷转换是另一条路径，不应用 provider 配置限额。接收借用 Redis wire 原始字节，先限额再解析 UTF-8/JSON，先读取小版本结构，再读取 typed v1 字段并检查解码 payload 长度；版本 1 还通过常量空间计数器扫描结构深度，包括被忽略的字段：最多接受 127 层容器，拒绝第 128 层；保留 Serde 递归限制。未知版本在 v1 字段形状和深度检查前返回。版本探针忽略字段的 scratch 可随嵌套增长，但已先执行 wire 字节限额；不能因此将整个 decoder 描述为常量空间。

wire 限额优先：超限历史记录作为 poison，即使其版本本来未知。wire 限额内的未知 `u64` 整数版本报错并保留 PEL，其余畸形记录和超限 v1 payload 隔离、ACK 并返回 Gap。限制约束 provider 追加解析、复制分配，不保证 RESP 库接收任意 bulk 的硬内存上限或应用整体内存上限。

隔离 Lua 在检查源/目标类型及当前 PEL owner 后，于 Redis 内读取 wire、复制隔离记录，再执行 XACK。重复 `wire` 字段按最后一个值生效，与 Redis owned parser 一致。脚本执行中不能插入其他命令，但后续失败不会回滚已复制记录。回复丢失或部分失败可能产生重复隔离副本，须通过 `source_stream`、`group`、`source_id` 关联。OwnershipChanged 不确认其他 owner 的记录，源记录缺失不伪造 payload；tombstone 清除及隔离成功返回 Gap。隔离流保留由运维负责。

## 持久化、下游集成与迁移

Accept/Reject 确认 Redis PEL，Retry 只释放本地占用并保留 PEL。`XREADGROUP >` 返回的新消息携带 `provider_attempt = Some(1)`；pending 和 `XAUTOCLAIM` 恢复路径因当前没有传递历史次数而保持未知（`None`）。关闭不 ACK，不删除消费组或 stream。可选 `XADD MAXLEN ~` 会丢失未读/pending 历史；未读记录被裁剪不一定返回 Gap。至少一次投递要求业务幂等，claim idle 阈值应匹配 handler 时长。

XADD Accepted 不证明 fsync、副本持久化或业务完成。新 observer 连接上的 WAIT 无法为 provider 连接写入提供 fencing；Sentinel 验收应观察实际复制的消费组游标、PEL ID 和 owner。旧 consumer 清理须先停实例、确认 PEL 清空并满足业务保留要求，不引入自动 DELCONSUMER。

任务通知 consumer 按 TaskId 和最高 `state_version` 去重，拒绝旧版本，并查询任务服务的权威状态。通知失败不回滚已提交的任务或业务状态。Redis provider 本身不提供事务性 outbox；typed SQLite task service 为自身的任务生命周期转换提供可选 outbox 集成。

当前未发布变更引入有限超时、资源/字节默认值、新公开错误变体和未知结果规则。部署前检查限额，同时调整 idle/concurrency，停止盲目重试未知 publish，并保留未知 ACK 原意图；wire v1 不变。Cargo 版本为 `0.7.0`；发布和创建标签属于另外的操作。

## 验收证据

公开测试覆盖结算回复丢失/取消、接收命令序列、超时、资源限额、字节边界、隔离及 Redis/Sentinel 行为。独立下游 fixture 验证 runtime feature 和任务通知。文档验收从两种实际指南读取标记 Rust 代码块，按文档说明将模块片段与 main 组合，在独立 feature 图和真实 Redis 中编译运行，检查投递与空 PEL；example 二进制单独验证。

[覆盖率评估](coverage-review.zh_CN.md)区分历史数据和最终重构门禁，[工作负载基准](connection-reuse-benchmark.zh_CN.md)提供实际测量。文档构建成功或历史百分比不能证明当前全量验收已完成。
