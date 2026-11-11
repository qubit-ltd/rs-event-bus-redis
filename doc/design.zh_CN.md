# Redis Streams Provider 设计说明

[English](design.md) · [用户指南](user_guide.zh_CN.md) · [README](../README.zh_CN.md)

本文记录当前未发布工作树的实现合同及边界，Cargo 版本为 `0.7.0`。测试与基准报告另行提供验收证据；本文不表示已经发布新版本，也不宣称未经测量的性能结果。

## 职责与支持范围

provider 实现同步和运行时中立的异步 event-bus SPI。facade 负责类型化 codec、handler 执行和应用策略；Redis Streams 负责保留记录、消费组游标和待处理条目。业务幂等、可靠通知事务、Redis 持久化/复制及保留策略由应用或部署负责。

同步、异步实现共用纯命令构造、协议归一、接收调度、wire 解码及结算决策，I/O、超时和取消仍由各自 adapter 处理。同步 API 不通过隐藏 executor 运行。支持 Redis 6.2+ standalone/Sentinel 与编码 payload；单实例和 Sentinel 连接可按需启用并校验证书的 TLS。不提供 Cluster、延迟投递、native payload、业务死信路由或恰好一次处理。

### TLS 传输

`redis.url` 决定主节点连接方式：`redis://` 保持明文，`rediss://` 要求 TLS。redis-rs rustls 会验证证书链和主机名，禁用验证的 TLS URL 会被拒绝。TLS 配置、证书、主机名或握手失败后不会降级为明文，也不会明文重试。Sentinel 模式下 URL 的 host 不用于发现主节点，但 scheme 决定发现出的主节点是否使用 TLS；Sentinel 节点则由独立的 `redis.sentinel.tls` 控制。

主节点与 Sentinel 的 CA、mTLS 身份分别配置：主节点使用 `redis.tls_ca_cert_path`、`redis.tls_client_cert_path`、`redis.tls_client_key_path`；Sentinel 使用 `redis.sentinel.tls_ca_cert_path`、`redis.sentinel.tls_client_cert_path`、`redis.sentinel.tls_client_key_path`。客户端证书与私钥必须成对设置。指定 CA 后，该端点使用自定义根；否则使用 rustls Web PKI 根。每个 PEM 文件须非空且最多 1 MiB。provider 构造时读取并解析文件；公开 TLS 配置错误不含路径、PEM 或依赖库诊断。已创建实例不会重载证书，轮换时需构造新的 provider 实例。TLS 只影响传输加密和对端身份，不改变持久化、复制、至少一次投递或 `XADD` 结果未知时的语义。不支持 Cluster TLS、证书热加载或关闭证书验证。

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

新终结意图在取得连接和命令名额后才固定。结算 EVAL 可能发送前通过短临界区进入 AckPending，不持有标准 mutex 跨 await 或网络操作。单 key EVAL 先用 XPENDING 检查指定 PEL 行 owner，仅匹配时调用 XACK。结果未知或取消保留原意图，仅允许相同 Accept/Reject 重试。合法 EVAL 整数回复 0 和 1 都允许完成同意图结算；-1 返回不可重试的 `settlement_ownership_changed`，保留 AckPending 和活跃名额。本地提交先锁进度、再锁恢复状态，在没有 await 的临界区更新进度和活跃名额；锁失败不会虚报成功。

首个 EVAL 尝试收到完整 Redis 顶层拒绝可以恢复 Open。此前已有未知结果时，后续拒绝不能证明前一次结算未执行，因此不会重新开放 token。嵌套 RESP 错误、畸形回复也不构成明确未执行证据。PEL 为空仍按幂等完成处理；owner 已变化时拒绝结算。该 Redis owner 校验不会阻止仍在运行的旧 handler 产生外部副作用，也不覆盖 consumer 名称 ABA。close/drop 释放 receiver 资源，不伪造 Retry 或重建 PEL。意图只约束当前 token/receiver 生命周期，不跨重启持久化，也不对其他 consumer 提供 fencing。

## 接收调度与恢复

每次 dispatch 只取一次动作，仅在得到真实回复后提交归一反馈。每轮 claim 和本 consumer pending 扫描各最多 8 条命令；tombstone 维护最多执行一次 XPENDING、四次 XRANGE 和总计四次隔离 EVAL。共享游标及为 Gap 保留的记录跨 receive 调用延续，时钟预算属于单次调用。

恢复调度状态属于订阅，并跨 receive 调用共享。新订阅第一次 receive 会立即扫描 pending。完整恢复轮次结束后，下一轮在 `redis.recovery_interval_ms`（默认 1,000 毫秒）后到期；截止前的调用跳过 claim 与本 consumer pending 扫描，直接读取新记录。Retry、receive 失败或已开始执行的 async receive 被取消时，会标记下一次调用立即恢复。未 poll 的 future 被丢弃不会改变调度。未完成的恢复轮次不会推进截止时间。

零超时最多进行一次 claim、一次本 consumer pending 查询和一次新消息查询，均不带 BLOCK，可因 Message/Gap 提前返回；跳过 tombstone 扫描，允许隔离一条畸形记录。`Duration::MAX` 通过不超过一秒的有限 BLOCK 循环等待。有限截止时间停止新增恢复工作并限制所选 BLOCK，不能强制终止已发出的命令。恢复保留活跃去重、Gap 后延后交付的有效记录及 Redis 6.2 tombstone 修复。

共享响应 parser 会在生成解析结果前校验 `XAUTOCLAIM` 游标及所有返回的 stream entry ID。非法 ID 会使 receive 结果变为未知，不会推进已保存的恢复游标，也不会触发 ACK 或隔离；重连后会从最后一个有效游标继续。继续兼容 Redis 6.2 的 nil tombstone。

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

standalone 同步短操作立即获取 RAII 命令名额，连接池锁不跨网络 I/O；每次 checkout 恢复命令 read/write 等待预算，I/O、协议错误或超时后丢弃连接。异步 standalone 短操作分别使用普通与结算 multiplexed 连接，按需初始化并按角色共享。这样未回复的发布请求不会阻塞独立连接上的结算 EVAL；两个角色缓存分别按代际失效。同一冷初始化轮次中的调用共享其成功或失败终态；失败结束后，后续调用可以重试。初始化 future 被取消时，该轮仍在等待的调用可以用自己的初始化 future 接手。不使用后台初始化任务或失败冷却期，连接超时也不提供端到端时限保证。单调递增代际使旧失败租约只能清除自身连接；溢出时拒绝，不循环复用。receiver 使用专用连接，响应预算为实际 BLOCK 加命令超时。

Sentinel 每次解析对配置节点各尝试一次，优先探测上次成功节点，查询 `SENTINEL get-master-addr-by-name`，校验 host/port，再验证候选 ROLE=master。resolver 对每个 endpoint 和启用的 SPI 模式最多保留一条空闲 Sentinel 查询连接。sync 网络 I/O 不持有槽锁；async 使用 single-flight 初始化及按代际失效。如果异步 Sentinel 查询已经开始、但尚未收到回复时 future 被取消，缓存的连接代际会失效，下一次解析会重新建连；尚未开始的查询或已经收到回复的 future 被取消时不会因此失效。这只让后续操作避开 multiplexed 连接上的旧回复，不保证 Sentinel 已停止处理旧请求。每次操作仍查询 Sentinel，并为 ROLE 和业务命令新建候选 master 连接。空闲 Sentinel 连接失效时，本轮该 endpoint 可能失败；下一次操作会重新建连，不增加本轮 endpoint 尝试次数。Sentinel/master ACL 分开，setup、探测及目标命令均受等待策略约束。候选 master socket 不进入 standalone 连接池/cache。ROLE 后仍可能切换，写入失败后不会透明重放 XADD。Sentinel socket 和用于普通短命令的候选 master socket 使用固定 1 MiB RESP 响应帧预算，并在 socket 生命周期内持续生效。receiver master socket 保留自身配置的帧预算，因此合法且大于 1 MiB 的 receiver 预算不会被缩小。这是响应帧上限，不是 RSS 上限，也不限制发布请求体。

短命令总额度分为普通与结算通道；普通命令耗尽时，保留名额仍允许 XACK 准入。receiver 连接不占用短命令名额。同一个已创建 SPI 实例及其 Arc clone 共用命令、receiver 预算，新的 `create_configured` 调用获得独立预算。registry 不会合并各实例预算，Redis 也没有全局 provider 准入上限；provider 不维护无界等待队列。receiver 在 setup 前取得名额，close/drop 释放，token 存活不会延长占用。发布先校验 event type 和原始 payload 大小，再取得一个普通命令名额；该名额随后覆盖有界 wire 编码、连接建立和 XADD。因此普通额度已满时会先返回 `resource_limit`，不会先执行编码并发现 headers/wire 错误；event type 和原始 payload 错误仍优先。名额也限制并发编码，但不代表进程 RSS 上限。失败、取消释放本地命令名额，multiplexed driver 或 Redis 仍可能随后完成在途请求。限额不能约束所有 server 任务/socket，也不是整个进程全部 client 的总上限。关闭资源不需要新增 receiver 名额。

同步超时只是每阶段、每次 I/O 等待的软限制。DNS、多地址尝试、setup 和持续小包都可能使整体调用超出预算。receive 截止时间约束调度，不是绝对墙钟期限。本设计不通过无法取消的辅助线程承诺同步硬截止时间。

## 结果确定性与错误合同

adapter 按实际操作阶段分类失败，不额外定义两状态 outcome enum。脱敏的 `RedisProviderError` 转换为稳定的 `SpiError::Operation` 类别。

| 结果 | kind | 重试提示及含义 |
| --- | --- | --- |
| 发布未知 | `outcome_unknown` | `Some(false)`，应用主动重发可能重复 |
| 接收未知 | `outcome_unknown` | `Some(true)`，通过 PEL/claim 恢复 |
| 结算未知 | `outcome_unknown` | `Some(true)`，相同 token 和原终结意图 |
| 隔离 EVAL 的顶层权限拒绝（`NOPERM`、`NOAUTH`、`WRONGPASS`） | `authentication` | `Some(false)`，Redis 在脚本执行前拒绝了 EVAL |
| 隔离未知/可能部分执行 | `outcome_unknown` | `Some(false)`，先检查源记录/PEL/owner |
| 准入拒绝 | `resource_limit` | `Some(true)`，该操作没有发出命令 |
| 发布字节超限 | `payload_too_large` / `wire_too_large` | `Some(false)`，不发送 XADD |
| wire 限额内且完整有效的未知版本 | `unsupported_wire_version` | `Some(false)`，保留 PEL；无效或嵌套过深的 JSON 会隔离 |

订阅创建只接受 RESP `+OK` 状态作为 `XGROUP CREATE` 成功确认。nil、整数、字符串、数组或嵌套错误回复均属畸形，返回 `operation=subscribe`、`kind=outcome_unknown`、`retryable=Some(false)`，且不增加额外重试。命令发送后发生超时或传输错误也表示结果未知，最多再尝试一次。若重试连接获取失败、Redis 拒绝重试，或重试再次发生传输错误/畸形回复，结果仍保持未知，因为后一次观察不能证明首次命令没有创建 group。重试收到 `+OK` 可确认创建；收到 `BUSYGROUP` 时，`New` 和显式 `resume` 可沿用已有游标，显式 `Earliest` 或 `At` 配合 `reject` 时仍返回未知。首次直接收到 `BUSYGROUP` 则是明确的已有组结果；显式使用 `Earliest` 或 `At` 时仍按 `existing_group_start_position_ignored` 策略处理。provider 不移动游标、不 ACK、也不删除任何对象。恢复前检查 `XINFO GROUPS` 和 `XPENDING`；仅当业务接受已有游标时使用 `resume`，若需新回放位置则选用新的 group。

配置和已知 Redis 拒绝仍使用脱敏分类。retryable 是上下文提示，不表示自动重试或无重复保证。业务命令发送前的失败不会固定结算意图。错误不包含 URL 密钥、Redis 原始文本或 payload。

## Wire v1 与隔离

wire 保持为 stream 的 `wire` 字段中的版本 1 JSON，保存 byte-array 编码 payload 和元数据。发布先检查原始 payload 长度，通过 Write sink 限制外层 wire 和中间 headers JSON，不先构造无限增长的最终 String。provider 编码借用调用方 event ID、content type、schema ID、ordering key 和 payload，不复制任意长度的元数据；只分配受限的 headers JSON 和完整 wire String。公开的 `WireFields::from_outbound` 便捷转换是另一条路径，不应用 provider 配置限额。接收先限制完整序列化 wire 字节，再解析并校验完整 JSON 文档，同时选择版本。Serde JSON 递归保护适用于所有版本和被忽略字段；无效或嵌套过深的 JSON 属于格式错误。只有完整有效的未知版本才会在不应用版本 1 字段布局的情况下返回 UnsupportedWireVersion。之后版本 1 再进行有界 typed 解码，并应用 payload 与 headers 限额。版本探针忽略字段的 scratch 可随嵌套增长，但已先执行 wire 字节限额；不能因此将整个 decoder 描述为常量空间。

wire 限额内且完整有效的未知 `u64` 整数版本会报错并保留 PEL；即使声明未知版本，无效或嵌套过深的 JSON 仍会隔离。wire、payload 或 headers 配置容量超限时返回 `receive_limit_exceeded`，源记录保留在 PEL 中，不确认也不隔离；其余确定性格式错误仍会隔离、ACK 并返回 Gap。此外，专用 receiver 连接会在构造 Redis 响应值前，把完整 RESP 响应帧限制为 `redis.max_wire_bytes + 65,536` 字节；普通连接沿用现有 parser 策略。这只是响应帧上限，不是进程 RSS 上限。receiver 响应帧超限时，provider 会关闭连接、释放 receiver 准入，以 `receive_response_too_large` 停止订阅，并保留可能产生的 PEL 记录。操作人员应先检查 `XPENDING`；由于 provider 未能可靠读取完整记录 ID 和 wire 数据，是否删除或隔离须人工决策。

隔离 Lua 在检查源/目标类型及当前 PEL owner 后，于 Redis 内读取 wire、复制隔离记录，再执行 XACK。重复 `wire` 字段按最后一个值生效，与 Redis owned parser 一致。顶层 EVAL 收到权限错误码（`NOPERM`、`NOAUTH` 或 `WRONGPASS`）表示 Redis 在脚本运行前拒绝命令，分类为不可重试的 `authentication`。脚本启动后的错误（包括内部命令或 key 的 ACL 拒绝）、无效状态回复、超时、断线和回复丢失均分类为 `outcome_unknown`；脚本执行中不能插入其他命令，但后续失败不会回滚已复制记录。未知结果可能已经写入副本，应同时检查源 PEL 和隔离流。回复丢失或部分失败可能产生重复隔离副本，须通过 `source_stream`、`group`、`source_id` 关联。OwnershipChanged 不确认其他 owner 的记录，源记录缺失不伪造 payload；tombstone 清除及隔离成功返回 Gap。隔离流保留由运维负责。

## 持久化、下游集成与迁移

Accept/Reject 确认 Redis PEL，Retry 只释放本地占用并保留 PEL。`XREADGROUP >` 返回的新消息携带 `provider_attempt = Some(1)`。每条准备交付的恢复记录会先额外执行一次精确 ID 的 `XPENDING` 明细查询；只有恰好一条明细的 ID 与预期记录匹配、owner 为当前 consumer，且非零次数可由 SPI 的 `u32` 表示时，才报告投递次数。查询失败、空结果、格式错误、次数为零或不可表示、以及 owner/ID 不匹配时，次数保持未知（`None`）。因此每条恢复消息多一次 Redis 命令，新消息路径不增加查询。关闭不 ACK，不删除消费组或 stream。可选 `XADD MAXLEN ~` 会丢失未读/pending 历史；未读记录被裁剪不一定返回 Gap。至少一次投递要求业务幂等，claim idle 阈值应匹配 handler 时长。

恢复命令可能先改变 PEL 元数据，再由 provider 过滤本 receiver 仍处于 active 状态的 ID。`XAUTOCLAIM` 和同一 consumer 的 `XREADGROUP` 历史读取都可能增加 Redis 投递次数，也可能刷新 idle。因此 `provider_attempt` 表示 Redis 投递次数，不表示 handler 执行次数。PEL idle 不能可靠表示 handler 处理时长，也不能单独证明 handler 停滞。运维诊断应结合 PEL owner/次数、consumer 状态和应用 handler 遥测。

## 安装与验证状态

- **本地源码集成：** 上文的 sibling path dependency 与 core patch 示例说明如何集成 provider `0.7.0`、core `0.21` 和 SPI `0.14` 的源码 checkout；它不验证 registry 安装。
- **源码候选包：** 2026-11-06 执行 `bash ci/check-package-candidate.sh` 后，归档提取、清单校验、no-default-features/sync-discovery/async-discovery/default 检查及 metadata 校验均通过。这验证了源码候选包及其依赖图，不代表已发布包或运行时行为。详见[2026-11-06 实施证据](implementation-evidence-20261106T032751350Z-1e227263.md)。
- **正式发布安装：** 当前版本尚无正式发布安装验收结论。这描述现有证据范围，不表示该版本当前一定存在或不存在于 registry。

当前 Cargo 清单中的 Redis 导入名为 `redis`，包名为 `qubit-redis`，精确固定到 `=2.0.0-rc.2`，来源为 registry。Registry consumer 验证有两种模式：默认 `source-candidate` 将版本与当前源码清单比较；`published` 跳过此比较，但保留精确版本、registry 来源、依赖边和唯一 core 身份校验。2026-11-03 的 `0.7.0 / 0.20.1 / 0.14.0` 查询只是历史观察，不能证明当前 registry 可用性或兼容性。

独立 fixture 执行器为 feature tree、构建和程序执行分别设置 120 秒、600 秒和 120 秒期限；每个 fixture/阶段都保存 stdout/stderr 日志，从 Cargo JSON 定位可执行文件，并仅在 Linux 上终止命令自己的进程组后回收 leader。该平台范围与仓库当前 Ubuntu CI 一致。

XADD Accepted 不证明 fsync、副本持久化或业务完成。新 observer 连接上的 WAIT 无法为 provider 连接写入提供 fencing；Sentinel 验收应观察实际复制的消费组游标、PEL ID 和 owner。旧 consumer 清理须先停实例、确认 PEL 清空并满足业务保留要求，不引入自动 DELCONSUMER。

任务通知 consumer 按 TaskId 和最高 `state_version` 去重，拒绝旧版本，并查询任务服务的权威状态。通知失败不回滚已提交的任务或业务状态。Redis provider 本身不提供事务性 outbox；typed SQLite task service 为自身的任务生命周期转换提供可选 outbox 集成。

当前未发布变更引入有限超时、资源/字节默认值、新公开错误变体和未知结果规则。部署前检查限额，同时调整 idle/concurrency，停止盲目重试未知 publish，并保留未知 ACK 原意图；wire v1 不变。Cargo 版本为 `0.7.0`；发布和创建标签属于另外的操作。

## 验收证据

公开测试覆盖结算回复丢失/取消、接收命令序列、超时、资源限额、字节边界、隔离及 Redis/Sentinel 行为。独立下游 fixture 验证 runtime feature 和任务通知。文档验收从两种实际指南读取标记 Rust 代码块，按文档说明将模块片段与 main 组合，在独立 feature 图和真实 Redis 中编译运行，检查投递与空 PEL；example 二进制单独验证。

[覆盖率评估](coverage-review.zh_CN.md)区分历史数据和最终重构门禁，[工作负载基准](connection-reuse-benchmark.zh_CN.md)提供实际测量。文档构建成功或历史百分比不能证明当前全量验收已完成。
