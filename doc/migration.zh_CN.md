# 迁移到 Redis provider 0.7

[English](migration.md) · [用户指南](user_guide.zh_CN.md)

## 当前版本配套关系

当前 provider manifest 版本为 0.7，并依赖 core 0.20。使用此工作区版本
时请保持这两个版本配套。下文记录 provider 0.6 到 0.7 的 API 迁移；该次
迁移最初发布时配套 core 0.19，早于 core 0.20 的准备。

### Core 0.20 配套说明

Redis 不提供逐目标接纳信息。调用 `publish_checked` 时应使用
`AdmissionRequirement::ProviderOrDestinationAccepted`；要求更严格的逐目标
条件会在发布前返回 `CheckedPublishError::UnsupportedVisibility`。provider 接纳
不表示 handler 已处理事件，也不代表 Redis 已将数据持久化到磁盘。Core 默认在
检测到投递缺口后停止订阅；只有应用能接受消息遗漏且已有恢复策略时，才选择
`GapPolicy::Continue`。新读取的 stream entry 报告 provider attempt `Some(1)`；
pending 和 claim 恢复消息的次数未知（`None`）。现有 wire v1 记录、stream、消费组和
pending entry 无需格式迁移。保留策略仍可能删除未读取或 pending 历史并造成缺口。

## TLS 为可选配置

启用 TLS 不会改变现有部署：`redis://` 仍走明文，默认
`redis.sentinel.tls=false` 也会让 Sentinel 连接保持明文。要对单实例 Redis
或 Sentinel 发现出的主节点启用 TLS，将 `redis.url` 改为 `rediss://`；Sentinel
自身是否启用 TLS 则单独设置 `redis.sentinel.tls=true`。主节点私有 CA 和 mTLS
使用 `redis.tls_ca_cert_path`、`redis.tls_client_cert_path`、
`redis.tls_client_key_path`；Sentinel 使用 `redis.sentinel.tls_ca_cert_path`、
`redis.sentinel.tls_client_cert_path`、`redis.sentinel.tls_client_key_path`。示例见
[用户指南的 TLS 章节](user_guide.zh_CN.md#tls-连接)。无需迁移 wire、stream、
消费组或 SPI。TLS 会验证证书链及主机名，失败时不会降级明文，也不改变投递和
`XADD` 结果未知时的语义。每个 PEM 文件最多 1 MiB；替换证书后须重建 provider
实例才能轮换身份。

## 从 provider 0.6 升级到 0.7

provider 0.7 最初迁移时与 `qubit-event-bus` 0.19 配套。当前 provider 0.7
manifest 使用 core 0.20。把重复设置 `durability(Durable)`、`start_position(...)` 和可选 `consumer_group(...)` 的代码改为 `RedisSubscriptionProfile::new(start_position).consumer_group(group).options()`；不使用消费组时省略 `consumer_group`。profile 要求明确指定 `StartPosition`，并始终生成 durable options；后续 `.durability(Ephemeral)` 会被 Redis capability 检查拒绝。

由 `XREADGROUP >` 返回的新消息会通过 `provider_attempt()` 报告 `Some(1)`。pending 和 `XAUTOCLAIM` 恢复消息仍为 `None`，因为 provider 尚未传递历史投递次数。既有 wire v1 记录、Redis group 和结算行为保持可用。Core 0.19 的 `CodecRegistry::register` 遇到重复载荷类型也会返回错误，详见[核心迁移指南](https://github.com/qubit-ltd/rs-event-bus/blob/main/doc/migration.zh_CN.md)。

将 `qubit-event-bus-redis` 从 0.5 升级到 0.6 时，须同时采用
`qubit-event-bus` 0.18。一起更新直接依赖、下游 fixture 和 lockfile，不能混用
不同 SPI minor。`decode(&EncodedPayload)`、`PublishFailure` 和
`PayloadLimits` 的迁移见[核心迁移指南](https://github.com/qubit-ltd/rs-event-bus/blob/main/doc/migration.zh_CN.md)。
codec 应精确验证元数据；需要读取历史 schema 时，明确记录并实现允许的版本集合。

## 已有消费组起始位置变更

当订阅复用已有消费组，同时请求 `StartPosition::Earliest` 或
`StartPosition::At(...)` 时，这是破坏性行为变更。Redis 会保留已有 group
的游标，`XGROUP CREATE` 无法对已有 group 应用请求的起始位置。默认策略
`redis.existing_group_start=reject` 现在会返回不可重试的
`existing_group_start_position_ignored`，不再静默沿用旧游标。`StartPosition::New`
仍会从已有 group 保存的游标继续。若要从新位置创建 group，请更换 group 名称；
若要明确保留旧行为并继续读取已有游标，请显式设置 `resume`：

```rust
use qubit_event_bus::model::{StartPosition, SubscribeOptions};

let options = SubscribeOptions::<String>::builder()
    .start_position(StartPosition::Earliest)
    .provider_option("redis.existing_group_start", "resume")
    .build();
```

`resume` 不会执行 `XGROUP SETID`，也不会移动已保存游标。该 option 只接受
`reject` 或 `resume`；未知的 `redis.*` 键或非法值会在 Redis 网络 I/O 前失败。
不要原样重试 `existing_group_start_position_ignored`：应更换 group 名称或起始
位置，或者显式选择 `resume`。

## 迁移 Redis 命令准入设置

短命令总额度现在默认 64，其中 8 个名额保留给结算。专用 receiver 连接使用独立的 256 个默认上限，不占短命令额度。配置 `redis.max_concurrent_commands=1` 将被拒绝；没有保留旧行为的开关。总额度至少为 2。省略保留数时默认 `min(8, 总额 - 1)`；只有需要自定义保留数时才显式设置 `redis.reserved_settlement_commands`。命令限制属于每个已创建的 provider 实例，因此还须单独核算多个实例和其他 Redis 客户端。

## 替换调度配置并明确结算策略

旧的 `SyncDeliverySchedulerConfig` / `DeliveryAdmissionConfig` 及对应 facade setter/getter 已删除。原来的执行与队列预算不能直接当成新的持有预算，应明确选择四个正数限额：

```rust,ignore
// Before: qubit-event-bus 0.17 only.
use qubit_event_bus::facade::DeliveryAdmissionConfig;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::facade::SyncDeliverySchedulerConfig;

let facade = EventBusFacadeConfig::new()
    .with_sync_delivery_scheduler(SyncDeliverySchedulerConfig::new(4, 256)?)
    .with_delivery_admission(DeliveryAdmissionConfig::new(256)?);
```

升级到 core 0.18 后，在创建总线时调用下面的函数，并保留已有 codec registry 配置：

```rust
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::time::Duration;

use qubit_event_bus::error::ConfigurationError;
use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::facade::SettlementRetryConfig;

fn facade_config() -> Result<EventBusFacadeConfig, ConfigurationError> {
    let scheduling = DeliverySchedulingConfig::new(
        NonZeroUsize::new(4).expect("positive running limit"),
        NonZeroUsize::new(256).expect("positive owned limit"),
        NonZeroUsize::new(32).expect("positive per-subscription limit"),
        NonZeroUsize::new(256).expect("positive subscription limit"),
    )?;
    let settlement = SettlementRetryConfig::new(
        NonZeroU32::new(5).expect("positive attempt limit"),
        Duration::from_secs(5),
        Duration::from_millis(10),
        Duration::from_secs(1),
    )?;
    Ok(EventBusFacadeConfig::new()
        .with_delivery_scheduling(scheduling)
        .with_settlement_retry(settlement))
}
```

参数依次是同时运行的 handler、全局持有投递、每订阅持有投递和注册订阅数，默认 4/256/32/256。运行量与每订阅持有量均不能超过全局持有量。持有量覆盖接收预留、排队、handler 与结算，异步暂停 session 仍计入注册数。旧 queue=0 不再表示直接交接：若符合原有背压意图，可以设置 owned=running，同时明确选择正数的每订阅和订阅数量限制。这是新的容量模型，不是等价的队列参数换算。

`SettlementRetryConfig` 默认总尝试五次（含首次）、累计五秒、首次退避 10 ms、最大退避一秒。设置一次尝试可禁用重试。只有 `retryable() == Some(true)` 才重试；`Some(false)` 和 `None` 都停止，后者归类为 `RetryabilityUnknown`。panic、无效 token 和基础设施失败也会停止。elapsed 预算约束尝试与退避，不能强制取消在途调用；超过截止点返回成功仍算成功。

## 更新诊断与恢复流程

升级后应在每个运行进程采集 `RedisProviderDiagnostics::snapshots()`，以进程身份加 `instance_id`、`mode`、`namespace` 标记样本。ID 仅在单个进程内唯一；SPI 释放后从目录消失，重建实例或进程后计数重新开始。`general_in_flight` 包含从普通通道准入的结算，`settlement_in_flight` 是预留通道，两者相加才是当前短命令占用。快照字段分别读取，provider 计数也不能替代 facade delivery metrics 或 Redis `XPENDING`。核对升级前后的准入限额及新增拒绝计数，再按[运维指南](user_guide.zh_CN.md#11-怎样维护消费组和处理下游通知)采集 Redis 数据和设置告警。

`Diagnostic::SettlementFailed.error` 由 `Box<str>` 改为 `Arc<SpiError>`，`attempt` 表示从 1 开始的 SPI 尝试次数。新增 `Diagnostic::SettlementStopped` 携带最终 `attempts` 和 `termination`。更新匹配代码，读取结构化错误字段并保留原因链，不按格式化字符串分类；匹配非穷尽 diagnostic enum 时保留 `_` 分支。`terminal_failure()` 中的 `SubscriptionStopReason::Settlement` 保存相同上下文，清理失败不能覆盖首个终止原因。

通过 `bus.delivery_metrics()` 和 `subscription.delivery_metrics().metrics` 区分排队、运行中 handler、结算重试和终止失败。保存快照后关闭失败订阅，修复原因，再使用相同 namespace/topic/group 创建新的持久订阅。pending 历史只有在尚未被裁剪且满足 `redis.claim_min_idle_ms` 条件时才能认领；更改 `StartPosition` 不会回退已有 group。若 Redis 已执行 `XACK` 但回复丢失，可能已无待恢复消息。

优雅关闭除了返回报告，也可能返回 `Err(ShutdownError::TimedOut)`。两次有界等待策略必须先处理首个超时，再尝试一次；第二次仍未完成则交外部监督器处理，详见[用户指南](user_guide.zh_CN.md)。不能用 `Immediate` 作为超时救援。调用方等待有界不代表不合作的工作或进程必然退出。

wire 版本 1 继续支持。部署前测试真实保留的 stream 数据；Rust API 升级不需要
删除 stream 或消费组。限额内格式错误的版本 1 记录仍采用既有隔离并确认路径；
合法但不支持的版本保留 pending 状态，等待兼容 consumer。

新增正数 provider options，默认 `redis.max_wire_bytes=8388608`、
`redis.max_payload_bytes=1048576`、`redis.max_headers_bytes=65536`。
facade 另有默认各 1 MiB 的编码发布/接收限额，两层应一起配置。
发布在复制 payload 前检查，headers/wire 序列化在 `XADD` 前有界执行；接收在
复制字符串前检查 wire 字节，并在字段解码过程中限制增长。这不限制 Redis
客户端首次 RESP 分配，也不是进程总内存预算。

接收 wire、payload 或 headers 超限会返回 `receive_limit_exceeded` 并停止
facade 订阅，源 PEL 记录保留，不执行 `XACK`、`XDEL` 或隔离。
查看 `terminal_failure()`，修复限额或 codec，再以同一 group 创建新的持久
订阅；用 `XPENDING` 验证恢复，不能为消除错误清空 pending 数据。

SPI 发布失败现在声明 `PublishEffect`。提交前打开连接失败及明确 server 拒绝
是 `NotAccepted`；query 开始后的断连、超时或响应转换失败是
`MayHaveBeenAccepted`。默认 `DuplicateRiskPolicy::Forbid` 阻止未知接纳的自动
重发，自定义重试规则不能绕过；`AllowDuplicates` 只允许原策略继续判断。
前序未知效果保留在最终失败中，后来成功的 `duplicate_possible()` 也报告风险。
RetryPolicy 是软预算，不是所有执行中命令的硬超时。取消已启动发布后，记录仍
可能在 Redis 中，应保留 EventId 并核对业务副作用。

Redis 不按 EventId 自动去重，`XADD` 接纳也不证明 fsync 或 handler 完成。
facade 死信转发与源 `XACK` 是两个操作，消费者必须容忍重复逻辑死信。
部署前针对实际 Redis 版本运行 provider feature matrix、conformance、
有界解码、回复丢失和持久恢复测试。


## 有损 Stream 保留策略

Redis Stream 默认不裁剪。改变保留策略前，先用 `XINFO GROUPS <stream-key>` 列出所有 group，用 `XPENDING <stream-key> <group>` 逐组核对 PEL，并确认未读位置、未来回放需求和审计期限；源 stream 和隔离流的 `XLEN` 分别查看。准确 key 使用 `naming::stream_key`、`naming::group_name`、`naming::poison_key` 生成。任何事项未确认，都维持无限保留。只有业务明确接受历史丢失，才同时配置 `redis.stream_maxlen_approx=<正整数>` 与 `redis.allow_lossy_retention=true`。近似 `XADD MAXLEN ~` 可能删除未读或 pending payload；未读记录丢失不一定出现 Gap。启用后再次检查各组和隔离流，仅按业务与审计策略归档或移除隔离证据。`XADD` 接纳不代表 fsync 或副本持久化保证。完整流程见[人工决策清单](user_guide.zh_CN.md#人工决定保留策略)。
