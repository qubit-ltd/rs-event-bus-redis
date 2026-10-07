# Redis Streams 用户指南

**读者：** 使用 `qubit-event-bus` 0.20 和 `qubit-event-bus-redis` 0.7 的 Rust 服务开发者。本指南以订单发布服务和账单消费服务为例，说明如何通过 Redis 共享事件，同时让应用代码继续使用 event-bus facade。

[English](user_guide.md) · [README](../README.zh_CN.md) · [API 文档](https://docs.rs/qubit-event-bus-redis)

## 1. 添加并选择 provider

将 facade 和 provider 都作为直接依赖。provider crate 默认启用 `discovery`，会把 `redis-streams` 提交到同步和异步 provider inventory。应用在启动时调用 `EventBusRegistry::discover()` 或 `AsyncEventBusRegistry::discover()`，再按 ID 选择 Redis。

```toml
[dependencies]
qubit-event-bus = { version = "0.20", features = ["discovery"] }
qubit-event-bus-redis = "0.7"
qubit-spi = "0.13"
```

core 和 provider 应使用对应的 minor 版本，并一起更新 lockfile。仓库 examples 基于当前 checkout；升级已有服务前，先按迁移指南检查版本变化和恢复步骤。

采用自动发现时，provider 关闭默认 feature 后应选择 `["sync", "discovery"]` 或 `["async", "discovery"]`，facade 也须启用 `discovery`。采用手动注册时，provider 只需 `["sync"]` 或 `["async"]`，facade 无须启用发现功能。`async` 可以使用 Redis client 的 Smol adapter，也能在 Tokio host 中运行。crate 不会启动 Tokio runtime 或生成订阅 worker；异步 facade 的 runner 由应用现有 executor 驱动。

Redis 需要 6.2 或更高版本。集成测试使用 Docker 启动隔离的 Redis 6.2、Redis 7 和 Sentinel 进程。

## 2. 为跨进程 payload 注册 codec

Redis 保存编码字节，因此通过 facade 使用的每种 payload 类型都要提供 `EventCodec<T>`。下面以 UTF-8 `String` 为例。生产应用可替换为 JSON、Protobuf 或带 schema 的 codec。

<!-- doc-example: codec -->
<!-- BEGIN DOC UTF8 CODEC -->
```rust
use std::error::Error;
use std::sync::Arc;

use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::CodecError;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::spi::EncodedPayload;

/// Encodes owned strings as UTF-8 bytes.
/// Uses the default strict metadata validation for this content type and no
/// schema.
pub(crate) struct Utf8Codec(pub(crate) ContentType);

impl EventCodec<String> for Utf8Codec {
    fn content_type(&self) -> &ContentType {
        &self.0
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }

    fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from(value.as_bytes()))
    }

    fn decode(&self, payload: &EncodedPayload) -> Result<String, CodecError> {
        String::from_utf8(payload.bytes().to_vec()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}

pub(crate) fn facade_config() -> Result<EventBusFacadeConfig, Box<dyn Error>> {
    let mut codecs = CodecRegistry::new();
    codecs.register::<String>(Arc::new(Utf8Codec(ContentType::new("text/plain")?)))?;
    Ok(EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs)))
}
```
<!-- END DOC UTF8 CODEC -->

这是可复用的模块片段。将它放到下方任一 `main` 之前的 `src/main.rs` 中；完整程序通过 `facade_config()` 注册 codec，不依赖读者补充隐藏对象。创建 `CodecRegistry` 并注册 `Utf8Codec`，再将它放入 `EventBusFacadeConfig`。缺少 codec 时，facade 会在访问 Redis 前拒绝该类型的发布或订阅。

## 3. 使用同步 SPI 发布和消费

服务配置只需要 Redis URL 和 namespace。显式选择 `redis-streams`，避免 registry fallback 意外使用其他 provider。

<!-- doc-example: sync-discovery -->
```rust
use std::time::Duration;

use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus_redis as _;
use qubit_event_bus_redis::diagnostics::RedisProviderDiagnostics;
use qubit_event_bus_redis::RedisSubscriptionProfile;
use qubit_spi::ProviderSelection;
use qubit_event_bus::EventBusRegistry;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".into());
    let namespace = std::env::var("REDIS_NAMESPACE").unwrap_or_else(|_| "orders-guide-sync".into());
    let options: ProviderOptions = [
        ("redis.url".into(), url),
        ("redis.namespace".into(), namespace.clone()),
    ].into();
    let config = EventBusConfig::default()
        .with_selection(ProviderSelection::named("redis-streams")?)
        .with_provider_options(options)
        .with_facade_config(facade_config()?);
    let registry = EventBusRegistry::discover()?;
    let bus = registry.create(&config)?;
    for snapshot in RedisProviderDiagnostics::snapshots()
        .into_iter()
        .filter(|snapshot| snapshot.namespace() == namespace.as_str())
    {
        println!("Redis SPI {} {:?}: general={} settlement={} receivers={} rejected={}",
            snapshot.instance_id(), snapshot.mode(), snapshot.general_in_flight(),
            snapshot.settlement_in_flight(), snapshot.active_receivers(),
            snapshot.command_rejections());
    }
    let topic = Topic::<String>::new("orders.created")?;
    let (sender, receiver) = std::sync::mpsc::channel();
    let subscription = bus.subscribe(
        SubscribeRequest::new("billing-worker", topic.clone())?.with_options(
            RedisSubscriptionProfile::new(StartPosition::Earliest)
                .consumer_group(ConsumerGroup::new("billing")?)
                .options()
                .build(),
        ),
        move |delivery| {
            sender.send(delivery.payload().clone())
                .map_err(|source| qubit_event_bus::DeliveryError::Handler { source: Box::new(source) })
        },
    )?;
    bus.publish(PublishRequest::new(topic, "order-42".to_owned())?)?;
    let received = receiver.recv_timeout(Duration::from_secs(5))?;
    println!("consumed order event: {received}");
    subscription.cancel()?;
    let report = bus.shutdown(ShutdownMode::Graceful { timeout: Duration::from_secs(5) })?;
    if report.outcome != ShutdownOutcome::Complete {
        return Err("graceful shutdown did not complete".into());
    }
    Ok(())
}
```

此入门练习先安装账单 handler，再发布 `order-42`，等待 handler 发出通知，打印 `consumed order event: order-42`，最后取消订阅并关闭总线。生产 handler 应完成并提交应用自己的账单变更后才返回 `Ok(())`；打印和 channel 通知只是练习的验收信号。总线和订阅应由服务的启动、关闭模块持有。新建的 `Earliest` 消费组也能读取已保留的记录。生产服务若使用 `New`，应先注册 consumer，再依赖后续发布的事件。Redis Streams 只接受 `Durable` 订阅；`Ephemeral` 会在执行 Redis I/O 前被拒绝。服务断连或关闭订阅时，group 会保留，未结算记录仍在 pending entries list 中。Redis 只在首次创建 group 时应用起始位置。

限额内格式错误的版本 1 wire 记录会被原子复制到按 group 隔离的 stream（`qubit:poison:*`），并从源 group 确认。隔离记录包含 `source_stream`、`source_id`、`group`、稳定的 `reason`、原始 `wire` 字节和 `wire_missing`。隔离成功后返回 `Gap`，不会把 payload 放进错误信息。可用 `XRANGE <quarantine-key> - + COUNT 100` 读取一页记录，并用 `XLEN`、`XPENDING` 监控隔离流、源 stream 和 pending 数量；完整检查须继续翻页。provider 不会自动裁剪这些 stream；运维应按保留策略归档或清理隔离数据。

投递语义仍是至少一次。如果 handler 运行时间超过 `redis.claim_min_idle_ms`，其他 consumer 可能接管该 pending 记录。若 `XADD` 回复丢失，发布结果未知，facade 默认安全门阻止自动重发；人工或明确允许的重试仍可能重复。`redis.max_unsettled_per_subscription` 限制本地活跃投递数，默认值为 100。

默认不限制 stream 长度。只有在完成下文的保留评估并接受历史丢失后，才在上方 `ProviderOptions` map 中同时加入 `("redis.stream_maxlen_approx".into(), "100000".into())` 和 `("redis.allow_lossy_retention".into(), "true".into())`。此后每次发布都会使用 `XADD MAXLEN ~ 100000`。Redis 可能裁剪尚未消费的记录，或消费组 pending 列表仍引用的记录；裁剪不保证投递，旧消息可能无法恢复。需要保留 pending 历史用于恢复时应保持此选项关闭。格式错误记录的隔离 stream 遵循单独的运维保留策略。

如果记录在任何 consumer 读取前被裁剪，Redis 不会通过 `ReceiveOutcome::Gap` 报告它们。provider 无法识别缺失的记录 ID，因此启用裁剪意味着可能发生 delivery API 无法发现的历史丢失，应用应在外部监控保留状态。

指定 `ConsumerGroup` 后，namespace、topic 和 group 相同的实例会共同分工。不同 group 各自维护读取位置，因此都能收到自己的副本。未指定 group 时，subscriber ID 用作 group 身份。

每个 Redis 订阅都会获得随机生成的 `qubit:consumer:<uuid>` consumer 名称。facade 的本地订阅 ID 不具备全局唯一性，因此不会用作 Redis consumer 身份。进程重启后会创建新 consumer；旧名称对应的 pending 记录仍保留在 group 中，达到 `redis.claim_min_idle_ms` 后可由 `XAUTOCLAIM` 恢复。

当前 wire 格式支持版本 1。wire 未超过 `redis.max_wire_bytes` 且版本为不等于 1 的合法 `u64` 整数时，provider 返回 kind 为 `unsupported_wire_version` 的非重试 `SpiError::Operation`，源记录保留在 pending 中，不会隔离或确认。应停止旧 consumer，部署支持该版本的 provider，再重启同一 group 的 consumer。使用 `XPENDING` 检查旧记录是否已恢复并结算。不要为了消除错误而直接清除 pending 记录。无效 JSON、缺失或非数字版本，以及格式错误的版本 1 记录仍会进入隔离流。

恢复工作按时间间隔分批限额：claim 和本 consumer pending 扫描各最多执行 8 条命令；tombstone 修复每轮最多执行一次 `XPENDING`、四次 `XRANGE` 和四次隔离 `EVAL`。扫描游标会跨间隔和 receive 调用保留。`Duration::ZERO` 执行有界非阻塞恢复（最多一次 claim、一次本 consumer pending 查询和一次新消息查询），跳过 tombstone 扫描，并允许隔离一条格式错误的记录。`Duration::MAX` 表示无限等待，内部以 1 秒的有限 Redis 阻塞读取循环实现。有限超时是调度截止时间，限制继续发起恢复工作及实际 Redis `BLOCK` 时长。零超时表示不在 Redis 等待新消息，不表示网络操作必须在 0ms 内完成。阻塞读取的响应等待预算为实际 `BLOCK` 加 `redis.command_timeout_ms`。同步 socket 超时只是每次 I/O 等待的软限制；DNS、多地址尝试、setup 各阶段及持续小包传输都可能使整体调用超出预算。已经发出的命令不会被强制停止。

## 4. 驱动异步 SPI

异步 bus 的创建、发布、订阅和接收操作都会返回 runtime-neutral future。小型示例用 `futures_lite::future::block_on` 驱动；长期运行的服务通常会在现有 executor 上轮询这些 future，并让订阅与其他服务工作并发运行。

<!-- doc-example: async-discovery -->
```rust
use std::time::Duration;

use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus_redis as _;
use qubit_event_bus_redis::RedisSubscriptionProfile;
use qubit_spi::ProviderSelection;
use futures_channel::oneshot;
use futures_lite::future;
use qubit_event_bus::AsyncEventBusRegistry;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    future::block_on(async {
        let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".into());
        let namespace = std::env::var("REDIS_NAMESPACE").unwrap_or_else(|_| "orders-guide-async".into());
        let options: ProviderOptions = [
            ("redis.url".into(), url),
            ("redis.namespace".into(), namespace),
        ].into();
        let config = EventBusConfig::default()
            .with_selection(ProviderSelection::named("redis-streams")?)
            .with_provider_options(options)
            .with_facade_config(facade_config()?);
        let registry = AsyncEventBusRegistry::discover()?;
        let bus = registry.create(&config).await?;
        let topic = Topic::<String>::new("orders.created")?;
        let mut subscription = bus.subscribe(
            SubscribeRequest::new("billing-worker", topic.clone())?.with_options(
                RedisSubscriptionProfile::new(StartPosition::Earliest)
                    .consumer_group(ConsumerGroup::new("billing")?)
                    .options()
                    .build(),
            ),
        ).await?;
        bus.publish(PublishRequest::new(topic, "order-43".to_owned())?).await?;
        let (sender, received) = oneshot::channel();
        let sender = Arc::new(std::sync::Mutex::new(Some(sender)));
        let run = subscription.run(move |delivery| {
            let sender = Arc::clone(&sender);
            let value = delivery.payload().clone();
            async move {
                println!("consumed order event: {value}");
                if let Some(sender) = sender.lock().expect("notification lock").take() {
                    let _ = sender.send(());
                }
                Ok::<(), qubit_event_bus::DeliveryError>(())
            }
        });
        let stop = async {
            received.await?;
            // Keep polling the runner while shutdown drains the handler and settlement.
            let report = bus.shutdown(ShutdownMode::Graceful { timeout: Duration::from_secs(5) }).await?;
            if report.outcome != ShutdownOutcome::Complete {
                return Err("graceful shutdown did not complete".into());
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        };
        let (run_result, stop_result) = future::zip(run, stop).await;
        run_result?;
        stop_result?;
        subscription.close().await?;
        Ok(())
    })
}
```

将 codec 片段放在此 `main` 之前，并添加依赖 `futures-lite = "2"` 和 `futures-channel = "0.3"`。程序打印 `consumed order event: order-43`；handler 完成后发出关闭通知，继续并行轮询 runner 与优雅关闭，直到两者结束。异步 facade 同样通过 codec registry 编解码 payload。`AsyncSubscription::run` 由调用方驱动，future 应由应用 executor 轮询。丢弃尚未完成的 `receive` future 不会确认消息。Redis 会把记录留在消费组的 pending entries list；当前 consumer 后续可以再次读取，其他 consumer 则可在 idle threshold 到期后通过 `XAUTOCLAIM` 接管。


## 关闭自动发现后手动注册

以下为模块片段。使用对应片段替换同步或异步程序中的 registry import，并将 `...Registry::discover()?` 改为 `order_registry()?`；保留 codec 和其余 main。文档测试会关闭 provider 默认 feature，在不启用 `discovery` 的独立项目中编译并运行两种变体。

<!-- doc-example: sync-manual -->
```rust
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus_redis::sync::RedisEventBusProvider;

fn order_registry() -> Result<EventBusRegistry, Box<dyn std::error::Error>> {
    let registry = EventBusRegistry::new();
    registry.register(RedisEventBusProvider)?;
    Ok(registry)
}
```

<!-- doc-example: async-manual -->
```rust
use qubit_event_bus::AsyncEventBusRegistry;
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;

fn order_registry() -> Result<AsyncEventBusRegistry, Box<dyn std::error::Error>> {
    let registry = AsyncEventBusRegistry::new();
    registry.register(AsyncRedisEventBusProvider)?;
    Ok(registry)
}
```

## 可运行示例

仓库包含完整的 facade 示例，会注册 UTF-8 codec、发布事件、消费事件并关闭订阅和总线：

```bash
cargo run --example sync_orders -- redis://127.0.0.1/ local-sync-orders
cargo run --example async_orders -- redis://127.0.0.1/ local-async-orders
REDIS_SENTINEL_NODES=127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381 \
REDIS_SENTINEL_SERVICE_NAME=qeventbus \
  cargo run --example sentinel_orders -- local-sentinel-orders
```

异步示例会先消费事件，再等待用户按 Enter 关闭；这样 handler 能先完成，runner 随后取消其下一次接收。每条命令都需要对应的 standalone Redis 或 Sentinel 服务，并启用该示例所需的 features。

## 5. 配置 Redis 与 Sentinel

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `redis.url` | `redis://127.0.0.1/` | 单实例 Redis URL；不允许在 URL 中直接写 username/password。 |
| `redis.namespace` | `qubit` | 用于生成 stream 和消费组 key 的命名空间。 |
| `redis.claim_min_idle_ms` | `30000` | 其他 consumer 可以认领 pending entry 前所需的空闲毫秒数。 |
| `redis.recovery_interval_ms` | `1000` | 恢复扫描间隔，也适用于连续 receive 调用；范围为 50 至 60,000 毫秒。第一次 receive 会立即扫描。 |
| `redis.max_concurrent_commands` | `64` | 短命令总准入上限，范围为 2 至 4,096。 |
| `redis.reserved_settlement_commands` | `8` | 专供结算的短命令名额；必须大于零且小于总命令上限。总命令数低于 9 且未显式配置时，默认值为总额减 1。 |
| `redis.max_active_receivers` | `256` | 专用活跃 receiver 连接上限，范围为 1 至 4,096。 |
| `redis.max_unsettled_per_subscription` | `100` | 每个订阅已投递但未结算的消息上限；达到上限后 receive 会等待。 |
| `redis.max_wire_bytes` | `8388608` | 序列化 wire 总字节上限。 |
| `redis.max_payload_bytes` | `1048576` | 解码后 payload 字节上限。 |
| `redis.max_headers_bytes` | `65536` | 二次 JSON 解析前的 headers 字符串字节上限。 |
| `redis.max_idle_connections` | `8` | 同步 standalone 命令连接的最大空闲复用数，范围为 1 到 64。阻塞接收器的专用连接另计。 |
| `redis.stream_maxlen_approx` | 未设置 | 可选的近似 stream 条目上限，通过 `XADD MAXLEN ~` 应用；可能裁剪未读或 pending 记录。 |
| `redis.allow_lossy_retention` | `false` | 设置 `redis.stream_maxlen_approx` 时必须同时将此项设为 `true`，明确接受历史记录可能丢失。 |
| `redis.username_env` | 未设置 | 保存 Redis ACL username 的环境变量名称。 |
| `redis.password_env` | 未设置 | 保存 Redis ACL password 的环境变量名称。 |
| `redis.sentinel.nodes` | 未设置 | 最多 16 个逗号分隔的 Sentinel `host:port` 地址；拒绝空 host 和非法或零端口。 |
| `redis.sentinel.service_name` | 未设置 | Sentinel master 服务名；与 `nodes` 一起配置。 |
| `redis.sentinel.username_env` | 未设置 | 保存 Sentinel ACL username 的环境变量名称。 |
| `redis.sentinel.password_env` | 未设置 | 保存 Sentinel ACL password 的环境变量名称。 |

启用 Sentinel 时，`redis.sentinel.nodes` 和 `redis.sentinel.service_name` 必须同时设置。此时 Redis URL 仍须是合法 URL，但不会用于查找 master。Sentinel 连接不进入 standalone 空闲池；解析通过有等待预算的 `SENTINEL get-master-addr-by-name` 和候选节点 `ROLE` 探测完成。每次解析对最多 16 个节点各尝试一次，优先使用上次成功的节点。同步 standalone 短命令最多复用 `redis.max_idle_connections` 条空闲连接；异步 standalone 发布与结算共享 multiplexed 命令连接。并发冷启动共用一次初始化；代际检查防止旧连接的失败清除新连接。Sentinel 路径不缓存 master socket；ROLE 验证后仍可能发生切换并拒绝命令，provider 不会透明重放 XADD。接收器使用独立连接，阻塞读取不会占用短命令通道。

凭据应放在服务运行环境中。provider options 保存环境变量名称；`RedisEventBusConfig` 的 `Debug` 会隐藏 URL 和凭据。不要把明文密钥放入 provider options、URL、命令行参数或日志。此版本没有启用 `redis-rs` 的 TLS 参数；增加 TLS 支持前，应将 Redis 流量限制在可信网络内。

## 6. 理解投递、重试和清理

provider 将一条 JSON wire record 写入 Redis Stream 的 `wire` 字段。内容包括协议版本、事件 ID、时间戳、headers、可选排序 key、content type、可选 schema ID 和 payload 字节。未知版本会返回 `unsupported_wire_version`，并保留在消费组 pending 列表中，详见下方恢复说明。`XADD` 成功返回 `Accepted`；如果连接在收到回复前中断，调用方无法确定记录是否写入，重试发布可能产生重复事件。

| 操作 | Redis 行为 | 对应用的影响 |
| --- | --- | --- |
| `Accept` | 执行 `XACK` | 从消费组 pending 列表移除消息。 |
| `Reject` | 执行 `XACK` | 结束这次投递；不会创建死信 stream。 |
| `Retry` | 保留 pending 状态 | 当前 consumer 可重新读取，超过 idle threshold 后也可被认领。 |
| 关闭或 drop | 不会隐式执行 `XACK` | 未结算消息仍可恢复。 |
| 取消异步 receive | 不会隐式执行 `XACK` | 已读记录留在 PEL 中，供后续读取或认领。 |

Redis 提供至少一次投递，因此 handler 应具备幂等性。新消息经 `XREADGROUP >` 读取时，`delivery.context().provider_attempt()` 为 `Some(1)`；pending 和 `XAUTOCLAIM` 恢复的消息为 `None`，因为当前 provider 没有传递历史投递次数。facade 本地重试次数请读取 `retry_attempt`。如果 handler 运行时间超过 `redis.claim_min_idle_ms`，另一个 consumer 可能在原 handler 仍执行时认领同一事件。idle threshold 应覆盖常见和最慢的处理时间；重复代价高时，应用还应按业务 ID 去重。

每个订阅的未结算消息达到 `redis.max_unsettled_per_subscription` 后会暂停接收新记录。结算或 retry 后会释放容量。该设置限制进程内的投递压力，不会限制 Redis stream 增长。

每个订阅第一次 `receive` 都会立即检查 pending。后续 receive 调用共享恢复截止时间：在 `redis.recovery_interval_ms`（默认 1,000 毫秒，范围 50–60,000）到期前只读新记录，不重复扫描；长时间等待的 receive 仍会在间隔到期时继续恢复扫描。Retry、receive 错误或已 poll 的 async receive 被取消时，会强制下一次调用执行恢复。调小间隔能更快接管达到 idle threshold 的 pending 消息，同时会增加 Redis 扫描命令。

`StartPosition::New` 会在当前 stream 尾部创建 group；`Earliest` 会从 `0-0` 开始创建新 group；`At("milliseconds-sequence")` 使用 Redis Stream ID。group 一旦创建，读取游标由 Redis 保留；之后更改请求的 start position 不会重置现有 group。

未设置 `redis.stream_maxlen_approx` 时，provider 不会裁剪 stream，也不会自动删除 group。应监控 Redis 内存和 stream 增长。删除 stream 或 group 前，先停止 consumer 并决定如何处理 pending 消息；删除 pending record 可能导致 `ReceiveOutcome::Gap`。Redis persistence 和 replication 配置需符合业务恢复目标：`Accepted` 不代表已 fsync，Sentinel 切换也可能丢失尚未复制的写入。

### 限制 facade 工作量并收敛结算失败

同步与异步总线都通过 `EventBusFacadeConfig::with_delivery_scheduling` 配置 `DeliverySchedulingConfig`。四个正数限额默认依次为：**同时运行 4 个 handler、全局持有 256 条投递、每订阅持有 32 条投递、注册 256 个订阅**。持有量包括接收预留、已收待执行、运行中和结算中的投递。`max_running_handlers`、`max_owned_per_subscription` 均不能超过 `max_owned_deliveries`。provider 自身默认 100 条未结算记录的限额仍然生效；这些限制约束数量，不代表进程总内存预算。暂停的异步 session 仍占用订阅名额，直到关闭或终止清理完成。

等待中的热点键和结算退避不会占用 handler 执行名额。A 与 B 之间的公平调度要求 B 有可预留的持有额度，或者已被接收且可执行；不能据此保证穿透任意长度、尚未读取的 A 积压。Redis 声明不支持按键顺序，新调度器也不会赋予 Redis 该保证。

结算策略使用 `EventBusFacadeConfig::with_settlement_retry(SettlementRetryConfig::new(...)?);`，完整参数见[迁移示例](migration.zh_CN.md)。默认**总共尝试 5 次（含首次），从首次 SPI 尝试前开始计时最多 5 秒，首次退避 10 ms，最大退避 1 秒**。只有 `SpiError::retryable() == Some(true)` 允许重试；`Some(false)` 立即停止，`None` 以 `RetryabilityUnknown` 停止。panic、无效 token、时钟或 timer 失败也会终止。预算在尝试之间检查，不能取消已阻塞的 Redis 命令；超过截止点返回成功仍按成功处理。异步暂停取消了在途 settle 时，已经开始的 attempt 仍计入预算，恢复后继续使用原有有限预算。

结算终止时，facade 先记录首个原因，再清理并停止新的接收和 handler 启动。已启动的工作可继续完成，关闭失败不会覆盖原始原因。通过 `subscription.terminal_failure()` 获取 `SubscriptionStopReason::Settlement`，其中包含事件 ID、disposition、attempts、termination 和结构化 `Arc<SpiError>`。每次失败产生 `SettlementFailed`，首次终止产生一次 `SettlementStopped`；应把这些上下文保存到 telemetry。

使用 `bus.delivery_metrics()` 和 `subscription.delivery_metrics().metrics` 查看 `reserved_receives`、`queued`、`running_handlers`、`settling`、`lane_waiting`、`settlement_attempts`、`settlement_retries`、`settlement_terminal_failures`、`completed`、handler/settlement 耗时样本数、总量、最大值及 `oldest_owned_age`。`lane_waiting` 是 `queued` 的子集，重试计数只累计首次之后真正进入 SPI 的调用。并发变化期间的快照不保证事务一致性。关闭后的订阅句柄保留计数，总线累计值也不会随订阅移除而消失。这些 facade 快照不表示 Redis PEL 或重连状态，还需检查 `XPENDING` 和 `XINFO GROUPS`。

恢复时先保存首个原因和快照，按错误修复连接、codec、限额或策略，完成失败订阅的关闭，然后使用**相同 namespace、topic 和 group** 创建新的 `Durable` 订阅。新的 consumer 达到 `redis.claim_min_idle_ms` 条件后可认领保留的 pending 消息；`StartPosition` 不会重置已有 group。能否恢复取决于记录保留情况和 claim 策略：裁剪或删除可能破坏 pending 历史，已执行 `XACK` 但回复丢失时也没有可认领的消息。同一 token、同一 disposition 的重复结算是幂等的，但业务处理仍不保证恰好一次。

### 限制 facade 工作量并收敛结算失败

同步与异步总线都通过 `EventBusFacadeConfig::with_delivery_scheduling` 配置 `DeliverySchedulingConfig`。四个正数限额默认依次为：**同时运行 4 个 handler、全局持有 256 条投递、每订阅持有 32 条投递、注册 256 个订阅**。持有量包括接收预留、已收待执行、运行中和结算中的投递。`max_running_handlers`、`max_owned_per_subscription` 均不能超过 `max_owned_deliveries`。provider 自身默认 100 条未结算记录的限额仍然生效；这些限制约束数量，不代表进程总内存预算。暂停的异步 session 仍占用订阅名额，直到关闭或终止清理完成。

等待中的热点键和结算退避不会占用 handler 执行名额。A 与 B 之间的公平调度要求 B 有可预留的持有额度，或者已被接收且可执行；不能据此保证穿透任意长度、尚未读取的 A 积压。Redis 声明不支持按键顺序，新调度器也不会赋予 Redis 该保证。

结算策略使用 `EventBusFacadeConfig::with_settlement_retry(SettlementRetryConfig::new(...)?);`，完整参数见[迁移示例](migration.zh_CN.md)。默认**总共尝试 5 次（含首次），从首次 SPI 尝试前开始计时最多 5 秒，首次退避 10 ms，最大退避 1 秒**。只有 `SpiError::retryable() == Some(true)` 允许重试；`Some(false)` 立即停止，`None` 以 `RetryabilityUnknown` 停止。panic、无效 token、时钟或 timer 失败也会终止。预算在尝试之间检查，不能取消已阻塞的 Redis 命令；超过截止点返回成功仍按成功处理。异步暂停取消了在途 settle 时，已经开始的 attempt 仍计入预算，恢复后继续使用原有有限预算。

结算终止时，facade 先记录首个原因，再清理并停止新的接收和 handler 启动。已启动的工作可继续完成，关闭失败不会覆盖原始原因。通过 `subscription.terminal_failure()` 获取 `SubscriptionStopReason::Settlement`，其中包含事件 ID、disposition、attempts、termination 和结构化 `Arc<SpiError>`。每次失败产生 `SettlementFailed`，首次终止产生一次 `SettlementStopped`；应把这些上下文保存到 telemetry。

使用 `bus.delivery_metrics()` 和 `subscription.delivery_metrics().metrics` 查看 `reserved_receives`、`queued`、`running_handlers`、`settling`、`lane_waiting`、`settlement_attempts`、`settlement_retries`、`settlement_terminal_failures`、`completed`、handler/settlement 耗时样本数、总量、最大值及 `oldest_owned_age`。`lane_waiting` 是 `queued` 的子集，重试计数只累计首次之后真正进入 SPI 的调用。并发变化期间的快照不保证事务一致性。关闭后的订阅句柄保留计数，总线累计值也不会随订阅移除而消失。这些 facade 快照不表示 Redis PEL 或重连状态，还需检查 `XPENDING` 和 `XINFO GROUPS`。

恢复时先保存首个原因和快照，按错误修复连接、codec、限额或策略，完成失败订阅的关闭，然后使用**相同 namespace、topic 和 group** 创建新的 `Durable` 订阅。新的 consumer 达到 `redis.claim_min_idle_ms` 条件后可认领保留的 pending 消息；`StartPosition` 不会重置已有 group。能否恢复取决于记录保留情况和 claim 策略：裁剪或删除可能破坏 pending 历史，已执行 `XACK` 但回复丢失时也没有可认领的消息。同一 token、同一 disposition 的重复结算是幂等的，但业务处理仍不保证恰好一次。

### 限制单条记录并恢复不兼容的 consumer

`redis.max_wire_bytes`、`redis.max_payload_bytes`、`redis.max_headers_bytes` 默认分别为 8,388,608、1,048,576、65,536 字节。值必须为正整数，零、非法数字和溢出都是配置错误。三个限额独立，payload 未超限仍可能因 JSON 膨胀使 wire 超限。facade 的 `PayloadLimits` 另有默认各 1 MiB 的双向限制，需要一起配置。

发布先在复制 payload 前检查大小，再用有界 writer 序列化 headers/wire，成功后才调用 `XADD`。接收先借用 wire 字节检查长度，再复制字符串；先解析版本，随后对版本 1 字段做有界解码，不构造完整 JSON Value 树。payload 每次 push 前检查增长，headers 字符串在二次 JSON 解析前检查，嵌套 JSON 保持深度保护。这不阻止 Redis 客户端首次分配 RESP frame。

`receive_limit_exceeded` 停止订阅，保留 PEL 记录，不执行 `XACK`、`XDEL` 或隔离。合法但不支持的 wire 版本同样保留。修复容量或部署兼容 provider/codec 后，使用同一 group 创建新订阅，并通过 `XPENDING` 验证认领结果。不要删除 pending 记录掩盖问题。限额内格式错误的版本 1 数据仍走隔离；旧版本发布的 wire 版本 1 继续可读。

codec 接收 `&EncodedPayload`，默认精确验证 content type 和可选 schema；`None` 与具名 schema 不同。需要支持旧 schema 时明确重写验证方法。元数据不兼容或 codec panic 会使 facade 停止接收而不结算；查看 `terminal_failure()`，修复 codec，再创建新持久订阅。普通 `CodecError::Decode` 仍以 `XACK` 拒绝坏消息。

### 处理结果未知的 `XADD`

公开 facade 错误为 `PublishFailure`，保留事件 ID、效果和原因。提交前 wire/配置失败、打开连接失败和明确 server 拒绝为 `NotAccepted`；进入 query 后的断连、超时或响应转换失败为 `MayHaveBeenAccepted`。没有收到回复不能证明 Redis 拒绝记录。

默认 `DuplicateRiskPolicy::Forbid` 在自定义规则前停止未知接纳的自动重试。`AllowDuplicates` 只允许已配置的策略继续判断。未知效果跨尝试保留，后来成功回执以 `duplicate_possible()` 报告风险。provider 尝试开始后被 retry 取消，结果未知；丢弃公开 future 则没有失败返回值。须保留 EventId 并核对业务结果。RetryPolicy 是软预算，不承诺所有执行中的 Redis 命令均能硬超时取消。

provider 和 Redis Streams 都不按 EventId 自动去重，消费者应保证业务副作用幂等。facade 死信转发与源 `XACK` 是独立操作：转发成功后确认失败，可能再次产生同一逻辑死信。转发结果未知时使用相同安全门、保留持久源消息并停止源订阅。

## 7. 排查常见问题

- **Registry 中没有 `redis-streams`：** 将 `qubit-event-bus-redis` 作为直接依赖，启用 `discovery`，并调用对应的同步或异步 registry `discover()`。
- **类型化发布或订阅提示缺少 codec：** 在 `EventBusFacadeConfig` 的 `CodecRegistry` 中为该 payload 类型注册 `EventCodec<T>`。
- **Provider 拒绝配置：** 检查 `redis.url`、`redis.namespace`、成对配置的 Sentinel 参数，以及环境变量名称引用的凭据是否存在。URL 中的明文凭据会被拒绝，以免出现在诊断信息中。
- **Consumer 没收到旧事件：** 使用新 group 并设置 `StartPosition::Earliest`；已有 group 会沿用 Redis 中保存的游标。
- **消息接管太早或太晚：** 根据 handler 的正常和最坏执行时长调整 `redis.claim_min_idle_ms`。系统仍可能重复投递。
- **出现 gap：** 检查运维侧的 `XDEL`/`XTRIM` 操作及 pending entries，并查看隔离流的 `reason` 和 `source_id`，再决定后续清理策略。Gap 既可能表示 pending 源记录缺失（tombstone），也可能表示隔离成功；隔离会复制并确认条目，源记录仍可能保留在 stream 中。
- **Sentinel 无法连接：** 检查 Sentinel 地址、master 服务名、ACL 环境变量引用、quorum，以及应用主机能否访问 Sentinel 返回的 Redis master 地址。

此版本没有内建 PEL 或重连指标。运维时可以使用 Redis `XPENDING`、`XINFO STREAM`、`XINFO GROUPS` 和 `XINFO CONSUMERS`，并在应用 telemetry 中记录 provider 错误和 publish receipt ID。

## 8. 正常关闭

同步总线由关闭模块调用 `subscription.cancel()` 等待 worker 结束，再关闭总线。异步总线可以像上方程序一样，继续轮询 runner，并同时执行优雅关闭以等待已准入 handler 及结算完成，最后关闭句柄。关闭 receiver 或选择立即关闭时，未结算消息可能留在 Redis；关闭本身不代表消息已接受。

## 9. 失败后怎样决定是否重试

应用通过 `SpiError::Operation` 的 `operation`、`kind` 和 `retryable` 判断失败类别。错误只提供稳定、脱敏的诊断，不暴露 Redis 原始文本。重试提示限定允许重试的上下文，不会触发透明重放，也不保证处理无重复。

| 失败 | kind / retryable | 应用处理 |
| --- | --- | --- |
| 发布已发送，但回复丢失、超时或畸形 | `outcome_unknown` / `Some(false)` | Redis 可能已保存事件。按业务 ID 核对；只有明确接受重复风险时才重新发布。provider 不透明重放 XADD。 |
| 接收/claim 已发送，结果未知 | `outcome_unknown` / `Some(true)` | 本次不会虚构投递。继续 receive，通过本 consumer PEL 或 claim 恢复。 |
| XACK 已发送，结果未知 | `outcome_unknown` / `Some(true)` | 对同一 token 重试原来的 `Accept` 或 `Reject`；不能改为 `Retry` 或另一终结决定。 |
| 隔离脚本结果未知或可能部分执行 | `outcome_unknown` / `Some(false)` | 检查源记录、PEL、owner 及隔离副本，不盲目重放脚本。 |
| I/O 前准入名额耗尽 | `resource_limit` / `Some(true)` | 退避、降低并发或关闭闲置 receiver；被拒绝的操作没有发送业务命令。 |
| 发布超过 payload / wire 限额 | `payload_too_large` / `wire_too_large`，`Some(false)` | 缩小编码 payload/元数据，或有意提高配置；不会发送 XADD。 |
| wire 限额内的未知版本 | `unsupported_wire_version` / `Some(false)` | 升级 reader，保留 PEL，直到兼容 reader 完成结算。 |

SPI 的结算状态只约束当前 receiver/token：

| 状态 | 允许的请求 | 结果 |
| --- | --- | --- |
| Open | `Retry` | 释放本地活跃名额，保留 Redis PEL，在本地提交 Retry。 |
| Open | `Accept` / `Reject` | 先取得连接与命令名额，在 XACK 可能发送前固定意图。 |
| AckPending(原意图) | 仅原来的终结意图 | 重发 XACK；合法整数回复 0 或 1 后完成本地结算。 |
| Applied(原意图) | 仅原意图 | 幂等成功，不执行 Redis I/O。 |
| AckPending / Applied | 冲突意图 | 返回无效结算 token，不发送 Redis 命令。 |

尚未 poll 的 settle future，以及连接获取失败或取消，都保留 Open。首个 XACK 尝试若收到完整的 **Redis 顶层拒绝回复**，可以恢复 Open，因为该命令未执行。断线、超时、畸形回复、嵌套 RESP 错误或取消会保留 AckPending；只要此前已有结果未知的尝试，后续明确拒绝也不能重新开放 token。未知 ACK 保留活跃名额，直到相同意图完成或 receiver 关闭。意图没有跨进程持久化，重启后不能据此推断历史，也不提供跨 consumer fencing。

## 10. 怎样限制资源和消息尺寸

同一个已创建 SPI 实例及其共享 Arc clone 共用预算。每次新的 `create_configured` 调用都会建立独立 client 预算；不同 registry 选出的实例不会共用整个进程或 Redis server 的全局准入限制。`max_concurrent_commands` 约束已准入的短操作，默认总额度为 64；其中 `reserved_settlement_commands`（默认 8）在普通命令额度耗尽时仍留给结算。专用 receiver 连接使用独立的 `max_active_receivers` 限制（默认 256），不占短命令名额。总命令数至少为 2；旧值 1 属于不兼容配置，会被拒绝。降低总额度时，未显式配置的结算保留数会自动调整为 `min(8, 总额 - 1)`；只有要自定义保留数时才需显式配置。失败、取消会释放本地命令名额；close/drop 释放 receiver 名额，token 继续存活也不会占用该名额。取消**不保证**multiplexed driver 停止请求，也不保证 Redis server 上已无在途命令。provider 不维护无界准入等待队列；专用阻塞读取与共享短命令通道分开。

活跃 receiver 上限不保证并发 poll 的每轮恢复工作都能准入；claim 等短恢复命令共用普通命令通道。高并发 poll 应处理可重试的 `resource_limit`，可限制 poll 并发或调整预算；提高预算不保证消除拒绝，也不是服务端连接硬总上限。准入按 provider 实例隔离，observer、Sentinel 和其他服务连接不一定计入这些限制。

`max_idle_connections` 默认为 8，且不得超过 `max_concurrent_commands`。短操作上限设为 8 以下时，应同时降低空闲保留数，例如同时设置 `redis.max_concurrent_commands=4` 和 `redis.max_idle_connections=4`；只降低前者会被配置校验拒绝。新增字节、时间、数量配置均为有限范围内的正十进制数，不能用 0 关闭限制。

默认 1MiB payload 限额按编码字节计量，8MiB wire 限额计入完整 JSON，包括 byte-array 膨胀和元数据。payload 合法不代表任意 headers 都能装入 wire。发布先检查 payload，再通过有界 sink 写出 JSON；中间 headers JSON 也受限。provider 编码借用调用方元数据和 payload，分配的 headers/wire String 都有界；直接调用 `WireFields::from_outbound` 转换不会应用这些 provider 配置限额。接收先检查借用的原始 wire，再解析 UTF-8/JSON 和版本 1 payload。历史超限记录以 `oversized_wire`/`oversized_payload` 隔离并返回 Gap；wire 限额优先于未知版本处理。版本 1 拒绝第 128 层 JSON 容器，包括被忽略的字段；未知版本在 v1 字段形状和深度校验前返回。

限制约束 provider 后续序列化、解析的追加分配。RESP 字节已由 Redis client 库接收，因此它不是抵御恶意超大 RESP bulk 的硬隔离边界，也不是整个进程的绝对内存上限。应结合可信 Redis/网络、server `maxmemory`、应用并发及源 stream/隔离流保留策略管理资源。隔离流没有自动保留上限。

## 11. 怎样维护消费组和处理下游通知

### 采集 provider 与 Redis 的证据

前面的同步示例在创建总线后调用 `RedisProviderDiagnostics::snapshots()`；异步 SPI 也使用同一入口。采集时保持总线存活，按预期的 `namespace` 和 `mode` 筛选，再用进程身份加 `instance_id` 标记各条时序数据。ID 在单个进程内递增且不会复用；实例及剩余持有者释放后，它就不再出现在目录中。同一进程重建 SPI 会分配新 ID，计数从零开始；只有进程重启才会重置 ID 序列。ID 不是 Redis stream ID，也不能充当持久标识。未启用 sync/async feature 时返回空列表。`snapshots()` 构造快照时逐字段读取原子值，getter 返回已采样的值，不会重新读取原子状态。因此，多个字段并非同一瞬间的事务快照；快照不包含端点、凭据、payload 或原始 Redis 错误。

`general_in_flight` 是已占用的普通短命令名额，包含从普通通道准入的结算；`settlement_in_flight` 只计算已占用的结算预留名额。两者相加可估算采样时的短命令占用，`active_receivers` 则表示专用 receiver 租约。这三个是在构造快照时采样的 gauge。其余 getter 返回该 SPI 生命周期内单调递增、达到上限后饱和的进程内计数的采样值：

| Getter | 计数时机 |
| --- | --- |
| `command_rejections`、`receiver_rejections` | 短命令或 receiver 准入失败，每个被拒请求计一次。 |
| `connection_attempts`、`connection_failures` | provider 实际打开连接的调用及其失败；池/cache 命中不计。一次 Sentinel 打开操作即使探测多个节点，也只计一次。 |
| `publish_accepted`、`publish_unknown` | `XADD` 确认接纳，或发布调用返回结果未知；取消后没有返回值的异步发布不计入任何一种结果。 |
| `receive_unknown`、`settlement_unknown` | SPI receive 或 settle 调用返回 `outcome_unknown`，每次调用计一次。 |
| `recovery_claim_commands` | 实际发出的 `XAUTOCLAIM` 命令数，不是认领的记录数。 |
| `quarantine_succeeded`、`delivery_gaps` | 已确认成功的隔离副本，以及实际返回 facade 的 Gap；tombstone Gap 不一定对应隔离副本。 |

facade 的 delivery metrics 描述排队、执行中的 handler 和结算重试；这里的 SPI 计数反映 provider 准入和 Redis 操作结果。两者都不代表 Redis 的 PEL 数量或内存占用，后者必须查询 Redis。先用公开 `naming` API 的 `stream_key(namespace, topic)`、`group_name(namespace, topic, subscriber, Some(group))`（以 subscriber 为 group 时传 `None`）和 `poison_key(namespace, topic, generated_group_name)` 生成实际名称，不能猜测 `qubit:*` 的完整 key。还应通过 `XINFO GROUPS` 列出实际存在的所有组，而不只检查配置文件中的组。

```text
XLEN <stream-key>
XPENDING <stream-key> <group>
XPENDING <stream-key> <group> - + 100
XINFO GROUPS <stream-key>
XINFO CONSUMERS <stream-key> <group>
XLEN <quarantine-key>
XRANGE <quarantine-key> - + COUNT 100
INFO MEMORY
```

通过有权限的运维连接执行这些命令。`XPENDING` 汇总提供各组 pending 数量；详细查询应翻页，找出 idle 最久的记录及其 owner。`XINFO GROUPS` 给出已有组最后投递位置，`XINFO CONSUMERS` 用于查看 consumer 活动情况。Redis 6.2 不能依赖 Redis 7 的 `lag` 字段，应结合游标和应用处理进度判断未读工作。`INFO MEMORY` 是整个 Redis 实例的指标，不属于某个 provider；应与该 Redis 部署的内存预算比较。命令活动可补充查看 `INFO commandstats`。隔离流单独增长，须独立采集 `XLEN`；`XRANGE ... COUNT 100` 只读取一页，完整检查应以上一页末尾 ID 为排他下界继续查询，直到没有后续记录。

初始采集频率可设为每分钟一次：进程内快照、源/隔离流长度、group/consumer 和 PEL 汇总、PEL 最久 idle、使用 outbox 的应用的最老行年龄，以及 Redis 内存；同时计算计数器五分钟增量。示例告警是 `command_rejections` 或 `receiver_rejections` 在五分钟内增长、outbox 最老行超过应用通知延迟 SLO、PEL 最久 idle 超过 `2 × redis.claim_min_idle_ms`，或 Redis 已用内存超过该实例分配预算的 75%。频率与阈值均须按业务负载和处置能力调整。重建 SPI 后，新 ID 的计数从零开始；进程重启还会重置 ID 序列。这些重置都不能说明压力已消失。先排查准入压力、停滞的 handler、outbox 发布和 Redis 内存，再调整限额。

### 人工决定保留策略

1. 用 `XINFO GROUPS` 列出所有现存 group，包括其他服务创建的组。逐组查看 `XPENDING`、`XINFO CONSUMERS`，核对未读位置与所需历史，并记录 owner 和最久 idle。不能只凭 `XLEN` 断定可以裁剪。
2. 确认未来是否会创建回放组、需要回溯到哪里，以及业务与审计要求保留到何时。记录由谁接受未读消息和 pending payload 可能丢失。
3. 只要 group、PEL 记录、未读位置、未来回放需求或审计期限有任何一项未确认，就维持源 stream 默认无限保留，通过限制生产者、修复 consumer 或经评审的归档/迁移方案解决容量问题。stream 增长需要调查，不能直接视为裁剪许可。
4. 明确接受历史丢失后，才同时配置 `redis.stream_maxlen_approx=<正整数>` 和 `redis.allow_lossy_retention=true`。`XADD MAXLEN ~` 是近似且有损的：它可能静默删除未读历史，也可能删除 PEL 仍引用的 payload，导致工作无法恢复或产生 Gap，不能当成无损内存治理。启用后再次核查所有 group。
5. 隔离流应另用 `XLEN` 和抽样 `XRANGE` 检查，依据 `source_stream`、`source_id`、`group`、`reason` 核对业务影响。只有满足证据和审计要求后，才归档或移除隔离记录。provider 不会自动裁剪隔离流。

随机 consumer 名称会随重启积累，provider 不自动执行 `DELCONSUMER`。清理旧 consumer 前须停止对应实例，确认其 PEL 已清空并满足业务保留要求。close 不确认消息、不删除 group 或 stream。provider 没有内置指标 exporter，也不会自动保留或清理 stream/隔离流。

Redis 持久化和复制由部署负责。`Accepted` 只证明 XADD 被接受，不证明 fsync、副本持久化、handler 成功或账单提交。在新 observer 连接执行 `WAIT`，不能为另一条 provider 连接的写入提供 fencing 保证。Sentinel 提升可能丢失尚未复制的写入或消费组状态；恢复验收须检查实际游标、pending ID 和 owner。

### 部署就绪检查表

上线前和 Redis 故障转移后，都应结合部署的恢复目标，用实际 stream key 和消费组执行以下只读检查：

| 检查项 | 只读命令 | 观察内容与运维判断 |
| --- | --- | --- |
| 持久化与重启恢复 | `INFO persistence` | 查看当前 Redis 版本提供的持久化活动、最近保存/写入状态等字段，并确认它们符合部署的恢复目标。该输出不能证明某条具体事件已经 fsync。 |
| 复制与故障转移 | `INFO replication` | 查看节点角色、副本连接/同步状态和复制偏移量。结合当前拓扑评估恢复预期；副本已连接或偏移量已更新，都不能证明每条已接纳事件在主从提升后仍然存在。 |
| Stream 保留与增长 | `XLEN <stream-key>` | 连续记录长度并与保留和容量规划对照。单看长度无法判断还有多少未读或 pending 工作。 |
| 消费组进度 | `XINFO GROUPS <stream-key>` | 检查实际存在的消费组、pending 数量和最后投递位置；只有服务端版本提供 `lag` 时才查看该字段。将组进度与应用处理情况、回放需求对照。 |
| 待处理投递与恢复归属 | `XPENDING <stream-key> <group>` 和 `XPENDING <stream-key> <group> - + 100` | 检查 pending 数量、consumer 归属、投递等待时间，并翻页覆盖所有记录；后续页从上一页最后返回的 ID 之后继续，避免重复边界记录。结合 handler 时长和恢复策略判断工作是否持续推进，或需要调查。 |

验收时要区分不同完成阶段：`Accepted` 表示 Redis 已回复接受 `XADD`；持久化和 fsync 取决于 Redis 部署设置；副本确认是独立的复制事件，本身也不代表副本已落盘；消费者 ACK（`XACK`）是在处理后结算投递，不等于应用数据库事务提交；业务提交由应用负责。provider 不会把这些阶段合并成原子操作。投递语义是至少一次，因此消费者的业务副作用应可幂等，例如按业务 ID 去重，并在使用带版本通知时只保留最高 `state_version`。`XADD MAXLEN ~` 明确属于有损保留策略，可能裁掉未读或 pending 历史；只有完成上文的保留评估并接受相应丢失后才能启用。

处理 typed `qubit-task` 通知时，consumer 应按 `TaskId` 去重并保留最高 `state_version`，忽略重复、旧版本通知，并查询任务服务取得权威状态。typed SQLite task service 可启用可选 outbox，在自己的状态事务中捕获生命周期变更，并重试向 Redis 发布。投递仍是至少一次；这不会令无关的应用业务事务与任务状态或 Redis 原子提交。

## 12. 从 provider 0.6 升级

provider 0.7 最初迁移时与 core 0.19 配套；当前 provider 0.7 manifest 使用 core 0.20。新版 `RedisSubscriptionProfile` 根据明确的 `StartPosition` 构造 durable 订阅选项；创建新订阅时使用它，并保留预期的消费组。核心 codec 注册现在会拒绝重复载荷类型并返回 `Result`；要传播错误，使用 `?`，确需替换时才调用 `replace`。`InboundMessage` 增加可选 provider attempt 元数据，但 `into_parts` tuple 保持原样。保留 wire 版本 1 数据和现有消费组；发布前应验证 pending 记录，并按上文分别采集 provider 快照与 Redis PEL/内存。[迁移指南](migration.zh_CN.md)列出完整变更。


参阅[设计说明](design.zh_CN.md)、[覆盖率证据](coverage-review.zh_CN.md)和[工作负载基准](connection-reuse-benchmark.zh_CN.md)。性能、覆盖率须以各自测量为证；本指南没有宣称新的吞吐量或最终覆盖率结果。

## 支持范围

支持 Redis 单实例和 Sentinel、Redis 6.2+、编码 payload、消费组、accept/retry/reject 结算，以及从 Redis stream position 重放。暂不支持 Cluster、native payload、顺序保证、延迟投递、自动生命周期清理、死信路由和 TLS 配置。provider 不承诺恰好一次处理。


## 有损 Stream 保留策略

Redis Stream 默认不裁剪。启用近似 `MAXLEN` 裁剪时，必须同时设置 `redis.stream_maxlen_approx=<正整数>` 和 `redis.allow_lossy_retention=true`。裁剪可能删除尚未读取或仍处于 pending 状态的条目；订阅默认会在缺口后停止。验证保留策略时检查 `XLEN`、`XPENDING` 和 `XINFO`。`XADD` 接纳不代表 fsync 或副本持久化保证。
