# Redis Streams 用户指南

**读者：** 使用 `qubit-event-bus` 0.14 和 `qubit-event-bus-redis` 0.1 的 Rust 服务开发者。本指南以订单发布服务和账单消费服务为例，说明如何通过 Redis 共享事件，同时让应用代码继续使用 event-bus facade。

[English](user_guide.md) · [README](../README.zh_CN.md) · [API 文档](https://docs.rs/qubit-event-bus-redis)

## 1. 添加并选择 provider

将 facade 和 provider 都作为直接依赖。provider crate 默认启用 `discovery`，会把 `redis-streams` 提交到同步和异步 provider inventory。应用在启动时调用 `EventBusRegistry::discover()` 或 `AsyncEventBusRegistry::discover()`，再按 ID 选择 Redis。

```toml
[dependencies]
qubit-event-bus = { version = "0.14", features = ["discovery"] }
qubit-event-bus-redis = "0.1"
qubit-spi = "0.13"
```

如果应用只使用一种 SPI，可设置 `default-features = false`，再选择 `features = ["sync"]` 或 `features = ["async"]`。`async` 可以使用 Redis client 的 Smol adapter，也能在 Tokio host 中运行。crate 不会启动 Tokio runtime 或生成订阅 worker；异步 facade 的 runner 由应用现有 executor 驱动。

Redis 需要 6.2 或更高版本。集成测试使用 Docker 启动隔离的 Redis 6.2、Redis 7 和 Sentinel 进程。

## 2. 为跨进程 payload 注册 codec

Redis 保存编码字节，因此通过 facade 使用的每种 payload 类型都要提供 `EventCodec<T>`。下面以 UTF-8 `String` 为例。生产应用可替换为 JSON、Protobuf 或带 schema 的 codec。

```rust
use std::sync::Arc;

use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::CodecError;
use qubit_event_bus::model::ContentType;

struct Utf8Codec(ContentType);

impl EventCodec<String> for Utf8Codec {
    fn content_type(&self) -> &ContentType { &self.0 }
    fn schema_id(&self) -> Option<&qubit_event_bus::model::SchemaId> { None }
    fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from(value.as_bytes()))
    }
    fn decode(&self, bytes: &[u8]) -> Result<String, CodecError> {
        String::from_utf8(bytes.to_vec()).map_err(|source| CodecError::Decode { source: Box::new(source) })
    }
}
```

创建 `CodecRegistry` 并注册 `Utf8Codec`，再将它放入 `EventBusFacadeConfig`。缺少 codec 时，facade 会在访问 Redis 前拒绝该类型的发布或订阅。

## 3. 使用同步 SPI 发布和消费

服务配置只需要 Redis URL 和 namespace。显式选择 `redis-streams`，避免 registry fallback 意外使用其他 provider。

```rust,no_run
use std::sync::Arc;

use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::model::{ContentType, ConsumerGroup, PublishRequest, StartPosition, SubscribeRequest, SubscriptionDurability, Topic};
use qubit_event_bus::registry::{EventBusConfig, EventBusRegistry};
use qubit_event_bus::model::ProviderOptions;
use qubit_spi::ProviderSelection;

fn start_service() -> Result<(), Box<dyn std::error::Error>> {
    let mut codecs = CodecRegistry::new();
    codecs.register::<String>(Arc::new(Utf8Codec(ContentType::new("text/plain")?)));
    let facade = EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs));
    let options: ProviderOptions = [
        ("redis.url".into(), "redis://127.0.0.1/".into()),
        ("redis.namespace".into(), "orders".into()),
    ].into();
    let config = EventBusConfig::default()
        .with_selection(ProviderSelection::named("redis-streams")?)
        .with_provider_options(options)
        .with_facade_config(facade);
    let registry = EventBusRegistry::discover()?;
    let bus = registry.create(&config)?;
    let topic = Topic::<String>::new("orders.created")?;
    bus.publish(PublishRequest::new(topic.clone(), "order-42".to_owned())?)?;

    let request = SubscribeRequest::builder()
        .subscriber_id(qubit_event_bus::SubscriberId::new("billing-worker")?)
        .topic(topic)
        .consumer_group(ConsumerGroup::new("billing")?)
        .durability(SubscriptionDurability::Durable)
        .start_position(StartPosition::Earliest)
        .build()?;
    let subscription = bus.subscribe(request, |delivery| {
        println!("process order event: {}", delivery.payload());
    })?;
    // 长期运行的服务保留 subscription，并在关闭时取消它。
    drop(subscription);
    Ok(())
}
```

示例先写入事件，再创建 `Earliest` 消费组，因此新组能读取已保留的记录。生产服务若使用 `New`，应先注册 consumer，再依赖后续发布的事件。服务断连时，Durable group 会保留在 Redis 中。目前即使订阅请求是 `Ephemeral`，provider 也会保留消费组。

指定 `ConsumerGroup` 后，namespace、topic 和 group 相同的实例会共同分工。不同 group 各自维护读取位置，因此都能收到自己的副本。未指定 group 时，subscriber ID 用作 group 身份。

## 4. 驱动异步 SPI

异步 bus 的创建、发布、订阅和接收操作都会返回 runtime-neutral future。小型示例用 `futures_lite::future::block_on` 驱动；长期运行的服务通常会在现有 executor 上轮询这些 future，并让订阅与其他服务工作并发运行。

```rust,no_run
use futures_lite::future::block_on;
use qubit_event_bus::registry::{AsyncEventBusRegistry, EventBusConfig};
use qubit_event_bus::model::{ProviderOptions, PublishRequest, Topic};
use qubit_spi::ProviderSelection;

fn publish_async() -> Result<(), Box<dyn std::error::Error>> {
    block_on(async {
        let options: ProviderOptions = [
            ("redis.url".into(), "redis://127.0.0.1/".into()),
            ("redis.namespace".into(), "orders".into()),
        ].into();
        let config = EventBusConfig::default()
            .with_selection(ProviderSelection::named("redis-streams")?)
            .with_provider_options(options);
        let registry = AsyncEventBusRegistry::discover()?;
        let bus = registry.create(&config).await?;
        let topic = Topic::<String>::new("orders.created")?;
        bus.publish(PublishRequest::new(topic, "order-43".to_owned())?).await?;
        Ok(())
    })
}
```

异步 facade 也要配置与同步示例相同的 codec registry。`AsyncSubscription::run` 由调用方驱动，future 应由应用 executor 轮询。丢弃尚未完成的 `receive` future 不会确认消息。Redis 会把记录留在消费组的 pending entries list；当前 consumer 后续可以再次读取，其他 consumer 则可在 idle threshold 到期后通过 `XAUTOCLAIM` 接管。

## 5. 配置 Redis 与 Sentinel

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `redis.url` | `redis://127.0.0.1/` | 单实例 Redis URL；不允许在 URL 中直接写 username/password。 |
| `redis.namespace` | `qubit` | 用于生成 stream 和消费组 key 的命名空间。 |
| `redis.claim_min_idle_ms` | `30000` | 其他 consumer 可以认领 pending entry 前所需的空闲毫秒数。 |
| `redis.max_unsettled_per_subscription` | `100` | 每个订阅已投递但未结算的消息上限；达到上限后 receive 会等待。 |
| `redis.username_env` | 未设置 | 保存 Redis ACL username 的环境变量名称。 |
| `redis.password_env` | 未设置 | 保存 Redis ACL password 的环境变量名称。 |
| `redis.sentinel.nodes` | 未设置 | Sentinel 的逗号分隔 `host:port` 地址。 |
| `redis.sentinel.service_name` | 未设置 | Sentinel master 服务名；与 `nodes` 一起配置。 |
| `redis.sentinel.username_env` | 未设置 | 保存 Sentinel ACL username 的环境变量名称。 |
| `redis.sentinel.password_env` | 未设置 | 保存 Sentinel ACL password 的环境变量名称。 |

启用 Sentinel 时，`redis.sentinel.nodes` 和 `redis.sentinel.service_name` 必须同时设置。此时 Redis URL 仍须是合法 URL，但不会用于查找 master。每条命令都会通过 Sentinel 获取连接，因此后续命令可以在故障转移后重新定位晋升的 master。

凭据应放在服务运行环境中。provider options 保存环境变量名称；`RedisEventBusConfig` 的 `Debug` 会隐藏 URL 和凭据。不要把明文密钥放入 provider options、URL、命令行参数或日志。此版本没有启用 `redis-rs` 的 TLS 参数；增加 TLS 支持前，应将 Redis 流量限制在可信网络内。

## 6. 理解投递、重试和清理

provider 将一条 JSON wire record 写入 Redis Stream 的 `wire` 字段。内容包括协议版本、事件 ID、时间戳、headers、可选排序 key、content type、可选 schema ID 和 payload 字节。未知版本会返回 provider 错误。`XADD` 成功返回 `Accepted`；如果连接在收到回复前中断，调用方无法确定记录是否写入，重试发布可能产生重复事件。

| 操作 | Redis 行为 | 对应用的影响 |
| --- | --- | --- |
| `Accept` | 执行 `XACK` | 从消费组 pending 列表移除消息。 |
| `Reject` | 执行 `XACK` | 结束这次投递；不会创建死信 stream。 |
| `Retry` | 保留 pending 状态 | 当前 consumer 可重新读取，超过 idle threshold 后也可被认领。 |
| 关闭或 drop | 不会隐式执行 `XACK` | 未结算消息仍可恢复。 |
| 取消异步 receive | 不会隐式执行 `XACK` | 已读记录留在 PEL 中，供后续读取或认领。 |

Redis 提供至少一次投递，因此 handler 应具备幂等性。如果 handler 运行时间超过 `redis.claim_min_idle_ms`，另一个 consumer 可能在原 handler 仍执行时认领同一事件。idle threshold 应覆盖常见和最慢的处理时间；重复代价高时，应用还应按业务 ID 去重。

每个订阅的未结算消息达到 `redis.max_unsettled_per_subscription` 后会暂停接收新记录。结算或 retry 后会释放容量。该设置限制进程内的投递压力，不会限制 Redis stream 增长。

`StartPosition::New` 会在当前 stream 尾部创建 group；`Earliest` 会从 `0-0` 开始创建新 group；`At("milliseconds-sequence")` 使用 Redis Stream ID。group 一旦创建，读取游标由 Redis 保留；之后更改请求的 start position 不会重置现有 group。

provider 不会裁剪 stream 或删除 group。应监控 Redis 内存和 stream 增长。删除 stream 或 group 前，先停止 consumer 并决定如何处理 pending 消息；删除 pending record 可能导致 `ReceiveOutcome::Gap`。Redis persistence 和 replication 配置需符合业务恢复目标：`Accepted` 不代表已 fsync，Sentinel 切换也可能丢失尚未复制的写入。

## 7. 排查常见问题

- **Registry 中没有 `redis-streams`：** 将 `qubit-event-bus-redis` 作为直接依赖，启用 `discovery`，并调用对应的同步或异步 registry `discover()`。
- **类型化发布或订阅提示缺少 codec：** 在 `EventBusFacadeConfig` 的 `CodecRegistry` 中为该 payload 类型注册 `EventCodec<T>`。
- **Provider 拒绝配置：** 检查 `redis.url`、`redis.namespace`、成对配置的 Sentinel 参数，以及环境变量名称引用的凭据是否存在。URL 中的明文凭据会被拒绝，以免出现在诊断信息中。
- **Consumer 没收到旧事件：** 使用新 group 并设置 `StartPosition::Earliest`；已有 group 会沿用 Redis 中保存的游标。
- **消息接管太早或太晚：** 根据 handler 的正常和最坏执行时长调整 `redis.claim_min_idle_ms`。系统仍可能重复投递。
- **Stream 维护后出现 gap：** 检查运维侧的 `XDEL`/`XTRIM` 操作及 pending entries，再决定后续清理策略。gap 表示 Redis 中已找不到至少一条 pending record。
- **Sentinel 无法连接：** 检查 Sentinel 地址、master 服务名、ACL 环境变量引用、quorum，以及应用主机能否访问 Sentinel 返回的 Redis master 地址。

此版本没有内建 PEL 或重连指标。运维时可以使用 Redis `XPENDING`、`XINFO STREAM` 和 `XINFO GROUPS`，并在应用 telemetry 中记录 provider 错误和 publish receipt ID。

## 8. 正常关闭

同步总线优雅关闭前，先取消订阅。异步总线关闭前，停止或关闭 `AsyncSubscription`，然后等待 `AsyncEventBus::shutdown`。立即关闭会让未结算消息留在 Redis；关闭本身不代表消息已接受。

## 支持范围

支持 Redis 单实例和 Sentinel、Redis 6.2+、编码 payload、消费组、accept/retry/reject 结算，以及从 Redis stream position 重放。暂不支持 Cluster、native payload、顺序保证、延迟投递、自动裁剪/删除、死信路由和 TLS 配置。provider 不承诺恰好一次处理。
