# Redis Streams 用户指南

**读者：** 使用当前 sibling source checkout 的 Rust 服务开发者；manifest 版本为 provider 0.7.0、core 0.21 和 SPI 0.14。当前发布安装证据见[安装与验证状态](design.zh_CN.md#安装与验证状态)。

[English](user_guide.md) · [README](../README.zh_CN.md) · [API 文档](https://docs.rs/qubit-event-bus-redis)

## 1. 添加并选择 provider

在 Qubit 源码 workspace 中，将两个 sibling checkout 都作为直接依赖。provider crate 默认启用 `discovery`，会把 `redis-streams` 提交到同步和异步 provider inventory。应用在启动时调用 `EventBusRegistry::discover()` 或 `AsyncEventBusRegistry::discover()`，再按 ID 选择 Redis。

<!-- current-source-dependencies -->
```toml
[dependencies]
qubit-event-bus = { version = "^0.21", path = "../rs-event-bus", features = ["discovery"] }
qubit-event-bus-redis = { version = "0.7.0", path = "../rs-event-bus-redis" }
qubit-spi = "^0.14"

[patch.crates-io]
qubit-event-bus = { path = "../rs-event-bus" }
```

这些路径适用于 sibling checkout；应按 consumer manifest 所在位置调整。patch 必须放在应用 workspace 根清单中。provider 的 registry core 依赖不会因应用直接依赖 path 版本而自动替换，因此两个依赖 path 和 patch path 必须指向同一个 core checkout。本示例用于本地源码集成。升级现有服务前，请先按迁移指南检查版本变化和恢复步骤。

当前 Cargo 清单中的 Redis 导入名为 `redis`，包名为 `qubit-redis`，精确固定到 `=2.0.0-rc.2`，来源为 registry。

Registry 验证有两种模式。默认 `source-candidate` 会将输入的 provider/core 版本与当前源码清单比较；`published` 跳过此比较，但仍要求精确稳定版本、registry 来源、唯一 core 包身份及正确的 provider→core 依赖边。两种模式都会执行 prepare、manifest 检查、生成锁文件、feature 构建和 metadata 校验。示例：`bash ci/check-registry-consumer.sh --mode published --provider-version X.Y.Z --core-version X.Y.Z --spi-version X.Y.Z`。

采用自动发现时，provider 关闭默认 feature 后应选择 `["sync", "discovery"]` 或 `["async", "discovery"]`，facade 也须启用 `discovery`。采用手动注册时，provider 只需 `["sync"]` 或 `["async"]`，facade 无须启用发现功能。`async` 可以使用 Redis client 的 Smol adapter，也能在 Tokio host 中运行。crate 不会启动 Tokio runtime 或生成订阅 worker；异步 facade 的 runner 由应用现有 executor 驱动。

Redis 需要 6.2 或更高版本。集成测试使用 Docker 启动隔离的 Redis 6.2、Redis 7 和 Sentinel 进程。fixture 会校验 `INFO server` 返回的实际版本，并输出 `backend`、`requested` 和 `actual_version`。本地 fallback 至少需要 Redis 6.2；显式 6.2/7 fixture 必须匹配所请求的版本系列，版本不符或探测失败都会报错。Docker 无法启动时，显式版本测试可通过 `RER_REDIS_6_2_SERVER` 或 `RER_REDIS_7_SERVER` 指定对应本地二进制；默认 fixture 则要求 `PATH` 中的 `redis-server` 达到支持版本。

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

服务配置显式选择 `redis-streams`，避免 registry fallback 意外使用其他 provider。示例还设置了 namespace，并选择沿用已有消费组；此选项用于重复运行演示，不会改变 provider 默认的 `reject` 策略。

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
                .provider_option("redis.existing_group_start", "resume")
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

此入门练习先安装账单 handler，再发布 `order-42`，等待 handler 发出通知，打印 `consumed order event: order-42`，最后取消订阅并关闭总线。生产 handler 应完成并提交应用自己的账单变更后才返回 `Ok(())`；打印和 channel 通知只是练习的验收信号。总线和订阅应由服务的启动、关闭模块持有。第一次运行会从 `Earliest` 创建消费组；再次运行时，订阅通过 `redis.existing_group_start=resume` 显式选择沿用 Redis 保存的游标。Redis 只在首次创建 group 时应用起始位置；恢复时不会执行 `XGROUP SETID`，也不会重放已确认的历史记录。更换 namespace 可隔离演示数据；已有 pending 记录仍可能先于本次发布的事件交付。新建的 `Earliest` 消费组也能读取已保留的记录。生产服务若使用 `New`，应先注册 consumer，再依赖后续发布的事件。Redis Streams 只接受 `Durable` 订阅；`Ephemeral` 会在执行 Redis I/O 前被拒绝。服务断连或关闭订阅时，group 会保留，未结算记录仍在 pending entries list 中。

限额内格式错误的版本 1 wire 记录由单条 Lua 脚本串行处理：先用 `XADD` 写入按 group 隔离的记录（`qubit:poison:*`），再用 `XACK` 确认源记录。隔离记录包含 `source_stream`、`source_id`、`group`、稳定的 `reason`、原始 `wire` 字节和 `wire_missing`。顶层 EVAL 权限错误码（`NOPERM`、`NOAUTH` 或 `WRONGPASS`）证明 Redis 在脚本执行前拒绝命令，返回不可重试的 `authentication`。脚本内部命令或 key 的 ACL 拒绝、其他脚本错误、无效状态回复、超时、断线或回复丢失均返回 `outcome_unknown`；后续步骤失败时脚本不会回滚已成功写入的隔离记录。未知结果可能已经写入副本，但源记录仍在 PEL。处理前应同时检查源 PEL 与隔离流，并核对源 ID、group、pending owner、`source_stream` 和 `source_id`。确认隔离成功后返回 `Gap`，不会把 payload 放进错误信息。可用 `XRANGE <quarantine-key> - + COUNT 100` 读取一页记录，并用 `XLEN`、`XPENDING` 监控隔离流、源 stream 和 pending 数量；完整检查须继续翻页。provider 不会自动裁剪这些 stream；运维应按保留策略归档或清理隔离数据。

仍可恢复的 pending 记录可能再次投递。如果 handler 运行时间超过 `redis.claim_min_idle_ms`，其他 consumer 可能接管该记录。若 `XADD` 回复丢失，发布结果未知，facade 默认安全门阻止自动重发；人工或明确允许的重试仍可能重复。`redis.max_unsettled_per_subscription` 限制本地活跃投递数，默认值为 100。

默认不限制 stream 长度。只有在完成下文的保留评估并接受历史丢失后，才在上方 `ProviderOptions` map 中同时加入 `("redis.stream_maxlen_approx".into(), "100000".into())` 和 `("redis.allow_lossy_retention".into(), "true".into())`。此后每次发布都会使用 `XADD MAXLEN ~ 100000`。Redis 可能裁剪尚未消费的记录，或消费组 pending 列表仍引用的记录；裁剪不保证投递，旧消息可能无法恢复。需要保留 pending 历史用于恢复时应保持此选项关闭。格式错误记录的隔离 stream 遵循单独的运维保留策略。

如果记录在任何 consumer 读取前被裁剪，Redis 不会通过 `ReceiveOutcome::Gap` 报告它们。provider 无法识别缺失的记录 ID，因此启用裁剪意味着可能发生 delivery API 无法发现的历史丢失，应用应在外部监控保留状态。

指定 `ConsumerGroup` 后，namespace、topic 和 group 相同的实例会共同分工。不同 group 各自维护读取位置，因此都能收到自己的副本。未指定 group 时，subscriber ID 用作 group 身份。

每个 Redis 订阅都会获得随机生成的 `qubit:consumer:<uuid>` consumer 名称。facade 的本地订阅 ID 不具备全局唯一性，因此不会用作 Redis consumer 身份。进程重启后会创建新 consumer；旧名称对应的 pending 记录仍保留在 group 中，达到 `redis.claim_min_idle_ms` 后可由 `XAUTOCLAIM` 恢复。

当前 wire 格式支持版本 1。wire 未超过 `redis.max_wire_bytes` 时，decoder 会先校验完整 JSON 文档和 Serde 递归保护，再判断版本。完整有效的未知 `u64` 整数版本会返回 kind 为 `unsupported_wire_version` 的非重试 `SpiError::Operation`，源记录保留在 pending 中，不会隔离或确认。即使声明未知版本，无效或嵌套过深的 JSON、缺失或非数字版本，以及格式错误的版本 1 记录仍会进入隔离流。应停止旧 consumer，部署支持该版本的 provider，再重启同一 group 的 consumer。使用 `XPENDING` 检查旧记录是否已恢复并结算。不要为了消除错误而直接清除 pending 记录。

恢复工作按时间间隔分批限额：claim 和本 consumer pending 扫描各最多执行 8 条命令；tombstone 修复每轮最多执行一次 `XPENDING`、四次 `XRANGE` 和四次隔离 `EVAL`。扫描游标会跨间隔和 receive 调用保留。`Duration::ZERO` 执行有界非阻塞恢复（最多一次 claim、一次本 consumer pending 查询和一次新消息查询），跳过 tombstone 扫描，并允许隔离一条格式错误的记录。`Duration::MAX` 表示无限等待，内部以 1 秒的有限 Redis 阻塞读取循环实现。有限超时是调度截止时间，限制继续发起恢复工作及实际 Redis `BLOCK` 时长。零超时表示不在 Redis 等待新消息，不表示网络操作必须在 0ms 内完成。阻塞读取的响应等待预算为实际 `BLOCK` 加 `redis.command_timeout_ms`。同步 socket 超时只是每次 I/O 等待的软限制；DNS、多地址尝试、setup 各阶段及持续小包传输都可能使整体调用超出预算。已经发出的命令不会被强制停止。

Redis 恢复扫描可能改变 pending entry 的元数据，即使 provider 在本地过滤了仍处于 active 状态的消息、没有再次返回它。`XAUTOCLAIM` 和读取本 consumer pending 的 `XREADGROUP` 都可能刷新 PEL idle 并增加 Redis 投递次数。恢复消息的 `provider_attempt` 报告的是 Redis 投递次数，不是 handler 执行次数；单独查看 `XPENDING` idle 既不能确定 handler 已运行多久，也不能证明它已停滞。排查恢复时应结合 PEL owner、投递次数、consumer 状态和应用 handler 遥测。

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
                    .provider_option("redis.existing_group_start", "resume")
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
            // The callback signals before it returns; wait for settlement while
            // the runner remains polled, then shut down with no owned delivery.
            while subscription.delivery_metrics().metrics.completed == 0 {
                future::yield_now().await;
            }
            let report = bus.shutdown(ShutdownMode::Graceful { timeout: Duration::from_secs(5) }).await?;
            if report.outcome != ShutdownOutcome::Complete {
                return Err("graceful shutdown did not complete".into());
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        };
        let (run_result, stop_result) = future::zip(run, stop).await;
        run_result?;
        stop_result?;
        Ok(())
    })
}
```

将 codec 片段放在此 `main` 之前，并添加依赖 `futures-lite = "2"` 和 `futures-channel = "0.3"`。程序打印 `consumed order event: order-43`，在持续轮询 runner 的同时等待投递完成结算，然后执行优雅关闭。异步 facade 同样通过 codec registry 编解码 payload。`AsyncSubscription::run` 由调用方驱动，future 应由应用 executor 轮询。丢弃尚未完成的 `receive` future 不会确认消息。Redis 会把记录留在消费组的 pending entries list；当前 consumer 后续可以再次读取，其他 consumer 则可在 idle threshold 到期后通过 `XAUTOCLAIM` 接管。


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

异步示例会先消费事件，再等待用户按 Enter 关闭；这样 handler 能先完成，runner 随后取消其下一次接收。示例使用持久消费组，并在订阅级显式选择沿用已有组保存的游标；新组仍从 `Earliest` 开始。进程退出后 Redis 仍会保留 stream 数据和 group；要重新开始干净的演示，可换一个 namespace。已有 pending 记录可能先于本次运行发布的事件交付。每条命令都需要对应的 standalone Redis 或 Sentinel 服务，并启用该示例所需的 features。

## 5. 配置 Redis 与 Sentinel

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `redis.url` | `redis://127.0.0.1/` | Redis 连接 URL；`redis://` 使用明文，`rediss://` 要求经过验证的 TLS。URL 中不能直接写 username/password。Sentinel 模式下，scheme 决定发现出的主节点连接方式。 |
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

### TLS 连接

TLS 默认关闭。保留 `redis.url=redis://...` 时，连接行为与原有明文配置相同；改用 `rediss://...` 后，provider 会对 Redis 单实例或 Sentinel 发现的主节点启用 TLS。连接时会验证证书链和主机名，也会拒绝关闭验证的 URL 参数。握手、证书或主机名校验失败会直接报错，不会改用明文重试。Sentinel 节点由独立的 `redis.sentinel.tls` 控制，因此可以分别选择明文 Sentinel + TLS 主节点，或 TLS Sentinel + TLS 主节点。主节点和 Sentinel 的 CA、客户端身份互不共用。

| 配置项 | 作用 |
| --- | --- |
| `redis.tls_ca_cert_path` | Redis 端点的可选 PEM CA 文件。未设置时，provider 保持 redis-rs 的默认信任根选择不变：本 crate 的默认 feature 图使用内置 WebPKI 根；下游 feature 统一可能额外启用系统根。除非同时设置客户端证书和私钥路径，否则不会配置客户端身份。仅适用于 `rediss://`。 |
| `redis.tls_client_cert_path` / `redis.tls_client_key_path` | Redis 端点的可选 mTLS 证书和私钥，必须同时设置；仅适用于 `rediss://`。 |
| `redis.sentinel.tls` | 是否对 Sentinel 节点启用经过验证的 TLS，默认 `false`；必须配置 `redis.sentinel.nodes`。 |
| `redis.sentinel.tls_ca_cert_path` | Sentinel 节点专用 PEM CA 文件；要求 `redis.sentinel.tls=true`。 |
| `redis.sentinel.tls_client_cert_path` / `redis.sentinel.tls_client_key_path` | Sentinel 节点专用 mTLS 身份，必须成对设置并启用 Sentinel TLS。 |

单实例使用私有 CA 和 mTLS 时，在现有 provider options 中加入：

```text
redis.url=rediss://redis.example.net:6379/0
redis.tls_ca_cert_path=/etc/redis/ca.pem
redis.tls_client_cert_path=/etc/redis/client.pem
redis.tls_client_key_path=/etc/redis/client-key.pem
```

如果 Sentinel 与发现出的主节点都使用 TLS，除了上面的主节点配置，再加入：

```text
redis.sentinel.nodes=sentinel-a.example.net:26379,sentinel-b.example.net:26379
redis.sentinel.service_name=primary
redis.sentinel.tls=true
redis.sentinel.tls_ca_cert_path=/etc/redis/sentinel-ca.pem
redis.sentinel.tls_client_cert_path=/etc/redis/sentinel-client.pem
redis.sentinel.tls_client_key_path=/etc/redis/sentinel-client-key.pem
```

若 Sentinel 使用明文、只有主节点使用 TLS，则不设置 `redis.sentinel.tls`（或设为 `false`），并让 `redis.url` 使用 `rediss://...`。除非 Sentinel 服务明确配置为信任该身份，否则不要把主节点证书复用为 Sentinel 证书。每个 PEM 文件都必须非空且不超过 1 MiB。provider 创建实例时读取并解析证书；读取或格式错误只返回通用 TLS 配置错误，不暴露路径、证书内容或底层库详情。替换证书后需重建 provider 实例，既有实例不会自动重载。TLS 故障不会改变 `XADD` 结果未知时的处理规则，也不会自动重放命令。

启用 Sentinel 时，`redis.sentinel.nodes` 和 `redis.sentinel.service_name` 必须同时设置。此时 Redis URL 仍须是合法 URL，但不会用于查找 master。每个 resolver 对每个 endpoint 和启用的 SPI 模式最多保留一条空闲 Sentinel 查询连接；每次操作仍执行 `SENTINEL get-master-addr-by-name`，并为 `ROLE` 和业务命令新建候选 master 连接。空闲 Sentinel socket 失效时，本轮该 endpoint 可能失败；下一次操作会重新建连，每次解析仍对每个 endpoint 最多尝试一次。Sentinel socket 和用于普通短命令的候选 master socket 使用固定 1 MiB RESP 响应帧预算，并在 socket 生命周期内持续生效。receiver master socket 保留自身配置的响应帧预算，包括合法且大于 1 MiB 的值。这是响应帧上限，不是进程 RSS 上限，也不限制发布请求体。每次解析对最多 16 个节点各尝试一次，优先使用上次成功的节点。同步 standalone 短命令最多复用 `redis.max_idle_connections` 条空闲连接；异步 standalone 操作分别使用按角色共享、按需初始化的普通与结算 multiplexed 连接。未回复的发布请求不会阻塞结算 EVAL，连接失败只会清除对应角色的代际缓存。同一冷初始化轮次中的调用共享成功或失败；失败后，后续调用可以重试。初始化 future 被取消时，该轮仍在等待的调用可以用自己的初始化 future 接手。不使用后台初始化任务或失败冷却期，连接超时也不提供端到端时限保证。代际检查防止旧连接的失败清除新连接。ROLE 验证后仍可能发生切换并拒绝命令，provider 不会透明重放 XADD。接收器使用独立连接，阻塞读取不会占用短命令通道。

凭据应放在服务运行环境中。provider options 保存环境变量名称；`RedisEventBusConfig` 的 `Debug` 会隐藏 URL、凭据和 TLS 文件路径。不要把明文密钥放入 provider options、URL、命令行参数或日志。TLS 保护传输并验证服务端身份，但不会改变 Redis 的持久化、复制、投递或业务幂等语义。

### 按职责配置 Redis ACL

应用账号只应拥有其实际订阅的 topic/group 所需命令，以及对应的 key 范围。provider 使用 Redis 客户端的默认 RESP2 协议：配置 password（可同时配置 ACL username）后，连接初始化会执行 `AUTH`，不需要 `HELLO`。数据库选择由 `redis.url` 中的数据库编号控制；仅当编号非零时（例如 URL 以 `/3` 结尾），客户端才会在连接初始化时执行 `SELECT`。客户端还会在新连接上尝试 `CLIENT SETINFO LIB-NAME` 和 `CLIENT SETINFO LIB-VER`，但会忽略命令回复；这只是可选元数据，不是 provider 正常工作的前提。Redis 6.2 不支持 `CLIENT SETINFO`。同步 standalone 空闲连接被复用前，连接池可能用 `PING` 检查连接。

| 账号或操作路径 | 允许的命令 | key 范围 |
| --- | --- | --- |
| 发布者 | `XADD`；同步 standalone 空闲连接健康检查还会用到 `PING` | 对配置 topic 的 stream 执行写入。 |
| 持久订阅与结算 | `XGROUP CREATE`、`XREADGROUP`、`XAUTOCLAIM`、`XPENDING`、`XRANGE`、`EVAL`（结算脚本调用 `XPENDING` 和 `XACK`）；同步 standalone 空闲连接健康检查还会用到 `PING` | 对 topic stream 读写。终态结算通过一条原子 EVAL 在 XACK 前校验当前 PEL owner。恢复命令用于不同接收路径，每次 receive 不一定都会执行全部命令。stream 尚不存在时，`XGROUP CREATE` 还需要创建该 key 的权限。 |
| 畸形记录隔离与 tombstone 修复 | `EVAL`，以及脚本内部的 `TYPE`、`XPENDING`、`XRANGE`、`XADD`、`XACK` | 源 stream 和该消费组专用的隔离 stream。脚本会检查两个 key 的类型、查询并读取源记录、写入隔离副本，再确认源记录。若 ACL key 规则或内部命令规则拒绝访问，仅授权 `EVAL` 仍然不够。 |
| Sentinel 发现账号 | `SENTINEL get-master-addr-by-name`，以及该连接配置 password 时执行的 `AUTH` | 不访问应用 stream key。Sentinel 凭据应与 master 凭据分开。支持 ACL 子命令规则的 Redis 可授予 `+sentinel|get-master-addr-by-name`；不支持子命令规则的服务器只能用更宽的 `+sentinel` 命令权限，应先核对该版本 ACL 能力。 |
| Sentinel 返回的 master 账号 | 按应用需要授予发布/订阅命令，另加 master 校验所用的 `ROLE` | 与 standalone 相同的应用 stream 和隔离 key。master 使用 `redis.username_env`、`redis.password_env`，不读取 Sentinel 的凭据变量。 |
| 运维/观察账号 | 按运维任务授予 `XINFO`、`XLEN`、`XPENDING`、`XRANGE`、`INFO` 等 | 只读检查指定的源 key 和隔离 key。请与应用账号分开管理。 |

可以使用公开 `naming` helper 核对实际 key。下面固定采用 namespace `guide-acl`、topic `events`、subscriber `billing-worker` 和显式 group `billing`：stream key 是 `qubit:stream:9:guide-acl:6:events`，group 名称是 `qubit:group:9:guide-acl:6:events:7:billing`，隔离 key 是 `qubit:poison:9:guide-acl:6:events:43:qubit:group:9:guide-acl:6:events:7:billing`。group 名称只是 stream 命令的参数，本身不是 Redis key。Redis ACL 的 key pattern 使用 glob 匹配，不是逐字匹配；`*`、`?`、方括号表达式和转义符都有模式含义。示例组件不含 glob 元字符。如果 namespace/topic/group 中含有这些字符，ACL pattern 可能匹配额外 key；应限制这类配置值，或仔细审查最终 pattern。添加其他 topic/group 时，可以逐项授权对应 stream 与隔离 key，也可以制定范围明确、经过实际验证的 pattern。

管理员可以按下面的结构为单一 topic/group 创建应用用户。生产密钥应通过受保护的 ACL 管理渠道替换示例占位符；不要把真实密钥直接粘贴到 shell history：

```text
ACL SETUSER event-worker reset on >replace-with-managed-secret \\
  ~qubit:stream:9:guide-acl:6:events \\
  ~qubit:poison:9:guide-acl:6:events:43:qubit:group:9:guide-acl:6:events:7:billing \\
  +ping +xadd +xgroup|create +xreadgroup +xautoclaim +xpending +xrange +xack \\
  +eval +type
```

正常终态结算和隔离均需要 `+eval`；终态结算还需要脚本内 `XPENDING` 与 `XACK` 权限。`+type` 用于隔离路径；省略它会使该路径不可用，但不能因此省略 `+eval`。缺少隔离权限会使畸形记录无法完成处理，必须按运维流程处置。若 URL 指向非零数据库，还要按 Redis ACL 策略允许客户端初始化时的 `SELECT`。如果 Redis 支持 `CLIENT SETINFO` 且希望记录客户端元数据，可以授予对应的 `setinfo` 子命令；拒绝该命令不会阻止 provider 完成连接初始化。不要用 `+@all` 代替逐项授权。Sentinel 需要单独配置发现权限；master 账号配置应用命令和 `ROLE`；运维观察则使用另一身份。

遇到 `NOPERM` 时，先从 Redis 错误或服务端审计日志确认被拒的命令和 key，再按上表检查对应路径。尤其确认 stream 和隔离 key pattern 都已授权、EVAL 及脚本实际调用的命令均可执行，并核对非零数据库 URL 是否触发了 `SELECT`。Sentinel 模式还要分别验证发现命令，以及使用正确凭据的 master `ROLE` 和 stream 权限。只有顶层 EVAL 的 `NOPERM`、`NOAUTH` 或 `WRONGPASS` 才归为不可重试的 `authentication`；脚本内部 ACL 拒绝归为 `outcome_unknown`，因为此前的写入可能已成功。仅更换密码不能补上缺失的命令或 key 权限。隔离结果未知时，用管理员或 observer 账号同时检查 `XPENDING` 和隔离 stream；Redis Lua 不会在后续 `XACK` 失败时回滚已执行的隔离 `XADD`。

## 6. 理解投递、重试和清理

provider 将一条 JSON wire record 写入 Redis Stream 的 `wire` 字段。内容包括协议版本、事件 ID、时间戳、headers、可选排序 key、content type、可选 schema ID 和 payload 字节。未知版本会返回 `unsupported_wire_version`，并保留在消费组 pending 列表中，详见下方恢复说明。`XADD` 成功返回 `Accepted`；如果连接在收到回复前中断，调用方无法确定记录是否写入，重试发布可能产生重复事件。

| 操作 | Redis 行为 | 对应用的影响 |
| --- | --- | --- |
| `Accept` | 一条 `EVAL`：检查指定 PEL 项 owner，再执行 `XACK` | 只有当前 consumer 仍拥有该项时才移除消息。owner 已变化时返回不可重试的 `settlement_ownership_changed`；停止该订阅，并用新 receiver 恢复消费组。 |
| `Reject` | 一条 `EVAL`：检查指定 PEL 项 owner，再执行 `XACK` | 只有当前 consumer 仍拥有该项时才结束投递；不会创建死信 stream。 |
| `Retry` | 保留 pending 状态 | 当前 consumer 可重新读取，超过 idle threshold 后也可被认领。 |
| 关闭或 drop | 不会隐式执行 `XACK` | 未结算消息仍可恢复。 |
| 取消异步 receive | 不会隐式执行 `XACK` | 已读记录留在 PEL 中，供后续读取或认领。 |

终态结算通过一条单 key `EVAL` 检查指定 PEL 项的当前 owner，然后才调用 `XACK`；不会单独发送裸 `XACK`。本地意图状态决定如何解释和重试结果：

| 本地状态与结果 | 处理结果 |
| --- | --- |
| `AckPending(original)`；EVAL 返回 `0` 或 `1` | 原意图变为 `Applied`；两个回复都确认完成。 |
| `AckPending(original)`；EVAL 返回 `-1` | 返回不可重试的 `settlement_ownership_changed`（`false`）；保留 `AckPending` 和活跃槽。停止受影响的订阅，再用新 receiver 恢复消费组。 |
| `AckPending(original)`；回复未知 | 保留原意图，只能重试同一意图；冲突意图会被拒绝。 |
| `Applied(original)`；再次请求相同意图 | 幂等成功且不执行 I/O；冲突意图会被拒绝。 |

首次 EVAL 收到完整、明确的 Redis 拒绝时，意图仍保持开放，可以再次尝试；ACK 结果一旦不确定，原意图就固定。owner 检查只保护 Redis 确认状态，不是业务 fencing，也不能阻止旧 handler 产生外部副作用。

Redis 提供至少一次投递，因此 handler 应具备幂等性。新消息经 `XREADGROUP >` 读取时，`delivery.context().provider_attempt()` 为 `Some(1)`。每条 pending 或 `XAUTOCLAIM` 恢复消息交付前，provider 会针对该 ID 查询 Redis `XPENDING` 明细；只有恰好一条明细的 ID 和当前 consumer owner 都匹配，且非零次数可由 SPI 的 `u32` 表示时，才报告 Redis 投递次数。查询失败、无结果、格式错误、次数为零或不可表示、以及校验不匹配时均为 `None`。每条恢复消息因此多一次 Redis 命令，新消息不增加查询。facade 本地重试次数请读取 `retry_attempt`。如果 handler 运行时间超过 `redis.claim_min_idle_ms`，另一个 consumer 可能在原 handler 仍执行时认领同一事件。idle threshold 应覆盖常见和最慢的处理时间；重复代价高时，应用还应按业务 ID 去重。

仍保留在 Redis 中的 stream entry 可能再次投递，因此 handler 必须具备幂等性。处理 `qubit-task` 通知时，应使用持久 consumer checkpoint、保留最高 `state_version`，并在发现版本缺口时查询任务服务的权威状态。

每个订阅的未结算消息达到 `redis.max_unsettled_per_subscription` 后会暂停接收新记录。结算或 retry 后会释放容量。该设置限制进程内的投递压力，不会限制 Redis stream 增长。

每个订阅第一次 `receive` 都会立即检查 pending。后续 receive 调用共享恢复截止时间：在 `redis.recovery_interval_ms`（默认 1,000 毫秒，范围 50–60,000）到期前只读新记录，不重复扫描；长时间等待的 receive 仍会在间隔到期时继续恢复扫描。Retry、receive 错误或已 poll 的 async receive 被取消时，会强制下一次调用执行恢复。调小间隔能更快接管达到 idle threshold 的 pending 消息，同时会增加 Redis 扫描命令。

`StartPosition::New` 会在当前 stream 尾部创建 group；如果 group 已存在，则从已保存游标继续。`Earliest` 会从 `0-0` 开始创建新 group；`At("milliseconds-sequence")` 使用 Redis Stream ID。Redis 会保留已有 group 的游标，因此现在对已有 group 使用 `Earliest` 或 `At` 会返回不可重试的 `existing_group_start_position_ignored`，不会静默忽略请求的起始位置。若要从新位置建组，请更换 group 名称。若你明确要保留 `Earliest` 或 `At` 配置但继续读取旧游标，请显式启用：

<!-- doc-example: existing-group-resume -->
```rust
use qubit_event_bus::model::{StartPosition, SubscribeOptions, SubscriptionDurability};

let options = SubscribeOptions::<String>::builder()
    .durability(SubscriptionDurability::Durable)
    .start_position(StartPosition::Earliest)
    .provider_option("redis.existing_group_start", "resume")
    .build();
```

默认策略为 `reject`，唯一合法值是 `reject` 和 `resume`。`resume` 接受 Redis 中已有游标，绝不会执行 `XGROUP SETID`，因此不会修改或回退该游标。未知的 `redis.*` 订阅选项或非法值会在 Redis 网络 I/O 前失败。

只有 RESP `+OK` 状态才确认 `XGROUP CREATE`。nil、整数、字符串（包括 bulk `OK` 和 `PONG`）、数组或嵌套错误回复都属于畸形回复，返回字段为 `operation=subscribe`、`kind=outcome_unknown`、`retryable=Some(false)` 的 `SpiError::Operation`，且不会因此增加重试。首次命令发送后超时或连接故障也表示结果未知，最多再尝试一次。如果重试无法连接、被拒绝（包括 `NOPERM` 或 `READONLY`）、再次发生传输错误或收到畸形回复，结果仍为未知，因为第二次观察无法证明首次命令没有创建 group。重试收到 `+OK` 可确认 group；收到 `BUSYGROUP` 时，`New` 或显式 `resume` 接受已有游标，也可继续成功。`Earliest` 或 `At` 配合默认 `reject` 策略时仍返回未知。停止盲目重试；使用公开 naming helper 获取 stream 和 group 名称，再运行 `XINFO GROUPS <stream>` 与 `XPENDING <stream> <group>` 检查状态。只有业务允许继续已有游标时，才显式设置 `redis.existing_group_start=resume`；如果必须按请求的新位置回放，则在评估重复处理后创建新的 group 名称。provider 不会通过 `XGROUP SETID`、删除 group 或 ACK 记录来自动恢复该结果。首次请求直接收到 `BUSYGROUP` 仍属于明确的已有组位置冲突。

未设置 `redis.stream_maxlen_approx` 时，provider 不会裁剪 stream，也不会自动删除 group。应监控 Redis 内存和 stream 增长。删除 stream 或 group 前，先停止 consumer 并决定如何处理 pending 消息；删除 pending record 可能导致 `ReceiveOutcome::Gap`。Redis persistence 和 replication 配置需符合业务恢复目标：`Accepted` 不代表已 fsync，Sentinel 切换也可能丢失尚未复制的写入。

### 限制 facade 工作量并收敛结算失败

同步与异步总线都通过 `EventBusFacadeConfig::with_delivery_scheduling` 配置 `DeliverySchedulingConfig`。四个正数限额默认依次为：**同时运行 4 个 handler、全局持有 256 条投递、每订阅持有 32 条投递、注册 256 个订阅**。持有量包括接收预留、已收待执行、运行中和结算中的投递。`max_running_handlers`、`max_owned_per_subscription` 均不能超过 `max_owned_deliveries`。provider 自身默认 100 条未结算记录的限额仍然生效；这些限制约束数量，不代表进程总内存预算。暂停的异步 session 仍占用订阅名额，直到关闭或终止清理完成。

等待中的热点键和结算退避不会占用 handler 执行名额。A 与 B 之间的公平调度要求 B 有可预留的持有额度，或者已被接收且可执行；不能据此保证穿透任意长度、尚未读取的 A 积压。Redis 声明不支持按键顺序，新调度器也不会赋予 Redis 该保证。

结算策略使用 `EventBusFacadeConfig::with_settlement_retry(SettlementRetryConfig::new(...)?);`，完整参数见[迁移示例](migration.zh_CN.md)。默认**总共尝试 5 次（含首次），从首次 SPI 尝试前开始计时最多 5 秒，首次退避 10 ms，最大退避 1 秒**。只有 `SpiError::retryable() == Some(true)` 允许重试；`Some(false)` 立即停止，`None` 以 `RetryabilityUnknown` 停止。panic、无效 token、时钟或 timer 失败也会终止。预算在尝试之间检查，不能取消已阻塞的 Redis 命令；超过截止点返回成功仍按成功处理。异步暂停取消了在途 settle 时，已经开始的 attempt 仍计入预算，恢复后继续使用原有有限预算。

结算终止时，facade 先记录首个原因，再清理并停止新的接收和 handler 启动。已启动的工作可继续完成，关闭失败不会覆盖原始原因。通过 `subscription.terminal_failure()` 获取 `SubscriptionStopReason::Settlement`，其中包含事件 ID、disposition、attempts、termination 和结构化 `Arc<SpiError>`。每次失败产生 `SettlementFailed`，首次终止产生一次 `SettlementStopped`；应把这些上下文保存到 telemetry。

使用 `bus.delivery_metrics()` 和 `subscription.delivery_metrics().metrics` 查看 `reserved_receives`、`queued`、`running_handlers`、`settling`、`lane_waiting`、`settlement_attempts`、`settlement_retries`、`settlement_terminal_failures`、`completed`、handler/settlement 耗时样本数、总量、最大值及 `oldest_owned_age`。`lane_waiting` 是 `queued` 的子集，重试计数只累计首次之后真正进入 SPI 的调用。并发变化期间的快照不保证事务一致性。关闭后的订阅句柄保留计数，总线累计值也不会随订阅移除而消失。这些 facade 快照不表示 Redis PEL 或重连状态，还需检查 `XPENDING` 和 `XINFO GROUPS`。

恢复时先保存首个原因和快照，按错误修复连接、codec、限额或策略，完成失败订阅的关闭，然后使用**相同 namespace、topic 和 group** 创建新的 `Durable` 订阅。新的 consumer 达到 `redis.claim_min_idle_ms` 条件后可认领保留的 pending 消息；`StartPosition` 不会重置已有 group。能否恢复取决于记录保留情况和 claim 策略：裁剪或删除可能破坏 pending 历史，已执行 `XACK` 但回复丢失时也没有可认领的消息。同一 token、同一 disposition 的重复结算是幂等的，但业务处理仍不保证恰好一次。

### 限制单条记录并恢复不兼容的 consumer

`redis.max_wire_bytes`、`redis.max_payload_bytes`、`redis.max_headers_bytes` 默认分别为 8,388,608、1,048,576、65,536 字节。值必须为正整数，零、非法数字和溢出都是配置错误。三个限额独立，payload 未超限仍可能因 JSON 膨胀使 wire 超限。facade 的 `PayloadLimits` 另有默认各 1 MiB 的双向限制，需要一起配置。

发布先在复制 payload 前检查大小，再用有界 writer 序列化 headers/wire，成功后才调用 `XADD`。接收先借用 wire 字节检查长度，再复制字符串；先解析版本，随后对版本 1 字段做有界解码，不构造完整 JSON Value 树。payload 每次 push 前检查增长，headers 字符串在二次 JSON 解析前检查，嵌套 JSON 保持深度保护。专用 receiver 连接还会在构造完整响应值前，将 RESP 响应帧限制为 `redis.max_wire_bytes + 65,536` 字节。解码预算会约束 provider 随字段解码而追加的分配。这些保护不构成进程 RSS 上限；receiver 帧上限也不适用于其他连接读取的响应。解码后的数据结构、其他连接和 Redis 本身都有独立的内存开销。

可读取记录超过 wire、payload 或 headers 配置容量时返回 `receive_limit_exceeded`，源记录保留在 PEL 中，不执行 `XACK`、`XDEL` 或隔离。`receive_response_too_large` 表示完整 RESP 帧超过 receiver 连接的独立上限；连接会关闭、订阅停止并释放 receiver 准入，可能形成的 PEL 记录保持不变。合法但不支持的 wire 版本同样保留。先用 `XPENDING` 检查记录及 owner，再修复限额或部署兼容 provider/codec，并以同一 group 创建新订阅验证恢复。是否删除或隔离保留记录应由运维人员评估决定，不要为掩盖错误而清理。限额内格式错误的版本 1 数据仍走隔离；旧版本发布的 wire 版本 1 继续可读。

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

同步总线由关闭模块调用 `subscription.cancel()` 等待 worker 结束，再关闭总线。异步总线可以像上方程序一样，继续轮询 runner，并同时执行优雅关闭以等待已准入 handler 及结算完成；总线关闭时会一并关闭订阅句柄。关闭 receiver 或选择立即关闭时，未结算消息可能留在 Redis；关闭本身不代表消息已接受。

## 9. 失败后怎样决定是否重试

应用通过 `SpiError::Operation` 的 `operation`、`kind` 和 `retryable` 判断失败类别。错误只提供稳定、脱敏的诊断，不暴露 Redis 原始文本。重试提示限定允许重试的上下文，不会触发透明重放，也不保证处理无重复。

| 失败 | kind / retryable | 应用处理 |
| --- | --- | --- |
| 发布已发送，但回复丢失、超时或畸形 | `outcome_unknown` / `Some(false)` | Redis 可能已保存事件。按业务 ID 核对；只有明确接受重复风险时才重新发布。provider 不透明重放 XADD。 |
| 接收/claim 已发送，结果未知 | `outcome_unknown` / `Some(true)` | 本次不会虚构投递。继续 receive，通过本 consumer PEL 或 claim 恢复。 |
| 结算 EVAL 已发送，结果未知 | `outcome_unknown` / `Some(true)` | 对同一 token 重试原来的 `Accept` 或 `Reject`；不能改为 `Retry` 或另一终结决定。 |
| 隔离脚本结果未知或可能部分执行 | `outcome_unknown` / `Some(false)` | 检查源记录、PEL、owner 及隔离副本，不盲目重放脚本。 |
| I/O 前准入名额耗尽 | `resource_limit` / `Some(true)` | 退避、降低并发或关闭闲置 receiver；被拒绝的操作没有发送业务命令。 |
| 发布超过 payload / wire 限额 | `payload_too_large` / `wire_too_large`，`Some(false)` | 缩小编码 payload/元数据，或有意提高配置；不会发送 XADD。 |
| wire 限额内且完整有效的未知版本 | `unsupported_wire_version` / `Some(false)`；无效或递归过深的 JSON 会隔离 | 升级 reader，保留 PEL，直到兼容 reader 完成结算。 |

SPI 的结算状态只约束当前 receiver/token：

| 状态 | 允许的请求 | 结果 |
| --- | --- | --- |
| Open | `Retry` | 释放本地活跃名额，保留 Redis PEL，在本地提交 Retry。 |
| Open | `Accept` / `Reject` | 先取得连接与命令名额，在 owner-check EVAL 可能发送前固定意图。 |
| AckPending(原意图) | 仅原来的终结意图 | 重发相同 owner-check EVAL；回复 0 或 1 后完成本地结算，-1 则返回 `settlement_ownership_changed` 并保留 AckPending。 |
| Applied(原意图) | 仅原意图 | 幂等成功，不执行 Redis I/O。 |
| AckPending / Applied | 冲突意图 | 返回无效结算 token，不发送 Redis 命令。 |

尚未 poll 的 settle future，以及连接获取失败或取消，都保留 Open。首个 owner-check EVAL 若收到完整的 **Redis 顶层拒绝回复**，可以恢复 Open，因为该命令未执行。断线、超时、畸形回复、嵌套 RESP 错误或取消会保留 AckPending；只要此前已有结果未知的尝试，后续明确拒绝也不能重新开放 token。未知 ACK 保留活跃名额，直到相同意图完成或 receiver 关闭。意图没有跨进程持久化，重启后不能据此推断历史，也不提供跨 consumer fencing。

## 10. 怎样限制资源和消息尺寸

同一个已创建 SPI 实例及其共享 Arc clone 共用预算。每次新的 `create_configured` 调用都会建立独立 client 预算；不同 registry 选出的实例不会共用整个进程或 Redis server 的全局准入限制。`max_concurrent_commands` 约束已准入的短操作，默认总额度为 64；其中 `reserved_settlement_commands`（默认 8）在普通命令额度耗尽时仍留给结算。专用 receiver 连接使用独立的 `max_active_receivers` 限制（默认 256），不占短命令名额。总命令数至少为 2；旧值 1 属于不兼容配置，会被拒绝。降低总额度时，未显式配置的结算保留数会自动调整为 `min(8, 总额 - 1)`；只有要自定义保留数时才需显式配置。失败、取消会释放本地命令名额；close/drop 释放 receiver 名额，token 继续存活也不会占用该名额。取消**不保证**multiplexed driver 停止请求，也不保证 Redis server 上已无在途命令。provider 不维护无界准入等待队列；专用阻塞读取与共享短命令通道分开。

claim、pending、attempt 明细和 tombstone 维护命令通过 receiver 专用连接执行，不占普通短命令名额。恢复工作受 receiver 租约、每轮命令预算和有限 I/O 等待约束；单轮预算耗尽后，剩余工作会推迟到后续 receive。`max_concurrent_commands` 限制已准入的短操作，不代表全部 Redis 命令或服务端连接上限。receiver 准入按 provider 实例隔离，observer、Sentinel 和其他服务连接不计入这些限制。

`max_idle_connections` 默认为 8，且不得超过 `max_concurrent_commands`。短操作上限设为 8 以下时，应同时降低空闲保留数，例如同时设置 `redis.max_concurrent_commands=4` 和 `redis.max_idle_connections=4`；只降低前者会被配置校验拒绝。新增字节、时间、数量配置均为有限范围内的正十进制数，不能用 0 关闭限制。

默认 1MiB payload 限额按编码字节计量，8MiB wire 限额计入完整 JSON，包括 byte-array 膨胀和元数据。payload 合法不代表任意 headers 都能装入 wire。发布会先检查 event type 和原始 payload 大小，再取得一个普通命令名额，然后通过有界 sink 编码 wire；该名额覆盖 wire 编码、连接建立和 `XADD` 操作。普通名额已满时，`resource_limit` 会先于只能在编码阶段发现的错误（例如无效 headers 或 wire 超限）；event type 和原始 payload 错误仍优先返回。命令并发上限也限制了同时编码的发布数，但不能据此推断进程 RSS 上限。中间 headers JSON 同样受限。provider 编码借用调用方元数据和 payload，分配的 headers/wire String 都有界；直接调用 `WireFields::from_outbound` 转换不会应用这些 provider 配置限额。接收先检查完整 wire 字节限额，再解析 JSON；完整 JSON 语法和 Serde 递归保护适用于所有版本及被忽略字段。完整有效的未知版本跳过版本 1 字段校验，而无效或嵌套过深的 JSON 按 poison 策略处理。版本 1 随后应用有界字段、payload 和 headers 校验。可读取历史记录超过 wire、payload 或 headers 容量时返回 `receive_limit_exceeded` 并保留 pending；确定性格式错误进入隔离。专用 receiver 的 RESP 帧上限与这些解码记录限额相互独立。

解码预算约束 provider 追加的序列化和解析分配。专用 receiver 连接另有 `redis.max_wire_bytes + 65,536` 字节的 RESP 响应帧限制，并在构造响应值前执行。该帧限制不适用于其他连接，也不是进程 RSS 上限。应结合可信 Redis/网络、server `maxmemory`、应用并发及源 stream/隔离流保留策略管理资源。隔离流没有自动保留上限。

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

建议先按每分钟一次采样 `subscription.delivery_metrics().metrics`，并保留五分钟观察窗口；同时采集进程内快照、源/隔离流长度、group/consumer 与 PEL 汇总、使用 outbox 时最老行的年龄，以及 Redis 内存。原有告警继续保留：`command_rejections` 或 `receiver_rejections` 在五分钟内增长、outbox 最老行超过应用的通知延迟 SLO，或 Redis 已用内存超过该实例预算的 75% 时应告警。PEL idle 高于 `2 × redis.claim_min_idle_ms` 可作为调查线索；低于该值不能说明消费健康。

恢复扫描可能刷新 PEL idle，却没有把消息再次交给应用：`XAUTOCLAIM` 和读取本 consumer pending 历史的 `XREADGROUP` 路径都可能刷新 idle。因此，PEL idle 既不是消息首次进入 pending 的时间，也不是 handler 已运行的时长。可用以下组合信号发现停滞：`oldest_owned_age` 持续超过应用处理/结算 SLO，同时 `running_handlers > 0` 或 `settling > 0`，而 `completed` 没有增长。即使 `completed` 在增长，只要某条 owned 工作的 `oldest_owned_age` 持续超出 SLO，仍应调查；其他消息完成并不能排除单条消息卡住。`completed` 统计成功结束的投递生命周期，其中包括已结算的 `Retry` 和 `Reject`，不代表业务处理成功。`oldest_owned_age` 覆盖已归属的接收预留和投递，不只是用户 handler 的执行时间。`None` 表示没有可用时间戳或无法计算，不能当作零。各字段分别采样，不构成同一时刻的事务快照。

例如每分钟采样一次、每秒进行一次恢复扫描、`claim_min_idle_ms=60000`，而旧告警阈值为 PEL idle 120 秒。若一条消息始终未完成，每轮恢复都可能刷新它的 idle，导致该阈值一直不触发；但当订阅侧归属时间持续超过应用 SLO、且生命周期完成数不变时，组合信号仍会要求调查。这些数值只用于说明场景，不是经过测试的建议值，也不是 provider 保证。

订阅被重建时，其生命周期指标和本地年龄会重置；SPI 计数也会在新 instance ID 下重新开始，进程重启还会重置 ID 序列。跨实例追踪时，应由外部采集器按 Redis deployment/namespace、stream、group 和 pending ID 记录首次观测时间；owner 改变时不要重置该时间。分页或采样不完整、数据缺失或 group 重建都会造成历史不确定。首次观测时间只能作为 pending 时长的下界，不是 Redis 首次投递时间。对于 `rs-task` 通知，应核对权威 task 状态与 consumer projection 中最高的 `state_version`；outbox 年龄只反映发布积压。调查时保留错误、PEL ID、owner 和业务状态，并按应用的人工恢复与保留流程处理。采样频率和阈值应结合负载及处置能力调整；变更限额前，先调查准入压力、handler 停滞、outbox 发布和 Redis 内存。

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

设置预期时要区分各完成阶段：`Accepted` 表示 Redis 已回复接受 `XADD`，不表示 fsync、复制、消费者收到通知或业务提交。任务状态与 outbox 行在同一 SQLite 事务中提交。收到 `Accepted` 前 outbox 行会保留并重试，因此可能重复发布；收到后 outbox 行会被删除。Redis 在持久化前故障，可能丢失已删除 outbox 行对应的通知。不能保证消费者端到端至少一次收到通知。仍保留在 stream 中的 entry 可能再次投递，因此消费者应使用持久 checkpoint 保证幂等，保留最高 `state_version`，并在发现版本缺口时查询权威任务状态。provider 不会原子提交任务状态、Redis 持久化、消费者收件和业务提交。`XADD MAXLEN ~` 明确属于有损保留策略，可能裁掉未读或 pending 历史；只有完成上文的保留评估并接受相应丢失后才能启用。

处理 typed `qubit-task` 通知时，consumer 应按 `TaskId` 去重并保留最高 `state_version`，忽略重复、旧版本通知，并在版本缺口时查询任务服务取得权威状态。typed SQLite task service 可启用可选 outbox，在与任务状态相同的事务中捕获生命周期变更；收到 `Accepted` 前重试发布，收到后删除 outbox 行。这不能保证消费者端到端至少一次收到通知，也不会令无关的应用业务事务与任务状态或 Redis 原子提交。

尾部通知丢失后可能再无后续事件暴露缺口。若应用需要有界恢复投影，可以把已接纳的任务 ID 保存在自己的跟踪表中，并按应用设定的周期分批核对。先在投影事务外查询任务服务 checkpoint，再在事务内重读 consumer 版本，仅权威版本更高时写入。查询失败、返回 `None` 或版本落后时继续保留跟踪；终态投影提交后再退役。对账只能修复当前状态：Redis 接纳不保证消费者收到通知，对账也无法重建缺失的状态迁移或副作用，更不提供 exactly-once 处理保证。

## 12. 从 provider 0.6 升级

当前源码 manifest 版本为 provider 0.7.0/core 0.21。2026-11-04 registry 查询只是历史观察，不能证明当前 registry 可用性；当前发布安装证据见[安装与验证状态](design.zh_CN.md#安装与验证状态)。`RedisSubscriptionProfile` 根据明确的 `StartPosition` 构造 durable 订阅选项；创建新订阅时使用它，并保留预期的消费组。核心 codec 注册现在会拒绝重复载荷类型并返回 `Result`；要传播错误，使用 `?`，确需替换时才调用 `replace`。`InboundMessage` 增加可选 provider attempt 元数据，但 `into_parts` tuple 保持原样。保留 wire 版本 1 数据和现有消费组；发布前应验证 pending 记录，并按上文分别采集 provider 快照与 Redis PEL/内存。[迁移指南](migration.zh_CN.md)列出完整变更。


参阅[设计说明](design.zh_CN.md)、[覆盖率证据](coverage-review.zh_CN.md)和[工作负载基准](connection-reuse-benchmark.zh_CN.md)。性能、覆盖率须以各自测量为证；本指南没有宣称新的吞吐量或最终覆盖率结果。

## 支持范围

支持 Redis 单实例和 Sentinel、Redis 6.2+、单实例及 Sentinel 的证书校验 TLS、编码 payload、消费组、accept/retry/reject 结算，以及从 Redis stream position 重放。暂不支持 Cluster、native payload、顺序保证、延迟投递、自动生命周期清理和死信路由。provider 不承诺恰好一次处理。


## 有损 Stream 保留策略

Redis Stream 默认不裁剪。启用近似 `MAXLEN` 裁剪时，必须同时设置 `redis.stream_maxlen_approx=<正整数>` 和 `redis.allow_lossy_retention=true`。裁剪可能删除尚未读取或仍处于 pending 状态的条目；订阅默认会在缺口后停止。验证保留策略时检查 `XLEN`、`XPENDING` 和 `XINFO`。`XADD` 接纳不代表 fsync 或副本持久化保证。
