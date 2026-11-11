# Qubit Redis Event Bus

[![Rust CI](https://github.com/qubit-ltd/rs-event-bus-redis/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-event-bus-redis/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-event-bus-redis/coverage-badge.json)](https://qubit-ltd.github.io/rs-event-bus-redis/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-event-bus-redis.svg?color=blue)](https://crates.io/crates/qubit-event-bus-redis)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

`qubit-event-bus-redis` 为使用 Qubit Event Bus 的应用提供同步和运行时中立的异步 Redis Streams provider。服务可以把编码后的事件写入 Redis，再由另一个进程通过同一套类型化 facade 消费并确认；provider 通过 SPI 自动发现。

## 安装

下面的示例展示如何通过 sibling path dependency 集成本地源码。当前发布安装状态见
[安装与验证状态](doc/design.zh_CN.md#安装与验证状态)。

<!-- current-source-dependencies -->
```toml
[dependencies]
qubit-event-bus = { version = "^0.21", path = "../rs-event-bus", features = ["discovery"] }
qubit-event-bus-redis = { version = "0.7.0", path = "../rs-event-bus-redis" }
qubit-spi = "^0.14"

[patch.crates-io]
qubit-event-bus = { path = "../rs-event-bus" }
```

此 patch 必须放在应用 workspace 根清单中。provider 的 registry core 依赖不会因应用直接依赖 path 版本而自动替换；若应用位于其他目录，请调整两个依赖 path 和 patch path，使其指向同一个 core checkout。

当前 Cargo 清单中的 Redis 导入名为 `redis`，包名为 `qubit-redis`，精确固定到 `=2.0.0-rc.2`，来源为 registry。

默认启用 `sync`、`async` 和 `discovery`。如需只保留一种 SPI 并使用自动发现，可在 `qubit-event-bus-redis` 上设置 `default-features = false`，选择 `features = ["sync", "discovery"]` 或 `features = ["async", "discovery"]`。只在 `qubit-event-bus` 上启用 discovery 不会注册 Redis provider。`XAUTOCLAIM` 恢复需要 Redis 6.2 或更高版本。


## 快速开始

订单服务可在启动时选择 `redis-streams` provider，注册 payload codec，再通过类型化 facade 发布和消费。下面的完整同步程序展示了整条路径：handler 会报告每个事件 ID 和载荷；发布端只有收到本次事件的 ID 后才报告成功。程序可能先处理已有 pending 事件；整个等待过程共用五秒期限。Redis 确认不代表应用数据库事务已经提交。

**运行条件：** 使用 Redis 6.2 或更高版本；若 Redis 不在 `redis://127.0.0.1/`，请设置 `REDIS_URL`。程序默认使用唯一 namespace `orders-readme-sync`；如需隔离演示数据，可将 `REDIS_NAMESPACE` 设为一个新值。`billing` 是持久消费组：首次创建时，`Earliest` 会从保留数据的开头读取；后续运行通过显式设置 `redis.existing_group_start=resume` 沿用 Redis 保存的游标，不会回退或重放已确认的记录。此前尚未结算的记录仍可能先于本次新事件送达。

<!-- doc-example: readme-sync-discovery -->
```rust
use std::sync::Arc;
use std::time::Duration;

use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::CodecError;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus_redis as _;
use qubit_event_bus_redis::RedisSubscriptionProfile;
use qubit_spi::ProviderSelection;

struct Utf8Codec(ContentType);

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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut codecs = CodecRegistry::new();
    codecs.register::<String>(Arc::new(Utf8Codec(ContentType::new("text/plain")?)))?;
    let facade = EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs));

    let namespace = std::env::var("REDIS_NAMESPACE")
        .unwrap_or_else(|_| "orders-readme-sync".into());
    let options: ProviderOptions = [
        (
            "redis.url".into(),
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".into()),
        ),
        ("redis.namespace".into(), namespace),
    ]
    .into();
    let config = EventBusConfig::default()
        .with_selection(ProviderSelection::named("redis-streams")?)
        .with_provider_options(options)
        .with_facade_config(facade);
    let bus = EventBusRegistry::discover()?.create(&config)?;
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
            sender.send((delivery.event().id().clone(), delivery.payload().clone())).map_err(|source| {
                qubit_event_bus::DeliveryError::Handler {
                    source: Box::new(source),
                }
            })
        },
    )?;
    let request = PublishRequest::new(topic, "order-42".to_owned())?;
    let expected_id = request.envelope().id().clone();
    bus.publish(request)?;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let received = loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Err("timed out waiting for this run's event".into());
        }
        let (event_id, payload) = receiver.recv_timeout(remaining)?;
        if event_id == expected_id {
            break payload;
        }
        println!("recovered earlier event: {}", event_id.as_str());
    };
    println!("consumed order event: {received}; event_id={}", expected_id.as_str());

    subscription.cancel()?;
    let report = bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(5),
    })?;
    if report.outcome != ShutdownOutcome::Complete {
        return Err("graceful shutdown did not complete".into());
    }
    Ok(())
}
```
<!-- END DOC EXAMPLE -->

程序成功时会打印 `consumed order event: order-42; event_id=…`，并在优雅关闭完成后退出。生产环境的 handler 应先提交应用自己的账单变更，再返回成功；这里的打印仅用于入门验证，不代表数据库事务已提交。进程退出后，Redis 仍会保留 stream 数据和消费组。[用户指南](doc/user_guide.zh_CN.md) 介绍异步执行、Sentinel、恢复及运维检查。仓库中的完整示例也可通过 `cargo run --example sync_orders -- redis://127.0.0.1/ local-sync-orders` 和 `cargo run --example async_orders -- redis://127.0.0.1/ local-async-orders` 运行。
## 为什么需要这个项目

event-bus SPI 允许应用更换传输实现，而不改业务 handler。Redis Streams 保留消息记录和消费组确认状态，帮助服务实例断连后继续消费。自动发现也省去了启动阶段手动注册 provider 的代码。

## 提供的能力

- 同步 `EventBusSpi` 和异步 `AsyncEventBusSpi`，provider ID 为 `redis-streams`。
- 支持 Redis 单实例与 Sentinel 主节点发现；异步调用可由 Smol 或 Tokio host 驱动，应用无需启动 Tokio。
- 使用带版本的 stream 记录保存编码 payload、headers、事件 ID、content type，以及可选 schema 和排序元数据。
- 支持消费组、`Accept`/`Reject` 确认、通过待处理列表执行 `Retry`，并用 `XAUTOCLAIM` 恢复消息。
- 提供有界的命令与 receiver 准入、payload/wire 容量限制、恢复调度，以及进程内 `RedisProviderDiagnostics::snapshots()` 诊断。Redis 订阅必须显式使用 `Durable`。
- 测试会通过 Docker 启动隔离的 Redis 6.2、Redis 7 和 Sentinel 服务。

Redis 至少一次投递，业务 handler 应能处理重复事件。`XADD` 被接纳只表示 Redis 接受了命令，不代表已 fsync、已送达消费者或业务处理完成；若发布回复丢失，结果未知，不应盲目重试。stream 默认不裁剪，超过接收容量的记录会留在 pending。终态结算需要 Redis ACL 允许 `EVAL`、`XPENDING` 和 `XACK`。恢复、RESP/wire 限制、结果未知、重试和权限细节见[投递与重试指南](doc/user_guide.zh_CN.md#6-理解投递重试和清理)、[资源与消息大小限制](doc/user_guide.zh_CN.md#10-怎样限制资源和消息尺寸)、[Redis ACL 指南](doc/user_guide.zh_CN.md#按职责配置-redis-acl)及[结算迁移说明](doc/migration.zh_CN.md#结算-owner-校验)。

已有消费组会按明确规则处理订阅起始位置，详见[消费组迁移说明](doc/migration.zh_CN.md#已有消费组起始位置变更)。单实例和 Sentinel 连接均可按需启用 TLS。当前不支持 Cluster、native/delayed delivery 或死信路由；stream 和消费组由运维人员负责清理。

## 延伸阅读

Redis 端点通过 `rediss://` 启用 TLS；Sentinel TLS 则由独立的 `redis.sentinel.tls` 控制，并使用各自的 CA 和客户端身份配置。每个自定义 PEM 文件最多 1 MiB。配置示例和证书轮换说明见[用户指南的 TLS 章节](doc/user_guide.zh_CN.md#tls-连接)。

- [用户指南](doc/user_guide.zh_CN.md)（[English](doc/user_guide.md)）
- [设计与迁移边界](doc/design.zh_CN.md)（[English](doc/design.md)）
- [工作负载基准](doc/connection-reuse-benchmark.zh_CN.md)（[English](doc/connection-reuse-benchmark.md)）
- [覆盖率评估](doc/coverage-review.zh_CN.md)（[English](doc/coverage-review.md)）
- [API 文档](https://docs.rs/qubit-event-bus-redis)
- [English README](README.md)

## 测试

```bash
# 使用默认 feature 集运行测试
cargo test

# 使用项目声明的全部 feature 运行测试
cargo test --all-features

# 运行项目 CI 检查
./.infra/bin/ci-check.sh

# 检查代码覆盖率
./.infra/bin/coverage.sh
```

格式化请运行 `./.infra/bin/align-ci.sh`；它会按项目规则改写文件。`./.infra/bin/ci-check.sh` 用于验证 CI 要求。单独运行 `cargo fmt --check` 不能替代项目的正式格式门禁。

## 许可证

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

本项目基于 Apache License 2.0 授权。完整许可证文本请参阅
[LICENSE](LICENSE)。

## 贡献

欢迎贡献。请遵循 Rust API 指南，及时更新公共 API 文档与测试。提交
Pull Request 前，先运行 `./.infra/bin/align-ci.sh` 按项目规则格式化，再运行 `./.infra/bin/ci-check.sh` 验证 CI 要求。单独运行 `cargo fmt --check` 不能替代项目的格式门禁。

## 作者

**Haixing Hu** - *Qubit Co. Ltd.*

仓库地址：[https://github.com/qubit-ltd/rs-event-bus-redis](https://github.com/qubit-ltd/rs-event-bus-redis)
