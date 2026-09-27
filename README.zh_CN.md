# Qubit Redis Event Bus

[![Rust CI](https://github.com/qubit-ltd/rs-event-bus-redis/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-event-bus-redis/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-event-bus-redis/coverage-badge.json)](https://qubit-ltd.github.io/rs-event-bus-redis/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-event-bus-redis.svg?color=blue)](https://crates.io/crates/qubit-event-bus-redis)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

`qubit-event-bus-redis` 为使用 Qubit Event Bus 的应用提供同步和运行时中立的异步 Redis Streams provider。服务可以把编码后的事件写入 Redis，再由另一个进程通过同一套类型化 facade 消费并确认；provider 通过 SPI 自动发现。

## 安装

```toml
[dependencies]
qubit-event-bus = { version = "0.14", features = ["discovery"] }
qubit-event-bus-redis = "0.1"
qubit-spi = "0.13"
```

默认启用 `sync`、`async` 和 `discovery`。也可以关闭默认 feature，只选择部署所需的 `sync` 或 `async`。`XAUTOCLAIM` 恢复需要 Redis 6.2 或更高版本。

## 快速开始

订单服务可以在启动阶段选择 Redis provider。应用像普通依赖一样链接该 crate；provider 会提交到两个 registry inventory，业务代码仍通过 `qubit-event-bus` facade 发布和订阅类型化消息。

```rust,no_run
use std::sync::Arc;

use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::registry::EventBusRegistry;
use qubit_event_bus::model::ProviderOptions;
use qubit_spi::ProviderSelection;

fn create_order_bus() -> Result<qubit_event_bus::EventBus, Box<dyn std::error::Error>> {
    let registry = EventBusRegistry::discover()?;
    let options: ProviderOptions = [
        ("redis.url".into(), "redis://127.0.0.1/".into()),
        ("redis.namespace".into(), "orders".into()),
    ].into();
    let config = EventBusConfig::default()
        .with_selection(ProviderSelection::named("redis-streams")?)
        .with_provider_options(options)
        .with_facade_config(EventBusFacadeConfig::new().with_codec_registry(Arc::new(CodecRegistry::new())));
    Ok(registry.create(&config)?)
}
```

使用 Redis 的 payload 类型时，需要在 facade 的 `CodecRegistry` 中注册对应的 `EventCodec<T>`。之后即可调用常规的类型化 `publish` 和 `subscribe`。完整 codec、consumer、异步及 Sentinel 示例见[用户指南](doc/user_guide.zh_CN.md)。

## 为什么需要这个项目

event-bus SPI 允许应用更换传输实现，而不改业务 handler。Redis Streams 保留消息记录和消费组确认状态，帮助服务实例断连后继续消费。自动发现也省去了启动阶段手动注册 provider 的代码。

## 提供的能力

- 同步 `EventBusSpi` 和异步 `AsyncEventBusSpi`，provider ID 为 `redis-streams`。
- 支持 Redis 单实例与 Sentinel 主节点发现；异步调用可由 Smol 或 Tokio host 驱动，应用无需启动 Tokio。
- 使用带版本的 stream 记录保存编码 payload、headers、事件 ID、content type，以及可选 schema 和排序元数据。
- 支持消费组、`Accept`/`Reject` 确认、通过待处理列表执行 `Retry`，并用 `XAUTOCLAIM` 恢复消息。
- 限制每个订阅的未结算活跃投递数，并原子隔离格式错误的 stream 记录；Redis 订阅必须显式使用 `Durable`。
- 测试会通过 Docker 启动隔离的 Redis 6.2、Redis 7 和 Sentinel 服务。

Redis 使用至少一次投递，业务 handler 应能处理重复事件。`XADD` 成功只表示 Redis 接受了命令，不能证明记录已经 fsync 或完成处理。默认不会裁剪 stream。设置 `redis.stream_maxlen_approx` 可显式启用 Redis `XADD MAXLEN ~ N`；近似保留策略可能删除尚未消费或仍处于 pending 的历史记录并产生缺口，仅在业务接受这类损失时使用。当前不支持 Cluster、native/delayed delivery、TLS 配置或死信策略。stream 和消费组由运维人员负责清理。

## 延伸阅读

- [用户指南](doc/user_guide.zh_CN.md)（[English](doc/user_guide.md)）
- [API 文档](https://docs.rs/qubit-event-bus-redis)
- [English README](README.md)

## 测试

```bash
# 使用默认 feature 集运行测试
cargo test

# 使用项目声明的全部 feature 运行测试
cargo test --all-features

# 运行项目 CI 检查
./ci-check.sh

# 检查代码覆盖率
./coverage.sh
```

## 许可证

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

本项目基于 Apache License 2.0 授权。完整许可证文本请参阅
[LICENSE](LICENSE)。

## 贡献

欢迎贡献。请遵循 Rust API 指南，及时更新公共 API 文档与测试，并在提交
Pull Request 前运行 `./align-ci.sh`格式化代码，运行`./ci-check.sh`对齐CI要求。

## 作者

**Haixing Hu** - *Qubit Co. Ltd.*

仓库地址：[https://github.com/qubit-ltd/rs-event-bus-redis](https://github.com/qubit-ltd/rs-event-bus-redis)
