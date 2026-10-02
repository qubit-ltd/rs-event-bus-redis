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
qubit-event-bus = { version = "0.19", features = ["discovery"] }
qubit-event-bus-redis = "0.7"
qubit-spi = "0.13"
```

默认启用 `sync`、`async` 和 `discovery`。如需只保留一种 SPI 并继续使用 registry 自动发现，在 `qubit-event-bus-redis` 上设置 `default-features = false`，选择 `features = ["sync", "discovery"]` 或 `features = ["async", "discovery"]`。只在 `qubit-event-bus` 上启用 discovery 不会注册 Redis provider。`XAUTOCLAIM` 恢复需要 Redis 6.2 或更高版本。

## 快速开始

订单服务可以在启动阶段选择 Redis provider。应用像普通依赖一样链接该 crate；启用 `discovery` 后，provider 会提交到所启用 SPI 对应的 registry inventory，业务代码仍通过 `qubit-event-bus` facade 发布和订阅类型化消息。

```bash
cargo run --example sync_orders -- redis://127.0.0.1/ local-sync-orders
cargo run --example async_orders -- redis://127.0.0.1/ local-async-orders
```

启动 Redis 后，两个示例会注册 UTF-8 codec、发布订单事件、消费并关闭资源。[用户指南](doc/user_guide.zh_CN.md) 给出使用唯一 namespace、持久 `billing` 消费组和新组起始游标的完整命令，也提供 Docker fixture 与 Sentinel 配置。

## 为什么需要这个项目

event-bus SPI 允许应用更换传输实现，而不改业务 handler。Redis Streams 保留消息记录和消费组确认状态，帮助服务实例断连后继续消费。自动发现也省去了启动阶段手动注册 provider 的代码。

## 提供的能力

- 同步 `EventBusSpi` 和异步 `AsyncEventBusSpi`，provider ID 为 `redis-streams`。
- 支持 Redis 单实例与 Sentinel 主节点发现；异步调用可由 Smol 或 Tokio host 驱动，应用无需启动 Tokio。
- 使用带版本的 stream 记录保存编码 payload、headers、事件 ID、content type，以及可选 schema 和排序元数据。
- 支持消费组、`Accept`/`Reject` 确认、通过待处理列表执行 `Retry`，并用 `XAUTOCLAIM` 恢复消息。
- 提供 client 级命令/receiver 准入限制、有限连接/命令等待、payload/wire 字节限制，以及每个订阅的未结算投递上限；格式错误或超限的历史记录通过单条 Lua 脚本隔离；Redis 订阅必须显式使用 `Durable`。
- 测试会通过 Docker 启动隔离的 Redis 6.2、Redis 7 和 Sentinel 服务。

Redis 短命令额度默认是 64，其中 8 个名额专供结算；专用 receiver 连接有独立的 256 个默认上限。这些准入规则属于破坏性变更：`redis.max_concurrent_commands=1` 会被拒绝。降低总额度时，未显式设置的结算保留数会自动调整为 `min(8, 总额 - 1)`；只有需要自定义保留数时才设置 `redis.reserved_settlement_commands`。详见[迁移指南](doc/migration.zh_CN.md)。每个持久订阅第一次 receive 都会检查 pending；之后恢复时钟跨调用保存，并按 `redis.recovery_interval_ms`（默认 1,000 毫秒）重新扫描。Retry、receive 失败或已开始轮询的 future 被取消时，会强制下一次 receive 执行恢复。额度按 provider 实例隔离，不是 Redis 或进程级全局额度；应监控命令拒绝、`XPENDING`、stream `XLEN` 和隔离流增长。

Redis 使用至少一次投递，业务 handler 应能处理重复事件。`XADD` 成功只表示 Redis 接受了命令，不能证明记录已经 fsync 或完成处理。默认不会裁剪 stream。设置 `redis.stream_maxlen_approx` 可显式启用 Redis `XADD MAXLEN ~ N`；近似保留策略可能删除尚未消费或仍处于 pending 的历史记录并产生缺口，仅在业务接受这类损失时使用。当前不支持 Cluster、native/delayed delivery、TLS 配置或死信策略。stream 和消费组由运维人员负责清理。

provider 对单条 wire、payload 和解码后的 headers 字符串设置有限容量，默认分别为 8 MiB、1 MiB 和 64 KiB；facade 另有默认各 1 MiB 的编码发布/接收限制。接收超限会停止订阅，保留 pending 记录，不确认也不隔离。公开发布错误可通过 `PublishFailure.effect()` 判断效果；`XADD` 回复丢失属于未知结果，默认禁止盲目重发。仍支持 wire 版本 1。升级步骤见[迁移指南](doc/migration.zh_CN.md)。

Core 0.19 分别限制 handler 运行数、全局持有投递数、每订阅持有量和注册订阅数。`RedisSubscriptionProfile` 要求明确指定起始位置，并构造 durable options；Redis 新读取的 stream entry 报告 provider attempt `Some(1)`，pending 和 claim 恢复的历史次数仍未知。结算只对明确可重试的错误执行有限重试，重试性未知时停止订阅。[用户指南](doc/user_guide.zh_CN.md) 说明首个终止原因、投递指标、持久恢复，以及限制等待时间但不保证强制退出进程的关闭策略。

## 延伸阅读

- [用户指南](doc/user_guide.zh_CN.md)（[English](doc/user_guide.md)）
- [设计与迁移边界](doc/design.zh_CN.md)（[English](doc/design.md)）
- [工作负载基准](doc/connection-reuse-benchmark.zh_CN.md)（[English](doc/connection-reuse-benchmark.md)）
- [覆盖率评估](doc/coverage-review.zh_CN.md)（[English](doc/coverage-review.md)）
- [API 文档](https://docs.rs/qubit-event-bus-redis)
- [English README](README.md)

## 有损 Stream 保留策略

Redis Stream 默认不裁剪。启用近似 `MAXLEN` 裁剪时，必须同时设置 `redis.stream_maxlen_approx=<正整数>` 和 `redis.allow_lossy_retention=true`。裁剪可能删除尚未读取或仍处于 pending 状态的条目；订阅默认会在缺口后停止。验证保留策略时检查 `XLEN`、`XPENDING` 和 `XINFO`。`XADD` 接纳不代表 fsync 或副本持久化保证。

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
