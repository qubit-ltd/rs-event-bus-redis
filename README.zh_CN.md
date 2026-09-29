# Qubit Redis Event Bus

[![Rust CI](https://github.com/qubit-ltd/rs-event-bus-redis/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-event-bus-redis/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-event-bus-redis/coverage-badge.json)](https://qubit-ltd.github.io/rs-event-bus-redis/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-event-bus-redis.svg?color=blue)](https://crates.io/crates/qubit-event-bus-redis)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

`qubit-event-bus-redis` 为使用 Qubit Event Bus 的应用提供同步和运行时中立的异步 Redis Streams provider。服务可以把编码后的事件写入 Redis，再由另一个进程通过同一套类型化 facade 消费并确认；provider 通过 SPI 自动发现。

当前工作树的 Cargo 版本仍为 `0.4.0`，其中限额及错误处理的变更尚未发布。升级前请阅读指南的迁移说明；本文不表示已经发布 `0.5.0`。

## 安装

```toml
[dependencies]
qubit-event-bus = { version = "0.17", features = ["discovery"] }
qubit-event-bus-redis = "0.5"
qubit-spi = "0.13"
```

这里的版本写法用于展示依赖关系。使用本文所述未发布变更时，应将 provider 依赖改为指向当前 checkout 的 `path`；上游 facade 尚未发布的快照也应使用已核对的本地路径。仓库 example 命令运行当前源码，不能据此推断 registry 版本可用。

默认启用 `sync`、`async` 和 `discovery`。也可以关闭默认 feature，只选择部署所需的 `sync` 或 `async`。`XAUTOCLAIM` 恢复需要 Redis 6.2 或更高版本。

## 快速开始

订单服务可以在启动阶段选择 Redis provider。应用像普通依赖一样链接该 crate；provider 会提交到两个 registry inventory，业务代码仍通过 `qubit-event-bus` facade 发布和订阅类型化消息。

```bash
cargo run --example sync_orders -- redis://127.0.0.1/ local-sync-orders
cargo run --example async_orders -- redis://127.0.0.1/ local-async-orders
```

启动 Redis 后，两个示例会注册 UTF-8 codec、发布订单事件、消费并关闭资源。同步示例等待 handler 完成；异步示例打印已消费事件后等待 Enter。[用户指南](doc/user_guide.zh_CN.md) 提供可直接复制的 Markdown 程序、自动发现与手动注册所需的准确 features，以及 Sentinel 配置。

## 为什么需要这个项目

event-bus SPI 允许应用更换传输实现，而不改业务 handler。Redis Streams 保留消息记录和消费组确认状态，帮助服务实例断连后继续消费。自动发现也省去了启动阶段手动注册 provider 的代码。

## 提供的能力

- 同步 `EventBusSpi` 和异步 `AsyncEventBusSpi`，provider ID 为 `redis-streams`。
- 支持 Redis 单实例与 Sentinel 主节点发现；异步调用可由 Smol 或 Tokio host 驱动，应用无需启动 Tokio。
- 使用带版本的 stream 记录保存编码 payload、headers、事件 ID、content type，以及可选 schema 和排序元数据。
- 支持消费组、`Accept`/`Reject` 确认、通过待处理列表执行 `Retry`，并用 `XAUTOCLAIM` 恢复消息。
- 提供 client 级命令/receiver 准入限制、有限连接/命令等待、payload/wire 字节限制，以及每个订阅的未结算投递上限；格式错误或超限的历史记录通过单条 Lua 脚本隔离；Redis 订阅必须显式使用 `Durable`。
- 测试会通过 Docker 启动隔离的 Redis 6.2、Redis 7 和 Sentinel 服务。

Redis 使用至少一次投递，业务 handler 应能处理重复事件。`XADD` 成功只表示 Redis 接受了命令，不能证明记录已经 fsync 或完成处理。默认不会裁剪 stream。设置 `redis.stream_maxlen_approx` 可显式启用 Redis `XADD MAXLEN ~ N`；近似保留策略可能删除尚未消费或仍处于 pending 的历史记录并产生缺口，仅在业务接受这类损失时使用。当前不支持 Cluster、native/delayed delivery、TLS 配置或死信策略。stream 和消费组由运维人员负责清理。

provider 对单条 wire、payload 和解码后的 headers 字符串设置有限容量，默认分别为 8 MiB、1 MiB 和 64 KiB；facade 另有默认各 1 MiB 的编码发布/接收限制。接收超限会停止订阅，保留 pending 记录，不确认也不隔离。公开发布错误可通过 `PublishFailure.effect()` 判断效果；`XADD` 回复丢失属于未知结果，默认禁止盲目重发。仍支持 wire 版本 1。升级步骤见[迁移指南](doc/migration.zh_CN.md)。

## 延伸阅读

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
