# Qubit Redis Event Bus

[![Rust CI](https://github.com/qubit-ltd/rs-event-bus-redis/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-event-bus-redis/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-event-bus-redis/coverage-badge.json)](https://qubit-ltd.github.io/rs-event-bus-redis/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-event-bus-redis.svg?color=blue)](https://crates.io/crates/qubit-event-bus-redis)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-event-bus-redis` adds synchronous and runtime-neutral asynchronous Redis Streams providers to applications using Qubit Event Bus. A service can publish an encoded event to Redis, then let another process consume and acknowledge it through the same typed facade and automatically discovered SPI provider.

## Installation

```toml
[dependencies]
qubit-event-bus = { version = "0.20", features = ["discovery"] }
qubit-event-bus-redis = "0.7"
qubit-spi = "0.13"
```

The default features enable `sync`, `async`, and `discovery`. To keep one SPI while using registry discovery, set `default-features = false` on `qubit-event-bus-redis` and select `features = ["sync", "discovery"]` or `features = ["async", "discovery"]`. Enabling discovery only on `qubit-event-bus` does not register the Redis provider. Redis 6.2 or later is required for `XAUTOCLAIM` recovery.

## Quick Start

An order service can select the Redis provider during startup. The provider is linked as an ordinary dependency and, with `discovery`, submitted to each enabled SPI's registry inventory; the application still uses the `qubit-event-bus` facade for typed publish and subscribe calls.

```bash
cargo run --example sync_orders -- redis://127.0.0.1/ local-sync-orders
cargo run --example async_orders -- redis://127.0.0.1/ local-async-orders
```

With a running Redis service, each example registers a UTF-8 codec, publishes an order event, consumes it, and closes its resources. The [user guide](doc/user_guide.md) provides complete commands with a unique namespace, a durable `billing` group, and the new-group start cursor, plus the Docker fixture command and Sentinel setup.

## Why This Project Exists

The event-bus SPI lets an application choose a transport without changing business handlers. Redis Streams provides retained records and consumer-group acknowledgements, which helps a service resume consumption after an application instance disconnects. Automatic provider discovery removes provider-specific registration glue from startup wiring.

## What It Provides

- Sync `EventBusSpi` and async `AsyncEventBusSpi` implementations with the provider ID `redis-streams`.
- Redis standalone and Sentinel master discovery; async calls can be driven by Smol or Tokio hosts without requiring the application to start Tokio.
- Stream records with a versioned encoded payload, headers, event ID, content type, and optional schema and ordering metadata.
- Consumer groups, `Accept`/`Reject` acknowledgement, `Retry` through the pending entries list, and recovery with `XAUTOCLAIM`.
- Per-client command/receiver admission, finite connection/command waits, payload/wire byte limits, and per-subscription unsettled tracking; malformed or oversized history is quarantined with a serialized Lua script; Redis subscriptions must explicitly use `Durable`.
- Process-local `RedisProviderDiagnostics::snapshots()` for live SPI admission, connection, publish and recovery counters; Redis PEL and memory still require Redis commands. The [operations guide](doc/user_guide.md#11-operate-groups-and-downstream-notifications) covers collection, alerts and manual retention decisions.
- Test fixtures that start isolated Redis 6.2, Redis 7, and Sentinel services in Docker.

The Redis command budget defaults to 64 short operations, with 8 slots reserved for
settlement; dedicated receiver connections have a separate default cap of 256.
These admission rules are a breaking change: `redis.max_concurrent_commands=1`
is rejected. When the total is lowered, the omitted settlement reservation
adapts to `min(8, total - 1)`; set `redis.reserved_settlement_commands` only when
a different reserve is needed. See the [migration guide](doc/migration.md).
Each durable subscription checks pending work on its first receive, then carries
its recovery schedule across calls and rescans at `redis.recovery_interval_ms`
(default 1,000 ms). Retry, receive failure, or cancellation after polling forces
the next receive to recover. Admission is per provider instance, not global to
Redis or the process; monitor command rejections, `XPENDING`, stream `XLEN`, and
quarantine growth.

Redis delivery is at least once. Handlers should tolerate duplicates. A successful `XADD` means Redis accepted the command; it does not prove the record was fsynced or processed. By default streams are not trimmed. Set both `redis.stream_maxlen_approx` and `redis.allow_lossy_retention=true` to opt into Redis `XADD MAXLEN ~ N`; this approximate retention can remove unread or pending history and cause gaps, so use it only after a manual loss review. The provider does not implement Cluster, native/delayed delivery, TLS configuration, or a dead-letter policy. Stream and group cleanup is an operator task.

Each wire record, payload, and decoded headers string has a finite provider limit (8 MiB, 1 MiB, and 64 KiB by default), in addition to the facade's 1 MiB encoded publish/receive limits. Receive overflow stops the subscription and retains the pending entry without acknowledgement or quarantine. Public publication errors report `PublishFailure.effect()`; lost `XADD` replies are uncertain and default retry policy forbids blind resubmission. Version 1 wire data remains supported. See the [migration guide](doc/migration.md).

Core 0.20 bounds running handlers, owned deliveries, per-subscription ownership, and registered subscriptions separately. `RedisSubscriptionProfile` requires an explicit start position and builds durable options; new stream entries report provider attempt `Some(1)`, while pending and claimed entries remain unknown. Settlement retries are finite and require explicitly retryable errors; unknown retryability stops the subscription. The [user guide](doc/user_guide.md) covers first-cause diagnostics, delivery metrics, durable recovery, provider snapshot sampling, and bounded shutdown waits that do not guarantee forced process exit.

## Learn More

- [User guide](doc/user_guide.md) ([简体中文](doc/user_guide.zh_CN.md))
- [Design and migration boundaries](doc/design.md) ([简体中文](doc/design.zh_CN.md))
- [Workload benchmark](doc/connection-reuse-benchmark.md) ([简体中文](doc/connection-reuse-benchmark.zh_CN.md))
- [Coverage review](doc/coverage-review.md) ([简体中文](doc/coverage-review.zh_CN.md))
- [API documentation](https://docs.rs/qubit-event-bus-redis)
- [简体中文 README](README.zh_CN.md)

## Lossy stream retention

Redis streams are not trimmed by default. To enable approximate `MAXLEN` trimming, configure both `redis.stream_maxlen_approx=<positive integer>` and `redis.allow_lossy_retention=true`. Trimming can remove unread or pending entries and cause delivery gaps; subscriptions stop on gaps by default. Monitor `XLEN`, `XPENDING`, and `XINFO` when validating retention behavior. An accepted `XADD` is not an fsync or replication guarantee.

## Testing

```bash
# Run tests with the default feature set
cargo test

# Run tests with all declared features
cargo test --all-features

# Project CI checks
./ci-check.sh

# Check code coverage
./coverage.sh
```

## License

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full license text.

## Contributing

Contributions are welcome. Please follow the Rust API guidelines, keep public
API documentation and tests current, and run `./align-ci.sh` to format code and
`./ci-check.sh` to satisfy CI requirements before submitting a pull request.

## Author

**Haixing Hu** - *Qubit Co. Ltd.*

Repository: [https://github.com/qubit-ltd/rs-event-bus-redis](https://github.com/qubit-ltd/rs-event-bus-redis)
