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
qubit-event-bus = { version = "0.15", features = ["discovery"] }
qubit-event-bus-redis = "0.4"
qubit-spi = "0.13"
```

The default features enable `sync`, `async`, and `discovery`. Disable defaults and select `sync` or `async` to keep a deployment's feature set smaller. Redis 6.2 or later is required for `XAUTOCLAIM` recovery.

## Quick Start

An order service can select the Redis provider during startup. The provider is linked as an ordinary dependency and submitted to both registry inventories; the application still uses the `qubit-event-bus` facade for typed publish and subscribe calls.

```rust,no_run
use std::sync::Arc;

use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::registry::EventBusRegistry;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus_redis as _;
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

Register an `EventCodec<T>` in the facade's `CodecRegistry` for every payload type used with Redis. Then call the normal typed `publish` and `subscribe` APIs. See the [user guide](doc/user_guide.md) for a complete codec, consumer, async, and Sentinel example.

Runnable publish-consume-close examples are available as `sync_orders`, `async_orders`, and `sentinel_orders`. Run them with `cargo run --example <name> -- <redis-url> <namespace>`; the Sentinel example reads `REDIS_SENTINEL_NODES` and `REDIS_SENTINEL_SERVICE_NAME` from the environment. See the user guide for exact commands and lifecycle notes.

## Why This Project Exists

The event-bus SPI lets an application choose a transport without changing business handlers. Redis Streams provides retained records and consumer-group acknowledgements, which helps a service resume consumption after an application instance disconnects. Automatic provider discovery removes provider-specific registration glue from startup wiring.

## What It Provides

- Sync `EventBusSpi` and async `AsyncEventBusSpi` implementations with the provider ID `redis-streams`.
- Redis standalone and Sentinel master discovery; async calls can be driven by Smol or Tokio hosts without requiring the application to start Tokio.
- Stream records with a versioned encoded payload, headers, event ID, content type, and optional schema and ordering metadata.
- Consumer groups, `Accept`/`Reject` acknowledgement, `Retry` through the pending entries list, and recovery with `XAUTOCLAIM`.
- Bounded per-subscription unsettled delivery tracking and atomic quarantine for malformed stream records; Redis subscriptions must explicitly use `Durable`.
- Test fixtures that start isolated Redis 6.2, Redis 7, and Sentinel services in Docker.

Redis delivery is at least once. Handlers should tolerate duplicates. A successful `XADD` means Redis accepted the command; it does not prove the record was fsynced or processed. By default streams are not trimmed. Set `redis.stream_maxlen_approx` to opt into Redis `XADD MAXLEN ~ N`; this approximate retention can remove unread or pending history and cause gaps, so use it only when that loss policy is acceptable. The provider does not implement Cluster, native/delayed delivery, TLS configuration, or a dead-letter policy. Stream and group cleanup is an operator task.

## Learn More

- [User guide](doc/user_guide.md) ([简体中文](doc/user_guide.zh_CN.md))
- [API documentation](https://docs.rs/qubit-event-bus-redis)
- [简体中文 README](README.zh_CN.md)

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
