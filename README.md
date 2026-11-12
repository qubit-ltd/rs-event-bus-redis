# Qubit Redis Event Bus

[![Rust CI](https://github.com/qubit-ltd/rs-event-bus-redis/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-event-bus-redis/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-event-bus-redis/coverage-badge.json)](https://qubit-ltd.github.io/rs-event-bus-redis/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-event-bus-redis.svg?color=blue)](https://crates.io/crates/qubit-event-bus-redis)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-event-bus-redis` adds synchronous and runtime-neutral asynchronous Redis Streams providers to applications using Qubit Event Bus. A service can publish an encoded event to Redis, then let another process consume and acknowledge it through the same typed facade and automatically discovered SPI provider.

## Installation

The example below shows local source integration using sibling path
dependencies. The current published-install status is summarized in the
[installation and validation status](doc/design.md#installation-and-validation-status).

<!-- current-source-dependencies -->
```toml
[dependencies]
qubit-event-bus = { version = "^0.21", path = "../rs-event-bus", features = ["discovery"] }
qubit-event-bus-redis = { version = "0.7.0", path = "../rs-event-bus-redis" }
qubit-spi = "^0.14"

[patch.crates-io]
qubit-event-bus = { path = "../rs-event-bus" }
```

Put the patch in the application workspace root manifest. The provider's registry core dependency is not replaced by the application's direct path dependency; adjust both dependency paths and the patch path to the same core checkout if your application lives elsewhere.

The current Cargo manifest imports Redis as `redis`, uses package `qubit-redis` at exact version `=2.0.0-rc.2`, and resolves it from the registry.

The default features enable `sync`, `async`, and `discovery`. To keep one SPI, set `default-features = false` on `qubit-event-bus-redis` and select `features = ["sync", "discovery"]` or `features = ["async", "discovery"]`. Enabling discovery only on `qubit-event-bus` does not register the Redis provider. Redis 6.2 or later is required for `XAUTOCLAIM` recovery.


## Quick Start

An order service can select the `redis-streams` provider during startup, register the payload codec, and use the typed facade to publish and consume. The program below is the complete synchronous path: its handler reports each event ID and payload, while the publisher reports success only after receiving this run's event ID. Existing pending events may be handled first. The five-second deadline covers the whole wait, and a Redis acknowledgement does not prove that an application database transaction committed.

**Prerequisites:** run Redis 6.2 or newer and set `REDIS_URL` if it is not at `redis://127.0.0.1/`. Each run uses the single namespace `orders-readme-sync` by default; set `REDIS_NAMESPACE` to a fresh value for an isolated run. `billing` is a durable consumer group. On its first creation, `Earliest` reads retained stream entries from the beginning. On later runs, the explicit `redis.existing_group_start=resume` option keeps the group's saved cursor; it does not rewind or replay acknowledged entries. Existing pending entries may be delivered before this run's new event.

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

A successful run prints `consumed order event: order-42; event_id=…` and exits only after graceful shutdown completes. Production handlers should commit the application's billing change before returning success; the printed line is an exercise signal, not proof of a database transaction. Redis retains stream data and groups after exit. The [user guide](doc/user_guide.md) covers async execution, Sentinel, recovery, and operational checks. The repository's complete examples are also available with `cargo run --example sync_orders -- redis://127.0.0.1/ local-sync-orders` and `cargo run --example async_orders -- redis://127.0.0.1/ local-async-orders`.

## Why This Project Exists

The event-bus SPI lets an application choose a transport without changing business handlers. Redis Streams provides retained records and consumer-group acknowledgements, which helps a service resume consumption after an application instance disconnects. Automatic provider discovery removes provider-specific registration glue from startup wiring.

## What It Provides

- Sync `EventBusSpi` and async `AsyncEventBusSpi` implementations with the provider ID `redis-streams`.
- Redis standalone and Sentinel master discovery; async calls can be driven by Smol or Tokio hosts without requiring the application to start Tokio.
- Stream records with a versioned encoded payload, headers, event ID, content type, and optional schema and ordering metadata.
- Consumer groups, `Accept`/`Reject` acknowledgement, `Retry` through the pending entries list, and recovery with `XAUTOCLAIM`.
- Bounded command and receiver admission, payload and wire limits, recovery scheduling, and process-local `RedisProviderDiagnostics::snapshots()`. Redis subscriptions must explicitly use `Durable`.
- Test fixtures that start isolated Redis 6.2, Redis 7, and Sentinel services in Docker.

Retained, recoverable Redis Stream entries can be redelivered, so handlers must
tolerate duplicates. An accepted publication does not guarantee end-to-end
consumer receipt at least once. An accepted `XADD` confirms Redis accepted the
command; it does not prove fsync, consumer delivery, or application work
completed. A lost publish reply has an unknown effect and must not be blindly
retried. Streams are not trimmed by default, and records over configured receive
capacity remain pending. Terminal settlement
requires Redis ACL permission for `EVAL`, `XPENDING`, and `XACK`. See the
[delivery and retry guide](doc/user_guide.md#6-understand-delivery-retry-and-cleanup),
[resource limits](doc/user_guide.md#10-bound-resources-and-message-sizes),
[Redis ACL guide](doc/user_guide.md#grant-redis-acl-access-by-role), and
[settlement migration notes](doc/migration.md#settlement-owner-check) for recovery,
RESP and wire limits, retention decisions, and operational details.

For existing groups, the configured start position is handled explicitly; see the
[consumer group migration notes](doc/migration.md#existing-consumer-group-start-position-change).
TLS is opt-in for standalone and Sentinel connections. The provider does not
implement Cluster, native or delayed delivery, or dead-letter routing; operators
manage stream and group cleanup.

## Learn More

TLS uses `rediss://` for Redis itself. Sentinel TLS has a separate `redis.sentinel.tls` switch and separate CA/client identity options. Custom PEM files are limited to 1 MiB each; see the [TLS configuration guide](doc/user_guide.md#tls-connections) for examples and rotation behavior.

- [User guide](doc/user_guide.md) ([简体中文](doc/user_guide.zh_CN.md))
- [Design and migration boundaries](doc/design.md) ([简体中文](doc/design.zh_CN.md))
- [Workload benchmark](doc/connection-reuse-benchmark.md) ([简体中文](doc/connection-reuse-benchmark.zh_CN.md))
- [Coverage review](doc/coverage-review.md) ([简体中文](doc/coverage-review.zh_CN.md))
- [API documentation](https://docs.rs/qubit-event-bus-redis)
- [简体中文 README](README.zh_CN.md)

## Testing

```bash
# Run tests with the default feature set
cargo test

# Run tests with all declared features
cargo test --all-features

# Project CI checks
./.infra/bin/ci-check.sh

# Check code coverage
./.infra/bin/coverage.sh
```

For formatting, `./.infra/bin/align-ci.sh` applies the project's formatting rules and may rewrite files; `./.infra/bin/ci-check.sh` verifies the CI requirements. A standalone `cargo fmt --check` does not replace these project checks.

## License

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full license text.

## Contributing

Contributions are welcome. Please follow the Rust API guidelines, keep public
API documentation and tests current, run `./.infra/bin/align-ci.sh` to apply the project's formatting rules, then run `./.infra/bin/ci-check.sh` to verify CI requirements before submitting a pull request. `cargo fmt --check` alone does not replace the project's formatting gate.

## Author

**Haixing Hu** - *Qubit Co. Ltd.*

Repository: [https://github.com/qubit-ltd/rs-event-bus-redis](https://github.com/qubit-ltd/rs-event-bus-redis)
