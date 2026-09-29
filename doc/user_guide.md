# Redis Streams User Guide

**For:** Rust service developers using `qubit-event-bus` 0.15 and `qubit-event-bus-redis` 0.4. This guide shows how an order publisher and billing consumer share events through Redis while keeping application code on the event-bus facade.

[简体中文](user_guide.zh_CN.md) · [README](../README.md) · [API docs](https://docs.rs/qubit-event-bus-redis)

## 1. Add the provider and select it

Add both the facade and provider as direct dependencies. `discovery` is on by default in the provider crate; it submits `redis-streams` to the synchronous and asynchronous provider inventories. Your application calls `EventBusRegistry::discover()` or `AsyncEventBusRegistry::discover()` and selects the provider by ID.

```toml
[dependencies]
qubit-event-bus = { version = "0.15", features = ["discovery"] }
qubit-event-bus-redis = "0.4"
qubit-spi = "0.13"
```

Use `default-features = false` and choose `features = ["sync"]` or `features = ["async"]` if the application only uses one SPI. `async` supports the Redis client's Smol adapter and Tokio host detection. The crate does not start a Tokio runtime or spawn a subscription worker; the facade's async runner is polled by the application's executor.

Redis 6.2 or newer is required. The integration tests use Docker to start isolated Redis 6.2, Redis 7, and Sentinel processes.

## 2. Register a codec for portable payloads

Redis stores encoded bytes, so every payload type used by the facade needs an `EventCodec<T>`. This example uses UTF-8 `String` messages. Production applications can replace it with JSON, Protobuf, or another schema-aware codec.

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

Create a `CodecRegistry`, register `Utf8Codec`, and place it in `EventBusFacadeConfig`. If the codec is missing, the facade rejects a typed publish or subscription before calling Redis.

## 3. Publish and consume synchronously

The service config contains only a Redis URL and a namespace. Select `redis-streams` explicitly so registry fallback cannot silently choose another backend.

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
    // A long-running service retains `subscription` and cancels it during shutdown.
    drop(subscription);
    Ok(())
}
```

The example writes before it creates the `Earliest` group, so that group can read the retained event. In production, register the consumer before relying on `New`, which starts after the group is created. Redis Streams accepts only `Durable` subscriptions; `Ephemeral` is rejected before Redis I/O. Groups remain when a service disconnects, and closing a subscription leaves unsettled records pending. Redis applies the start position only when it first creates a group.

Malformed wire records are moved atomically to a group-specific quarantine stream (`qubit:poison:*`) and acknowledged from the source group. The quarantine record stores `source_stream`, `source_id`, `group`, a stable `reason`, the original `wire` bytes, and `wire_missing`. A successful quarantine is returned as a `Gap` without exposing payload bytes. Inspect entries with `XRANGE <quarantine-key> - +`; monitor quarantine length, source stream length, and pending entries with `XLEN` and `XPENDING`. The provider never trims these streams automatically, so operators should archive or remove quarantine records under their retention policy.

Delivery remains at least once. If a handler runs longer than `redis.claim_min_idle_ms`, another consumer may claim its pending entry. A publish whose `XADD` reply is lost has an unknown outcome and may be duplicated if retried. `redis.max_unsettled_per_subscription` bounds locally active deliveries (default 100).

During a long `receive` call, recovery scans repeat every `redis.recovery_interval_ms` milliseconds (default 1,000; valid range 50–60,000). Shorter intervals reduce the wait before idle pending records can be reclaimed, at the cost of more Redis scan commands.

Streams are unlimited by default. To enable approximate trimming, add `("redis.stream_maxlen_approx".into(), "100000".into())` to the `ProviderOptions` map shown above. Every publish then uses `XADD MAXLEN ~ 100000`. Redis may trim unread records or entries still referenced by a consumer group's pending list; trimming does not guarantee delivery and can make old work unrecoverable. Keep this disabled when pending history must remain available for recovery. Malformed-record quarantine streams follow their separate operator retention policy.

Redis does not report a `ReceiveOutcome::Gap` for records trimmed before any consumer read them. The provider cannot identify those missing IDs, so applications must treat opt-in trimming as possible silent history loss and monitor retention outside the delivery API.

With a `ConsumerGroup` set, instances using the same namespace, topic, and group share work. A different group gets its own stream cursor and receives its own copy. Without an explicit group, the subscriber ID becomes the group identity.

Each Redis subscription receives a random `qubit:consumer:<uuid>` consumer name. The facade's local subscription ID is not globally unique and is not used as the Redis consumer identity. A process restart creates a new consumer; pending records belonging to the old name remain in the group and can be recovered by `XAUTOCLAIM` after `redis.claim_min_idle_ms`.

The wire format currently supports version 1. A valid numeric version other than 1 returns a non-retryable `SpiError::Operation` with kind `unsupported_wire_version`; the source entry stays pending and is not quarantined or acknowledged. Stop old consumers, deploy a provider that understands the new version, then restart consumers in the same group. Verify recovery with `XPENDING` until the old entry is settled. Do not clear the pending entry to silence the error. Invalid JSON, missing or non-numeric versions, and malformed version 1 records continue to use quarantine.

Recovery work is bounded per interval: claim and own-pending scans have separate eight-command limits, while tombstone repair is capped at one `XPENDING`, four `XRANGE`, and four total quarantine `EVAL` commands. Scan cursors continue across intervals and calls. `Duration::ZERO` performs bounded non-blocking recovery (at most one claim, one own-pending read, and one new-message read), skips tombstone scans, and may quarantine one malformed entry. `Duration::MAX` waits indefinitely by issuing finite one-second Redis blocking reads. A finite timeout limits extra recovery round trips and the Redis `BLOCK` duration; it cannot forcibly cancel a single network command already in progress.

## 4. Run the asynchronous SPI

Async bus creation, publish, subscribe, and receive return runtime-neutral futures. The example uses `futures_lite::future::block_on` for a small application. A long-running service usually polls these futures on its existing executor and runs the subscription concurrently with other service work.

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

The async facade also requires the same codec registry as the sync example. `AsyncSubscription::run` is caller driven; its future belongs on the application's executor. Dropping a pending `receive` future does not acknowledge the record. Redis keeps it in the consumer group's pending entries list, and a later receive by that consumer or `XAUTOCLAIM` by another consumer can recover it.

## Runnable examples

The repository includes complete facade examples that register the UTF-8 codec, publish an event, consume it, and close the subscription and bus:

```bash
cargo run --example sync_orders -- redis://127.0.0.1/ local-sync-orders
cargo run --example async_orders -- redis://127.0.0.1/ local-async-orders
REDIS_SENTINEL_NODES=127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381 \
REDIS_SENTINEL_SERVICE_NAME=qeventbus \
  cargo run --example sentinel_orders -- local-sentinel-orders
```

The async example runs until it consumes the event, then waits for Enter before closing; this lets the handler finish before the runner cancels its next receive. Each command needs the matching standalone Redis or Sentinel service and the features required by that example.

## 5. Configure Redis and Sentinel

| Option | Default | Meaning |
| --- | --- | --- |
| `redis.url` | `redis://127.0.0.1/` | Standalone Redis connection URL; inline username and password are rejected. |
| `redis.namespace` | `qubit` | Prefix scope used to derive stream and group keys. |
| `redis.claim_min_idle_ms` | `30000` | Minimum pending idle time before another consumer can claim an entry. |
| `redis.recovery_interval_ms` | `1000` | Recovery scan interval during a long receive call; accepts 50 through 60,000 ms. |
| `redis.max_unsettled_per_subscription` | `100` | Maximum delivered but unsettled messages held by one subscription; receive waits while the limit is reached. |
| `redis.max_idle_connections` | `8` | Maximum idle synchronous standalone command connections retained for reuse; accepts 1 through 64. Dedicated blocking receiver connections are counted separately. |
| `redis.stream_maxlen_approx` | unset | Optional approximate stream entry limit applied with `XADD MAXLEN ~`; may trim unread or pending records. |
| `redis.username_env` | unset | Environment variable name containing the Redis ACL username. |
| `redis.password_env` | unset | Environment variable name containing the Redis ACL password. |
| `redis.sentinel.nodes` | unset | Comma-separated Sentinel `host:port` endpoints. |
| `redis.sentinel.service_name` | unset | Sentinel master service name; required with `nodes`. |
| `redis.sentinel.username_env` | unset | Environment variable name containing the Sentinel ACL username. |
| `redis.sentinel.password_env` | unset | Environment variable name containing the Sentinel ACL password. |

When Sentinel is configured, both `redis.sentinel.nodes` and `redis.sentinel.service_name` are required. The URL remains syntactically valid but is not used to locate the master. Sentinel connections are resolved through the Sentinel client instead of entering the standalone idle pool, so commands after failover can resolve the promoted master. Standalone synchronous short commands reuse up to `redis.max_idle_connections` idle connections; async standalone publish and settlement share a multiplexed command connection. Receiver reads use their own connection so blocking reads do not occupy the short-command path.

Credentials belong in the service environment. Provider options contain environment variable names, and `RedisEventBusConfig` redacts its URL and credentials from `Debug`. Do not put raw secrets in provider options, URLs, command-line arguments, or logs. This release does not enable TLS options in `redis-rs`; keep Redis traffic on a trusted network until TLS support is added.

## 6. Understand delivery, retry, and cleanup

The provider stores one JSON wire record under the `wire` stream field. It contains a protocol version, event ID, timestamp, headers, optional ordering key, content type, optional schema ID, and payload bytes. Unknown versions fail with a provider error. `XADD` success returns an `Accepted` acknowledgement; if the connection drops before the reply arrives, the caller cannot know whether Redis stored the record. Retrying a publish can create a duplicate.

| Action | Redis behavior | Application consequence |
| --- | --- | --- |
| `Accept` | `XACK` | Removes the message from the group's pending list. |
| `Reject` | `XACK` | Terminates delivery; no dead-letter stream is created. |
| `Retry` | Leaves the message pending | It can be read again by this consumer or claimed after the idle threshold. |
| Close or drop | No implicit `XACK` | Unsettled messages remain recoverable. |
| Cancel async receive | No implicit `XACK` | A consumed record remains in Redis PEL for a later receive/claim. |

Redis Streams provide at-least-once delivery, so make handlers idempotent. A slow handler can exceed `redis.claim_min_idle_ms`; another consumer may claim the same pending event while the first handler is still running. Choose an idle threshold that fits handler latency and retain business-level deduplication where duplicates are costly.

Each subscription stops receiving new entries while its unsettled count reaches `redis.max_unsettled_per_subscription`. Settling or retrying an entry frees capacity. This bounds in-process delivery pressure; it does not limit Redis stream growth.

`StartPosition::New` creates a group at the current stream tail. `Earliest` starts a new group at `0-0`. `At("milliseconds-sequence")` supplies a Redis Stream ID. Once a group exists, Redis retains its cursor, so changing the requested start position does not rewind that existing group.

The provider does not trim streams or delete groups. Monitor Redis memory and stream growth. Before deleting a stream or group, stop consumers and decide how to handle every pending event; deleting pending records can produce `ReceiveOutcome::Gap`. Configure Redis persistence and replication to match the application's recovery objectives: `Accepted` does not mean fsynced, and Sentinel replication can lose writes that were not replicated before promotion.

## 7. Diagnose common failures

- **Registry has no `redis-streams` entry:** keep `qubit-event-bus-redis` as a direct dependency, enable its `discovery` feature, and call the matching sync or async registry's `discover()`.
- **Typed publish or subscribe reports a codec requirement:** register an `EventCodec<T>` in `EventBusFacadeConfig` for that topic payload type.
- **Provider rejects configuration:** check `redis.url`, `redis.namespace`, paired Sentinel settings, and whether referenced credential environment variables exist. Inline URL credentials are rejected to avoid leaking them in diagnostics.
- **Consumer does not receive old events:** use a new group with `StartPosition::Earliest`; an existing group keeps its stored Redis cursor.
- **A consumer takes over too soon or too late:** adjust `redis.claim_min_idle_ms` to the handler's normal and worst-case duration. Re-delivery remains possible.
- **A gap appears after stream maintenance:** inspect operator `XDEL`/`XTRIM` activity and pending entries before further cleanup. A gap means Redis no longer has one or more pending records.
- **Sentinel cannot connect:** verify each endpoint, master service name, ACL environment references, quorum, and that the Redis master addresses returned by Sentinel are reachable from the application host.

There are no built-in PEL or reconnect metrics in this release. Use Redis `XPENDING`, `XINFO STREAM`, and `XINFO GROUPS` during operations, and record provider errors and publish receipt IDs in application telemetry.

## 8. Stop cleanly

Cancel synchronous subscriptions before graceful bus shutdown. For async buses, close or stop the `AsyncSubscription`, then await `AsyncEventBus::shutdown`. Immediate close leaves unsettled stream entries in Redis; it never implies acceptance.

## Support boundary

Supported: Redis standalone and Sentinel, Redis 6.2+, encoded payloads, consumer groups, accepted/retry/reject settlement, and replay from Redis stream positions. Not supported: Cluster, native payloads, ordering guarantees, delayed delivery, automatic trimming/deletion, dead-letter routing, and TLS configuration. The provider does not claim exactly-once processing.
