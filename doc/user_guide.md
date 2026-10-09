# Redis Streams User Guide

**For:** Rust service developers using `qubit-event-bus` 0.20 and `qubit-event-bus-redis` 0.7. This guide shows how an order publisher and billing consumer share events through Redis while keeping application code on the event-bus facade.

[简体中文](user_guide.zh_CN.md) · [README](../README.md) · [API docs](https://docs.rs/qubit-event-bus-redis)

## 1. Add the provider and select it

Add both the facade and provider as direct dependencies. `discovery` is on by default in the provider crate; it submits `redis-streams` to the synchronous and asynchronous provider inventories. Your application calls `EventBusRegistry::discover()` or `AsyncEventBusRegistry::discover()` and selects the provider by ID.

```toml
[dependencies]
qubit-event-bus = { version = "0.20", features = ["discovery"] }
qubit-event-bus-redis = "0.7"
qubit-spi = "0.13"
```

Use matching core and provider minor versions, and update the lockfile with both packages together. The repository examples use the current checkout; consult the migration guide before upgrading an existing deployment.

For discovery, disable defaults and select `features = ["sync", "discovery"]` or `["async", "discovery"]` on the provider, and enable facade `discovery`. For manual registration, provider `features = ["sync"]` or `["async"]` suffice; facade discovery is unnecessary. `async` enables the Redis client's Smol adapter; its futures can be polled by a Smol or Tokio host. The crate does not start a Tokio runtime or spawn a subscription worker; the facade's async runner is polled by the application's executor.

Redis 6.2 or newer is required. The integration tests use Docker to start isolated Redis 6.2, Redis 7, and Sentinel processes.

## 2. Register a codec for portable payloads

Redis stores encoded bytes, so every payload type used by the facade needs an `EventCodec<T>`. This example uses UTF-8 `String` messages. Production applications can replace it with JSON, Protobuf, or another schema-aware codec.

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

This is a reusable module fragment. Paste it above either `main` below in `src/main.rs`; the complete programs call `facade_config()` and need no hidden objects. Create a `CodecRegistry`, register `Utf8Codec`, and place it in `EventBusFacadeConfig`. If the codec is missing, the facade rejects a typed publish or subscription before calling Redis.

## 3. Publish and consume synchronously

The service config selects `redis-streams` explicitly so registry fallback cannot silently choose another backend. The example also sets a namespace and opts into resuming an existing group; this option is for repeatable demos and does not change the provider's default `reject` policy.

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

This entry exercise installs the billing handler, publishes `order-42`, waits for its notification, and prints `consumed order event: order-42` before cancelling and shutting down. A production handler must commit the application’s billing change before returning `Ok(())`; printing or notifying a channel is only the exercise’s observable result. Keep the bus and subscription in the application startup/shutdown owner. The first run creates a group at `Earliest`; later runs opt in at the subscription level with `redis.existing_group_start=resume` and continue from Redis's saved cursor. Redis applies the start position only when it first creates a group: resuming never issues `XGROUP SETID` and does not replay already acknowledged history. Change the namespace to isolate demo data. Existing pending entries can still be delivered first. A new `Earliest` group can also read retained events. In production, register the consumer before relying on `New`, which starts after the group is created. Redis Streams accepts only `Durable` subscriptions; `Ephemeral` is rejected before Redis I/O. Groups remain when a service disconnects, and closing a subscription leaves unsettled records pending.

Malformed in-limit version 1 wire records are copied atomically to a group-specific quarantine stream (`qubit:poison:*`) and acknowledged from the source group. The quarantine record stores `source_stream`, `source_id`, `group`, a stable `reason`, the original `wire` bytes, and `wire_missing`. A successful quarantine is returned as a `Gap` without exposing payload bytes. Inspect a page with `XRANGE <quarantine-key> - + COUNT 100`; monitor quarantine length, source stream length, and pending entries with `XLEN` and `XPENDING`. Page through the remaining IDs for a complete inspection. The provider never trims these streams automatically, so operators should archive or remove quarantine records under their retention policy.

Delivery remains at least once. If a handler runs longer than `redis.claim_min_idle_ms`, another consumer may claim its pending entry. A publish whose `XADD` reply is lost has an unknown outcome; the default facade uncertainty gate prevents automatic resubmission, and manual or explicitly permitted retries may duplicate it. `redis.max_unsettled_per_subscription` bounds locally active deliveries (default 100).

The first `receive` on each subscription checks pending entries immediately. Later receive calls share a recovery deadline: calls before `redis.recovery_interval_ms` (default 1,000 ms, valid range 50–60,000) read new entries without repeating the scans; long-running receives still revisit recovery when the interval expires. Retry, a receive error, or cancellation after polling an async receive forces recovery on the next call. Shorter intervals reduce the wait before eligible idle pending entries can be reclaimed, at the cost of more Redis scan commands.

Streams are unlimited by default. Only after accepting historical loss in the retention review below, add both `("redis.stream_maxlen_approx".into(), "100000".into())` and `("redis.allow_lossy_retention".into(), "true".into())` to the `ProviderOptions` map shown above. Every publish then uses `XADD MAXLEN ~ 100000`. Redis may trim unread records or entries still referenced by a consumer group's pending list; trimming does not guarantee delivery and can make old work unrecoverable. Keep this disabled when pending history must remain available for recovery. Malformed-record quarantine streams follow their separate operator retention policy.

Redis does not report a `ReceiveOutcome::Gap` for records trimmed before any consumer read them. The provider cannot identify those missing IDs, so applications must treat opt-in trimming as possible silent history loss and monitor retention outside the delivery API.

With a `ConsumerGroup` set, instances using the same namespace, topic, and group share work. A different group gets its own stream cursor and receives its own copy. Without an explicit group, the subscriber ID becomes the group identity.

Each Redis subscription receives a random `qubit:consumer:<uuid>` consumer name. The facade's local subscription ID is not globally unique and is not used as the Redis consumer identity. A process restart creates a new consumer; pending records belonging to the old name remain in the group and can be recovered by `XAUTOCLAIM` after `redis.claim_min_idle_ms`.

The wire format currently supports version 1. Within `redis.max_wire_bytes`, a valid unsigned 64-bit integer version other than 1 returns a non-retryable `SpiError::Operation` with kind `unsupported_wire_version`; the source entry stays pending and is not quarantined or acknowledged. Stop old consumers, deploy a provider that understands the new version, then restart consumers in the same group. Verify recovery with `XPENDING` until the old entry is settled. Do not clear the pending entry to silence the error. Invalid JSON, missing or non-numeric versions, and malformed version 1 records continue to use quarantine.

Recovery work is bounded per interval: claim and own-pending scans have separate eight-command limits, while tombstone repair is capped at one `XPENDING`, four `XRANGE`, and four total quarantine `EVAL` commands. Scan cursors continue across intervals and calls. `Duration::ZERO` performs bounded non-blocking recovery (at most one claim, one own-pending read, and one new-message read), skips tombstone scans, and may quarantine one malformed entry. `Duration::MAX` waits indefinitely by issuing finite one-second Redis blocking reads. A finite timeout is a scheduling deadline: it limits new recovery work and actual Redis `BLOCK` duration. Zero means no server-side waiting for new records, not a zero-millisecond network deadline. Blocking response waits use actual `BLOCK` plus `redis.command_timeout_ms`. Sync socket waits are soft per-I/O limits: DNS, multiple addresses, setup stages and sustained small packets can exceed the whole-call budget. An in-flight command is not forcibly stopped.

## 4. Run the asynchronous SPI

Async bus creation, publish, subscribe, and receive return runtime-neutral futures. The example uses `futures_lite::future::block_on` for a small application. A long-running service usually polls these futures on its existing executor and runs the subscription concurrently with other service work.

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
            // Keep polling the runner while shutdown drains the handler and settlement.
            let report = bus.shutdown(ShutdownMode::Graceful { timeout: Duration::from_secs(5) }).await?;
            if report.outcome != ShutdownOutcome::Complete {
                return Err("graceful shutdown did not complete".into());
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        };
        let (run_result, stop_result) = future::zip(run, stop).await;
        run_result?;
        stop_result?;
        subscription.close().await?;
        Ok(())
    })
}
```

Paste the codec fragment above this `main`; add `futures-lite = "2"` and `futures-channel = "0.3"` to the dependencies. This program prints `consumed order event: order-43`, signals shutdown after the handler finishes, and polls the runner alongside graceful shutdown until both complete. The async facade uses the same codec registry as the sync example. `AsyncSubscription::run` is caller driven; its future belongs on the application's executor. Dropping a pending `receive` future does not acknowledge the record. Redis keeps it in the consumer group's pending entries list, and a later receive by that consumer or `XAUTOCLAIM` by another consumer can recover it.


## Manual registration without discovery

The following are module fragments. In the sync or async program, replace the registry import and `...Registry::discover()?` with the matching fragment and `order_registry()?`; retain the codec and the rest of the main. The documentation tests compile and run each variant with provider defaults disabled and without `discovery`.

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

## Runnable examples

The repository includes complete facade examples that register the UTF-8 codec, publish an event, consume it, and close the subscription and bus:

```bash
cargo run --example sync_orders -- redis://127.0.0.1/ local-sync-orders
cargo run --example async_orders -- redis://127.0.0.1/ local-async-orders
REDIS_SENTINEL_NODES=127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381 \
REDIS_SENTINEL_SERVICE_NAME=qeventbus \
  cargo run --example sentinel_orders -- local-sentinel-orders
```

The async example runs until it consumes the event, then waits for Enter before closing; this lets the handler finish before the runner cancels its next receive. These examples use durable groups and opt in at the subscription level to resume an existing group's saved cursor, while new groups still start at `Earliest`. Redis retains stream data and groups after the process exits; a fresh demonstration can use a new namespace. Existing pending entries may be delivered before the event published by the current run. Each command needs the matching standalone Redis or Sentinel service and the features required by that example.

## 5. Configure Redis and Sentinel

| Option | Default | Meaning |
| --- | --- | --- |
| `redis.url` | `redis://127.0.0.1/` | Redis connection URL; `redis://` is plaintext and `rediss://` requires verified TLS. Inline username and password are rejected. In Sentinel mode, the scheme selects the discovered master's transport. |
| `redis.namespace` | `qubit` | Prefix scope used to derive stream and group keys. |
| `redis.claim_min_idle_ms` | `30000` | Minimum pending idle time before another consumer can claim an entry. |
| `redis.recovery_interval_ms` | `1000` | Interval for recovery scans, including across repeated receive calls; accepts 50 through 60,000 ms. First receive scans immediately. |
| `redis.max_concurrent_commands` | `64` | Total short-command admission limit, 2 through 4,096. |
| `redis.reserved_settlement_commands` | `8` | Short-command slots reserved for settlement; must be positive and below the total command limit. For a total below 9, the omitted default is total minus one. |
| `redis.max_active_receivers` | `256` | Maximum dedicated live receiver connections, 1 through 4,096. |
| `redis.max_unsettled_per_subscription` | `100` | Maximum delivered but unsettled messages held by one subscription; receive waits while the limit is reached. |
| `redis.max_wire_bytes` | `8388608` | Maximum serialized wire bytes. |
| `redis.max_payload_bytes` | `1048576` | Maximum decoded payload bytes. |
| `redis.max_headers_bytes` | `65536` | Maximum decoded headers string bytes before JSON parsing. |
| `redis.max_idle_connections` | `8` | Maximum idle synchronous standalone command connections retained for reuse; accepts 1 through 64. Dedicated blocking receiver connections are counted separately. |
| `redis.stream_maxlen_approx` | unset | Optional approximate stream entry limit applied with `XADD MAXLEN ~`; may trim unread or pending records. |
| `redis.allow_lossy_retention` | `false` | Must be `true` together with `redis.stream_maxlen_approx`; explicitly accepts possible historical loss. |
| `redis.username_env` | unset | Environment variable name containing the Redis ACL username. |
| `redis.password_env` | unset | Environment variable name containing the Redis ACL password. |
| `redis.sentinel.nodes` | unset | At most sixteen comma-separated Sentinel `host:port` endpoints; empty hosts and invalid/zero ports are rejected. |
| `redis.sentinel.service_name` | unset | Sentinel master service name; required with `nodes`. |
| `redis.sentinel.username_env` | unset | Environment variable name containing the Sentinel ACL username. |
| `redis.sentinel.password_env` | unset | Environment variable name containing the Sentinel ACL password. |

### TLS connections

TLS is opt-in. Keep `redis.url=redis://...` for the existing plaintext behavior, or use `rediss://...` to require verified TLS for the standalone Redis endpoint. The provider validates the certificate chain and hostname and rejects URL options that disable verification. A TLS handshake, certificate, or hostname failure is an error and never causes a plaintext retry. Sentinel discovery uses a separate TLS switch: the URL scheme selects TLS for the discovered master, while `redis.sentinel.tls` selects TLS for Sentinel nodes. The Sentinel and master CA roots and client identities are independent.

| Option | Meaning |
| --- | --- |
| `redis.tls_ca_cert_path` | Optional PEM CA bundle for the Redis endpoint; otherwise built-in Web PKI roots are used. Requires `rediss://`. |
| `redis.tls_client_cert_path` / `redis.tls_client_key_path` | Optional mTLS certificate and private key for Redis. Configure both together; requires `rediss://`. |
| `redis.sentinel.tls` | Enable verified TLS for Sentinel nodes; defaults to `false`. Requires `redis.sentinel.nodes`. |
| `redis.sentinel.tls_ca_cert_path` | Optional PEM CA bundle for Sentinel nodes. Requires `redis.sentinel.tls=true`. |
| `redis.sentinel.tls_client_cert_path` / `redis.sentinel.tls_client_key_path` | Optional mTLS identity for Sentinel nodes. Configure both together and enable Sentinel TLS. |

For a standalone deployment with a private CA and mTLS, add these provider options to the existing configuration:

```text
redis.url=rediss://redis.example.net:6379/0
redis.tls_ca_cert_path=/etc/redis/ca.pem
redis.tls_client_cert_path=/etc/redis/client.pem
redis.tls_client_key_path=/etc/redis/client-key.pem
```

To use TLS for both Sentinel and its discovered master, configure the master options above and add:

```text
redis.sentinel.nodes=sentinel-a.example.net:26379,sentinel-b.example.net:26379
redis.sentinel.service_name=primary
redis.sentinel.tls=true
redis.sentinel.tls_ca_cert_path=/etc/redis/sentinel-ca.pem
redis.sentinel.tls_client_cert_path=/etc/redis/sentinel-client.pem
redis.sentinel.tls_client_key_path=/etc/redis/sentinel-client-key.pem
```

For plaintext Sentinel with a TLS master, leave `redis.sentinel.tls` unset (or `false`) and keep `redis.url` as `rediss://...`. Do not reuse master certificates for Sentinel unless that service is configured to trust the same identity. Each configured PEM file must be nonempty and no larger than 1 MiB. The provider reads and parses these files when the provider instance is constructed; failures return a generic TLS configuration error without paths, certificate data, or library details. Rebuild the provider instance after replacing files to rotate certificates; existing instances do not reload them. A failed `XADD` retains the existing unknown-effect semantics and is not transparently replayed because of a TLS failure.

When Sentinel is configured, both `redis.sentinel.nodes` and `redis.sentinel.service_name` are required. The URL remains syntactically valid but is not used to locate the master. Sentinel connections use bounded `SENTINEL get-master-addr-by-name` and candidate `ROLE` probes instead of entering the standalone idle pool, so commands after failover can resolve the promoted master. Standalone synchronous short commands reuse up to `redis.max_idle_connections` idle connections; async standalone publish and settlement share a multiplexed command connection. Concurrent cold initialization is single-flight; generation checks prevent an old failed lease from invalidating its replacement. Sentinel paths do not cache those master sockets, try each of at most sixteen nodes once per resolution, and prefer the last successful node. A failover after ROLE can still reject a command; XADD is never transparently replayed. Receiver reads use their own connection so blocking reads do not occupy the short-command path.

Credentials belong in the service environment. Provider options contain environment variable names, and `RedisEventBusConfig` redacts its URL, credentials, and TLS file paths from `Debug`. Do not put raw secrets in provider options, URLs, command-line arguments, or logs. TLS encrypts the connection and authenticates the endpoint; it does not change Redis persistence, replication, delivery, or idempotency guarantees.

## 6. Understand delivery, retry, and cleanup

The provider stores one JSON wire record under the `wire` stream field. It contains a protocol version, event ID, timestamp, headers, optional ordering key, content type, optional schema ID, and payload bytes. Unknown versions fail with a provider error. A newly read `XREADGROUP >` message reports `delivery.context().provider_attempt() == Some(1)`. Pending and `XAUTOCLAIM` recovery messages report `None` because the historical Redis delivery count is not currently propagated; use `retry_attempt` for facade-local retries. `XADD` success returns an `Accepted` acknowledgement; if the connection drops before the reply arrives, the caller cannot know whether Redis stored the record. Retrying a publish can create a duplicate.

| Action | Redis behavior | Application consequence |
| --- | --- | --- |
| `Accept` | `XACK` | Removes the message from the group's pending list. |
| `Reject` | `XACK` | Terminates delivery; no dead-letter stream is created. |
| `Retry` | Leaves the message pending | It can be read again by this consumer or claimed after the idle threshold. |
| Close or drop | No implicit `XACK` | Unsettled messages remain recoverable. |
| Cancel async receive | No implicit `XACK` | A consumed record remains in Redis PEL for a later receive/claim. |

Redis Streams provide at-least-once delivery, so make handlers idempotent. A slow handler can exceed `redis.claim_min_idle_ms`; another consumer may claim the same pending event while the first handler is still running. Choose an idle threshold that fits handler latency and retain business-level deduplication where duplicates are costly.

Each subscription stops receiving new entries while its unsettled count reaches `redis.max_unsettled_per_subscription`. Settling or retrying an entry frees capacity. This bounds in-process delivery pressure; it does not limit Redis stream growth.

`StartPosition::New` creates a group at the current stream tail. If the group already exists, `New` resumes its stored cursor. `Earliest` starts a new group at `0-0`, and `At("milliseconds-sequence")` supplies a Redis Stream ID. Redis retains an existing group cursor, so the provider now rejects an existing group with `Earliest` or `At` using the non-retryable `existing_group_start_position_ignored` error; it does not silently ignore the requested start position. Use a different group name when you intend to create a group at a new position. If you intentionally want to resume the existing cursor while keeping an `Earliest` or `At` profile, opt in explicitly:

<!-- doc-example: existing-group-resume -->
```rust
use qubit_event_bus::model::{StartPosition, SubscribeOptions};

let options = SubscribeOptions::<String>::builder()
    .start_position(StartPosition::Earliest)
    .provider_option("redis.existing_group_start", "resume")
    .build();
```

The default policy is `reject`; the only accepted values are `reject` and `resume`. `resume` accepts the existing Redis cursor and never issues `XGROUP SETID`, so it does not change or rewind that cursor. An unknown `redis.*` subscription option or an invalid value fails before Redis network I/O.

Without `redis.stream_maxlen_approx`, the provider does not trim streams; it never deletes groups automatically. Monitor Redis memory and stream growth. Before deleting a stream or group, stop consumers and decide how to handle every pending event; deleting pending records can produce `ReceiveOutcome::Gap`. Configure Redis persistence and replication to match the application's recovery objectives: `Accepted` does not mean fsynced, and Sentinel replication can lose writes that were not replicated before promotion.

### Bound facade work and stop settlement failures

`EventBusFacadeConfig::with_delivery_scheduling` sets `DeliverySchedulingConfig` for both sync and async buses. Its four positive limits default to **4 running handlers, 256 owned deliveries, 32 owned per subscription, and 256 registered subscriptions**. Ownership includes receive reservations, queued messages, running handlers, and settlement. `max_running_handlers` and `max_owned_per_subscription` must not exceed `max_owned_deliveries`. The provider's separate 100-unsettled limit remains in effect; these are count limits, not a total memory budget. A paused async session still occupies a subscription slot until close or terminal cleanup.

A queued hot key and settlement backoff do not consume a running-handler slot. Fairness for an eligible B alongside A applies when B can reserve owned capacity or has already been received; it does not discover B behind an arbitrary unread A backlog. Redis declares no per-key ordering, so this scheduler does not add that unsupported guarantee.

Configure settlement with `EventBusFacadeConfig::with_settlement_retry(SettlementRetryConfig::new(...)?);` see the [migration example](migration.md). Defaults are **5 total attempts including the first, 5 seconds from just before the first SPI attempt, 10 ms initial backoff, and 1 second maximum backoff**. Only `SpiError::retryable() == Some(true)` allows retry. `Some(false)` stops immediately; `None` stops as `RetryabilityUnknown`. Panic, invalid token, clock, or timer failures also terminate. The budget is checked between attempts; it cannot cancel a blocked in-flight Redis command, and late success remains success. An async settle cancelled by pausing still consumes its started attempt; resuming continues the same finite budget.

When settlement terminates, the facade records the first cause before cleanup and stops new receives and handler starts. Already started work can finish; close failures do not overwrite the original cause. Inspect `subscription.terminal_failure()` for `SubscriptionStopReason::Settlement` with event ID, disposition, attempts, termination, and structured `Arc<SpiError>`. Diagnostics emit `SettlementFailed` per failed attempt and one `SettlementStopped` for terminal settlement. Preserve this context in telemetry.

Read `bus.delivery_metrics()` and `subscription.delivery_metrics().metrics` for `reserved_receives`, `queued`, `running_handlers`, `settling`, `lane_waiting`, `settlement_attempts`, `settlement_retries`, `settlement_terminal_failures`, `completed`, handler/settlement duration count, total and maximum, and `oldest_owned_age`. `lane_waiting` is part of `queued`; retries count actual SPI calls after the first. Snapshots are not transactional across concurrent changes. Closed subscription handles retain counters, and bus counters survive subscription removal. These facade snapshots do not measure Redis PEL or reconnect state: also inspect `XPENDING` and `XINFO GROUPS`.

To recover, record the first cause and snapshots, correct connectivity, codec, limits, or policy as appropriate, finish closing the failed subscription, and create a new `Durable` subscription with the **same namespace, topic, and group**. Its new consumer can claim retained pending work after `redis.claim_min_idle_ms`; `StartPosition` does not reset the existing group. Recovery depends on record retention and claim policy: trimming or deletion can destroy pending history, and an already applied `XACK` whose reply was lost leaves nothing to claim. Repeated settlement of the same token and disposition is idempotent; that does not make business handling exactly once.

### Bound one record and recover an incompatible consumer

The provider options `redis.max_wire_bytes`, `redis.max_payload_bytes`, and `redis.max_headers_bytes` default to 8,388,608, 1,048,576, and 65,536 bytes. Values must be positive integers; zero, invalid numbers, and overflow are configuration errors. They are independent: JSON expansion can exceed the wire limit even when payload bytes fit. The facade's separate `PayloadLimits` defaults to 1 MiB in both directions; configure both layers deliberately.

Publication checks payload before copying bytes and uses capped headers/wire serialization before `XADD`. Reception checks borrowed wire bytes before copying a string, parses the version without a complete JSON value tree, then bounds version 1 fields as they are decoded. Payload growth is checked before each push; headers string length is checked before parsing headers JSON, and nested JSON remains depth bounded. These checks do not prevent the Redis client from initially allocating a RESP frame.

`receive_limit_exceeded` stops the subscription and leaves the entry in the PEL without `XACK`, `XDEL`, or quarantine. Unsupported valid wire versions are retained the same way. Fix the limits or deploy a compatible provider/codec, then create a new subscription in the same group and verify reclamation with `XPENDING`. Do not delete pending records to hide the error. In-limit malformed version 1 data continues to use quarantine. Wire version 1 from older releases remains readable.

The codec receives `&EncodedPayload`, and default metadata validation requires exact content type and optional schema equality. `None` and a named schema are different. Override validation explicitly when the application supports an old schema. Metadata mismatch or codec panic stops facade reception without settlement; inspect `terminal_failure()`, fix the codec, and create a new durable subscription. Ordinary `CodecError::Decode` still rejects a bad message with `XACK`.

### Handle an uncertain `XADD` result

Public facade errors are `PublishFailure`, preserving event ID, effect, and cause. Pre-submission wire/configuration failure, failure to open a connection, and explicit server rejection are `NotAccepted`. Connection loss, timeout, or response conversion failure after entering query are `MayHaveBeenAccepted`; failure to receive a reply does not prove that Redis rejected the record.

Default `DuplicateRiskPolicy::Forbid` stops automatic retries after uncertain admission, before custom retry rules. `AllowDuplicates` only permits the configured retry policy to consider another attempt. Uncertainty remains across attempts; a later successful receipt reports `duplicate_possible()`. Retry cancellation after a provider attempt starts is uncertain, while dropping the public future produces no failure value. Retain the EventId and reconcile the business operation. RetryPolicy budgets are soft and do not promise universal hard cancellation of in-flight Redis commands.

Neither the Redis provider nor Redis Streams deduplicates publication by EventId. Deduplicate business effects in consumers. Facade dead-letter forwarding and source `XACK` are separate operations; successful forwarding followed by failed acknowledgement can produce another logical dead-letter. Unknown forwarding results retain durable source work and stop the source subscription under the same uncertainty gate.

## 7. Diagnose common failures

- **Registry has no `redis-streams` entry:** keep `qubit-event-bus-redis` as a direct dependency, enable its `discovery` feature, and call the matching sync or async registry's `discover()`.
- **Typed publish or subscribe reports a codec requirement:** register an `EventCodec<T>` in `EventBusFacadeConfig` for that topic payload type.
- **Provider rejects configuration:** check `redis.url`, `redis.namespace`, paired Sentinel settings, and whether referenced credential environment variables exist. Inline URL credentials are rejected to avoid leaking them in diagnostics.
- **Consumer does not receive old events:** use a new group with `StartPosition::Earliest`; an existing group keeps its stored Redis cursor.
- **A consumer takes over too soon or too late:** adjust `redis.claim_min_idle_ms` to the handler's normal and worst-case duration. Re-delivery remains possible.
- **A gap appears:** inspect operator `XDEL`/`XTRIM` activity and pending entries, and check the quarantine stream's `reason` and `source_id` before further cleanup. Gap can report a missing pending source record (tombstone) or successful quarantine; quarantine copies and acknowledges the entry, so the source record may still remain in the stream.
- **Sentinel cannot connect:** verify each endpoint, master service name, ACL environment references, quorum, and that the Redis master addresses returned by Sentinel are reachable from the application host.

There are no built-in PEL or reconnect metrics in this provider. Use Redis `XPENDING`, `XINFO STREAM`, `XINFO GROUPS`, and `XINFO CONSUMERS` during operations, and record provider errors and publish receipt IDs in application telemetry.

## 8. Stop cleanly

For synchronous buses, call `subscription.cancel()` from the shutdown owner to wait for the worker, then shut down the bus. For asynchronous buses, keep the runner polled while graceful bus shutdown drains admitted handlers and their settlement, as in the program above, and then close the handle. Closing a receiver or choosing immediate shutdown can leave unsettled entries in Redis; close itself never implies acceptance.

## 9. Decide whether a failed operation can be retried

Inspect `SpiError::Operation` fields (`operation`, `kind`, `retryable`) in your application error handling. Errors use stable, secret-safe categories rather than raw Redis diagnostics. Retry hints describe the permitted context; they do not cause transport replay or guarantee duplicate-free processing.

| Failure | Kind / retryable | Application response |
| --- | --- | --- |
| Publish sent, but reply lost, timeout or malformed | `outcome_unknown` / `Some(false)` | Redis may have stored the event. Reconcile by business ID; republish only when the application explicitly accepts duplicate risk. No transparent XADD replay. |
| Receive/claim sent, but outcome unknown | `outcome_unknown` / `Some(true)` | No invented delivery is returned. Resume receive and recover through own PEL or claim. |
| XACK sent, but outcome unknown | `outcome_unknown` / `Some(true)` | Retry the same token with its original `Accept` or `Reject` intent; never switch to `Retry` or the other terminal intent. |
| Quarantine script unknown/possibly partial | `outcome_unknown` / `Some(false)` | Inspect source/PEL/owner and existing copies; do not blindly replay the script. |
| Admission exhausted before I/O | `resource_limit` / `Some(true)` | Back off, lower concurrency or close unused receivers; no business command was sent by this rejected operation. |
| Publish exceeds raw payload / full wire limit | `payload_too_large` / `wire_too_large`, `Some(false)` | Reduce the encoded payload/metadata or deliberately raise limits. No XADD is sent. |
| Unknown version within wire limit | `unsupported_wire_version` / `Some(false)` | Upgrade the reader; keep the PEL entry until a compatible reader settles it. |

At the SPI boundary, settlement follows this receiver/token-local state machine:

| Progress | Allowed request | Result |
| --- | --- | --- |
| Open | `Retry` | Release the local active slot; leave Redis PEL intact; commit Retry locally. |
| Open | `Accept` / `Reject` | Acquire connection and command admission first, then fix intent immediately before XACK can be sent. |
| AckPending(original) | Original terminal intent only | Repeat XACK; a valid integer reply 0 or 1 completes local settlement. |
| Applied(original) | Original intent only | Idempotent success without Redis I/O. |
| AckPending / Applied | Any conflicting intent | Invalid settlement token; no Redis command. |

An unpolled settle future or failed/cancelled connection acquisition leaves Open. A first XACK attempt receiving a complete **top-level Redis rejection** can return to Open because that command was not applied. Socket failure, timeout, malformed replies, nested RESP errors, or cancellation preserve AckPending; once any earlier attempt was unknown, a later explicit rejection cannot reopen the token. Unknown ACK keeps its active slot until the same intent completes or the receiver closes. This constraint does not survive a process restart as persisted intent, and it provides no cross-consumer fencing.

## 10. Bound resources and message sizes

Limits apply to the same created SPI instance and its shared Arc clones. Each new `create_configured` call creates an independent client budget; instances selected through different registries do not share a Redis-global or process-global budget. `max_concurrent_commands` bounds admitted short operations; its default is 64. Of these, `reserved_settlement_commands` (default 8) stays available to settlement when general admission is full. Dedicated receiver connections use their separate `max_active_receivers` cap (default 256), not short-command slots. Total command concurrency must be at least 2; the old value 1 is rejected as a breaking configuration change. When lowering the total, the omitted reservation adapts to `min(8, total - 1)`; set it explicitly only if a different reserve is needed. Failure/cancellation releases local command admission; close/drop releases receiver admission even if tokens remain alive. Cancellation does **not** guarantee that a Redis multiplexed driver stops or that the server command is no longer in flight. There is no unbounded provider admission queue. Dedicated blocking reads remain separate from the shared short-command channel.

The active-receiver cap does not guarantee that every concurrent recovery poll is admitted: claim and other short recovery commands share the general command lane. Handle retryable `resource_limit` during concurrent polling by limiting poll concurrency or tuning the budgets; larger budgets do not guarantee admission or impose a hard server connection cap. Admission is per provider instance, and Redis/server-side clients from observers, Sentinel, and other services are not all counted by these limits.

`max_idle_connections` defaults to 8 and cannot exceed `max_concurrent_commands`. When configuring fewer than eight short operations, lower idle retention too, for example `redis.max_concurrent_commands=4` with `redis.max_idle_connections=4`; setting only the former rejects configuration. New byte/time/count options are positive decimal values with finite ranges; zero does not disable their limits.

The default 1MiB payload limit measures encoded bytes, while the 8MiB wire limit measures all JSON, including byte-array expansion and metadata. Large headers can exceed wire even when the payload is legal. Publish checks payload first and writes JSON through a bounded sink, including intermediate headers JSON. Provider encoding borrows caller-owned metadata and payload; its allocated headers/wire strings are bounded. Direct `WireFields::from_outbound` conversion does not apply these configured provider limits. Receive checks borrowed raw wire before UTF-8/JSON, then typed version-1 payload size. Historical oversized wire/payload becomes quarantine reason `oversized_wire`/`oversized_payload` and a Gap; the wire limit takes precedence even over an otherwise unknown version. Version 1 rejects the 128th nested JSON container, including ignored fields, while unknown versions return before v1 shape/depth validation.

These checks bound the provider’s additional serialization/parsing allocations. Redis RESP bytes have already been received by the client library: this is not a hard isolation boundary against maliciously large RESP bulk values, nor an absolute process memory cap. Combine a trusted Redis/network boundary with server `maxmemory`, application concurrency, and source/quarantine retention. Quarantine has no automatic retention limit.

## 11. Operate groups and downstream notifications

### Collect provider and Redis evidence

The sync example reads `RedisProviderDiagnostics::snapshots()` after creating the bus; the same call works for async SPIs. Keep the bus alive while sampling, select the expected `namespace` and `mode`, and label time series with process identity plus `instance_id`. IDs increase within one process, are not reused there, and disappear from the directory after the instance and its remaining owners are dropped. Recreating an SPI in the same process allocates a new ID and starts its counters at zero; only a process restart resets the ID sequence. These are not Redis stream IDs or durable identifiers. Without a sync or async provider feature, the call returns an empty list. `snapshots()` samples each atomic field separately while constructing a snapshot; each getter returns that captured value, not a fresh atomic read. The fields therefore are not transactionally consistent with one another. A snapshot contains no endpoint, credentials, payload, or raw Redis error.

`general_in_flight` is occupied general short-command slots, including settlements admitted there; `settlement_in_flight` is occupied reserved settlement slots only. Add the two to estimate short-command admission at sampling time, and use `active_receivers` for dedicated receiver leases. These three are gauges sampled when the snapshot is built. The remaining getters capture process-local, monotonically increasing, saturating counters for that SPI lifetime:

| Getter | Counted boundary |
| --- | --- |
| `command_rejections`, `receiver_rejections` | Failed short-command or receiver admission, once per rejected request. |
| `connection_attempts`, `connection_failures` | Actual provider connection-open calls and their failures; pool/cache hits do not count. One Sentinel open counts once even if it probes multiple nodes. |
| `publish_accepted`, `publish_unknown` | Confirmed `XADD` acceptance or a returned uncertain publish result. A cancelled async publish without a return counts neither. |
| `receive_unknown`, `settlement_unknown` | SPI receive or settle calls returning `outcome_unknown`, once per call. |
| `recovery_claim_commands` | Actual `XAUTOCLAIM` commands sent, rather than records claimed. |
| `quarantine_succeeded`, `delivery_gaps` | Confirmed quarantine copies and Gap outcomes returned to the facade; a tombstone Gap need not have a quarantine copy. |

Facade delivery metrics describe queued/running handlers and settlement retries. These SPI counters describe provider admission and Redis operation outcomes; neither reports Redis PEL depth or memory. Query Redis for those server-side facts. Derive keys from the public `naming` API with `stream_key(namespace, topic)`, `group_name(namespace, topic, subscriber, Some(group))` (or `None` when the subscriber is the group), and `poison_key(namespace, topic, generated_group_name)`. Use the resulting exact strings, not guessed `qubit:*` patterns. List actual groups with `XINFO GROUPS` as well as checking configured names.

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

Run these through an authenticated operations connection. `XPENDING` summary gives the group's pending count; page through the detailed form to find the oldest idle entry and its owner. `XINFO GROUPS` shows each existing group's last-delivered position, while `XINFO CONSUMERS` shows owner activity. Redis 6.2 may not provide the Redis 7 `lag` field, so compare positions and application progress without requiring `lag`. `INFO MEMORY` is server-wide, not a per-provider gauge; compare its used memory with the allocation budget for that Redis deployment. `INFO commandstats` can supplement command activity. Quarantine has independent growth and needs its own `XLEN`; `XRANGE ... COUNT 100` samples one page, so continue with an exclusive lower bound at the last returned ID until no entries remain for a complete inspection.

As a starting cadence, collect local snapshots, stream/quarantine lengths, group/consumer and PEL summaries, oldest PEL idle, application outbox oldest-row age where an outbox is used, and Redis memory every minute; compute five-minute counter increases. Alert when `command_rejections` or `receiver_rejections` grows during five minutes, the oldest outbox row exceeds the application's notification-delay SLO, the oldest PEL idle exceeds `2 × redis.claim_min_idle_ms`, or Redis used memory exceeds 75% of its allocated budget. Tune cadence and thresholds to the workload and incident response policy. A recreated SPI starts fresh counters under a new ID; a process restart also resets the ID sequence. Neither reset proves recovered capacity. Investigate admission pressure, stalled handlers, outbox publication, and Redis memory before changing limits.

### Decide retention manually

1. Inventory every current group with `XINFO GROUPS`, including groups owned by other services. For each group, inspect `XPENDING` and `XINFO CONSUMERS`, reconcile its unread position with required history, and record the owner and oldest idle entry. Do not infer safety from `XLEN` alone.
2. Confirm whether a group will be created later for replay, how far back it must read, and the business and audit retention deadline. Record who accepted any possible loss, including unread entries and pending payloads.
3. If any group, PEL entry, unread position, future replay need, or audit deadline is unknown, keep the default unlimited source-stream retention and address capacity with producer throttling, consumer repair, or a reviewed archival/migration plan. A growing stream is a capacity incident to investigate, not permission to trim.
4. Only after explicitly accepting historical loss, configure both `redis.stream_maxlen_approx=<positive integer>` and `redis.allow_lossy_retention=true`. `XADD MAXLEN ~` is approximate and lossy: it can silently remove unread history or remove payloads still referenced by PEL, leaving unrecoverable work or a Gap. It is not a lossless memory control. Recheck all groups after rollout.
5. Review the separate quarantine stream with `XLEN` and sampled `XRANGE`; correlate `source_stream`, `source_id`, `group`, and `reason` with business handling. Archive or remove quarantine entries only after evidence and audit requirements are satisfied. The provider never trims quarantine automatically.

Random consumer names accumulate across restarts; the provider does not automatically run `DELCONSUMER`. Remove an obsolete consumer only after stopping it, checking its PEL is empty and satisfying business retention requirements. Close does not ACK work, delete groups, or delete streams. There is no built-in metrics exporter or automatic stream/quarantine retention policy.

Redis persistence and replication are deployment responsibilities. `Accepted` proves acceptance of XADD, not fsync, replica durability, handler success, or billing commit. A `WAIT` issued on a new observer connection does not fence writes sent through a different provider connection. Sentinel promotion may lose unreplicated writes or group state; inspect actual cursor, pending IDs and owners when validating recovery.

### Deployment readiness checklist

Before rollout and after a Redis failover, review the deployment's recovery objectives and collect these read-only observations using the actual stream key and consumer group:

| Check | Read-only command | What to observe and how to judge it |
| --- | --- | --- |
| Persistence and restart recovery | `INFO persistence` | Review the configured persistence activity and recent save/write status fields exposed by this Redis version. Confirm they match the deployment's recovery objectives; this output does not prove that a particular event was fsynced. |
| Replication and failover | `INFO replication` | Review the node role, replica link/synchronization state, and replication offsets. Compare the current topology and recovery expectations; an offset or connected replica does not prove that every accepted event survives promotion. |
| Stream retention and growth | `XLEN <stream-key>` | Record the stream length over time and compare its growth with retention and capacity plans. Length alone says nothing about unread or pending work. |
| Consumer-group progress | `XINFO GROUPS <stream-key>` | Check the groups that actually exist, their pending counts and last-delivered positions; use `lag` only when the server version provides it. Compare group progress with application processing and replay needs. |
| Pending delivery and recovery ownership | `XPENDING <stream-key> <group>` and `XPENDING <stream-key> <group> - + 100` | Review pending count, consumer ownership, delivery age, and entries across pages. Continue each page after its last returned ID without repeating the boundary entry. Decide whether work is progressing or needs investigation against handler duration and recovery policy. |

Keep the stages of completion separate when setting expectations: `Accepted` means Redis replied that `XADD` was accepted; persistence and fsync depend on Redis deployment settings; replica confirmation is a separate replication event and does not itself prove replica disk persistence; consumer ACK (`XACK`) settles delivery after handling but is not an application database commit; and the business commit is owned by the application. The provider does not make these stages atomic. Delivery is at least once, so make consumer side effects idempotent—for example, deduplicate by business ID and retain the highest `state_version` where versioned notifications are used. Treat `XADD MAXLEN ~` as explicitly lossy: it can remove unread or pending history, so enable it only after the retention review above accepts that loss.

With typed `qubit-task` notifications, consumers should deduplicate by `TaskId` and retain the highest `state_version`, ignoring duplicate/older notifications and querying the task service for authoritative state. The typed SQLite task service can use its optional outbox integration to capture lifecycle changes with its own state transaction and retry Redis publication. Delivery remains at least once, and this does not make unrelated application business transactions atomic with task state or Redis.

## 12. Upgrade from provider 0.6

Upgrade `qubit-event-bus-redis` to 0.7 together with `qubit-event-bus` 0.20. `RedisSubscriptionProfile` builds durable subscription options from an explicit `StartPosition`; use it for new subscriptions and preserve the intended consumer group. Core codec registration now rejects duplicate payload types and returns `Result`; handle it with `?` or use `replace` deliberately. `InboundMessage` carries optional provider-attempt metadata without changing its existing `into_parts` tuple. Keep stored wire version 1 data and consumer groups; test retained pending entries before rollout. Start collecting provider snapshots and Redis PEL/memory separately as described above. The [migration guide](migration.md) covers the complete version-specific changes.


See [design](design.md), [coverage evidence](coverage-review.md), and [workload benchmark](connection-reuse-benchmark.md). Performance/coverage results require their own measured evidence; this guide makes no new throughput or final coverage claim.

## Support boundary

Supported: Redis standalone and Sentinel, Redis 6.2+, verified TLS for standalone and Sentinel connections, encoded payloads, consumer groups, accepted/retry/reject settlement, and replay from Redis stream positions. Not supported: Cluster, native payloads, ordering guarantees, delayed delivery, automatic lifecycle cleanup, and dead-letter routing. The provider does not claim exactly-once processing.


## Lossy stream retention

Redis streams are not trimmed by default. To enable approximate `MAXLEN` trimming, configure both `redis.stream_maxlen_approx=<positive integer>` and `redis.allow_lossy_retention=true`. Trimming can remove unread or pending entries and cause delivery gaps; subscriptions stop on gaps by default. Monitor `XLEN`, `XPENDING`, and `XINFO` when validating retention behavior. An accepted `XADD` is not an fsync or replication guarantee.
