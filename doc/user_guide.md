# Redis Streams User Guide

**For:** Rust service developers using the current sibling-source checkout, whose manifest versions are provider 0.7.0, core 0.21, and SPI 0.14. See the [installation and validation status](design.md#installation-and-validation-status) for the current release-installation evidence.

[简体中文](user_guide.zh_CN.md) · [README](../README.md) · [API docs](https://docs.rs/qubit-event-bus-redis)

## 1. Add the provider and select it

For a consumer in the Qubit source workspace, add both sibling checkouts as direct dependencies. `discovery` is on by default in the provider crate; it submits `redis-streams` to the synchronous and asynchronous provider inventories. Your application calls `EventBusRegistry::discover()` or `AsyncEventBusRegistry::discover()` and selects the provider by ID.

<!-- current-source-dependencies -->
```toml
[dependencies]
qubit-event-bus = { version = "^0.21", path = "../rs-event-bus", features = ["discovery"] }
qubit-event-bus-redis = { version = "0.7.0", path = "../rs-event-bus-redis" }
qubit-spi = "^0.14"

[patch.crates-io]
qubit-event-bus = { path = "../rs-event-bus" }
```

These paths are for sibling checkouts; adjust them to the consumer manifest's location. Put the patch in the application workspace root manifest. The provider's registry core dependency is not replaced by the application's direct path dependency, so both dependency paths and the patch path must point to the same core checkout. This example demonstrates local source integration. Consult the migration guide before upgrading an existing deployment.

The current Cargo manifest imports Redis as `redis`, uses package `qubit-redis` at exact version `=2.0.0-rc.2`, and resolves it from the registry.

Registry verification has two modes. `source-candidate` is the default and compares requested provider/core versions with this source manifest. `published` skips that comparison, but still requires exact stable versions, registry sources, a single core package identity, and the provider-to-core dependency edge. Both modes run prepare, manifest validation, lockfile generation, feature builds, and metadata verification. Example: `bash ci/check-registry-consumer.sh --mode published --provider-version X.Y.Z --core-version X.Y.Z --spi-version X.Y.Z`.

For discovery, disable defaults and select `features = ["sync", "discovery"]` or `["async", "discovery"]` on the provider, and enable facade `discovery`. For manual registration, provider `features = ["sync"]` or `["async"]` suffice; facade discovery is unnecessary. `async` enables the Redis client's Smol adapter; its futures can be polled by a Smol or Tokio host. The crate does not start a Tokio runtime or spawn a subscription worker; the facade's async runner is polled by the application's executor.

Redis 6.2 or newer is required. Integration tests use Docker to start isolated Redis 6.2, Redis 7, and Sentinel processes. The fixture verifies the actual `INFO server` version and logs `backend`, `requested`, and `actual_version`. Its local fallback requires Redis 6.2 or newer; explicit 6.2 and 7 fixtures require the requested version series and fail on a mismatch or failed probe. If Docker startup is unavailable, set `RER_REDIS_6_2_SERVER` or `RER_REDIS_7_SERVER` to the matching local server binary for explicit-version tests, or make a supported `redis-server` available on `PATH` for the default fixture.

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

Malformed in-limit version 1 wire records are handled by one Lua script that serially writes a group-specific quarantine record (`qubit:poison:*`) with `XADD`, then acknowledges the source entry with `XACK`. The quarantine record stores `source_stream`, `source_id`, `group`, a stable `reason`, the original `wire` bytes, and `wire_missing`. The script does not roll back a successful quarantine write if acknowledgement fails; if its reply is lost, the caller cannot determine from that reply alone which steps completed. Check the source ID, group, pending owner, and quarantine record before deciding what to do. A confirmed successful quarantine is returned as a `Gap` without exposing payload bytes. Inspect a page with `XRANGE <quarantine-key> - + COUNT 100`; monitor quarantine length, source stream length, and pending entries with `XLEN` and `XPENDING`. Page through the remaining IDs for a complete inspection. The provider never trims these streams automatically, so operators should archive or remove quarantine records under their retention policy.

Pending entries that remain recoverable can be redelivered. If a handler runs longer than `redis.claim_min_idle_ms`, another consumer may claim its pending entry. A publish whose `XADD` reply is lost has an unknown outcome; the default facade uncertainty gate prevents automatic resubmission, and manual or explicitly permitted retries may duplicate it. `redis.max_unsettled_per_subscription` bounds locally active deliveries (default 100).

The first `receive` on each subscription checks pending entries immediately. Later receive calls share a recovery deadline: calls before `redis.recovery_interval_ms` (default 1,000 ms, valid range 50–60,000) read new entries without repeating the scans; long-running receives still revisit recovery when the interval expires. Retry, a receive error, or cancellation after polling an async receive forces recovery on the next call. Shorter intervals reduce the wait before eligible idle pending entries can be reclaimed, at the cost of more Redis scan commands.

Redis recovery scans can change pending-entry metadata even when the provider filters an already active message and does not return it again. `XAUTOCLAIM` and the same-consumer pending `XREADGROUP` may refresh PEL idle time and increment Redis's delivery count. `provider_attempt` reports that Redis delivery count for recovered messages; it is not a count of handler executions, and `XPENDING` idle time alone does not establish how long a handler has run or prove that it is stalled. Diagnose recovery with the PEL owner and count together with consumer and application handler telemetry.

Streams are unlimited by default. Only after accepting historical loss in the retention review below, add both `("redis.stream_maxlen_approx".into(), "100000".into())` and `("redis.allow_lossy_retention".into(), "true".into())` to the `ProviderOptions` map shown above. Every publish then uses `XADD MAXLEN ~ 100000`. Redis may trim unread records or entries still referenced by a consumer group's pending list; trimming does not guarantee delivery and can make old work unrecoverable. Keep this disabled when pending history must remain available for recovery. Malformed-record quarantine streams follow their separate operator retention policy.

Redis does not report a `ReceiveOutcome::Gap` for records trimmed before any consumer read them. The provider cannot identify those missing IDs, so applications must treat opt-in trimming as possible silent history loss and monitor retention outside the delivery API.

With a `ConsumerGroup` set, instances using the same namespace, topic, and group share work. A different group gets its own stream cursor and receives its own copy. Without an explicit group, the subscriber ID becomes the group identity.

Each Redis subscription receives a random `qubit:consumer:<uuid>` consumer name. The facade's local subscription ID is not globally unique and is not used as the Redis consumer identity. A process restart creates a new consumer; pending records belonging to the old name remain in the group and can be recovered by `XAUTOCLAIM` after `redis.claim_min_idle_ms`.

The wire format currently supports version 1. Within `redis.max_wire_bytes`, the decoder validates the complete JSON document and Serde recursion guard before deciding whether a numeric version is unsupported. A fully valid unknown unsigned 64-bit version returns a non-retryable `SpiError::Operation` with kind `unsupported_wire_version`; the source entry stays pending and is not quarantined or acknowledged. Invalid JSON, including JSON that exceeds the recursion guard even when it declares a future version, and malformed version 1 records continue to use quarantine. Stop old consumers, deploy a provider that understands the new version, then restart consumers in the same group. Verify recovery with `XPENDING` until the old entry is settled. Do not clear the pending entry to silence the error.

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

Paste the codec fragment above this `main`; add `futures-lite = "2"` and `futures-channel = "0.3"` to the dependencies. This program prints `consumed order event: order-43`, waits until the delivery is settled while continuing to poll the runner, and then performs graceful shutdown. The async facade uses the same codec registry as the sync example. `AsyncSubscription::run` is caller driven; its future belongs on the application's executor. Dropping a pending `receive` future does not acknowledge the record. Redis keeps it in the consumer group's pending entries list, and a later receive by that consumer or `XAUTOCLAIM` by another consumer can recover it.


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
| `redis.tls_ca_cert_path` | Optional PEM CA bundle for the Redis endpoint. If unset, the provider leaves redis-rs' default root selection unchanged: this crate's default feature graph uses built-in WebPKI roots, while downstream feature unification may add native roots. No client identity is configured unless both client certificate and key paths are set. Requires `rediss://`. |
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

When Sentinel is configured, both `redis.sentinel.nodes` and `redis.sentinel.service_name` are required. The URL remains syntactically valid but is not used to locate the master. Each resolver retains at most one idle Sentinel query connection per endpoint and enabled SPI mode; each operation still runs `SENTINEL get-master-addr-by-name` and opens a new candidate master connection for `ROLE` and the business command. A stale idle Sentinel socket may fail that endpoint once; the next operation reconnects, while each endpoint is still attempted once per resolution. Sentinel sockets and candidate master sockets used for ordinary short commands have a fixed 1 MiB RESP response-frame budget that stays with each socket. Receiver master sockets retain their configured response-frame budget, including valid values above 1 MiB. This bounds response frames, not process RSS, and does not limit publish request bodies. Standalone synchronous short commands reuse up to `redis.max_idle_connections` idle connections; async standalone operations use separate multiplexed general and settlement connections, shared by role and initialized on demand. Settlement EVAL traffic therefore does not wait behind an unanswered publish request on the general connection, and failures invalidate only the affected role generation. Calls in one cold initialization round share its success or failure, and a later call can retry after failure. If the initializer future is cancelled, a caller still waiting in that round can take over with its own initializer. The provider uses no background initialization task or failure cooldown, and the connection timeout is not an end-to-end deadline. Generation checks prevent an old failed lease from invalidating its replacement. Sentinel paths try each of at most sixteen nodes once per resolution and prefer the last successful node. A failover after ROLE can still reject a command; XADD is never transparently replayed. Receiver reads use their own connection so blocking reads do not occupy the short-command path.

Credentials belong in the service environment. Provider options contain environment variable names, and `RedisEventBusConfig` redacts its URL, credentials, and TLS file paths from `Debug`. Do not put raw secrets in provider options, URLs, command-line arguments, or logs. TLS encrypts the connection and authenticates the endpoint; it does not change Redis persistence, replication, delivery, or idempotency guarantees.

### Grant Redis ACL access by role

Give the application account only the commands and keys for its configured topics and groups. This provider uses the Redis client's default RESP2 protocol: when a password is configured (with an optional ACL username), connection setup authenticates with `AUTH`; it does not require `HELLO`. Redis database selection is separate: the client sends `SELECT` only when the database in `redis.url` is nonzero (for example, `/3`). The client also attempts `CLIENT SETINFO LIB-NAME` and `CLIENT SETINFO LIB-VER` on new connections, but ignores their replies; these metadata commands are optional and are not needed for provider work. Redis 6.2 does not support `CLIENT SETINFO`. Synchronous standalone pooled connections may use `PING` when checking a reused idle connection.

| Account/path | Commands to allow | Key access |
| --- | --- | --- |
| Publisher | `XADD`; plus `PING` for synchronous standalone idle-connection health checks | Write the configured topic stream. |
| Durable subscriber and settlement | `XGROUP CREATE`, `XREADGROUP`, `XAUTOCLAIM`, `XPENDING`, `XRANGE`, `EVAL` (settlement script calls `XPENDING` and `XACK`); plus `PING` for synchronous standalone idle-connection health checks | Read/write the topic stream. Terminal settlement uses one atomic EVAL that checks the current PEL owner before XACK. Recovery commands run on different receive paths; one receive does not necessarily issue all of them. `XGROUP CREATE` also needs permission to create the stream when absent. |
| Malformed-record quarantine and tombstone repair | `EVAL`, and the script's internal `TYPE`, `XPENDING`, `XRANGE`, `XADD`, `XACK` commands | The source stream and its group-specific quarantine stream. The script checks both key types, inspects/reads the source, writes the quarantine record, and acknowledges the source entry. Granting only `EVAL` is insufficient if ACL key or command rules deny an internal operation. |
| Sentinel discovery account | `SENTINEL get-master-addr-by-name` and `AUTH` when that connection uses a password | No application stream keys. Keep Sentinel credentials separate from master credentials. On servers with ACL subcommand rules, grant `+sentinel|get-master-addr-by-name`; where subcommand rules are unavailable, `+sentinel` grants the broader Sentinel command and should be reviewed against that server's ACL capabilities. |
| Discovered master account | The publisher/subscriber commands required by the application, plus `ROLE` for master validation | The same application stream and quarantine keys as standalone mode. The master uses `redis.username_env` and `redis.password_env`, not the Sentinel credential variables. |
| Operations/observer account | As needed: `XINFO`, `XLEN`, `XPENDING`, `XRANGE`, `INFO` | Read-only access to the specific source and quarantine keys being inspected. Use a separate account from the application. |

The provider's stable names can be derived with the public `naming` helpers. For the fixed example namespace `guide-acl`, topic `events`, subscriber `billing-worker`, and explicit group `billing`, the stream key is `qubit:stream:9:guide-acl:6:events`, the group name is `qubit:group:9:guide-acl:6:events:7:billing`, and the quarantine key is `qubit:poison:9:guide-acl:6:events:43:qubit:group:9:guide-acl:6:events:7:billing`. The group name is an argument to stream commands, not a Redis key. Redis ACL key patterns use glob matching, not literal string matching: `*`, `?`, bracket expressions, and escapes have pattern meaning. The example components contain no glob metacharacters. If configured namespace/topic/group values contain glob metacharacters, the corresponding ACL pattern can match more keys than intended; constrain those values or review the resulting pattern. For additional topics or groups, grant the corresponding stream and quarantine keys explicitly or deliberately define and test a narrowly scoped pattern.

For that one topic/group, an administrator can provision an application user with rules along these lines (replace the secret through your protected ACL provisioning channel; do not paste a production secret into shell history):

```text
ACL SETUSER event-worker reset on >replace-with-managed-secret \\
  ~qubit:stream:9:guide-acl:6:events \\
  ~qubit:poison:9:guide-acl:6:events:43:qubit:group:9:guide-acl:6:events:7:billing \\
  +ping +xadd +xgroup|create +xreadgroup +xautoclaim +xpending +xrange +xack \\
  +eval +type
```

`+eval` is required for normal terminal settlement as well as quarantine. Terminal settlement also requires the script's `XPENDING` and `XACK` permissions. `+type` is used by quarantine; omitting it disables that path, but does not make `+eval` optional. Missing quarantine permissions leave malformed records unresolved and must be handled operationally. An installation that uses a nonzero database must also authorize the client's setup-time `SELECT` according to its Redis ACL policy. If the Redis server supports `CLIENT SETINFO` and you want client metadata to be recorded, you may grant its `setinfo` subcommand; denial does not block provider setup. Do not replace this list with `+@all`. A separately provisioned Sentinel account needs the Sentinel discovery permission above, while the master account needs its own application rules and `ROLE`; observer permissions belong to a separate operational identity.

When an operation fails with `NOPERM`, check the failing command and key in the Redis error or server audit log, then compare them with the path-specific rows above. Verify both the stream and quarantine key patterns, `EVAL` plus each command executed by its script, and whether a nonzero URL database caused `SELECT`. In Sentinel mode, verify Sentinel discovery permission and master `ROLE`/stream permissions with the right credential pair. Provider errors classify Redis `NOPERM` as non-retryable `authentication`; changing credentials alone will not fix a missing command or key rule. Use an administrator/observer connection to inspect `XPENDING` and the quarantine stream after a denied quarantine attempt: a failed script must not be treated as confirmed quarantine, and Redis Lua does not roll back a quarantine `XADD` if a later `XACK` fails.

## 6. Understand delivery, retry, and cleanup

The provider stores one JSON wire record under the `wire` stream field. It contains a protocol version, event ID, timestamp, headers, optional ordering key, content type, optional schema ID, and payload bytes. Unknown versions fail with a provider error. A newly read `XREADGROUP >` message reports `delivery.context().provider_attempt() == Some(1)`. Before delivering each recovered pending or `XAUTOCLAIM` entry, the provider queries Redis `XPENDING` details for that exact ID. It reports the Redis delivery count only when exactly one returned row has the matching ID and current consumer owner and the nonzero count fits the SPI's `u32` range; a failed, empty, malformed, zero, unrepresentable, or mismatched lookup reports `None`. This adds one Redis command for each recovered message, not for new messages. Use `retry_attempt` for facade-local retries. `XADD` success returns an `Accepted` acknowledgement; if the connection drops before the reply arrives, the caller cannot know whether Redis stored the record. Retrying a publish can create a duplicate.

| Action | Redis behavior | Application consequence |
| --- | --- | --- |
| `Accept` | One `EVAL`: exact-ID `XPENDING` owner check, then `XACK` | Removes the message only if this consumer still owns it. A changed owner returns non-retryable `settlement_ownership_changed`; stop that subscription and recover the group with a new receiver. |
| `Reject` | One `EVAL`: exact-ID `XPENDING` owner check, then `XACK` | Terminates delivery only if this consumer still owns it; no dead-letter stream is created. |
| `Retry` | Leaves the message pending | It can be read again by this consumer or claimed after the idle threshold. |
| Close or drop | No implicit `XACK` | Unsettled messages remain recoverable. |
| Cancel async receive | No implicit `XACK` | A consumed record remains in Redis PEL for a later receive/claim. |

Terminal settlement uses one single-key `EVAL` that checks the exact PEL entry's current owner before calling `XACK`; it never sends a bare `XACK`. The local intent state determines how to interpret and retry the result:

| Local state and result | Outcome |
| --- | --- |
| `AckPending(original)`; EVAL returns `0` or `1` | The original intent becomes `Applied`; both replies confirm completion. |
| `AckPending(original)`; EVAL returns `-1` | Return non-retryable `settlement_ownership_changed` (`false`); keep `AckPending` and the active slot. Stop the affected subscription, then recover the group with a new receiver. |
| `AckPending(original)`; reply is unknown | Keep the original intent and retry only that same intent. A conflicting intent is rejected. |
| `Applied(original)`; same intent requested | Idempotent success with no I/O. A conflicting intent is rejected. |

If the first EVAL receives a complete explicit Redis rejection, the intent remains open and can be attempted again; once an ACK outcome is uncertain, the original intent is fixed. The owner check protects Redis acknowledgement state only: it is not business fencing and cannot stop an old handler's external side effects.

Redis Streams provide at-least-once delivery, so make handlers idempotent. A slow handler can exceed `redis.claim_min_idle_ms`; another consumer may claim the same pending event while the first handler is still running. Choose an idle threshold that fits handler latency and retain business-level deduplication where duplicates are costly.

Still-resident Redis stream entries can be delivered again, so handlers must be idempotent. For `qubit-task` notifications, use a durable consumer checkpoint, retain the highest `state_version`, and query authoritative task state when a version gap appears.

Each subscription stops receiving new entries while its unsettled count reaches `redis.max_unsettled_per_subscription`. Settling or retrying an entry frees capacity. This bounds in-process delivery pressure; it does not limit Redis stream growth.

`StartPosition::New` creates a group at the current stream tail. If the group already exists, `New` resumes its stored cursor. `Earliest` starts a new group at `0-0`, and `At("milliseconds-sequence")` supplies a Redis Stream ID. Redis retains an existing group cursor, so the provider now rejects an existing group with `Earliest` or `At` using the non-retryable `existing_group_start_position_ignored` error; it does not silently ignore the requested start position. Use a different group name when you intend to create a group at a new position. If you intentionally want to resume the existing cursor while keeping an `Earliest` or `At` profile, opt in explicitly:

<!-- doc-example: existing-group-resume -->
```rust
use qubit_event_bus::model::{StartPosition, SubscribeOptions, SubscriptionDurability};

let options = SubscribeOptions::<String>::builder()
    .durability(SubscriptionDurability::Durable)
    .start_position(StartPosition::Earliest)
    .provider_option("redis.existing_group_start", "resume")
    .build();
```

The default policy is `reject`; the only accepted values are `reject` and `resume`. `resume` accepts the existing Redis cursor and never issues `XGROUP SETID`, so it does not change or rewind that cursor. An unknown `redis.*` subscription option or an invalid value fails before Redis network I/O.

`XGROUP CREATE` is confirmed only by the RESP `+OK` status. Nil, integer, string (including bulk `OK` and `PONG`), array, or nested error replies are malformed and produce `SpiError::Operation` with `operation=subscribe`, `kind=outcome_unknown`, and `retryable=Some(false)`; they are not retried. A timeout or connection failure after sending the first command is also unknown and allows at most one retry. If that retry cannot connect, is rejected (including `NOPERM` or `READONLY`), or again has a transport or malformed-reply failure, the result remains unknown because the second observation cannot prove the first command did not create the group. A retry `+OK` confirms the group. `BUSYGROUP` also allows success for `New` or explicit `resume`, which accept the existing cursor; with `Earliest` or `At` and the default `reject` policy it remains unknown. Stop blind retries. Use the public naming helpers to find the stream and group, then inspect `XINFO GROUPS <stream>` and `XPENDING <stream> <group>`. If continuing the stored cursor is safe, retry explicitly with `redis.existing_group_start=resume`; if the requested replay position is required, create a new group name after assessing duplicate processing. The provider does not issue `XGROUP SETID`, delete a group, or ACK records to recover this outcome. A first-attempt `BUSYGROUP` remains a definite existing-group position conflict.

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

Publication checks payload before copying bytes and uses capped headers/wire serialization before `XADD`. Reception checks borrowed wire bytes before copying a string, parses the version without a complete JSON value tree, then bounds version 1 fields as they are decoded. Payload growth is checked before each push; headers string length is checked before parsing headers JSON, and nested JSON remains depth bounded. In addition, the dedicated receiver connection rejects a RESP response frame above `redis.max_wire_bytes + 65,536` bytes before constructing its response value. Decoder budgets constrain provider allocations as fields are decoded. These protections are not a process RSS limit, and the receiver frame bound does not apply to responses read on other connections; decoded structures, other connections, and Redis itself have separate memory costs.

`receive_limit_exceeded` is returned when a readable record exceeds configured wire, payload, or header capacity; the source entry remains in the PEL without `XACK`, `XDEL`, or quarantine. `receive_response_too_large` means the complete RESP frame exceeded the receiver connection's separate bound; it closes the connection, stops the subscription, and releases receiver admission, while any resulting PEL entry remains untouched. Unsupported valid wire versions are retained the same way. First inspect the entry and owner with `XPENDING`; then fix limits or deploy a compatible provider/codec and create a new subscription in the same group to verify recovery. Deleting or isolating retained work is an operator decision; do not remove it just to hide the error. In-limit malformed version 1 data continues to use quarantine. Wire version 1 from older releases remains readable.

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
| Settlement EVAL sent, but outcome unknown | `outcome_unknown` / `Some(true)` | Retry the same token with its original `Accept` or `Reject` intent; never switch to `Retry` or the other terminal intent. |
| Quarantine script unknown/possibly partial | `outcome_unknown` / `Some(false)` | Inspect source/PEL/owner and existing copies; do not blindly replay the script. |
| Admission exhausted before I/O | `resource_limit` / `Some(true)` | Back off, lower concurrency or close unused receivers; no business command was sent by this rejected operation. |
| Publish exceeds raw payload / full wire limit | `payload_too_large` / `wire_too_large`, `Some(false)` | Reduce the encoded payload/metadata or deliberately raise limits. No XADD is sent. |
| Fully valid unknown version within the wire limit | `unsupported_wire_version` / `Some(false)`; malformed or excessively nested JSON is quarantined | Upgrade the reader; keep the PEL entry until a compatible reader settles it. |

At the SPI boundary, settlement follows this receiver/token-local state machine:

| Progress | Allowed request | Result |
| --- | --- | --- |
| Open | `Retry` | Release the local active slot; leave Redis PEL intact; commit Retry locally. |
| Open | `Accept` / `Reject` | Acquire connection and command admission first, then fix intent immediately before the owner-check EVAL can be sent. |
| AckPending(original) | Original terminal intent only | Repeat the same owner-check EVAL; reply 0 or 1 completes local settlement, while -1 returns `settlement_ownership_changed` and preserves AckPending. |
| Applied(original) | Original intent only | Idempotent success without Redis I/O. |
| AckPending / Applied | Any conflicting intent | Invalid settlement token; no Redis command. |

An unpolled settle future or failed/cancelled connection acquisition leaves Open. A first owner-check EVAL receiving a complete **top-level Redis rejection** can return to Open because that command was not applied. Socket failure, timeout, malformed replies, nested RESP errors, or cancellation preserve AckPending; once any earlier attempt was unknown, a later explicit rejection cannot reopen the token. Unknown ACK keeps its active slot until the same intent completes or the receiver closes. This constraint does not survive a process restart as persisted intent, and it provides no cross-consumer fencing.

## 10. Bound resources and message sizes

Limits apply to the same created SPI instance and its shared Arc clones. Each new `create_configured` call creates an independent client budget; instances selected through different registries do not share a Redis-global or process-global budget. `max_concurrent_commands` bounds admitted short operations; its default is 64. Of these, `reserved_settlement_commands` (default 8) stays available to settlement when general admission is full. Dedicated receiver connections use their separate `max_active_receivers` cap (default 256), not short-command slots. Total command concurrency must be at least 2; the old value 1 is rejected as a breaking configuration change. When lowering the total, the omitted reservation adapts to `min(8, total - 1)`; set it explicitly only if a different reserve is needed. Failure/cancellation releases local command admission; close/drop releases receiver admission even if tokens remain alive. Cancellation does **not** guarantee that a Redis multiplexed driver stops or that the server command is no longer in flight. There is no unbounded provider admission queue. Dedicated blocking reads remain separate from the shared short-command channel.

Claim, pending, attempt-detail, and tombstone-maintenance commands run on the receiver's dedicated connection and do not consume general short-command permits. Recovery work is bounded by the receiver lease, per-round command budget, and finite I/O waits; exhausting the round budget defers remaining work to a later receive. `max_concurrent_commands` limits admitted short operations, not all Redis commands or server connections. Receiver admission is per provider instance, and Redis/server-side clients from observers, Sentinel, and other services are not counted by these limits.

`max_idle_connections` defaults to 8 and cannot exceed `max_concurrent_commands`. When configuring fewer than eight short operations, lower idle retention too, for example `redis.max_concurrent_commands=4` with `redis.max_idle_connections=4`; setting only the former rejects configuration. New byte/time/count options are positive decimal values with finite ranges; zero does not disable their limits.

The default 1MiB payload limit measures encoded bytes, while the 8MiB wire limit measures all JSON, including byte-array expansion and metadata. Large headers can exceed wire even when the payload is legal. Publish checks payload first and writes JSON through a bounded sink, including intermediate headers JSON. Provider encoding borrows caller-owned metadata and payload; its allocated headers/wire strings are bounded. Direct `WireFields::from_outbound` conversion does not apply these configured provider limits. Receive checks the complete wire-byte limit before parsing; the full JSON syntax and Serde recursion guard apply to every version, including ignored fields. A fully valid unknown version skips version 1 field validation, while malformed or excessively nested JSON follows the poison policy. Version 1 then receives bounded typed field, payload, and header validation. A readable historical record exceeding wire, payload, or header capacity returns `receive_limit_exceeded` and remains pending; only deterministic malformed data enters quarantine. The dedicated receiver's RESP frame bound is independent of these decoded-record limits.

Decoder budgets bound the provider’s additional serialization and parsing allocations. Dedicated receiver connections separately enforce a RESP response-frame limit of `redis.max_wire_bytes + 65,536` bytes before constructing the response value. This frame limit does not apply to other connections and is not a process RSS limit. Combine these protections with a trusted Redis/network boundary, server `maxmemory`, application concurrency, and source/quarantine retention. Quarantine has no automatic retention limit.

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

As a starting cadence, sample `subscription.delivery_metrics().metrics` every minute and keep a five-minute window, alongside local snapshots, stream/quarantine lengths, group/consumer and PEL summaries, application outbox oldest-row age where used, and Redis memory. Preserve the existing alerts: alert when `command_rejections` or `receiver_rejections` grows during five minutes, the oldest outbox row exceeds the application's notification-delay SLO, or Redis used memory exceeds 75% of its allocated budget. PEL idle above `2 × redis.claim_min_idle_ms` is useful investigation evidence, but do not treat idle below that threshold as proof of health.

Recovery scans can refresh PEL idle without returning a delivery to the application: both `XAUTOCLAIM` and the own-pending-history `XREADGROUP` path may refresh it. Thus PEL idle is neither time since the entry first became pending nor handler duration. For a stall signal, investigate when `oldest_owned_age` stays above the application's handling/settlement SLO while `running_handlers > 0` or `settling > 0` and `completed` has not grown. Also investigate any continuously over-SLO `oldest_owned_age` even when `completed` grows; other work may be completing while one delivery remains stuck. `completed` counts successful delivery lifecycles, including settled `Retry` and `Reject` dispositions, not business success. `oldest_owned_age` covers owned receive reservations and deliveries, not just time inside the user handler. `None` means unavailable/no timestamp, not zero. Snapshot fields are sampled separately and are not transactionally consistent.

For example, with one-minute sampling, a one-second recovery interval, `claim_min_idle_ms=60000`, and an old 120-second PEL-idle threshold, an entry that never completes can have its idle refreshed by each recovery scan and never cross the idle threshold. The combined subscription signal still calls for investigation when ownership remains over the application SLO and no lifecycle completes. These values illustrate the failure mode; they are not tested recommendations or provider guarantees.

Subscription lifecycle metrics and local ages reset when that subscription is recreated; SPI counters likewise restart under a new instance ID, and process restart resets the ID sequence. To track age across instances, an external collector must retain first-observed time keyed by Redis deployment/namespace, stream, group, and pending ID. Owner changes must not reset that time. Incomplete or missing pages/samples and a rebuilt group make the history uncertain. First-observed time is only a lower bound on pending duration, not Redis's first-delivery time. For `rs-task` notifications, compare authoritative task state with the consumer projection's highest `state_version`; outbox age describes publication backlog only. On investigation, preserve the error, PEL ID and owner, and business state, then follow the application's manual recovery and retention procedures. Tune cadence and thresholds to workload and response policy; investigate admission pressure, handler stalls, outbox publication, and Redis memory before changing limits.

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

Keep the stages of completion separate when setting expectations. `Accepted` means Redis replied that `XADD` was accepted; it does not mean fsync, replication, consumer receipt, or business commit. For task notifications, the task state and outbox row commit in one SQLite transaction. Before `Accepted`, the outbox row is retained and retried, so publication can duplicate; after `Accepted`, the row is deleted. A Redis failure before persistence can therefore lose a notification whose outbox row was already deleted. Consumer receipt is not guaranteed at least once. Still-resident stream entries can redeliver, so make consumers idempotent with a durable checkpoint, retain the highest `state_version`, and query authoritative task state when a version gap appears. The provider does not make task state, Redis persistence, consumer receipt, and business commit atomic. Treat `XADD MAXLEN ~` as explicitly lossy: it can remove unread or pending history, so enable it only after the retention review above accepts that loss.

With typed `qubit-task` notifications, consumers should deduplicate by `TaskId`, retain the highest `state_version`, ignore duplicate or older notifications, and query the task service for authoritative state after a version gap. The typed SQLite task service can use its optional outbox integration to capture lifecycle changes in the same transaction as task state and retry Redis publication until `Accepted`; after that acknowledgement it deletes the outbox row. This does not guarantee consumer receipt at least once or make unrelated application business transactions atomic with task state or Redis.

A lost tail notification may have no later event to reveal the gap. Applications that need bounded projection recovery can persist accepted task IDs in their own tracking table and reconcile bounded batches on an application-chosen schedule. Query the task service checkpoint outside the projection transaction, re-read the consumer version inside the transaction, and write only when the authoritative version is higher. Retain tracking after query errors, `None`, or a checkpoint behind the projection; retire it after a terminal projection commits. This repairs current state only: Redis acceptance does not promise receipt, and reconciliation cannot recreate missing transitions or side effects or provide exactly-once processing.

## 12. Upgrade from provider 0.6

The current source manifest is provider 0.7.0 with core 0.21. The 2026-11-04 registry query is a historical observation and does not establish current registry availability; current release-installation evidence is summarized in the [installation and validation status](design.md#installation-and-validation-status). `RedisSubscriptionProfile` builds durable subscription options from an explicit `StartPosition`; use it for new subscriptions and preserve the intended consumer group. Core codec registration now rejects duplicate payload types and returns `Result`; handle it with `?` or use `replace` deliberately. `InboundMessage` carries optional provider-attempt metadata without changing its existing `into_parts` tuple. Keep stored wire version 1 data and consumer groups; test retained pending entries before rollout. Start collecting provider snapshots and Redis PEL/memory separately as described above. The [migration guide](migration.md) covers the complete version-specific changes.


See [design](design.md), [coverage evidence](coverage-review.md), and [workload benchmark](connection-reuse-benchmark.md). Performance/coverage results require their own measured evidence; this guide makes no new throughput or final coverage claim.

## Support boundary

Supported: Redis standalone and Sentinel, Redis 6.2+, verified TLS for standalone and Sentinel connections, encoded payloads, consumer groups, accepted/retry/reject settlement, and replay from Redis stream positions. Not supported: Cluster, native payloads, ordering guarantees, delayed delivery, automatic lifecycle cleanup, and dead-letter routing. The provider does not claim exactly-once processing.


## Lossy stream retention

Redis streams are not trimmed by default. To enable approximate `MAXLEN` trimming, configure both `redis.stream_maxlen_approx=<positive integer>` and `redis.allow_lossy_retention=true`. Trimming can remove unread or pending entries and cause delivery gaps; subscriptions stop on gaps by default. Monitor `XLEN`, `XPENDING`, and `XINFO` when validating retention behavior. An accepted `XADD` is not an fsync or replication guarantee.
