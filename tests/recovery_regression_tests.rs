// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Regression coverage for pending delivery recovery and durable group policy.

#![cfg(any(feature = "sync", feature = "async"))]

mod support;

use std::any::TypeId;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::thread::sleep;
use std::thread::spawn;
use std::time::Duration;
#[cfg(feature = "sync")]
use std::time::Instant;
use std::time::SystemTime;

#[cfg(feature = "async")]
use futures_lite::future::block_on;
#[cfg(feature = "async")]
use futures_lite::future::poll_once;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
#[cfg(feature = "async")]
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EncodedPayload;
#[cfg(feature = "sync")]
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
#[cfg(feature = "async")]
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
use qubit_event_bus_redis::diagnostics::{RedisProviderDiagnostics, RedisProviderSnapshot};
use qubit_event_bus_redis::naming::group_name;
use qubit_event_bus_redis::naming::poison_key;
use qubit_event_bus_redis::naming::stream_key;
#[cfg(feature = "sync")]
use qubit_event_bus_redis::sync::RedisEventBusProvider;
use qubit_id::Id;
#[cfg(feature = "async")]
use qubit_spi::AsyncServiceProvider;
#[cfg(feature = "sync")]
use qubit_spi::ServiceProvider;
use redis::Client;
use redis::Connection;
use redis::FromRedisValue;
use redis::RedisResult;
use redis::Value;
use redis::cmd;
use redis::streams::StreamRangeReply;
use support::controlled_redis::proxy::ControlledRedis;
use support::redis_server::RedisServer;

static SUBSCRIPTION_IDS: AtomicU64 = AtomicU64::new(10_000);

fn snapshot(namespace: &str) -> RedisProviderSnapshot {
    RedisProviderDiagnostics::snapshots()
        .into_iter()
        .find(|snapshot| snapshot.namespace() == namespace)
        .expect("live provider diagnostics")
}

/// Reads one Redis command invocation count from an isolated server.
///
/// The counter is zero when Redis has not yet recorded the command. A malformed
/// INFO value returns an error so the test does not silently lose evidence.
fn command_calls(connection: &mut Connection, command: &str) -> Result<u64, Box<dyn Error>> {
    let stats: String = cmd("INFO").arg("commandstats").query(connection)?;
    let prefix = format!("cmdstat_{command}:calls=");
    match stats.lines().find_map(|line| line.strip_prefix(&prefix)) {
        Some(value) => Ok(value.split(',').next().ok_or("missing call count")?.parse()?),
        None => Ok(0),
    }
}

/// Dropping an unpolled future leaves the clock intact, while cancelling a
/// polled receive forces a scan before another new-entry read.
#[cfg(feature = "async")]
#[test]
fn test_async_polled_receive_cancellation_forces_recovery() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let server = RedisServer::start()?;
        let mut settings = provider_options(&server, "cancelled-recovery-clock", 1);
        settings.insert("redis.recovery_interval_ms".into(), "60000".into());
        let bus = AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(settings))
            .await
            .map_err(|failure| failure.into_error())?;
        let mut receiver = bus
            .subscribe(request(
                "events",
                "cancel-worker",
                "cancel-group",
                SubscriptionDurability::Durable,
            )?)
            .await?;
        assert!(matches!(
            receiver.receive(Duration::ZERO).await?,
            ReceiveOutcome::TimedOut
        ));
        let mut observer = Client::open(server.url())?.get_connection()?;
        let completed_claims = command_calls(&mut observer, "xautoclaim")?;
        assert_eq!(snapshot("cancelled-recovery-clock").recovery_claim_commands(), completed_claims);
        drop(receiver.receive(Duration::from_secs(1)));
        let _ = bus.publish(event("events", "unpolled", b"payload")?).await?;
        let ReceiveOutcome::Message(first) = receiver.receive(Duration::ZERO).await? else {
            return Err("new record lost after unpolled receive drop".into());
        };
        let first_token = first.settlement().ok_or("first settlement token missing")?;
        receiver.settle(first_token, DeliveryDisposition::Accept).await?;
        assert_eq!(
            command_calls(&mut observer, "xautoclaim")?,
            completed_claims,
            "unpolled future must not force recovery"
        );
        let mut in_flight = receiver.receive(Duration::from_secs(2));
        assert!(
            poll_once(&mut in_flight).await.is_none(),
            "receive should await an empty stream"
        );
        drop(in_flight);
        let _ = bus.publish(event("events", "after-cancel", b"payload")?).await?;
        let ReceiveOutcome::Message(second) = receiver.receive(Duration::ZERO).await? else {
            return Err("cancelled receive did not recover the next message".into());
        };
        assert_eq!(second.id().as_str(), "after-cancel");
        assert!(
            command_calls(&mut observer, "xautoclaim")? > completed_claims,
            "polled cancellation must force a claim scan"
        );
        assert_eq!(
            snapshot("cancelled-recovery-clock").recovery_claim_commands(),
            command_calls(&mut observer, "xautoclaim")?
        );
        Ok::<(), Box<dyn Error>>(())
    })
}

/// Verifies normal zero-timeout traffic uses the subscription recovery clock
/// rather than claiming on every message.
#[cfg(feature = "sync")]
#[test]
fn test_sync_hot_path_avoids_repeated_autoclaim_for_one_thousand_messages() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let mut settings = provider_options(&server, "hot-recovery-clock", 1);
    settings.insert("redis.recovery_interval_ms".into(), "60000".into());
    let bus = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(settings))
        .map_err(|failure| failure.into_error())?;
    let mut receiver = bus.subscribe(request(
        "events",
        "hot-worker",
        "hot-group",
        SubscriptionDurability::Durable,
    )?)?;
    let mut observer = Client::open(server.url())?.get_connection()?;
    let before_claim = command_calls(&mut observer, "xautoclaim")?;
    let before_read = command_calls(&mut observer, "xreadgroup")?;
    for index in 0..1_000 {
        let id = format!("hot-{index}");
        let _ = bus.publish(event("events", &id, b"payload")?)?;
        let ReceiveOutcome::Message(message) = receiver.receive(Duration::ZERO)? else {
            return Err(format!("message {index} was not delivered by the zero-timeout read").into());
        };
        assert_eq!(message.id().as_str(), id);
        let token = message.settlement().ok_or("settlement token missing")?;
        receiver.settle(token, DeliveryDisposition::Accept)?;
    }
    let claims = command_calls(&mut observer, "xautoclaim")? - before_claim;
    assert_eq!(snapshot("hot-recovery-clock").recovery_claim_commands(), claims);
    let reads = command_calls(&mut observer, "xreadgroup")? - before_read;
    assert!(claims < 1_000, "hot path issued {claims} XAUTOCLAIM commands");
    assert!(
        reads >= 1_000,
        "new-entry reads must cover the thousand published messages"
    );
    Ok(())
}

/// An old PEL entry is reclaimed when the subscription's recovery interval
/// expires, even if no new stream record arrives to wake a blocking read.
#[cfg(feature = "sync")]
#[test]
fn test_sync_idle_pending_entry_reclaimed_after_subscription_clock_expires() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let mut settings = provider_options(&server, "periodic-pending-reclaim", 1);
    settings.insert("redis.recovery_interval_ms".into(), "50".into());
    settings.insert("redis.command_timeout_ms".into(), "200".into());
    let bus = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(settings))
        .map_err(|failure| failure.into_error())?;
    let mut recovering = bus.subscribe(request(
        "events",
        "recovery-worker",
        "periodic-group",
        SubscriptionDurability::Durable,
    )?)?;
    assert!(matches!(recovering.receive(Duration::ZERO)?, ReceiveOutcome::TimedOut));
    let mut original = bus.subscribe(request(
        "events",
        "original-worker",
        "periodic-group",
        SubscriptionDurability::Durable,
    )?)?;
    let _ = bus.publish(event("events", "periodic-pending", b"payload")?)?;
    let ReceiveOutcome::Message(_) = original.receive(Duration::ZERO)? else {
        return Err("original consumer did not acquire pending entry".into());
    };
    let started = Instant::now();
    let ReceiveOutcome::Message(recovered) = recovering.receive(Duration::from_secs(2))? else {
        return Err("periodic recovery missed the old PEL entry".into());
    };
    assert_eq!(recovered.id().as_str(), "periodic-pending");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "recovery exceeded receive budget"
    );
    Ok(())
}

/// Builds settings for `server`, `topic_namespace`, and `max_unsettled`;
/// returns options without I/O and uses immediate pending reclaim.
fn provider_options(server: &RedisServer, topic_namespace: &str, max_unsettled: usize) -> ProviderOptions {
    [
        ("redis.url".into(), server.url().into()),
        ("redis.namespace".into(), topic_namespace.into()),
        ("redis.claim_min_idle_ms".into(), "0".into()),
        ("redis.max_unsettled_per_subscription".into(), max_unsettled.to_string()),
        ("redis.max_idle_connections".into(), "2".into()),
    ]
    .into()
}

/// Builds an encoded event for `topic` and `id` containing `bytes`; returns
/// identifier/content-type validation errors without network I/O.
fn event(topic: &str, id: &str, bytes: &[u8]) -> Result<OutboundMessage, Box<dyn Error>> {
    Ok(OutboundMessage::new(
        TopicAddress::new(topic)?,
        EventId::new(id)?,
        SystemTime::now(),
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(bytes.to_vec()),
            ContentType::new("application/octet-stream")?,
            None,
        )),
    ))
}

/// Builds a request for `topic`, `subscriber`, `group`, and `durability`;
/// returns identifier validation errors without network I/O.
fn request(
    topic: &str,
    subscriber: &str,
    group: &str,
    durability: SubscriptionDurability,
) -> Result<SpiSubscriptionRequest, Box<dyn Error>> {
    Ok(SpiSubscriptionRequest::new(
        Id::new(SUBSCRIPTION_IDS.fetch_add(1, Ordering::Relaxed)),
        TopicAddress::new(topic)?,
        SubscriberId::new(subscriber)?,
        Some(ConsumerGroup::new(group)?),
        durability,
        StartPosition::Earliest,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    ))
}

/// Returns a lazy sync bus for `server`, `namespace`, and `max_unsettled`;
/// configuration/provider validation errors propagate without connecting.
#[cfg(feature = "sync")]
fn sync_bus(
    server: &RedisServer,
    namespace: &str,
    max_unsettled: usize,
) -> Result<Arc<dyn EventBusSpi>, Box<dyn Error>> {
    sync_bus_with_claim(server, namespace, max_unsettled, 0)
}

/// Returns a lazy sync bus for the endpoint and supplied namespace/active
/// bound, using `claim_min_idle_ms`; returns validation errors without I/O.
#[cfg(feature = "sync")]
fn sync_bus_with_claim(
    server: &RedisServer,
    namespace: &str,
    max_unsettled: usize,
    claim_min_idle_ms: usize,
) -> Result<Arc<dyn EventBusSpi>, Box<dyn Error>> {
    let mut options = provider_options(server, namespace, max_unsettled);
    options.insert("redis.claim_min_idle_ms".into(), claim_min_idle_ms.to_string());
    RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(options))
        .map_err(|failure| failure.into_error())
        .map_err(Into::into)
}

/// Returns a lazy async bus for `server`, `namespace`, and `max_unsettled`;
/// configuration/provider validation errors propagate without connecting.
#[cfg(feature = "async")]
async fn async_bus(
    server: &RedisServer,
    namespace: &str,
    max_unsettled: usize,
) -> Result<Arc<dyn AsyncEventBusSpi>, Box<dyn Error>> {
    async_bus_with_claim(server, namespace, max_unsettled, 0).await
}

/// Returns a lazy async bus for the endpoint and supplied namespace/active
/// bound, using `claim_min_idle_ms`; returns validation errors without I/O.
#[cfg(feature = "async")]
async fn async_bus_with_claim(
    server: &RedisServer,
    namespace: &str,
    max_unsettled: usize,
    claim_min_idle_ms: usize,
) -> Result<Arc<dyn AsyncEventBusSpi>, Box<dyn Error>> {
    let mut options = provider_options(server, namespace, max_unsettled);
    options.insert("redis.claim_min_idle_ms".into(), claim_min_idle_ms.to_string());
    AsyncRedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(options))
        .await
        .map_err(|failure| failure.into_error())
        .map_err(Into::into)
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_skips_unsettled_id_and_delivers_next() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = sync_bus(&server, "recovery-sync", 2)?;
    let _ = bus.publish(event("events", "sync-first", b"first")?)?;
    let _ = bus.publish(event("events", "sync-second", b"second")?)?;
    let mut receiver = bus.subscribe(request(
        "events",
        "sync-worker",
        "sync-group",
        SubscriptionDurability::Durable,
    )?)?;

    let ReceiveOutcome::Message(first) = receiver.receive(Duration::from_secs(2))? else {
        return Err("first message was not received".into());
    };
    assert_eq!(first.id().as_str(), "sync-first");

    let ReceiveOutcome::Message(second) = receiver.receive(Duration::from_secs(2))? else {
        return Err("second message was not received while the first remained unsettled".into());
    };
    assert_eq!(second.id().as_str(), "sync-second");

    assert!(matches!(receiver.receive(Duration::ZERO)?, ReceiveOutcome::TimedOut));
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_retry_releases_active_id_for_redelivery() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = sync_bus(&server, "retry-sync", 2)?;
    let _ = bus.publish(event("events", "retry-first", b"first")?)?;
    let mut receiver = bus.subscribe(request(
        "events",
        "retry-worker",
        "retry-group",
        SubscriptionDurability::Durable,
    )?)?;
    let ReceiveOutcome::Message(first) = receiver.receive(Duration::from_secs(2))? else {
        return Err("first message missing".into());
    };
    let token = first.settlement().ok_or("missing token")?;
    receiver.settle(token, DeliveryDisposition::Retry)?;
    let ReceiveOutcome::Message(retried) = receiver.receive(Duration::from_secs(2))? else {
        return Err("retried message missing".into());
    };
    assert_eq!(retried.id().as_str(), "retry-first");
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_rejects_foreign_settlement_and_is_idempotent_after_close() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = sync_bus(&server, "token-sync", 2)?;
    let _ = bus.publish(event("events", "token-event", b"payload")?)?;
    let mut owner = bus.subscribe(request(
        "events",
        "token-owner",
        "token-group",
        SubscriptionDurability::Durable,
    )?)?;
    let mut other = bus.subscribe(request(
        "events",
        "token-other",
        "token-group",
        SubscriptionDurability::Durable,
    )?)?;
    let ReceiveOutcome::Message(message) = owner.receive(Duration::from_secs(2))? else {
        return Err("message missing".into());
    };
    let token = message.settlement().ok_or("settlement token missing")?;
    assert!(other.settle(token, DeliveryDisposition::Accept).is_err());
    owner.settle(token, DeliveryDisposition::Accept)?;
    owner.settle(token, DeliveryDisposition::Accept)?;
    owner.close()?;
    assert!(matches!(owner.receive(Duration::ZERO)?, ReceiveOutcome::Closed));
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_retry_releases_active_id_for_redelivery() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus(&server, "retry-async", 2).await?;
        let _ = bus.publish(event("events", "retry-first-async", b"first")?).await?;
        let mut receiver = bus
            .subscribe(request(
                "events",
                "retry-worker-async",
                "retry-group-async",
                SubscriptionDurability::Durable,
            )?)
            .await?;
        let ReceiveOutcome::Message(first) = receiver.receive(Duration::from_secs(2)).await? else {
            return Err("first message missing".into());
        };
        let token = first.settlement().ok_or("missing token")?;
        receiver.settle(token, DeliveryDisposition::Retry).await?;
        let ReceiveOutcome::Message(retried) = receiver.receive(Duration::from_secs(2)).await? else {
            return Err("retried message missing".into());
        };
        assert_eq!(retried.id().as_str(), "retry-first-async");
        Ok::<(), Box<dyn Error>>(())
    })
}

#[cfg(feature = "async")]
#[test]
fn test_async_rejects_foreign_settlement_and_is_idempotent_after_close() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus(&server, "token-async", 2).await?;
        let _ = bus.publish(event("events", "token-event-async", b"payload")?).await?;
        let mut owner = bus
            .subscribe(request(
                "events",
                "token-owner-async",
                "token-group-async",
                SubscriptionDurability::Durable,
            )?)
            .await?;
        let mut other = bus
            .subscribe(request(
                "events",
                "token-other-async",
                "token-group-async",
                SubscriptionDurability::Durable,
            )?)
            .await?;
        let ReceiveOutcome::Message(message) = owner.receive(Duration::from_secs(2)).await? else {
            return Err("message missing".into());
        };
        let token = message.settlement().ok_or("settlement token missing")?;
        assert!(other.settle(token, DeliveryDisposition::Accept).await.is_err());
        owner.settle(token, DeliveryDisposition::Accept).await?;
        owner.settle(token, DeliveryDisposition::Accept).await?;
        owner.close().await?;
        assert!(matches!(owner.receive(Duration::ZERO).await?, ReceiveOutcome::Closed));
        Ok::<(), Box<dyn Error>>(())
    })
}

#[cfg(feature = "async")]
#[test]
fn test_async_reuses_standalone_short_command_connection() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus(&server, "connection-reuse", 2).await?;
        let mut observer = Client::open(server.url())?.get_connection()?;
        let before = total_connections(&mut observer)?;
        for index in 0..10 {
            let _ = bus
                .publish(event("events", &format!("connection-{index}"), b"payload")?)
                .await?;
        }
        let after = total_connections(&mut observer)?;
        assert!(
            after - before <= 2,
            "expected one reused command connection, saw {} new connections",
            after - before
        );
        let baseline_before = total_connections(&mut observer)?;
        for _ in 0..10 {
            let mut connection = Client::open(server.url())?.get_connection()?;
            let _: String = cmd("PING").query(&mut connection)?;
        }
        let baseline_after = total_connections(&mut observer)?;
        assert_eq!(
            baseline_after - baseline_before,
            10,
            "fresh-connection control should create one connection per command"
        );
        Ok::<(), Box<dyn Error>>(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_reuses_standalone_short_command_connection() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = sync_bus(&server, "connection-reuse-sync", 2)?;
    let mut observer = Client::open(server.url())?.get_connection()?;
    let before = total_connections(&mut observer)?;
    for index in 0..10 {
        let _ = bus.publish(event("events", &format!("connection-{index}"), b"payload")?)?;
    }
    let after = total_connections(&mut observer)?;
    assert!(
        after - before <= 2,
        "expected pooled short-command connection, saw {} new connections",
        after - before
    );
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_reuses_subscription_read_connection_across_timeouts() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = sync_bus(&server, "read-connection-sync", 2)?;
    let _ = bus.publish(event("warmup", "pool-warmup", b"payload")?)?;
    let mut observer = Client::open(server.url())?.get_connection()?;
    let before = total_connections(&mut observer)?;
    let mut receiver = bus.subscribe(request(
        "events",
        "read-worker",
        "read-group",
        SubscriptionDurability::Durable,
    )?)?;
    let after_subscribe = total_connections(&mut observer)?;
    assert_eq!(
        after_subscribe - before,
        1,
        "subscribe should open one dedicated connection"
    );
    assert!(matches!(
        receiver.receive(Duration::from_millis(5))?,
        ReceiveOutcome::TimedOut
    ));
    assert!(matches!(
        receiver.receive(Duration::from_millis(5))?,
        ReceiveOutcome::TimedOut
    ));
    let after = total_connections(&mut observer)?;
    assert_eq!(
        after - after_subscribe,
        0,
        "two completed receives should reuse the subscription's dedicated connection"
    );
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_reuses_subscription_read_connection_across_timeouts() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus(&server, "read-connection-async", 2).await?;
        let _ = bus.publish(event("warmup", "pool-warmup", b"payload")?).await?;
        let mut observer = Client::open(server.url())?.get_connection()?;
        let before = total_connections(&mut observer)?;
        let mut receiver = bus
            .subscribe(request(
                "events",
                "read-worker",
                "read-group",
                SubscriptionDurability::Durable,
            )?)
            .await?;
        let after_subscribe = total_connections(&mut observer)?;
        assert_eq!(
            after_subscribe - before,
            1,
            "subscribe should open one dedicated connection"
        );
        assert!(matches!(
            receiver.receive(Duration::from_millis(5)).await?,
            ReceiveOutcome::TimedOut
        ));
        assert!(matches!(
            receiver.receive(Duration::from_millis(5)).await?,
            ReceiveOutcome::TimedOut
        ));
        let after = total_connections(&mut observer)?;
        assert_eq!(
            after - after_subscribe,
            0,
            "two completed receives should reuse the subscription's dedicated connection"
        );
        Ok::<(), Box<dyn Error>>(())
    })
}

/// Reads the cumulative connection counter with blocking I/O on `connection`;
/// returns Redis, missing-statistic, or malformed-counter errors.
fn total_connections(connection: &mut Connection) -> Result<u64, Box<dyn Error>> {
    let info: String = cmd("INFO").arg("stats").query(connection)?;
    let value = info
        .lines()
        .find_map(|line| line.strip_prefix("total_connections_received:"))
        .ok_or("Redis INFO stats omitted total_connections_received")?;
    Ok(value.parse()?)
}

/// Writes four distinct malformed entries to `stream` on `connection`;
/// returns Redis I/O/command errors and may leave an already-written prefix.
fn insert_poison_fixtures(connection: &mut Connection, stream: &str) -> Result<(), Box<dyn Error>> {
    cmd("XADD")
        .arg(stream)
        .arg("*")
        .arg("other")
        .arg("field")
        .query::<String>(connection)?;
    cmd("XADD")
        .arg(stream)
        .arg("*")
        .arg("wire")
        .arg("not-json")
        .query::<String>(connection)?;
    cmd("XADD")
        .arg(stream)
        .arg("*")
        .arg("wire")
        .arg(&[0xff_u8][..])
        .query::<String>(connection)?;
    let invalid_id = r#"{"version":1,"event_id":"","timestamp_ms":0,"headers_json":"{}","ordering_key":null,"content_type":"application/octet-stream","schema_id":null,"payload":[]}"#;
    cmd("XADD")
        .arg(stream)
        .arg("*")
        .arg("wire")
        .arg(invalid_id)
        .query::<String>(connection)?;
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_skips_unsettled_id_and_delivers_next() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus(&server, "recovery-async", 2).await?;
        let _ = bus.publish(event("events", "async-first", b"first")?).await?;
        let _ = bus.publish(event("events", "async-second", b"second")?).await?;
        let mut receiver = bus
            .subscribe(request(
                "events",
                "async-worker",
                "async-group",
                SubscriptionDurability::Durable,
            )?)
            .await?;

        let ReceiveOutcome::Message(first) = receiver.receive(Duration::from_secs(2)).await? else {
            return Err("first message was not received".into());
        };
        assert_eq!(first.id().as_str(), "async-first");

        let ReceiveOutcome::Message(second) = receiver.receive(Duration::from_secs(2)).await? else {
            return Err("second message was not received while the first remained unsettled".into());
        };
        assert_eq!(second.id().as_str(), "async-second");

        assert!(matches!(
            receiver.receive(Duration::ZERO).await?,
            ReceiveOutcome::TimedOut
        ));
        Ok::<(), Box<dyn Error>>(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_poison_does_not_block_next() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = sync_bus(&server, "poison-sync", 2)?;
    let stream = stream_key("poison-sync", "events");
    let mut connection = Client::open(server.url())?.get_connection()?;
    insert_poison_fixtures(&mut connection, &stream)?;
    let _ = bus.publish(event("events", "sync-after-poison", b"valid")?)?;
    let mut receiver = bus.subscribe(request(
        "events",
        "sync-poison-worker",
        "sync-poison-group",
        SubscriptionDurability::Durable,
    )?)?;

    assert!(matches!(
        receiver.receive(Duration::from_secs(2))?,
        ReceiveOutcome::Gap(_)
    ));
    assert!(matches!(
        receiver.receive(Duration::from_secs(2))?,
        ReceiveOutcome::Gap(_)
    ));
    assert!(matches!(
        receiver.receive(Duration::from_secs(2))?,
        ReceiveOutcome::Gap(_)
    ));
    assert!(matches!(
        receiver.receive(Duration::from_secs(2))?,
        ReceiveOutcome::Gap(_)
    ));
    let quarantine = poison_key(
        "poison-sync",
        "events",
        &group_name("poison-sync", "events", "sync-poison-worker", Some("sync-poison-group")),
    );
    let quarantined: usize = cmd("XLEN").arg(quarantine).query(&mut connection)?;
    assert_eq!(quarantined, 4);
    assert_eq!(snapshot("poison-sync").quarantine_succeeded(), 4);
    assert_eq!(snapshot("poison-sync").delivery_gaps(), 4);
    let records: StreamRangeReply = cmd("XRANGE")
        .arg(poison_key(
            "poison-sync",
            "events",
            &group_name("poison-sync", "events", "sync-poison-worker", Some("sync-poison-group")),
        ))
        .arg("-")
        .arg("+")
        .query(&mut connection)?;
    assert_eq!(records.ids.len(), 4);
    let raw_wire = String::from_redis_value(records.ids[1].map.get("wire").ok_or("quarantine record omitted wire")?)?;
    assert_eq!(raw_wire, "not-json");
    let pending: Vec<Value> = cmd("XPENDING")
        .arg(&stream)
        .arg(group_name(
            "poison-sync",
            "events",
            "sync-poison-worker",
            Some("sync-poison-group"),
        ))
        .arg("-")
        .arg("+")
        .arg(10)
        .query(&mut connection)?;
    assert!(pending.is_empty());
    let ReceiveOutcome::Message(valid) = receiver.receive(Duration::from_secs(2))? else {
        return Err("valid message after poison records was not received".into());
    };
    assert_eq!(valid.id().as_str(), "sync-after-poison");
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_unknown_wire_version_stays_pending() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let namespace = "unsupported-version-sync";
    let bus = sync_bus(&server, namespace, 2)?;
    let stream = stream_key(namespace, "events");
    let unsupported = r#"{"version":2,"future_field":"kept by newer consumers"}"#;
    let mut connection = Client::open(server.url())?.get_connection()?;
    let message_id: String = cmd("XADD")
        .arg(&stream)
        .arg("*")
        .arg("wire")
        .arg(unsupported)
        .query(&mut connection)?;
    let mut receiver = bus.subscribe(request(
        "events",
        "version-worker",
        "version-group",
        SubscriptionDurability::Durable,
    )?)?;

    let error = match receiver.receive(Duration::from_secs(2)) {
        Ok(_) => return Err("unsupported wire version was not rejected".into()),
        Err(error) => error,
    };
    assert_eq!(error.kind(), "unsupported_wire_version");
    assert_eq!(error.retryable(), Some(false));
    let pending: Vec<Value> = cmd("XPENDING")
        .arg(&stream)
        .arg(group_name(namespace, "events", "version-worker", Some("version-group")))
        .arg(&message_id)
        .arg(&message_id)
        .arg(1)
        .query(&mut connection)?;
    assert_eq!(pending.len(), 1);
    let quarantined: usize = cmd("XLEN")
        .arg(poison_key(
            namespace,
            "events",
            &group_name(namespace, "events", "version-worker", Some("version-group")),
        ))
        .query(&mut connection)?;
    assert_eq!(quarantined, 0);
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_quarantine_failure_keeps_source_pending() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = sync_bus(&server, "poison-failure-sync", 2)?;
    let stream = stream_key("poison-failure-sync", "events");
    let group = group_name("poison-failure-sync", "events", "failure-worker", Some("failure-group"));
    let quarantine = poison_key("poison-failure-sync", "events", &group);
    let mut connection = Client::open(server.url())?.get_connection()?;
    cmd("XADD")
        .arg(&stream)
        .arg("*")
        .arg("other")
        .arg("field")
        .query::<String>(&mut connection)?;
    cmd("SET")
        .arg(&quarantine)
        .arg("wrong-type")
        .query::<String>(&mut connection)?;
    let mut receiver = bus.subscribe(request(
        "events",
        "failure-worker",
        "failure-group",
        SubscriptionDurability::Durable,
    )?)?;
    let error = match receiver.receive(Duration::from_secs(2)) {
        Err(error) => error,
        Ok(_) => return Err("quarantine type error should be returned".into()),
    };
    assert!(matches!(
        error,
        SpiError::Operation {
            retryable: Some(false),
            ..
        }
    ));
    let pending: Vec<Value> = cmd("XPENDING")
        .arg(&stream)
        .arg(&group)
        .arg("-")
        .arg("+")
        .arg(10)
        .query(&mut connection)?;
    assert_eq!(pending.len(), 1);
    cmd("DEL").arg(&quarantine).query::<usize>(&mut connection)?;
    assert!(matches!(
        receiver.receive(Duration::from_secs(2))?,
        ReceiveOutcome::Gap(_)
    ));
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_quarantine_failure_keeps_source_pending() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus(&server, "poison-failure-async", 2).await?;
        let stream = stream_key("poison-failure-async", "events");
        let group = group_name(
            "poison-failure-async",
            "events",
            "failure-worker-async",
            Some("failure-group-async"),
        );
        let quarantine = poison_key("poison-failure-async", "events", &group);
        let mut connection = Client::open(server.url())?.get_connection()?;
        cmd("XADD")
            .arg(&stream)
            .arg("*")
            .arg("other")
            .arg("field")
            .query::<String>(&mut connection)?;
        cmd("SET")
            .arg(&quarantine)
            .arg("wrong-type")
            .query::<String>(&mut connection)?;
        let mut receiver = bus
            .subscribe(request(
                "events",
                "failure-worker-async",
                "failure-group-async",
                SubscriptionDurability::Durable,
            )?)
            .await?;
        let error = match receiver.receive(Duration::from_secs(2)).await {
            Err(error) => error,
            Ok(_) => return Err("quarantine type error should be returned".into()),
        };
        assert!(matches!(
            error,
            SpiError::Operation {
                retryable: Some(false),
                ..
            }
        ));
        let pending: Vec<Value> = cmd("XPENDING")
            .arg(&stream)
            .arg(&group)
            .arg("-")
            .arg("+")
            .arg(10)
            .query(&mut connection)?;
        assert_eq!(pending.len(), 1);
        cmd("DEL").arg(&quarantine).query::<usize>(&mut connection)?;
        assert!(matches!(
            receiver.receive(Duration::from_secs(2)).await?,
            ReceiveOutcome::Gap(_)
        ));
        Ok::<(), Box<dyn Error>>(())
    })
}

#[cfg(feature = "async")]
#[test]
fn test_async_poison_does_not_block_next() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus(&server, "poison-async", 2).await?;
        let stream = stream_key("poison-async", "events");
        let mut connection = Client::open(server.url())?.get_connection()?;
        insert_poison_fixtures(&mut connection, &stream)?;
        let _ = bus.publish(event("events", "async-after-poison", b"valid")?).await?;
        let mut receiver = bus
            .subscribe(request(
                "events",
                "async-poison-worker",
                "async-poison-group",
                SubscriptionDurability::Durable,
            )?)
            .await?;

        for _ in 0..4 {
            assert!(matches!(
                receiver.receive(Duration::from_secs(2)).await?,
                ReceiveOutcome::Gap(_)
            ));
        }
        let mut verify = Client::open(server.url())?.get_connection()?;
        let group = group_name(
            "poison-async",
            "events",
            "async-poison-worker",
            Some("async-poison-group"),
        );
        let quarantine = poison_key("poison-async", "events", &group);
        let quarantined: usize = cmd("XLEN").arg(quarantine).query(&mut verify)?;
        assert_eq!(quarantined, 4);
        assert_eq!(snapshot("poison-async").quarantine_succeeded(), 4);
        assert_eq!(snapshot("poison-async").delivery_gaps(), 4);
        let records: StreamRangeReply = cmd("XRANGE")
            .arg(poison_key(
                "poison-async",
                "events",
                &group_name(
                    "poison-async",
                    "events",
                    "async-poison-worker",
                    Some("async-poison-group"),
                ),
            ))
            .arg("-")
            .arg("+")
            .query(&mut verify)?;
        assert_eq!(records.ids.len(), 4);
        let raw_wire =
            String::from_redis_value(records.ids[1].map.get("wire").ok_or("quarantine record omitted wire")?)?;
        assert_eq!(raw_wire, "not-json");
        let pending: Vec<Value> = cmd("XPENDING")
            .arg(&stream)
            .arg(group)
            .arg("-")
            .arg("+")
            .arg(10)
            .query(&mut verify)?;
        assert!(pending.is_empty());
        let ReceiveOutcome::Message(valid) = receiver.receive(Duration::from_secs(2)).await? else {
            return Err("valid message after poison records was not received".into());
        };
        assert_eq!(valid.id().as_str(), "async-after-poison");
        Ok::<(), Box<dyn Error>>(())
    })
}

#[cfg(feature = "async")]
#[test]
fn test_async_unknown_wire_version_stays_pending() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let server = RedisServer::start()?;
        let namespace = "unsupported-version-async";
        let bus = async_bus(&server, namespace, 2).await?;
        let stream = stream_key(namespace, "events");
        let unsupported = r#"{"version":2,"future_field":"kept by newer consumers"}"#;
        let mut connection = Client::open(server.url())?.get_connection()?;
        let message_id: String = cmd("XADD")
            .arg(&stream)
            .arg("*")
            .arg("wire")
            .arg(unsupported)
            .query(&mut connection)?;
        let mut receiver = bus
            .subscribe(request(
                "events",
                "version-worker",
                "version-group",
                SubscriptionDurability::Durable,
            )?)
            .await?;

        let error = match receiver.receive(Duration::from_secs(2)).await {
            Ok(_) => return Err("unsupported wire version was not rejected".into()),
            Err(error) => error,
        };
        assert_eq!(error.kind(), "unsupported_wire_version");
        assert_eq!(error.retryable(), Some(false));
        let pending: Vec<Value> = cmd("XPENDING")
            .arg(&stream)
            .arg(group_name(namespace, "events", "version-worker", Some("version-group")))
            .arg(&message_id)
            .arg(&message_id)
            .arg(1)
            .query(&mut connection)?;
        assert_eq!(pending.len(), 1);
        let quarantined: usize = cmd("XLEN")
            .arg(poison_key(
                namespace,
                "events",
                &group_name(namespace, "events", "version-worker", Some("version-group")),
            ))
            .query(&mut connection)?;
        assert_eq!(quarantined, 0);
        Ok::<(), Box<dyn Error>>(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_ephemeral_is_rejected_without_group() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = sync_bus(&server, "ephemeral-sync", 2)?;
    let key = stream_key("ephemeral-sync", "ephemeral-topic");
    let Err(error) = bus.subscribe(request(
        "ephemeral-topic",
        "sync-ephemeral-worker",
        "sync-ephemeral-group",
        SubscriptionDurability::Ephemeral,
    )?) else {
        return Err("ephemeral subscription unexpectedly succeeded".into());
    };
    assert!(matches!(
        error,
        SpiError::Operation {
            kind: "unsupported_subscription_durability",
            retryable: Some(false),
            ..
        }
    ));
    let mut connection = Client::open(server.url())?.get_connection()?;
    let exists: bool = cmd("EXISTS").arg(key).query(&mut connection)?;
    assert!(!exists, "rejected ephemeral subscription created a stream");
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_ephemeral_is_rejected_without_group() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus(&server, "ephemeral-async", 2).await?;
        let key = stream_key("ephemeral-async", "ephemeral-topic");
        let Err(error) = bus
            .subscribe(request(
                "ephemeral-topic",
                "async-ephemeral-worker",
                "async-ephemeral-group",
                SubscriptionDurability::Ephemeral,
            )?)
            .await
        else {
            return Err("ephemeral subscription unexpectedly succeeded".into());
        };
        assert!(matches!(
            error,
            SpiError::Operation {
                kind: "unsupported_subscription_durability",
                retryable: Some(false),
                ..
            }
        ));
        let mut connection = Client::open(server.url())?.get_connection()?;
        let exists: bool = cmd("EXISTS").arg(key).query(&mut connection)?;
        assert!(!exists, "rejected ephemeral subscription created a stream");
        Ok::<(), Box<dyn Error>>(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_empty_receive_observes_zero_and_bounded_timeouts() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = sync_bus(&server, "timeout-sync", 2)?;
    let mut receiver = bus.subscribe(request(
        "events",
        "timeout-worker",
        "timeout-group",
        SubscriptionDurability::Durable,
    )?)?;
    assert!(matches!(receiver.receive(Duration::ZERO)?, ReceiveOutcome::TimedOut));
    assert!(matches!(
        receiver.receive(Duration::from_millis(20))?,
        ReceiveOutcome::TimedOut
    ));
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_empty_receive_observes_zero_and_bounded_timeouts() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus(&server, "timeout-async", 2).await?;
        let mut receiver = bus
            .subscribe(request(
                "events",
                "timeout-worker",
                "timeout-group",
                SubscriptionDurability::Durable,
            )?)
            .await?;
        assert!(matches!(
            receiver.receive(Duration::ZERO).await?,
            ReceiveOutcome::TimedOut
        ));
        assert!(matches!(
            receiver.receive(Duration::from_millis(20)).await?,
            ReceiveOutcome::TimedOut
        ));
        Ok::<(), Box<dyn Error>>(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_zero_timeout_progresses_past_active_pending_records() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = sync_bus(&server, "scan-budget-sync", 64)?;
    for index in 0..24 {
        let _ = bus.publish(event("events", &format!("pending-{index}"), b"payload")?)?;
    }
    let mut receiver = bus.subscribe(request(
        "events",
        "scan-worker",
        "scan-group",
        SubscriptionDurability::Durable,
    )?)?;
    for _ in 0..24 {
        assert!(matches!(
            receiver.receive(Duration::from_secs(2))?,
            ReceiveOutcome::Message(_)
        ));
    }
    let _ = bus.publish(event("events", "after-pending", b"payload")?)?;
    let mut found = false;
    for _ in 0..24 {
        if let ReceiveOutcome::Message(message) = receiver.receive(Duration::ZERO)? {
            assert_eq!(message.id().as_str(), "after-pending");
            found = true;
            break;
        }
    }
    assert!(found, "zero-timeout scans did not progress to a new message");
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_zero_timeout_progresses_past_active_pending_records() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus(&server, "scan-budget-async", 64).await?;
        for index in 0..24 {
            let _ = bus
                .publish(event("events", &format!("pending-{index}"), b"payload")?)
                .await?;
        }
        let mut receiver = bus
            .subscribe(request(
                "events",
                "scan-worker",
                "scan-group",
                SubscriptionDurability::Durable,
            )?)
            .await?;
        for _ in 0..24 {
            assert!(matches!(
                receiver.receive(Duration::from_secs(2)).await?,
                ReceiveOutcome::Message(_)
            ));
        }
        let _ = bus.publish(event("events", "after-pending", b"payload")?).await?;
        let mut found = false;
        for _ in 0..24 {
            if let ReceiveOutcome::Message(message) = receiver.receive(Duration::ZERO).await? {
                assert_eq!(message.id().as_str(), "after-pending");
                found = true;
                break;
            }
        }
        assert!(found, "zero-timeout scans did not progress to a new message");
        Ok::<(), Box<dyn Error>>(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_duration_max_waits_for_a_message() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = sync_bus(&server, "timeout-max-sync", 2)?;
    let publisher = Arc::clone(&bus);
    let message = event("events", "max-timeout-event", b"payload")?;
    let thread = spawn(move || {
        sleep(Duration::from_millis(50));
        publisher.publish(message)
    });
    let mut receiver = bus.subscribe(request(
        "events",
        "max-timeout-worker",
        "max-timeout-group",
        SubscriptionDurability::Durable,
    )?)?;
    let ReceiveOutcome::Message(message) = receiver.receive(Duration::MAX)? else {
        return Err("Duration::MAX receive returned before message arrival".into());
    };
    assert_eq!(message.id().as_str(), "max-timeout-event");
    let _ = thread.join().map_err(|_| "publisher thread panicked")??;
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_long_receive_reclaims_after_idle_threshold_without_new_messages() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = sync_bus_with_claim(&server, "late-claim-sync", 2, 100)?;
    let _ = bus.publish(event("events", "late-claim-sync", b"payload")?)?;
    let mut first = bus.subscribe(request(
        "events",
        "first-worker",
        "late-claim-group",
        SubscriptionDurability::Durable,
    )?)?;
    let ReceiveOutcome::Message(first_message) = first.receive(Duration::from_secs(1))? else {
        return Err("first consumer did not receive the message".into());
    };
    assert_eq!(first_message.id().as_str(), "late-claim-sync");
    drop(first);

    let mut second = bus.subscribe(request(
        "events",
        "second-worker",
        "late-claim-group",
        SubscriptionDurability::Durable,
    )?)?;
    let ReceiveOutcome::Message(recovered) = second.receive(Duration::from_secs(3))? else {
        return Err("one long receive did not recover the idle pending message".into());
    };
    assert_eq!(recovered.id().as_str(), "late-claim-sync");
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_long_receive_reclaims_after_idle_threshold_without_new_messages() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus_with_claim(&server, "late-claim-async", 2, 100).await?;
        let _ = bus.publish(event("events", "late-claim-async", b"payload")?).await?;
        let mut first = bus
            .subscribe(request(
                "events",
                "first-worker",
                "late-claim-group",
                SubscriptionDurability::Durable,
            )?)
            .await?;
        let ReceiveOutcome::Message(first_message) = first.receive(Duration::from_secs(1)).await? else {
            return Err("first consumer did not receive the message".into());
        };
        assert_eq!(first_message.id().as_str(), "late-claim-async");
        drop(first);

        let mut second = bus
            .subscribe(request(
                "events",
                "second-worker",
                "late-claim-group",
                SubscriptionDurability::Durable,
            )?)
            .await?;
        let ReceiveOutcome::Message(recovered) = second.receive(Duration::from_secs(3)).await? else {
            return Err("one long receive did not recover the idle pending message".into());
        };
        assert_eq!(recovered.id().as_str(), "late-claim-async");
        Ok::<(), Box<dyn Error>>(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_deleted_pending_entry_is_cleared_on_redis_6_2_and_7() -> Result<(), Box<dyn Error>> {
    for image in ["6.2-alpine", "7-alpine"] {
        let server = RedisServer::start_version(image)?;
        let namespace = format!("deleted-sync-{image}");
        let bus = sync_bus(&server, &namespace, 2)?;
        let key = stream_key(&namespace, "events");
        let group = group_name(&namespace, "events", "deleted-worker", Some("deleted-group"));
        let mut connection = Client::open(server.url())?.get_connection()?;
        let _ = bus.publish(event("events", "deleted-sync", b"payload")?)?;
        let entries: StreamRangeReply = cmd("XRANGE")
            .arg(&key)
            .arg("-")
            .arg("+")
            .arg("COUNT")
            .arg(1)
            .query(&mut connection)?;
        let redis_id = entries
            .ids
            .into_iter()
            .next()
            .ok_or("published stream entry missing")?
            .id;
        let mut receiver = bus.subscribe(request(
            "events",
            "deleted-worker",
            "deleted-group",
            SubscriptionDurability::Durable,
        )?)?;
        let ReceiveOutcome::Message(message) = receiver.receive(Duration::from_secs(2))? else {
            return Err("pending test message was not received".into());
        };
        let token = message.settlement().ok_or("message has no settlement token")?;
        receiver.settle(token, DeliveryDisposition::Retry)?;

        let deleted: usize = cmd("XDEL").arg(&key).arg(redis_id).query(&mut connection)?;
        assert_eq!(deleted, 1);
        assert!(matches!(receiver.receive(Duration::ZERO)?, ReceiveOutcome::Gap(_)));
        assert_eq!(snapshot(&namespace).delivery_gaps(), 1);
        assert_eq!(snapshot(&namespace).quarantine_succeeded(), 0);
        assert!(matches!(receiver.receive(Duration::ZERO)?, ReceiveOutcome::TimedOut));
        let pending: Vec<Value> = cmd("XPENDING")
            .arg(&key)
            .arg(&group)
            .arg("-")
            .arg("+")
            .arg(10)
            .query(&mut connection)?;
        assert!(pending.is_empty(), "{image} retained a tombstone in the PEL");
    }
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_deleted_pending_entry_is_cleared_on_redis_6_2_and_7() -> Result<(), Box<dyn Error>> {
    block_on(async {
        for image in ["6.2-alpine", "7-alpine"] {
            let server = RedisServer::start_version(image)?;
            let namespace = format!("deleted-async-{image}");
            let bus = async_bus(&server, &namespace, 2).await?;
            let key = stream_key(&namespace, "events");
            let group = group_name(&namespace, "events", "deleted-worker", Some("deleted-group"));
            let mut connection = Client::open(server.url())?.get_connection()?;
            let _ = bus.publish(event("events", "deleted-async", b"payload")?).await?;
            let entries: StreamRangeReply = cmd("XRANGE")
                .arg(&key)
                .arg("-")
                .arg("+")
                .arg("COUNT")
                .arg(1)
                .query(&mut connection)?;
            let redis_id = entries
                .ids
                .into_iter()
                .next()
                .ok_or("published stream entry missing")?
                .id;
            let mut receiver = bus
                .subscribe(request(
                    "events",
                    "deleted-worker",
                    "deleted-group",
                    SubscriptionDurability::Durable,
                )?)
                .await?;
            let ReceiveOutcome::Message(message) = receiver.receive(Duration::from_secs(2)).await? else {
                return Err("pending test message was not received".into());
            };
            let token = message.settlement().ok_or("message has no settlement token")?;
            receiver.settle(token, DeliveryDisposition::Retry).await?;

            let deleted: usize = cmd("XDEL").arg(&key).arg(redis_id).query(&mut connection)?;
            assert_eq!(deleted, 1);
            assert!(matches!(
                receiver.receive(Duration::ZERO).await?,
                ReceiveOutcome::Gap(_)
            ));
            assert_eq!(snapshot(&namespace).delivery_gaps(), 1);
            assert_eq!(snapshot(&namespace).quarantine_succeeded(), 0);
            assert!(matches!(
                receiver.receive(Duration::ZERO).await?,
                ReceiveOutcome::TimedOut
            ));
            let pending: Vec<Value> = cmd("XPENDING")
                .arg(&key)
                .arg(&group)
                .arg("-")
                .arg("+")
                .arg(10)
                .query(&mut connection)?;
            assert!(pending.is_empty(), "{image} retained a tombstone in the PEL");
        }
        Ok::<(), Box<dyn Error>>(())
    })
}

#[cfg(feature = "async")]
#[test]
fn test_async_duration_max_waits_for_a_message() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus(&server, "timeout-max-async", 2).await?;
        let publisher = Arc::clone(&bus);
        let outbound = event("events", "max-timeout-event", b"payload")?;
        let thread = spawn(move || {
            sleep(Duration::from_millis(50));
            block_on(publisher.publish(outbound))
        });
        let mut receiver = bus
            .subscribe(request(
                "events",
                "max-timeout-worker",
                "max-timeout-group",
                SubscriptionDurability::Durable,
            )?)
            .await?;
        let ReceiveOutcome::Message(message) = receiver.receive(Duration::MAX).await? else {
            return Err("Duration::MAX receive returned before message arrival".into());
        };
        assert_eq!(message.id().as_str(), "max-timeout-event");
        let _ = thread.join().map_err(|_| "publisher thread panicked")??;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_xack_failure_keeps_token_retryable_and_slot_occupied() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = sync_bus(&server, "settle-error-sync", 1)?;
    let key = stream_key("settle-error-sync", "events");
    let _ = bus.publish(event("events", "settle-error-sync", b"payload")?)?;
    let mut receiver = bus.subscribe(request(
        "events",
        "settle-error-worker",
        "settle-error-group",
        SubscriptionDurability::Durable,
    )?)?;
    let ReceiveOutcome::Message(message) = receiver.receive(Duration::from_secs(2))? else {
        return Err("message missing".into());
    };
    let token = message.settlement().ok_or("settlement token missing")?;
    let mut connection = Client::open(server.url())?.get_connection()?;
    let _: String = cmd("SET").arg(key).arg("wrong-type").query(&mut connection)?;
    assert!(receiver.settle(token, DeliveryDisposition::Accept).is_err());
    assert!(receiver.settle(token, DeliveryDisposition::Accept).is_err());
    assert!(matches!(receiver.receive(Duration::ZERO)?, ReceiveOutcome::TimedOut));
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_xack_failure_keeps_token_retryable_and_slot_occupied() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus(&server, "settle-error-async", 1).await?;
        let key = stream_key("settle-error-async", "events");
        let _ = bus.publish(event("events", "settle-error-async", b"payload")?).await?;
        let mut receiver = bus
            .subscribe(request(
                "events",
                "settle-error-worker",
                "settle-error-group",
                SubscriptionDurability::Durable,
            )?)
            .await?;
        let ReceiveOutcome::Message(message) = receiver.receive(Duration::from_secs(2)).await? else {
            return Err("message missing".into());
        };
        let token = message.settlement().ok_or("settlement token missing")?;
        let mut connection = Client::open(server.url())?.get_connection()?;
        let _: String = cmd("SET").arg(key).arg("wrong-type").query(&mut connection)?;
        assert!(receiver.settle(token, DeliveryDisposition::Accept).await.is_err());
        assert!(receiver.settle(token, DeliveryDisposition::Accept).await.is_err());
        assert!(matches!(
            receiver.receive(Duration::ZERO).await?,
            ReceiveOutcome::TimedOut
        ));
        Ok::<(), Box<dyn Error>>(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_removed_group_error_has_unknown_retryability() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = sync_bus(&server, "claim-error-sync", 2)?;
    let key = stream_key("claim-error-sync", "events");
    let group = group_name("claim-error-sync", "events", "claim-worker", Some("claim-group"));
    let mut receiver = bus.subscribe(request(
        "events",
        "claim-worker",
        "claim-group",
        SubscriptionDurability::Durable,
    )?)?;
    let mut connection = Client::open(server.url())?.get_connection()?;
    let _: usize = cmd("XGROUP")
        .arg("DESTROY")
        .arg(key)
        .arg(group)
        .query(&mut connection)?;
    let error = match receiver.receive(Duration::ZERO) {
        Ok(_) => return Err("removed group should fail XAUTOCLAIM".into()),
        Err(error) => error,
    };
    assert!(matches!(error, SpiError::Operation { retryable: None, .. }));
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_removed_group_error_has_unknown_retryability() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus(&server, "claim-error-async", 2).await?;
        let key = stream_key("claim-error-async", "events");
        let group = group_name("claim-error-async", "events", "claim-worker", Some("claim-group"));
        let mut receiver = bus
            .subscribe(request(
                "events",
                "claim-worker",
                "claim-group",
                SubscriptionDurability::Durable,
            )?)
            .await?;
        let mut connection = Client::open(server.url())?.get_connection()?;
        let _: usize = cmd("XGROUP")
            .arg("DESTROY")
            .arg(key)
            .arg(group)
            .query(&mut connection)?;
        let error = match receiver.receive(Duration::ZERO).await {
            Ok(_) => return Err("removed group should fail XAUTOCLAIM".into()),
            Err(error) => error,
        };
        assert!(matches!(error, SpiError::Operation { retryable: None, .. }));
        Ok::<(), Box<dyn Error>>(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_receive_and_settle_unknown_count_once() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let proxy = ControlledRedis::start(server.url())?;
    let namespace = "outcome-metrics-sync";
    let options: ProviderOptions = [
        ("redis.url".into(), proxy.url()),
        ("redis.namespace".into(), namespace.into()),
        ("redis.claim_min_idle_ms".into(), "0".into()),
    ]
    .into();
    let bus = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(options))
        .map_err(|failure| failure.into_error())?;
    let _ = bus.publish(event("events", "unknown-sync", b"payload")?)?;
    let mut receiver = bus.subscribe(request("events", "unknown-worker", "unknown-group", SubscriptionDurability::Durable)?)?;
    proxy.replace_next_reply("XREADGROUP", b"+OK\r\n");
    let error = match receiver.receive(Duration::from_secs(2)) {
        Err(error) => error,
        Ok(_) => return Err("malformed read reply was accepted".into()),
    };
    assert_eq!(error.kind(), "outcome_unknown");
    assert_eq!(snapshot(namespace).receive_unknown(), 1);
    assert_eq!(snapshot(namespace).settlement_unknown(), 0);

    let ReceiveOutcome::Message(message) = receiver.receive(Duration::from_secs(2))? else {
        return Err("pending message not recovered".into());
    };
    let token = message.settlement().ok_or("settlement token missing")?;
    proxy.replace_next_reply("XACK", b"+OK\r\n");
    let error = receiver.settle(token, DeliveryDisposition::Accept).expect_err("malformed ACK reply");
    assert_eq!(error.kind(), "outcome_unknown");
    assert_eq!(snapshot(namespace).receive_unknown(), 1);
    assert_eq!(snapshot(namespace).settlement_unknown(), 1);
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_receive_and_settle_unknown_count_once() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let server = RedisServer::start()?;
        let proxy = ControlledRedis::start(server.url())?;
        let namespace = "outcome-metrics-async";
        let options: ProviderOptions = [
            ("redis.url".into(), proxy.url()),
            ("redis.namespace".into(), namespace.into()),
            ("redis.claim_min_idle_ms".into(), "0".into()),
        ]
        .into();
        let bus = AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(options))
            .await
            .map_err(|failure| failure.into_error())?;
        let _ = bus.publish(event("events", "unknown-async", b"payload")?).await?;
        let mut receiver = bus.subscribe(request("events", "unknown-worker", "unknown-group", SubscriptionDurability::Durable)?).await?;
        proxy.replace_next_reply("XREADGROUP", b"+OK\r\n");
        let error = match receiver.receive(Duration::from_secs(2)).await {
            Err(error) => error,
            Ok(_) => return Err("malformed read reply was accepted".into()),
        };
        assert_eq!(error.kind(), "outcome_unknown");
        assert_eq!(snapshot(namespace).receive_unknown(), 1);
        assert_eq!(snapshot(namespace).settlement_unknown(), 0);

        let ReceiveOutcome::Message(message) = receiver.receive(Duration::from_secs(2)).await? else {
            return Err("pending message not recovered".into());
        };
        let token = message.settlement().ok_or("settlement token missing")?;
        proxy.replace_next_reply("XACK", b"+OK\r\n");
        let error = receiver.settle(token, DeliveryDisposition::Accept).await.expect_err("malformed ACK reply");
        assert_eq!(error.kind(), "outcome_unknown");
        assert_eq!(snapshot(namespace).receive_unknown(), 1);
        assert_eq!(snapshot(namespace).settlement_unknown(), 1);
        Ok::<(), Box<dyn Error>>(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_drops_reader_connection_after_redis_receive_error() -> Result<(), Box<dyn Error>> {
    let mut server = RedisServer::start()?;
    let bus = sync_bus(&server, "read-error-sync", 2)?;
    let mut receiver = bus.subscribe(request(
        "events",
        "read-error-worker",
        "read-error-group",
        SubscriptionDurability::Durable,
    )?)?;
    let key = stream_key("read-error-sync", "events");
    let mut connection = Client::open(server.url())?.get_connection()?;
    let _: String = cmd("SET").arg(&key).arg("wrong-type").query(&mut connection)?;
    let error = match receiver.receive(Duration::ZERO) {
        Ok(_) => return Err("wrong-type stream should fail receive".into()),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        SpiError::Operation {
            retryable: Some(false),
            ..
        }
    ));
    let mut shutdown_connection = Client::open(server.url())?.get_connection()?;
    let _: RedisResult<()> = cmd("SHUTDOWN").arg("NOSAVE").query(&mut shutdown_connection);
    let error = match receiver.receive(Duration::ZERO) {
        Ok(_) => return Err("stopped Redis server should fail receive".into()),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        SpiError::Operation {
            retryable: Some(true),
            ..
        }
    ));
    server.restart()?;
    let key = stream_key("read-error-sync", "events");
    let group = group_name(
        "read-error-sync",
        "events",
        "read-error-worker",
        Some("read-error-group"),
    );
    let mut connection = Client::open(server.url())?.get_connection()?;
    let _: usize = cmd("DEL").arg(&key).query(&mut connection)?;
    let _: String = cmd("XGROUP")
        .arg("CREATE")
        .arg(key)
        .arg(group)
        .arg("0")
        .arg("MKSTREAM")
        .query(&mut connection)?;
    assert!(matches!(receiver.receive(Duration::ZERO)?, ReceiveOutcome::TimedOut));
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_drops_reader_connection_after_redis_receive_error() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let mut server = RedisServer::start()?;
        let bus = async_bus(&server, "read-error-async", 2).await?;
        let mut receiver = bus
            .subscribe(request(
                "events",
                "read-error-worker",
                "read-error-group",
                SubscriptionDurability::Durable,
            )?)
            .await?;
        let key = stream_key("read-error-async", "events");
        let mut connection = Client::open(server.url())?.get_connection()?;
        let _: String = cmd("SET").arg(&key).arg("wrong-type").query(&mut connection)?;
        let error = match receiver.receive(Duration::ZERO).await {
            Ok(_) => return Err("wrong-type stream should fail receive".into()),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            SpiError::Operation {
                retryable: Some(false),
                ..
            }
        ));
        let mut shutdown_connection = Client::open(server.url())?.get_connection()?;
        let _: RedisResult<()> = cmd("SHUTDOWN").arg("NOSAVE").query(&mut shutdown_connection);
        let error = match receiver.receive(Duration::ZERO).await {
            Ok(_) => return Err("stopped Redis server should fail receive".into()),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            SpiError::Operation {
                retryable: Some(true),
                ..
            }
        ));
        server.restart()?;
        let key = stream_key("read-error-async", "events");
        let group = group_name(
            "read-error-async",
            "events",
            "read-error-worker",
            Some("read-error-group"),
        );
        let mut connection = Client::open(server.url())?.get_connection()?;
        let _: usize = cmd("DEL").arg(&key).query(&mut connection)?;
        let _: String = cmd("XGROUP")
            .arg("CREATE")
            .arg(key)
            .arg(group)
            .arg("0")
            .arg("MKSTREAM")
            .query(&mut connection)?;
        assert!(matches!(
            receiver.receive(Duration::ZERO).await?,
            ReceiveOutcome::TimedOut
        ));
        Ok::<(), Box<dyn Error>>(())
    })
}
