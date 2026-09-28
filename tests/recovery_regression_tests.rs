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
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;

#[cfg(feature = "async")]
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_event_bus_redis::naming::group_name;
use qubit_event_bus_redis::naming::poison_key;
use qubit_event_bus_redis::naming::stream_key;
use qubit_id::Id;
use redis::Client;
use redis::FromRedisValue;
use redis::cmd;
use support::redis_server::RedisServer;

static SUBSCRIPTION_IDS: AtomicU64 = AtomicU64::new(10_000);

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

fn event(topic: &str, id: &str, bytes: &[u8]) -> Result<OutboundMessage, Box<dyn std::error::Error>> {
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

fn request(
    topic: &str,
    subscriber: &str,
    group: &str,
    durability: SubscriptionDurability,
) -> Result<SpiSubscriptionRequest, Box<dyn std::error::Error>> {
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

#[cfg(feature = "sync")]
fn sync_bus(
    server: &RedisServer,
    namespace: &str,
    max_unsettled: usize,
) -> Result<Arc<dyn qubit_event_bus::spi::EventBusSpi>, Box<dyn std::error::Error>> {
    use qubit_event_bus::EventBusConfig;
    use qubit_event_bus_redis::sync::RedisEventBusProvider;
    use qubit_spi::ServiceProvider;

    RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(provider_options(
            server,
            namespace,
            max_unsettled,
        )))
        .map_err(|failure| failure.into_error())
        .map_err(Into::into)
}

#[cfg(feature = "async")]
async fn async_bus(
    server: &RedisServer,
    namespace: &str,
    max_unsettled: usize,
) -> Result<Arc<dyn qubit_event_bus::spi::AsyncEventBusSpi>, Box<dyn std::error::Error>> {
    use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
    use qubit_spi::AsyncServiceProvider;

    AsyncRedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(provider_options(
            server,
            namespace,
            max_unsettled,
        )))
        .await
        .map_err(|failure| failure.into_error())
        .map_err(Into::into)
}

#[cfg(feature = "sync")]
#[test]
fn sync_skips_unsettled_id_and_delivers_next() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = sync_bus(&server, "recovery-sync", 2)?;
    bus.publish(event("events", "sync-first", b"first")?)?;
    bus.publish(event("events", "sync-second", b"second")?)?;
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
fn sync_retry_releases_active_id_for_redelivery() -> Result<(), Box<dyn std::error::Error>> {
    use qubit_event_bus::spi::DeliveryDisposition;
    let server = RedisServer::start()?;
    let bus = sync_bus(&server, "retry-sync", 2)?;
    bus.publish(event("events", "retry-first", b"first")?)?;
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
fn sync_rejects_foreign_settlement_and_is_idempotent_after_close() -> Result<(), Box<dyn std::error::Error>> {
    use qubit_event_bus::spi::DeliveryDisposition;
    let server = RedisServer::start()?;
    let bus = sync_bus(&server, "token-sync", 2)?;
    bus.publish(event("events", "token-event", b"payload")?)?;
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
fn async_retry_releases_active_id_for_redelivery() -> Result<(), Box<dyn std::error::Error>> {
    use qubit_event_bus::spi::DeliveryDisposition;
    futures_lite::future::block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus(&server, "retry-async", 2).await?;
        bus.publish(event("events", "retry-first-async", b"first")?).await?;
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
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[cfg(feature = "async")]
#[test]
fn async_rejects_foreign_settlement_and_is_idempotent_after_close() -> Result<(), Box<dyn std::error::Error>> {
    use qubit_event_bus::spi::DeliveryDisposition;
    futures_lite::future::block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus(&server, "token-async", 2).await?;
        bus.publish(event("events", "token-event-async", b"payload")?).await?;
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
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[cfg(feature = "async")]
#[test]
fn async_reuses_standalone_short_command_connection() -> Result<(), Box<dyn std::error::Error>> {
    futures_lite::future::block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus(&server, "connection-reuse", 2).await?;
        let mut observer = Client::open(server.url())?.get_connection()?;
        let before = total_connections(&mut observer)?;
        for index in 0..10 {
            bus.publish(event("events", &format!("connection-{index}"), b"payload")?)
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
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn sync_reuses_standalone_short_command_connection() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = sync_bus(&server, "connection-reuse-sync", 2)?;
    let mut observer = Client::open(server.url())?.get_connection()?;
    let before = total_connections(&mut observer)?;
    for index in 0..10 {
        bus.publish(event("events", &format!("connection-{index}"), b"payload")?)?;
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
fn sync_reuses_subscription_read_connection_across_timeouts() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = sync_bus(&server, "read-connection-sync", 2)?;
    let mut receiver = bus.subscribe(request(
        "events",
        "read-worker",
        "read-group",
        SubscriptionDurability::Durable,
    )?)?;
    let mut observer = Client::open(server.url())?.get_connection()?;
    let before = total_connections(&mut observer)?;
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
        after - before,
        1,
        "two completed receives should share one dedicated connection"
    );
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn async_reuses_subscription_read_connection_across_timeouts() -> Result<(), Box<dyn std::error::Error>> {
    futures_lite::future::block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus(&server, "read-connection-async", 2).await?;
        let mut receiver = bus
            .subscribe(request(
                "events",
                "read-worker",
                "read-group",
                SubscriptionDurability::Durable,
            )?)
            .await?;
        let mut observer = Client::open(server.url())?.get_connection()?;
        let before = total_connections(&mut observer)?;
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
            after - before,
            1,
            "two completed receives should share one dedicated connection"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

fn total_connections(connection: &mut redis::Connection) -> Result<u64, Box<dyn std::error::Error>> {
    let info: String = cmd("INFO").arg("stats").query(connection)?;
    let value = info
        .lines()
        .find_map(|line| line.strip_prefix("total_connections_received:"))
        .ok_or("Redis INFO stats omitted total_connections_received")?;
    Ok(value.parse()?)
}

fn insert_poison_fixtures(connection: &mut redis::Connection, stream: &str) -> Result<(), Box<dyn std::error::Error>> {
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
fn async_skips_unsettled_id_and_delivers_next() -> Result<(), Box<dyn std::error::Error>> {
    futures_lite::future::block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus(&server, "recovery-async", 2).await?;
        bus.publish(event("events", "async-first", b"first")?).await?;
        bus.publish(event("events", "async-second", b"second")?).await?;
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
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn sync_poison_does_not_block_next() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = sync_bus(&server, "poison-sync", 2)?;
    let stream = stream_key("poison-sync", "events");
    let mut connection = Client::open(server.url())?.get_connection()?;
    insert_poison_fixtures(&mut connection, &stream)?;
    bus.publish(event("events", "sync-after-poison", b"valid")?)?;
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
    let records: redis::streams::StreamRangeReply = cmd("XRANGE")
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
    let pending: Vec<redis::Value> = cmd("XPENDING")
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
fn sync_unknown_wire_version_stays_pending() -> Result<(), Box<dyn std::error::Error>> {
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
    let pending: Vec<redis::Value> = cmd("XPENDING")
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
fn sync_quarantine_failure_keeps_source_pending() -> Result<(), Box<dyn std::error::Error>> {
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
        qubit_event_bus::error::SpiError::Operation {
            retryable: Some(true),
            ..
        }
    ));
    let pending: Vec<redis::Value> = cmd("XPENDING")
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
fn async_quarantine_failure_keeps_source_pending() -> Result<(), Box<dyn std::error::Error>> {
    futures_lite::future::block_on(async {
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
            qubit_event_bus::error::SpiError::Operation {
                retryable: Some(true),
                ..
            }
        ));
        let pending: Vec<redis::Value> = cmd("XPENDING")
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
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[cfg(feature = "async")]
#[test]
fn async_poison_does_not_block_next() -> Result<(), Box<dyn std::error::Error>> {
    futures_lite::future::block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus(&server, "poison-async", 2).await?;
        let stream = stream_key("poison-async", "events");
        let mut connection = Client::open(server.url())?.get_connection()?;
        insert_poison_fixtures(&mut connection, &stream)?;
        bus.publish(event("events", "async-after-poison", b"valid")?).await?;
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
        let records: redis::streams::StreamRangeReply = cmd("XRANGE")
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
        let pending: Vec<redis::Value> = cmd("XPENDING")
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
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[cfg(feature = "async")]
#[test]
fn async_unknown_wire_version_stays_pending() -> Result<(), Box<dyn std::error::Error>> {
    futures_lite::future::block_on(async {
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
        let pending: Vec<redis::Value> = cmd("XPENDING")
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
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn sync_ephemeral_is_rejected_without_group() -> Result<(), Box<dyn std::error::Error>> {
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
        qubit_event_bus::error::SpiError::Operation {
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
fn async_ephemeral_is_rejected_without_group() -> Result<(), Box<dyn std::error::Error>> {
    futures_lite::future::block_on(async {
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
            qubit_event_bus::error::SpiError::Operation {
                kind: "unsupported_subscription_durability",
                retryable: Some(false),
                ..
            }
        ));
        let mut connection = Client::open(server.url())?.get_connection()?;
        let exists: bool = cmd("EXISTS").arg(key).query(&mut connection)?;
        assert!(!exists, "rejected ephemeral subscription created a stream");
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn sync_empty_receive_observes_zero_and_bounded_timeouts() -> Result<(), Box<dyn std::error::Error>> {
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
fn async_empty_receive_observes_zero_and_bounded_timeouts() -> Result<(), Box<dyn std::error::Error>> {
    futures_lite::future::block_on(async {
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
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn sync_zero_timeout_progresses_past_active_pending_records() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = sync_bus(&server, "scan-budget-sync", 64)?;
    for index in 0..24 {
        bus.publish(event("events", &format!("pending-{index}"), b"payload")?)?;
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
    bus.publish(event("events", "after-pending", b"payload")?)?;
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
fn async_zero_timeout_progresses_past_active_pending_records() -> Result<(), Box<dyn std::error::Error>> {
    futures_lite::future::block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus(&server, "scan-budget-async", 64).await?;
        for index in 0..24 {
            bus.publish(event("events", &format!("pending-{index}"), b"payload")?)
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
        bus.publish(event("events", "after-pending", b"payload")?).await?;
        let mut found = false;
        for _ in 0..24 {
            if let ReceiveOutcome::Message(message) = receiver.receive(Duration::ZERO).await? {
                assert_eq!(message.id().as_str(), "after-pending");
                found = true;
                break;
            }
        }
        assert!(found, "zero-timeout scans did not progress to a new message");
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn sync_duration_max_waits_for_a_message() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = sync_bus(&server, "timeout-max-sync", 2)?;
    let publisher = Arc::clone(&bus);
    let message = event("events", "max-timeout-event", b"payload")?;
    let thread = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
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
    thread.join().map_err(|_| "publisher thread panicked")??;
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn async_duration_max_waits_for_a_message() -> Result<(), Box<dyn std::error::Error>> {
    futures_lite::future::block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus(&server, "timeout-max-async", 2).await?;
        let publisher = Arc::clone(&bus);
        let outbound = event("events", "max-timeout-event", b"payload")?;
        let thread = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            futures_lite::future::block_on(publisher.publish(outbound))
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
        thread.join().map_err(|_| "publisher thread panicked")??;
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn sync_xack_failure_keeps_token_retryable_and_slot_occupied() -> Result<(), Box<dyn std::error::Error>> {
    use qubit_event_bus::spi::DeliveryDisposition;
    let server = RedisServer::start()?;
    let bus = sync_bus(&server, "settle-error-sync", 1)?;
    let key = stream_key("settle-error-sync", "events");
    bus.publish(event("events", "settle-error-sync", b"payload")?)?;
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
fn async_xack_failure_keeps_token_retryable_and_slot_occupied() -> Result<(), Box<dyn std::error::Error>> {
    use qubit_event_bus::spi::DeliveryDisposition;
    futures_lite::future::block_on(async {
        let server = RedisServer::start()?;
        let bus = async_bus(&server, "settle-error-async", 1).await?;
        let key = stream_key("settle-error-async", "events");
        bus.publish(event("events", "settle-error-async", b"payload")?).await?;
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
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn sync_claim_command_failure_is_reported_as_retryable() -> Result<(), Box<dyn std::error::Error>> {
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
    assert!(matches!(
        error,
        qubit_event_bus::error::SpiError::Operation {
            retryable: Some(true),
            ..
        }
    ));
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn async_claim_command_failure_is_reported_as_retryable() -> Result<(), Box<dyn std::error::Error>> {
    futures_lite::future::block_on(async {
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
        assert!(matches!(
            error,
            qubit_event_bus::error::SpiError::Operation {
                retryable: Some(true),
                ..
            }
        ));
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn sync_drops_reader_connection_after_redis_receive_error() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
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
        qubit_event_bus::error::SpiError::Operation {
            retryable: Some(true),
            ..
        }
    ));
    let mut shutdown_connection = Client::open(server.url())?.get_connection()?;
    let _: redis::RedisResult<()> = cmd("SHUTDOWN").arg("NOSAVE").query(&mut shutdown_connection);
    let error = match receiver.receive(Duration::ZERO) {
        Ok(_) => return Err("stopped Redis server should fail receive".into()),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        qubit_event_bus::error::SpiError::Operation {
            retryable: Some(true),
            ..
        }
    ));
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn async_drops_reader_connection_after_redis_receive_error() -> Result<(), Box<dyn std::error::Error>> {
    futures_lite::future::block_on(async {
        let server = RedisServer::start()?;
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
            qubit_event_bus::error::SpiError::Operation {
                retryable: Some(true),
                ..
            }
        ));
        let mut shutdown_connection = Client::open(server.url())?.get_connection()?;
        let _: redis::RedisResult<()> = cmd("SHUTDOWN").arg("NOSAVE").query(&mut shutdown_connection);
        let error = match receiver.receive(Duration::ZERO).await {
            Ok(_) => return Err("stopped Redis server should fail receive".into()),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            qubit_event_bus::error::SpiError::Operation {
                retryable: Some(true),
                ..
            }
        ));
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
