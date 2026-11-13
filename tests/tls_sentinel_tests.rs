// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Verifies Sentinel discovery of TLS Redis masters using the public SPI.

#![cfg(any(feature = "sync", feature = "async"))]

mod support;

use std::any::TypeId;
use std::error::Error;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_event_bus_redis::naming::group_name;
use qubit_event_bus_redis::naming::stream_key;
use qubit_id::Id;
use support::tls_sentinel::TlsSentinel;

static IDS: AtomicU64 = AtomicU64::new(1);

#[test]
fn test_concurrent_tls_sentinel_fixture_starts_use_distinct_ports() {
    let start = || std::thread::spawn(|| TlsSentinel::start(false).map_err(|error| error.to_string()));
    let first_thread = start();
    let second_thread = start();
    let first = first_thread
        .join()
        .expect("first fixture thread")
        .expect("first TLS Sentinel fixture");
    let second = second_thread
        .join()
        .expect("second fixture thread")
        .expect("second TLS Sentinel fixture");
    let first_ports: Vec<_> = first
        .endpoints()
        .split(',')
        .map(|endpoint| endpoint.rsplit(':').next().expect("endpoint port").to_owned())
        .collect();
    let second_ports: Vec<_> = second
        .endpoints()
        .split(',')
        .map(|endpoint| endpoint.rsplit(':').next().expect("endpoint port").to_owned())
        .collect();

    assert!(
        first_ports.iter().all(|port| !second_ports.contains(port)),
        "concurrent TLS Sentinel fixtures must own disjoint ports"
    );
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_plain_sentinel_discovers_tls_master() -> Result<(), Box<dyn Error>> {
    use qubit_event_bus_redis::sync::RedisEventBusProvider;
    use qubit_spi::ServiceProvider;

    let sentinel = TlsSentinel::start(false)?;
    let bus = RedisEventBusProvider
        .create_configured(&config(&sentinel, false, &namespace()))
        .map_err(|failure| failure.into_error())?;
    let _ = bus.publish(message("plain-sentinel-to-tls-master")?)?;
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_tls_sentinel_discovers_tls_master() -> Result<(), Box<dyn Error>> {
    use futures_lite::future::block_on;
    use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
    use qubit_spi::AsyncServiceProvider;

    let sentinel = TlsSentinel::start(true)?;
    let config = config(&sentinel, true, &namespace());
    let bus =
        block_on(AsyncRedisEventBusProvider.create_configured(&config)).map_err(|failure| failure.into_error())?;
    let event = message("tls-sentinel-to-tls-master")?;
    let _ = block_on(async { bus.publish(event).await })?;
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_tls_sentinel_rediscovers_tls_master_after_failover() -> Result<(), Box<dyn Error>> {
    use futures_lite::future::block_on;
    use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
    use qubit_spi::AsyncServiceProvider;

    let mut sentinel = stage(TlsSentinel::start(true), "start async TLS Sentinel fixture")?;
    let namespace = namespace();
    let bus = stage(
        block_on(AsyncRedisEventBusProvider.create_configured(&config(&sentinel, true, &namespace)))
            .map_err(|failure| failure.into_error()),
        "create async Redis provider",
    )?;
    let mut receiver = stage(
        block_on(bus.subscribe(subscription_request(101, "tls-async-worker")?)),
        "subscribe initial async receiver",
    )?;
    let before_failover = message("before-async-tls-sentinel-failover")?;
    let event_id = before_failover.id().as_str().to_owned();
    let _ = stage(
        block_on(async { bus.publish(before_failover).await }),
        "publish initial async event",
    )?;
    let ReceiveOutcome::Message(mut pending) = stage(
        block_on(receiver.receive(Duration::from_secs(5))),
        "receive initial async event",
    )?
    else {
        return Err("TLS Sentinel receiver did not receive the pre-failover record".into());
    };
    assert_eq!(pending.id().as_str(), event_id);
    // Retry releases this receiver's local active slot without XACK, leaving
    // the record pending so this same receiver can reclaim it after failover.
    let token = pending
        .take_settlement()
        .ok_or("initial async message has no settlement token")?;
    stage(
        block_on(receiver.settle(&token, DeliveryDisposition::Retry)),
        "release initial async delivery for retry",
    )?;
    let stream = stream_key(&namespace, "tls-sentinel-events");
    let group = group_name(
        &namespace,
        "tls-sentinel-events",
        "tls-async-worker",
        Some("tls-sentinel-group"),
    );
    let (pending_id, old_owner) = stage(
        sentinel.pending_identity(sentinel.original_master_port(), &stream, &group),
        "read initial async pending identity",
    )?;
    stage(
        sentinel.wait_for_pending_replication(&stream, &group, &pending_id, &old_owner),
        "wait for async pending replication",
    )?;
    let old_master = stage(sentinel.master_port(), "read initial async Sentinel master")?;
    stage(sentinel.stop_original_master(), "stop async test master")?;
    let deadline = std::time::Instant::now() + Duration::from_secs(35);
    let promoted_master = loop {
        if let Ok(port) = sentinel.master_port()
            && port != old_master
        {
            break port;
        }
        if std::time::Instant::now() >= deadline {
            return Err("Sentinel did not promote the TLS replica before the deadline".into());
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    assert_ne!(promoted_master, old_master);
    let receive_deadline = std::time::Instant::now() + Duration::from_secs(35);
    let mut old_connection_errors = 0;
    let mut recovered = None;
    while std::time::Instant::now() < receive_deadline {
        match block_on(receiver.receive(Duration::from_secs(1))) {
            Ok(ReceiveOutcome::Message(message)) => {
                recovered = Some(message);
                break;
            }
            Ok(ReceiveOutcome::TimedOut) => {}
            Ok(_) => {
                return Err("unexpected non-message TLS Sentinel async recovery outcome".into());
            }
            Err(error) => {
                old_connection_errors += 1;
                eprintln!("TLS Sentinel async receive error {old_connection_errors}/8: {error}");
                if old_connection_errors > 8 {
                    return Err(format!("too many old-connection receive errors: {error}").into());
                }
            }
        }
    }
    let mut recovered = recovered.ok_or("same async receiver did not recover the pending record before deadline")?;
    assert_eq!(
        recovered.id().as_str(),
        event_id,
        "recovery must preserve the business event ID"
    );
    let (recovered_id, _) = sentinel.pending_identity(promoted_master, &stream, &group)?;
    assert_eq!(recovered_id, pending_id, "recovery must preserve the Redis stream ID");
    let token = recovered
        .take_settlement()
        .ok_or("recovered message has no settlement token")?;
    block_on(receiver.settle(&token, DeliveryDisposition::Accept))?;
    sentinel.assert_pending_empty(promoted_master, &stream, &group)?;
    block_on(receiver.close())?;
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_tls_sentinel_rediscovers_tls_master_after_failover() -> Result<(), Box<dyn Error>> {
    use std::thread::sleep;
    use std::time::Instant;

    use qubit_event_bus_redis::sync::RedisEventBusProvider;
    use qubit_spi::ServiceProvider;

    let mut sentinel = stage(TlsSentinel::start(true), "start sync TLS Sentinel fixture")?;
    let namespace = namespace();
    let bus = stage(
        RedisEventBusProvider
            .create_configured(&config(&sentinel, true, &namespace))
            .map_err(|failure| failure.into_error()),
        "create sync Redis provider",
    )?;
    let mut receiver = stage(
        bus.subscribe(subscription_request(102, "tls-sync-worker")?),
        "subscribe initial sync receiver",
    )?;
    let before_failover = message("before-tls-sentinel-failover")?;
    let event_id = before_failover.id().as_str().to_owned();
    let _ = stage(bus.publish(before_failover), "publish initial sync event")?;
    let ReceiveOutcome::Message(mut pending) =
        stage(receiver.receive(Duration::from_secs(5)), "receive initial sync event")?
    else {
        return Err("TLS Sentinel receiver did not receive the pre-failover record".into());
    };
    assert_eq!(pending.id().as_str(), event_id);
    // Retry releases this receiver's local active slot without XACK, leaving
    // the record pending so this same receiver can reclaim it after failover.
    let token = pending
        .take_settlement()
        .ok_or("initial sync message has no settlement token")?;
    stage(
        receiver.settle(&token, DeliveryDisposition::Retry),
        "release initial sync delivery for retry",
    )?;
    let stream = stream_key(&namespace, "tls-sentinel-events");
    let group = group_name(
        &namespace,
        "tls-sentinel-events",
        "tls-sync-worker",
        Some("tls-sentinel-group"),
    );
    let (pending_id, old_owner) = stage(
        sentinel.pending_identity(sentinel.original_master_port(), &stream, &group),
        "read initial sync pending identity",
    )?;
    stage(
        sentinel.wait_for_pending_replication(&stream, &group, &pending_id, &old_owner),
        "wait for sync pending replication",
    )?;
    let old_master = stage(sentinel.master_port(), "read initial sync Sentinel master")?;
    stage(sentinel.stop_original_master(), "stop sync test master")?;
    let deadline = Instant::now() + Duration::from_secs(25);
    let promoted_master = loop {
        if let Ok(port) = sentinel.master_port()
            && port != old_master
        {
            break port;
        }
        if Instant::now() >= deadline {
            return Err("Sentinel did not promote the TLS replica before the deadline".into());
        }
        sleep(Duration::from_millis(100));
    };
    assert_ne!(promoted_master, old_master);
    let receive_deadline = Instant::now() + Duration::from_secs(35);
    let mut old_connection_errors = 0;
    let mut recovered = None;
    while Instant::now() < receive_deadline {
        match receiver.receive(Duration::from_secs(1)) {
            Ok(ReceiveOutcome::Message(message)) => {
                recovered = Some(message);
                break;
            }
            Ok(ReceiveOutcome::TimedOut) => {}
            Ok(_) => return Err("unexpected non-message TLS Sentinel sync recovery outcome".into()),
            Err(error) => {
                old_connection_errors += 1;
                eprintln!("TLS Sentinel sync receive error {old_connection_errors}/8: {error}");
                if old_connection_errors > 8 {
                    return Err(format!("too many old-connection receive errors: {error}").into());
                }
            }
        }
    }
    let mut recovered = recovered.ok_or("same sync receiver did not recover the pending record before deadline")?;
    assert_eq!(
        recovered.id().as_str(),
        event_id,
        "recovery must preserve the business event ID"
    );
    let (recovered_id, _) = sentinel.pending_identity(promoted_master, &stream, &group)?;
    assert_eq!(recovered_id, pending_id, "recovery must preserve the Redis stream ID");
    let token = recovered
        .take_settlement()
        .ok_or("recovered message has no settlement token")?;
    receiver.settle(&token, DeliveryDisposition::Accept)?;
    sentinel.assert_pending_empty(promoted_master, &stream, &group)?;
    receiver.close()?;
    Ok(())
}

fn config(sentinel: &TlsSentinel, sentinel_tls: bool, namespace: &str) -> EventBusConfig {
    let mut options: ProviderOptions = [
        (
            "redis.url".into(),
            format!("rediss://localhost:{}/", sentinel.original_master_port()),
        ),
        ("redis.namespace".into(), namespace.to_owned()),
        ("redis.claim_min_idle_ms".into(), "2000".into()),
        ("redis.sentinel.nodes".into(), sentinel.endpoints()),
        ("redis.sentinel.service_name".into(), "qeventbus".into()),
        ("redis.tls_ca_cert_path".into(), sentinel.ca_path().into()),
    ]
    .into();
    if sentinel_tls {
        options.insert("redis.sentinel.tls".into(), "true".into());
        options.insert("redis.sentinel.tls_ca_cert_path".into(), sentinel.ca_path().into());
    }
    EventBusConfig::default().with_provider_options(options)
}

fn namespace() -> String {
    format!("tls-sentinel-{}", IDS.fetch_add(1, Ordering::Relaxed))
}

fn stage<T, E: std::fmt::Display>(result: Result<T, E>, label: &str) -> Result<T, Box<dyn Error>> {
    result.map_err(|error| format!("{label}: {error}").into())
}

fn subscription_request(id: u64, subscriber: &str) -> Result<SpiSubscriptionRequest, Box<dyn Error>> {
    Ok(SpiSubscriptionRequest::new(
        Id::new(id),
        qubit_event_bus::spi::TopicAddress::new("tls-sentinel-events")?,
        SubscriberId::new(subscriber)?,
        Some(ConsumerGroup::new("tls-sentinel-group")?),
        SubscriptionDurability::Durable,
        StartPosition::Earliest,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    ))
}

fn message(id: &str) -> Result<OutboundMessage, Box<dyn Error>> {
    Ok(OutboundMessage::new(
        TopicAddress::new("tls-sentinel-events")?,
        EventId::new(format!("{id}-{}", IDS.fetch_add(1, Ordering::Relaxed)))?,
        SystemTime::now(),
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            std::sync::Arc::from(id.as_bytes().to_vec()),
            ContentType::new("application/octet-stream")?,
            None,
        )),
    ))
}
