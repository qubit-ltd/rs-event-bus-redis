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

use std::error::Error;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use support::tls_sentinel::TlsSentinel;

static IDS: AtomicU64 = AtomicU64::new(1);

#[cfg(feature = "sync")]
#[test]
fn test_sync_plain_sentinel_discovers_tls_master() -> Result<(), Box<dyn Error>> {
    use qubit_event_bus_redis::sync::RedisEventBusProvider;
    use qubit_spi::ServiceProvider;

    let sentinel = TlsSentinel::start(false)?;
    let bus = RedisEventBusProvider
        .create_configured(&config(&sentinel, false))
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
    let config = config(&sentinel, true);
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

    let mut sentinel = TlsSentinel::start(true)?;
    let config = config(&sentinel, true);
    let bus =
        block_on(AsyncRedisEventBusProvider.create_configured(&config)).map_err(|failure| failure.into_error())?;
    let before_failover = message("before-async-tls-sentinel-failover")?;
    let _ = block_on(async { bus.publish(before_failover).await })?;
    let old_master = sentinel.master_port()?;
    sentinel.stop_original_master()?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(25);
    let promoted_master = loop {
        if let Ok(port) = sentinel.master_port()
            && port != old_master
        {
            break port;
        }
        if std::time::Instant::now() >= deadline {
            return Err("Sentinel did not promote the TLS replica before the deadline".into());
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    };
    assert_ne!(promoted_master, old_master);
    let after_failover = message("after-async-tls-sentinel-failover")?;
    let _ = block_on(async { bus.publish(after_failover).await })?;
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_tls_sentinel_rediscovers_tls_master_after_failover() -> Result<(), Box<dyn Error>> {
    use std::thread::sleep;
    use std::time::Duration;
    use std::time::Instant;

    use qubit_event_bus_redis::sync::RedisEventBusProvider;
    use qubit_spi::ServiceProvider;

    let mut sentinel = TlsSentinel::start(true)?;
    let bus = RedisEventBusProvider
        .create_configured(&config(&sentinel, true))
        .map_err(|failure| failure.into_error())?;
    let _ = bus.publish(message("before-tls-sentinel-failover")?)?;
    let old_master = sentinel.master_port()?;
    sentinel.stop_original_master()?;
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
    let _ = bus.publish(message("after-tls-sentinel-failover")?)?;
    Ok(())
}

fn config(sentinel: &TlsSentinel, sentinel_tls: bool) -> EventBusConfig {
    let mut options: ProviderOptions = [
        (
            "redis.url".into(),
            format!("rediss://localhost:{}/", sentinel.original_master_port()),
        ),
        (
            "redis.namespace".into(),
            format!("tls-sentinel-{}", IDS.fetch_add(1, Ordering::Relaxed)),
        ),
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
