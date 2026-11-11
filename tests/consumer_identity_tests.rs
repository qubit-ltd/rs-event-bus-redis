// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Verifies Redis consumer names remain unique across bus instances.

#![cfg(any(feature = "sync", feature = "async"))]

mod support;

use std::any::TypeId;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

#[cfg(feature = "async")]
use futures_lite::future::block_on;
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
#[cfg(feature = "async")]
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
use qubit_event_bus_redis::naming::group_name;
use qubit_event_bus_redis::naming::stream_key;
#[cfg(feature = "sync")]
use qubit_event_bus_redis::sync::RedisEventBusProvider;
use qubit_id::Id;
#[cfg(feature = "async")]
use qubit_spi::AsyncServiceProvider;
#[cfg(feature = "sync")]
use qubit_spi::ServiceProvider;
use redis::Client;
use redis::Value;
use redis::cmd;
use support::redis_server::RedisServer;

/// Builds options for `server` and `namespace`, preserving pending ownership
/// by disabling immediate reclaim; returns settings without network I/O.
fn bus_options(server: &RedisServer, namespace: &str) -> ProviderOptions {
    [
        ("redis.url".into(), server.url().into()),
        ("redis.namespace".into(), namespace.into()),
        ("redis.claim_min_idle_ms".into(), "60000".into()),
    ]
    .into()
}

/// Builds the fixed durable group request without I/O; returns metadata
/// validation errors if any fixture identifier is rejected.
fn request() -> Result<SpiSubscriptionRequest, Box<dyn Error>> {
    request_at(StartPosition::Earliest)
}

/// Builds a durable request at `start_position` without I/O; returns metadata
/// validation errors if any fixture identifier is rejected.
fn request_at(start_position: StartPosition) -> Result<SpiSubscriptionRequest, Box<dyn Error>> {
    Ok(SpiSubscriptionRequest::new(
        Id::new(1),
        TopicAddress::new("events")?,
        SubscriberId::new("worker")?,
        Some(ConsumerGroup::new("workers")?),
        SubscriptionDurability::Durable,
        start_position,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    ))
}

/// Builds the fixed encoded identity event without I/O; returns metadata
/// validation errors if a fixture identifier or content type is rejected.
fn message() -> Result<OutboundMessage, Box<dyn Error>> {
    Ok(OutboundMessage::new(
        TopicAddress::new("events")?,
        EventId::new("identity-test-event")?,
        SystemTime::now(),
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(&b"payload"[..]),
            ContentType::new("application/octet-stream")?,
            None,
        )),
    ))
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_separate_buses_do_not_share_consumer_identity() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let namespace = "unique-sync-consumer";
    let options = bus_options(&server, namespace);
    let config = EventBusConfig::default().with_provider_options(options);
    let first_bus = RedisEventBusProvider
        .create_configured(&config)
        .map_err(|failure| failure.into_error())?;
    let second_bus = RedisEventBusProvider
        .create_configured(&config)
        .map_err(|failure| failure.into_error())?;
    let _ = first_bus.publish(message()?)?;
    let mut first = first_bus.subscribe(request()?)?;
    let mut second = second_bus.subscribe(request_at(StartPosition::New)?)?;

    assert!(matches!(
        first.receive(Duration::from_secs(2))?,
        ReceiveOutcome::Message(_)
    ));
    assert!(matches!(second.receive(Duration::ZERO)?, ReceiveOutcome::TimedOut));

    let group = group_name(namespace, "events", "worker", Some("workers"));
    let mut connection = Client::open(server.url())?.get_connection()?;
    let consumers: Vec<Value> = cmd("XINFO")
        .arg("CONSUMERS")
        .arg(stream_key(namespace, "events"))
        .arg(group)
        .query(&mut connection)?;
    assert_eq!(consumers.len(), 2);
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_separate_buses_do_not_share_consumer_identity() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let server = RedisServer::start()?;
        let namespace = "unique-async-consumer";
        let options = bus_options(&server, namespace);
        let config = EventBusConfig::default().with_provider_options(options);
        let first_bus = AsyncRedisEventBusProvider
            .create_configured(&config)
            .await
            .map_err(|failure| failure.into_error())?;
        let second_bus = AsyncRedisEventBusProvider
            .create_configured(&config)
            .await
            .map_err(|failure| failure.into_error())?;
        let _ = first_bus.publish(message()?).await?;
        let mut first = first_bus.subscribe(request()?).await?;
        let mut second = second_bus.subscribe(request_at(StartPosition::New)?).await?;

        assert!(matches!(
            first.receive(Duration::from_secs(2)).await?,
            ReceiveOutcome::Message(_)
        ));
        assert!(matches!(
            second.receive(Duration::ZERO).await?,
            ReceiveOutcome::TimedOut
        ));

        let group = group_name(namespace, "events", "worker", Some("workers"));
        let mut connection = Client::open(server.url())?.get_connection()?;
        let consumers: Vec<Value> = cmd("XINFO")
            .arg("CONSUMERS")
            .arg(stream_key(namespace, "events"))
            .arg(group)
            .query(&mut connection)?;
        assert_eq!(consumers.len(), 2);
        Ok::<(), Box<dyn Error>>(())
    })
}
