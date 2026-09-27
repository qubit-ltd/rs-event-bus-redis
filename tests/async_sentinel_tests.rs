// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Verifies asynchronous Sentinel failover recovery against isolated
//! containers.

#![cfg(feature = "async")]

mod support;

use std::any::TypeId;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

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
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
use qubit_spi::AsyncServiceProvider;
use support::sentinel::SentinelServer;

#[test]
fn test_async_sentinel_reconnects_after_master_failover() -> Result<(), Box<dyn std::error::Error>> {
    let mut sentinel = SentinelServer::start()?;
    let options: ProviderOptions = [
        ("redis.namespace".into(), "sentinel-async".into()),
        ("redis.sentinel.nodes".into(), sentinel.endpoints()),
        ("redis.sentinel.service_name".into(), "qeventbus".into()),
        ("redis.claim_min_idle_ms".into(), "0".into()),
    ]
    .into();
    let config = EventBusConfig::default().with_provider_options(options);
    let bus =
        block_on(AsyncRedisEventBusProvider.create_configured(&config)).map_err(|failure| failure.into_error())?;
    sentinel.stop_original_master()?;
    block_on(async {
        bus.publish(message("events", "after-failover-async", b"after")?)
            .await?;
        let request = SpiSubscriptionRequest::new(
            qubit_id::Id::new(2002),
            TopicAddress::new("events")?,
            SubscriberId::new("worker-two")?,
            Some(ConsumerGroup::new("sentinel-workers")?),
            SubscriptionDurability::Durable,
            StartPosition::Earliest,
            ProviderOptions::new(),
            TypeId::of::<Vec<u8>>(),
        );
        let mut receiver = bus
            .subscribe(request)
            .await
            .map_err(|error| std::io::Error::other(format!("subscribe before promotion: {error}")))?;
        let ReceiveOutcome::Message(received) = receiver.receive(Duration::from_secs(2)).await? else {
            return Err("post-promotion event was not received".into());
        };
        assert_eq!(received.id().as_str(), "after-failover-async");
        receiver
            .settle(
                received.settlement().ok_or("missing settlement token")?,
                DeliveryDisposition::Accept,
            )
            .await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[test]
fn test_async_sentinel_claims_unsettled_record_after_promotion() -> Result<(), Box<dyn std::error::Error>> {
    let mut sentinel = SentinelServer::start()?;
    let options: ProviderOptions = [
        ("redis.namespace".into(), "sentinel-async-recovery".into()),
        ("redis.sentinel.nodes".into(), sentinel.endpoints()),
        ("redis.sentinel.service_name".into(), "qeventbus".into()),
        ("redis.claim_min_idle_ms".into(), "0".into()),
    ]
    .into();
    let config = EventBusConfig::default().with_provider_options(options);
    let bus =
        block_on(AsyncRedisEventBusProvider.create_configured(&config)).map_err(|failure| failure.into_error())?;
    let original_port = sentinel.master_port()?;
    let pending_message = message("events", "pending-before-async-promotion", b"pending")?;
    block_on(async { bus.publish(pending_message).await })?;
    let client = redis::Client::open(format!("redis://127.0.0.1:{original_port}/"))?;
    let mut connection = client.get_connection()?;
    let replicas: usize = redis::cmd("WAIT").arg(1).arg(5_000).query(&mut connection)?;
    assert_eq!(replicas, 1);
    block_on(async {
        let request = SpiSubscriptionRequest::new(
            qubit_id::Id::new(2101),
            TopicAddress::new("events")?,
            SubscriberId::new("worker-one")?,
            Some(ConsumerGroup::new("sentinel-workers")?),
            SubscriptionDurability::Durable,
            StartPosition::Earliest,
            ProviderOptions::new(),
            TypeId::of::<Vec<u8>>(),
        );
        let mut receiver = bus
            .subscribe(request)
            .await
            .map_err(|error| std::io::Error::other(format!("subscribe after promotion: {error}")))?;
        let ReceiveOutcome::Message(received) = receiver.receive(Duration::from_secs(2)).await? else {
            return Err("initial consumer did not receive the pending record".into());
        };
        assert_eq!(received.id().as_str(), "pending-before-async-promotion");
        receiver.close().await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;
    sentinel.stop_original_master()?;
    block_on(async {
        let request = SpiSubscriptionRequest::new(
            qubit_id::Id::new(2102),
            TopicAddress::new("events")?,
            SubscriberId::new("worker-two")?,
            Some(ConsumerGroup::new("sentinel-workers")?),
            SubscriptionDurability::Durable,
            StartPosition::Earliest,
            ProviderOptions::new(),
            TypeId::of::<Vec<u8>>(),
        );
        let mut receiver = bus.subscribe(request).await?;
        let ReceiveOutcome::Message(pending) = receiver.receive(Duration::from_secs(2)).await? else {
            return Err("new consumer did not claim the pending record".into());
        };
        assert_eq!(pending.id().as_str(), "pending-before-async-promotion");
        receiver
            .settle(
                pending.settlement().ok_or("missing settlement token")?,
                DeliveryDisposition::Accept,
            )
            .await?;
        Ok(())
    })
}

fn message(topic: &str, id: &str, bytes: &[u8]) -> Result<OutboundMessage, Box<dyn std::error::Error>> {
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
