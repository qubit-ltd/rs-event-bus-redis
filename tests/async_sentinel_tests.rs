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
use std::error::Error;
use std::io::Error as IoError;
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
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::AsyncConformanceHooks;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::run_async;
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
use qubit_event_bus_redis::naming::group_name;
use qubit_event_bus_redis::naming::stream_key;
use qubit_id::Id;
use qubit_spi::AsyncServiceProvider;
use support::sentinel::SentinelServer;

#[test]
fn test_async_sentinel_reconnects_after_master_failover() -> Result<(), Box<dyn Error>> {
    let mut sentinel = SentinelServer::start()?;
    let options: ProviderOptions = [
        ("redis.namespace".into(), "sentinel-async".into()),
        ("redis.sentinel.nodes".into(), sentinel.endpoints()),
        ("redis.sentinel.service_name".into(), "qeventbus".into()),
        ("redis.claim_min_idle_ms".into(), "0".into()),
    ]
    .into();
    #[cfg(feature = "conformance")]
    let conformance_options = options.clone();
    let config = EventBusConfig::default().with_provider_options(options);
    let bus = block_on(AsyncRedisEventBusProvider.create_configured(&config))
        .map_err(|failure| failure.into_error())?;
    sentinel.stop_original_master()?;
    block_on(async {
        let _ = bus
            .publish(message("events", "after-failover-async", b"after")?)
            .await?;
        let request = SpiSubscriptionRequest::new(
            Id::new(2002),
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
            .map_err(|error| IoError::other(format!("subscribe before promotion: {error}")))?;
        let ReceiveOutcome::Message(received) = receiver.receive(Duration::from_secs(10)).await?
        else {
            return Err("post-promotion event was not received".into());
        };
        assert_eq!(received.id().as_str(), "after-failover-async");
        receiver
            .settle(
                received.settlement().ok_or("missing settlement token")?,
                DeliveryDisposition::Accept,
            )
            .await?;
        Ok::<(), Box<dyn Error>>(())
    })?;
    #[cfg(feature = "conformance")]
    {
        let report = block_on(run_async(
            || {
                let config =
                    EventBusConfig::default().with_provider_options(conformance_options.clone());
                async move {
                    AsyncRedisEventBusProvider
                        .create_configured(&config)
                        .await
                        .expect("Sentinel provider settings remain valid after promotion")
                }
            },
            &AsyncConformanceHooks::default(),
        ));
        report.assert_all_passed();
    }
    Ok(())
}

#[test]
fn test_async_sentinel_claims_unsettled_record_after_promotion() -> Result<(), Box<dyn Error>> {
    let mut sentinel = SentinelServer::start()?;
    let options: ProviderOptions = [
        ("redis.namespace".into(), "sentinel-async-recovery".into()),
        ("redis.sentinel.nodes".into(), sentinel.endpoints()),
        ("redis.sentinel.service_name".into(), "qeventbus".into()),
        ("redis.claim_min_idle_ms".into(), "0".into()),
    ]
    .into();
    let config = EventBusConfig::default().with_provider_options(options);
    let bus = block_on(AsyncRedisEventBusProvider.create_configured(&config))
        .map_err(|failure| failure.into_error())?;
    let original_port = sentinel.master_port()?;
    let pending_message = message("events", "pending-before-async-promotion", b"pending")?;
    let _ = block_on(async { bus.publish(pending_message).await })?;
    let mut first = block_on(async {
        let request = SpiSubscriptionRequest::new(
            Id::new(2101),
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
            .map_err(|error| IoError::other(format!("subscribe after promotion: {error}")))?;
        let ReceiveOutcome::Message(received) = receiver.receive(Duration::from_secs(2)).await?
        else {
            return Err("initial consumer did not receive the pending record".into());
        };
        assert_eq!(received.id().as_str(), "pending-before-async-promotion");
        Ok::<_, Box<dyn Error>>(receiver)
    })?;
    let stream = stream_key("sentinel-async-recovery", "events");
    let group = group_name(
        "sentinel-async-recovery",
        "events",
        "worker-one",
        Some("sentinel-workers"),
    );
    let (pending_id, old_owner) = sentinel.pending_identity(original_port, &stream, &group)?;
    sentinel.wait_for_pending(
        sentinel.replica_port(),
        &stream,
        &group,
        &pending_id,
        &old_owner,
    )?;
    block_on(first.close())?;
    sentinel.stop_original_master()?;
    let promoted_port = sentinel.master_port()?;
    sentinel.wait_for_pending(promoted_port, &stream, &group, &pending_id, &old_owner)?;
    block_on(async {
        let request = SpiSubscriptionRequest::new(
            Id::new(2102),
            TopicAddress::new("events")?,
            SubscriberId::new("worker-two")?,
            Some(ConsumerGroup::new("sentinel-workers")?),
            SubscriptionDurability::Durable,
            StartPosition::Earliest,
            ProviderOptions::new(),
            TypeId::of::<Vec<u8>>(),
        );
        let mut receiver = bus.subscribe(request).await?;
        let ReceiveOutcome::Message(pending) = receiver.receive(Duration::from_secs(10)).await?
        else {
            return Err("new consumer did not claim the pending record".into());
        };
        assert_eq!(pending.id().as_str(), "pending-before-async-promotion");
        let (claimed_id, new_owner) = sentinel.pending_identity(promoted_port, &stream, &group)?;
        assert_eq!(claimed_id, pending_id, "claim must preserve the stream ID");
        assert_ne!(
            new_owner, old_owner,
            "claim must transfer the pending owner"
        );
        receiver
            .settle(
                pending.settlement().ok_or("missing settlement token")?,
                DeliveryDisposition::Accept,
            )
            .await?;
        sentinel.assert_pending_empty(promoted_port, &stream, &group)?;
        receiver.close().await?;
        Ok(())
    })
}

/// Builds an encoded event for `topic` and `id` with `bytes`; returns metadata
/// validation errors without network I/O.
fn message(topic: &str, id: &str, bytes: &[u8]) -> Result<OutboundMessage, Box<dyn Error>> {
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
