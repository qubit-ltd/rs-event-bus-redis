// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Verifies synchronous Sentinel connection recovery against isolated
//! containers.

#![cfg(feature = "sync")]

mod support;

use std::any::TypeId;
use std::error::Error;
use std::sync::Arc;
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
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::ConformanceHooks;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::run_sync;
use qubit_event_bus_redis::naming::group_name;
use qubit_event_bus_redis::naming::stream_key;
use qubit_event_bus_redis::sync::RedisEventBusProvider;
use qubit_id::Id;
use qubit_spi::ServiceProvider;
use redis::Client;
use redis::cmd;
use support::sentinel::SentinelServer;

#[test]
fn test_sync_sentinel_reconnects_after_master_failover() -> Result<(), Box<dyn Error>> {
    let mut sentinel = SentinelServer::start()?;
    let options: ProviderOptions = [
        ("redis.namespace".into(), "sentinel-sync".into()),
        ("redis.sentinel.nodes".into(), sentinel.endpoints()),
        ("redis.sentinel.service_name".into(), "qeventbus".into()),
        ("redis.claim_min_idle_ms".into(), "0".into()),
    ]
    .into();
    #[cfg(feature = "conformance")]
    let conformance_options = options.clone();
    let bus = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(options))
        .map_err(|failure| failure.into_error())?;
    let original_master_port = sentinel.master_port()?;
    let _ = bus.publish(message("events", "pending-before-failover", b"pending")?)?;
    let mut first = bus.subscribe(subscription_request(1001, "worker-one")?)?;
    let ReceiveOutcome::Message(received) = first.receive(Duration::from_secs(2))? else {
        return Err("initial consumer did not receive the event".into());
    };
    assert_eq!(received.id().as_str(), "pending-before-failover");
    let stream = stream_key("sentinel-sync", "events");
    let group = group_name(
        "sentinel-sync",
        "events",
        "worker-one",
        Some("sentinel-group"),
    );
    let (pending_id, old_owner) =
        sentinel.pending_identity(original_master_port, &stream, &group)?;
    sentinel.wait_for_pending(
        sentinel.replica_port(),
        &stream,
        &group,
        &pending_id,
        &old_owner,
    )?;
    first.close()?;
    sentinel.stop_original_master()?;
    let promoted_port = sentinel.master_port()?;
    sentinel.wait_for_pending(promoted_port, &stream, &group, &pending_id, &old_owner)?;
    let _ = bus.publish(message("events", "after-failover", b"after")?)?;
    let promoted_client = Client::open(format!("redis://127.0.0.1:{}/", sentinel.master_port()?))?;
    let mut promoted_connection = promoted_client.get_connection()?;
    let stream_length: usize = cmd("XLEN")
        .arg(stream_key("sentinel-sync", "events"))
        .query(&mut promoted_connection)?;
    assert_eq!(
        stream_length, 2,
        "promoted master must contain both stream records"
    );
    let mut second = bus.subscribe(subscription_request(1002, "worker-two")?)?;
    let ReceiveOutcome::Message(mut pending) = second.receive(Duration::from_secs(10))? else {
        return Err("new consumer did not claim the pre-failover pending record".into());
    };
    assert_eq!(pending.id().as_str(), "pending-before-failover");
    let (claimed_id, new_owner) = sentinel.pending_identity(promoted_port, &stream, &group)?;
    assert_eq!(claimed_id, pending_id, "claim must preserve the stream ID");
    assert_ne!(
        new_owner, old_owner,
        "claim must transfer the pending owner"
    );
    let token = pending
        .take_settlement()
        .ok_or("pending record has no settlement token")?;
    second.settle(&token, DeliveryDisposition::Accept)?;
    sentinel.assert_pending_empty(promoted_port, &stream, &group)?;
    let ReceiveOutcome::Message(after) = second.receive(Duration::from_secs(10))? else {
        return Err("post-promotion message was not readable".into());
    };
    assert_eq!(after.id().as_str(), "after-failover");
    #[cfg(feature = "conformance")]
    run_sync(
        || {
            RedisEventBusProvider
                .create_configured(
                    &EventBusConfig::default().with_provider_options(conformance_options.clone()),
                )
                .expect("Sentinel provider settings remain valid after promotion")
        },
        &ConformanceHooks::default(),
    )
    .assert_all_passed();
    Ok(())
}

/// Builds a durable fixed-group request with `id` and `subscriber`; returns
/// metadata validation errors without I/O.
fn subscription_request(
    id: u64,
    subscriber: &str,
) -> Result<SpiSubscriptionRequest, Box<dyn Error>> {
    Ok(SpiSubscriptionRequest::new(
        Id::new(id),
        TopicAddress::new("events")?,
        SubscriberId::new(subscriber)?,
        Some(ConsumerGroup::new("sentinel-group")?),
        SubscriptionDurability::Durable,
        StartPosition::Earliest,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    ))
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
