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
use qubit_event_bus_redis::naming::stream_key;
use qubit_event_bus_redis::sync::RedisEventBusProvider;
use qubit_spi::ServiceProvider;
use support::sentinel::SentinelServer;

#[test]
fn test_sync_sentinel_reconnects_after_master_failover() -> Result<(), Box<dyn std::error::Error>> {
    let mut sentinel = SentinelServer::start()?;
    let options: ProviderOptions = [
        ("redis.namespace".into(), "sentinel-sync".into()),
        ("redis.sentinel.nodes".into(), sentinel.endpoints()),
        ("redis.sentinel.service_name".into(), "qeventbus".into()),
        ("redis.claim_min_idle_ms".into(), "0".into()),
    ]
    .into();
    let bus = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(options))
        .map_err(|failure| failure.into_error())?;
    let original_master_port = sentinel.master_port()?;
    bus.publish(message("events", "pending-before-failover", b"pending")?)?;
    let direct_client = redis::Client::open(format!("redis://127.0.0.1:{original_master_port}/"))?;
    let mut direct_connection = direct_client.get_connection()?;
    let replicas: usize = redis::cmd("WAIT").arg(1).arg(5_000).query(&mut direct_connection)?;
    assert_eq!(replicas, 1, "replica must contain the pending record before promotion");
    let mut first = bus.subscribe(subscription_request(1001, "worker-one")?)?;
    let ReceiveOutcome::Message(received) = first.receive(Duration::from_secs(2))? else {
        return Err("initial consumer did not receive the event".into());
    };
    assert_eq!(received.id().as_str(), "pending-before-failover");
    first.close()?;
    sentinel.stop_original_master()?;
    bus.publish(message("events", "after-failover", b"after")?)?;
    let promoted_client = redis::Client::open(format!("redis://127.0.0.1:{}/", sentinel.master_port()?))?;
    let mut promoted_connection = promoted_client.get_connection()?;
    let stream_length: usize = redis::cmd("XLEN")
        .arg(stream_key("sentinel-sync", "events"))
        .query(&mut promoted_connection)?;
    assert_eq!(stream_length, 2, "promoted master must contain both stream records");
    let mut second = bus.subscribe(subscription_request(1002, "worker-two")?)?;
    let ReceiveOutcome::Message(mut pending) = second.receive(Duration::from_secs(2))? else {
        return Err("new consumer did not claim the pre-failover pending record".into());
    };
    assert_eq!(pending.id().as_str(), "pending-before-failover");
    let token = pending
        .take_settlement()
        .ok_or("pending record has no settlement token")?;
    second.settle(&token, DeliveryDisposition::Accept)?;
    let ReceiveOutcome::Message(after) = second.receive(Duration::from_secs(2))? else {
        return Err("post-promotion message was not readable".into());
    };
    assert_eq!(after.id().as_str(), "after-failover");
    Ok(())
}

fn subscription_request(id: u64, subscriber: &str) -> Result<SpiSubscriptionRequest, Box<dyn std::error::Error>> {
    Ok(SpiSubscriptionRequest::new(
        qubit_id::Id::new(id),
        TopicAddress::new("events")?,
        SubscriberId::new(subscriber)?,
        Some(ConsumerGroup::new("sentinel-group")?),
        SubscriptionDurability::Durable,
        StartPosition::Earliest,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    ))
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
