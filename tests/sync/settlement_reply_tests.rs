// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Unknown-XACK intent remains fixed until identical settlement succeeds.
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
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_event_bus_redis::naming::group_name;
use qubit_event_bus_redis::naming::stream_key;
use qubit_event_bus_redis::sync::RedisEventBusProvider;
use qubit_id::Id;
use qubit_spi::ServiceProvider;
use redis::Client;
use redis::Value;
use redis::cmd;

use crate::support::controlled_redis::proxy::ControlledRedis;
use crate::support::redis_server::RedisServer;
type TestResult = Result<(), Box<dyn Error>>;

/// Creates a single-slot bus through the controlled endpoint.
fn create_bus(url: &str) -> Result<Arc<dyn EventBusSpi>, Box<dyn Error>> {
    let options: ProviderOptions = [
        ("redis.url".into(), url.into()),
        ("redis.namespace".into(), "settlement-tests".into()),
        ("redis.claim_min_idle_ms".into(), "0".into()),
        ("redis.max_unsettled_per_subscription".into(), "1".into()),
    ]
    .into();
    let config = EventBusConfig::default().with_provider_options(options);
    RedisEventBusProvider
        .create_configured(&config)
        .map_err(|failure| failure.into_error())
        .map_err(Into::into)
}
/// Builds an encoded event for the settlement fixture.
fn message(id: &str) -> Result<OutboundMessage, Box<dyn Error>> {
    Ok(OutboundMessage::new(
        TopicAddress::new("settlement")?,
        EventId::new(id)?,
        SystemTime::UNIX_EPOCH,
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
/// Creates a durable earliest-position receiver.
fn request() -> Result<SpiSubscriptionRequest, Box<dyn Error>> {
    Ok(SpiSubscriptionRequest::new(
        Id::new(1),
        TopicAddress::new("settlement")?,
        SubscriberId::new("worker")?,
        Some(ConsumerGroup::new("group")?),
        SubscriptionDurability::Durable,
        StartPosition::Earliest,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    ))
}
/// Observes the real PEL through an independent connection.
fn assert_ack_applied(server: &RedisServer) -> TestResult {
    let mut observer = Client::open(server.url())?.get_connection()?;
    let pending: Vec<Value> = cmd("XPENDING")
        .arg(stream_key("settlement-tests", "settlement"))
        .arg(group_name("settlement-tests", "settlement", "worker", Some("group")))
        .arg("-")
        .arg("+")
        .arg(10)
        .query(&mut observer)?;
    assert!(
        pending.is_empty(),
        "Redis must apply XACK before the reply is lost or cancelled"
    );
    Ok(())
}

#[test]
fn test_nested_wrongtype_reply_retains_unknown_xack_intent() -> TestResult {
    let server = RedisServer::start()?;
    let proxy = ControlledRedis::start(server.url())?;
    let bus = create_bus(&proxy.url())?;

    let _ = bus.publish(message("nested")?)?;
    let mut receiver = bus.subscribe(request()?)?;
    let ReceiveOutcome::Message(received) = receiver.receive(Duration::from_secs(2))? else {
        return Err("event missing".into());
    };
    let token = received.settlement().ok_or("token missing")?;
    proxy.replace_next_reply("XACK", b"*1\r\n-WRONGTYPE injected\r\n");
    assert!(receiver.settle(token, DeliveryDisposition::Accept).is_err());
    assert_ack_applied(&server)?;
    assert!(
        receiver.settle(token, DeliveryDisposition::Retry).is_err(),
        "nested WRONGTYPE is an invalid reply, not a top-level rejection"
    );
    assert!(receiver.settle(token, DeliveryDisposition::Reject).is_err());
    receiver.settle(token, DeliveryDisposition::Accept)?;
    receiver.close()?;
    Ok(())
}
