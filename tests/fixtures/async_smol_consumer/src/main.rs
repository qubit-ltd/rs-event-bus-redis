// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Exercises the provider using isolated production async features.

use std::any::TypeId;
use std::env::args;
use std::error::Error;
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
use qubit_id::Id;
use qubit_spi::AsyncServiceProvider;

/// Reads the Redis URL from the first process argument and exercises public
/// SPI.
///
/// Returns success after real publish/receive/ACK/close, or
/// argument/configuration/ transport errors. Requires the host executor and
/// performs Redis network IO; assertions panic for incorrect delivery identity,
/// bytes, or redelivery behavior.
async fn exercise() -> Result<(), Box<dyn Error>> {
    let url = args().nth(1).ok_or("expected Redis URL")?;
    let options: ProviderOptions = [
        ("redis.url".into(), url),
        ("redis.namespace".into(), "isolated-runtime".into()),
    ]
    .into();
    let bus = AsyncRedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(options))
        .await
        .map_err(|failure| failure.into_error())?;
    let topic = TopicAddress::new("runtime.events")?;
    let mut receiver = bus
        .subscribe(SpiSubscriptionRequest::new(
            Id::new(101),
            topic.clone(),
            SubscriberId::new("runtime-worker")?,
            Some(ConsumerGroup::new("runtime-group")?),
            SubscriptionDurability::Durable,
            StartPosition::Earliest,
            ProviderOptions::new(),
            TypeId::of::<Vec<u8>>(),
        ))
        .await?;
    let payload: Arc<[u8]> = Arc::from(b"independent production features".as_slice());
    let _ = bus.publish(OutboundMessage::new(
        topic,
        EventId::new("isolated-runtime-event")?,
        SystemTime::now(),
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            payload.clone(),
            ContentType::new("text/plain")?,
            None,
        )),
    ))
    .await?;
    let ReceiveOutcome::Message(received) = receiver.receive(Duration::from_secs(5)).await? else {
        return Err("provider did not deliver under the fixture executor".into());
    };
    assert_eq!(received.id().as_str(), "isolated-runtime-event");
    let TransportPayload::Encoded(encoded) = received.payload() else {
        return Err("expected encoded transport payload".into());
    };
    assert_eq!(encoded.bytes(), payload.as_ref());
    receiver
        .settle(
            received.settlement().ok_or("missing settlement token")?,
            DeliveryDisposition::Accept,
        )
        .await?;
    assert!(matches!(
        receiver.receive(Duration::ZERO).await?,
        ReceiveOutcome::TimedOut
    ));
    receiver.close().await?;
    println!("isolated publish/receive/ACK/close passed");
    Ok(())
}

/// Blocks on the Smol-compatible production consumer scenario.
///
/// Reads the Redis URL from the first process argument. Returns success or
/// propagated argument/configuration/transport errors after real Redis IO.
fn main() -> Result<(), Box<dyn Error>> {
    block_on(exercise())
}
