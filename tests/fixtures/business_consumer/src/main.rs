// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Exercises discovery and a typed business facade in a standalone consumer.

use std::env::args;
use std::error::Error;
use std::sync::Arc;
use std::sync::mpsc::channel;
use std::time::Duration;

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::SubscriberId;
use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::CodecError;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus_redis as _;
use qubit_spi::ProviderSelection;

/// Runs discovered-provider business facade delivery against the CLI Redis URL.
///
/// Performs blocking Redis/channel IO. Returns argument/configuration/codec/
/// provider/channel errors, and panics if delivered bytes or shutdown are
/// incorrect.
fn main() -> Result<(), Box<dyn Error>> {
    let redis_url = args()
        .nth(1)
        .ok_or("usage: redis-event-bus-business-consumer REDIS_URL")?;
    let options: ProviderOptions = [
        ("redis.url".into(), redis_url.into()),
        ("redis.namespace".into(), "business-consumer-fixture".into()),
    ]
    .into();
    let mut codecs = CodecRegistry::new();
    codecs.register::<String>(Arc::new(Utf8Codec(ContentType::new("text/plain")?)));
    let facade = EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs));
    let config = EventBusConfig::default()
        .with_provider_options(options)
        .with_selection(ProviderSelection::named("redis-streams")?)
        .with_facade_config(facade);
    let registry = EventBusRegistry::discover()?;
    let bus = registry.create(&config)?;
    let topic = Topic::<String>::new("business.events")?;
    let (sender, receiver) = channel();
    let subscription = bus.subscribe(
        SubscribeRequest::builder()
            .subscriber_id(SubscriberId::new("business-consumer")?)
            .topic(topic.clone())
            .start_position(StartPosition::Earliest)
            .durability(SubscriptionDurability::Durable)
            .build()?,
        move |delivery| {
            let _ = sender.send(delivery.payload().clone());
        },
    )?;
    bus.publish(PublishRequest::new(topic, "business facade works".to_owned())?)?;
    assert_eq!(receiver.recv_timeout(Duration::from_secs(3))?, "business facade works");
    subscription.cancel()?;
    let shutdown = bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(3),
    })?;
    assert!(matches!(shutdown.outcome, ShutdownOutcome::Complete));
    Ok(())
}

/// Supplies the application string codec used by the discovered provider.
struct Utf8Codec(ContentType);

impl EventCodec<String> for Utf8Codec {
    fn content_type(&self) -> &ContentType {
        &self.0
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }

    fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from(value.as_bytes()))
    }

    fn decode(&self, payload: &EncodedPayload) -> Result<String, CodecError> {
        String::from_utf8(payload.bytes().to_vec()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}
