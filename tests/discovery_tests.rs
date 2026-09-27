// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Confirms the Redis provider is submitted to both provider inventories.

#![cfg(feature = "discovery")]

mod support;

use qubit_event_bus::AsyncEventBusRegistry;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::CodecError;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::model::Topic;
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
use qubit_event_bus_redis::sync::RedisEventBusProvider;
use qubit_spi::ProviderSelection;
use support::redis_server::RedisServer;

#[test]
fn test_sync_registry_discovers_and_creates_redis_provider() -> Result<(), Box<dyn std::error::Error>> {
    let _ = std::any::type_name::<RedisEventBusProvider>();
    let server = RedisServer::start()?;
    let registry = EventBusRegistry::discover()?;
    assert!(registry.provider_ids().iter().any(|id| id.as_str() == "redis-streams"));
    let mut codecs = CodecRegistry::new();
    codecs.register::<String>(std::sync::Arc::new(Utf8Codec(ContentType::new("text/plain")?)));
    let facade = EventBusFacadeConfig::new().with_codec_registry(std::sync::Arc::new(codecs));
    let config = redis_config(server.url())
        .with_selection(ProviderSelection::named("redis-streams")?)
        .with_facade_config(facade);
    let bus = registry.create(&config)?;
    let topic = Topic::<String>::new("discovery.events")?;
    let (sender, receiver) = std::sync::mpsc::channel();
    let subscription = bus.subscribe(
        SubscribeRequest::builder()
            .subscriber_id(qubit_event_bus::SubscriberId::new("discovery-worker")?)
            .topic(topic.clone())
            .start_position(StartPosition::Earliest)
            .durability(SubscriptionDurability::Durable)
            .build()?,
        move |delivery| {
            let _ = sender.send(delivery.payload().clone());
        },
    )?;
    bus.publish(PublishRequest::new(topic, "automatically discovered".to_owned())?)?;
    assert_eq!(
        receiver.recv_timeout(std::time::Duration::from_secs(3))?,
        "automatically discovered"
    );
    drop(subscription);
    Ok(())
}

struct Utf8Codec(ContentType);

impl EventCodec<String> for Utf8Codec {
    fn content_type(&self) -> &ContentType {
        &self.0
    }
    fn schema_id(&self) -> Option<&qubit_event_bus::model::SchemaId> {
        None
    }
    fn encode(&self, value: &String) -> Result<std::sync::Arc<[u8]>, CodecError> {
        Ok(std::sync::Arc::from(value.as_bytes()))
    }
    fn decode(&self, bytes: &[u8]) -> Result<String, CodecError> {
        String::from_utf8(bytes.to_vec()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}

#[test]
fn test_async_registry_discovers_and_creates_redis_provider() -> Result<(), Box<dyn std::error::Error>> {
    let _ = std::any::type_name::<AsyncRedisEventBusProvider>();
    let server = RedisServer::start()?;
    let registry = AsyncEventBusRegistry::discover()?;
    assert!(registry.provider_ids().iter().any(|id| id.as_str() == "redis-streams"));
    let config = redis_config(server.url()).with_selection(ProviderSelection::named("redis-streams")?);
    let _bus = futures_lite::future::block_on(registry.create(&config))?;
    Ok(())
}

fn redis_config(url: &str) -> EventBusConfig {
    let options: ProviderOptions = [
        ("redis.url".into(), url.into()),
        ("redis.namespace".into(), "discovery".into()),
    ]
    .into();
    EventBusConfig::default().with_provider_options(options)
}
