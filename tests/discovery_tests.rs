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

use std::any::type_name;
use std::error::Error;
#[cfg(feature = "sync")]
use std::path::Path;
#[cfg(feature = "sync")]
use std::process::Command;
#[cfg(feature = "sync")]
use std::sync::Arc;
#[cfg(feature = "sync")]
use std::sync::mpsc::channel;
#[cfg(feature = "sync")]
use std::time::Duration;

#[cfg(feature = "async")]
use futures_lite::future::block_on;
#[cfg(feature = "async")]
use qubit_event_bus::AsyncEventBusRegistry;
use qubit_event_bus::EventBusConfig;
#[cfg(feature = "sync")]
use qubit_event_bus::EventBusRegistry;
#[cfg(feature = "sync")]
use qubit_event_bus::SubscriberId;
#[cfg(feature = "sync")]
use qubit_event_bus::codec::CodecRegistry;
#[cfg(feature = "sync")]
use qubit_event_bus::codec::EventCodec;
#[cfg(feature = "sync")]
use qubit_event_bus::error::CodecError;
#[cfg(feature = "sync")]
use qubit_event_bus::facade::EventBusFacadeConfig;
#[cfg(feature = "sync")]
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::ProviderOptions;
#[cfg(feature = "sync")]
use qubit_event_bus::model::PublishRequest;
#[cfg(feature = "sync")]
use qubit_event_bus::model::SchemaId;
#[cfg(feature = "sync")]
use qubit_event_bus::model::StartPosition;
#[cfg(feature = "sync")]
use qubit_event_bus::model::SubscribeRequest;
#[cfg(feature = "sync")]
use qubit_event_bus::model::SubscriptionDurability;
#[cfg(feature = "sync")]
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::EncodedPayload;
#[cfg(all(feature = "async", feature = "conformance"))]
use qubit_event_bus::spi::conformance::AsyncConformanceHooks;
#[cfg(all(feature = "sync", feature = "conformance"))]
use qubit_event_bus::spi::conformance::ConformanceHooks;
#[cfg(all(feature = "async", feature = "conformance"))]
use qubit_event_bus::spi::conformance::run_async;
#[cfg(all(feature = "sync", feature = "conformance"))]
use qubit_event_bus::spi::conformance::run_sync;
#[cfg(feature = "async")]
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
#[cfg(feature = "sync")]
use qubit_event_bus_redis::sync::RedisEventBusProvider;
#[cfg(all(feature = "async", feature = "conformance"))]
use qubit_spi::AsyncServiceProvider;
use qubit_spi::ProviderSelection;
#[cfg(all(feature = "sync", feature = "conformance"))]
use qubit_spi::ServiceProvider;
use support::redis_server::RedisServer;

#[test]
#[cfg(feature = "sync")]
fn test_sync_registry_discovers_and_creates_redis_provider() -> Result<(), Box<dyn Error>> {
    let _ = type_name::<RedisEventBusProvider>();
    let server = RedisServer::start()?;
    let registry = EventBusRegistry::discover()?;
    assert!(registry.provider_ids().iter().any(|id| id.as_str() == "redis-streams"));
    let mut codecs = CodecRegistry::new();
    codecs.register::<String>(Arc::new(Utf8Codec(ContentType::new("text/plain")?)));
    let facade = EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs));
    let config = redis_config(server.url())
        .with_selection(ProviderSelection::named("redis-streams")?)
        .with_facade_config(facade);
    let bus = registry.create(&config)?;
    let topic = Topic::<String>::new("discovery.events")?;
    let (sender, receiver) = channel();
    let subscription = bus.subscribe(
        SubscribeRequest::builder()
            .subscriber_id(SubscriberId::new("discovery-worker")?)
            .topic(topic.clone())
            .start_position(StartPosition::Earliest)
            .durability(SubscriptionDurability::Durable)
            .build()?,
        move |delivery| {
            let _ = sender.send(delivery.payload().clone());
        },
    )?;
    let _ = bus.publish(PublishRequest::new(topic, "automatically discovered".to_owned())?)?;
    assert_eq!(
        receiver.recv_timeout(Duration::from_secs(3))?,
        "automatically discovered"
    );
    #[cfg(feature = "conformance")]
    run_sync(
        || {
            RedisEventBusProvider
                .create_configured(&redis_config(server.url()))
                .unwrap()
        },
        &ConformanceHooks::default(),
    )
    .assert_all_passed();
    drop(subscription);
    Ok(())
}

#[cfg(feature = "sync")]
struct Utf8Codec(ContentType);

#[cfg(feature = "sync")]
impl EventCodec<String> for Utf8Codec {
    /// Returns the codec's configured content type without I/O.
    fn content_type(&self) -> &ContentType {
        &self.0
    }
    /// Returns None because this UTF-8 fixture has no schema.
    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }
    /// Copies `value` to owned UTF-8 bytes; returns success without I/O.
    fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from(value.as_bytes()))
    }
    /// Decodes `bytes` as UTF-8, returning invalid UTF-8 as a codec error.
    fn decode(&self, payload: &EncodedPayload) -> Result<String, CodecError> {
        String::from_utf8(payload.bytes().to_vec()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}

#[test]
#[cfg(feature = "async")]
fn test_async_registry_discovers_and_creates_redis_provider() -> Result<(), Box<dyn Error>> {
    let _ = type_name::<AsyncRedisEventBusProvider>();
    let server = RedisServer::start()?;
    let registry = AsyncEventBusRegistry::discover()?;
    assert!(registry.provider_ids().iter().any(|id| id.as_str() == "redis-streams"));
    let config = redis_config(server.url()).with_selection(ProviderSelection::named("redis-streams")?);
    let _bus = block_on(registry.create(&config))?;
    #[cfg(feature = "conformance")]
    {
        let server_url = server.url().to_owned();
        let report = block_on(run_async(
            || {
                let server_url = server_url.clone();
                async move {
                    AsyncRedisEventBusProvider
                        .create_configured(&redis_config(&server_url))
                        .await
                        .expect("Redis provider options are valid")
                }
            },
            &AsyncConformanceHooks::default(),
        ));
        report.assert_all_passed();
    }
    Ok(())
}

#[test]
#[cfg(feature = "sync")]
fn test_business_consumer_binary_links_provider_without_provider_type_imports() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let mut command = Command::new("cargo");
    command
        .arg("run")
        .arg("--locked")
        .arg("--quiet")
        .arg("--manifest-path")
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/business_consumer/Cargo.toml"))
        .arg("--")
        .arg(server.url());
    #[cfg(coverage)]
    command.env(
        "CARGO_TARGET_DIR",
        concat!(env!("CARGO_MANIFEST_DIR"), "/target/coverage-business-consumer"),
    );
    let output = command.output()?;
    assert!(
        output.status.success(),
        "business consumer fixture failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

/// Returns provider settings for `url` in the discovery namespace without I/O.
fn redis_config(url: &str) -> EventBusConfig {
    let options: ProviderOptions = [
        ("redis.url".into(), url.into()),
        ("redis.namespace".into(), "discovery".into()),
    ]
    .into();
    EventBusConfig::default().with_provider_options(options)
}
