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
            .subscriber_id(SubscriberId::new("discovery-worker")?)
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
    fn content_type(&self) -> &ContentType {
        &self.0
    }
    fn schema_id(&self) -> Option<&SchemaId> {
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
#[cfg(feature = "async")]
fn test_async_registry_discovers_and_creates_redis_provider() -> Result<(), Box<dyn std::error::Error>> {
    let _ = std::any::type_name::<AsyncRedisEventBusProvider>();
    let server = RedisServer::start()?;
    let registry = AsyncEventBusRegistry::discover()?;
    assert!(registry.provider_ids().iter().any(|id| id.as_str() == "redis-streams"));
    let config = redis_config(server.url()).with_selection(ProviderSelection::named("redis-streams")?);
    let _bus = futures_lite::future::block_on(registry.create(&config))?;
    #[cfg(feature = "conformance")]
    {
        let server_url = server.url().to_owned();
        let report = futures_lite::future::block_on(run_async(
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
fn test_business_consumer_binary_links_provider_without_provider_type_imports() -> Result<(), Box<dyn std::error::Error>>
{
    let server = RedisServer::start()?;
    let mut command = std::process::Command::new("cargo");
    command
        .arg("run")
        .arg("--quiet")
        .arg("--manifest-path")
        .arg("tests/fixtures/business_consumer/Cargo.toml")
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

fn redis_config(url: &str) -> EventBusConfig {
    let options: ProviderOptions = [
        ("redis.url".into(), url.into()),
        ("redis.namespace".into(), "discovery".into()),
    ]
    .into();
    EventBusConfig::default().with_provider_options(options)
}
