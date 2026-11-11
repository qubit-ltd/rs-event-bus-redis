// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::time::SystemTime;

use futures_lite::future::block_on;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_spi::AsyncServiceProvider;
use qubit_spi::ProviderMetadata;
use qubit_spi::ServiceProvider;

use crate::r#async::AsyncRedisEventBusProvider;
use crate::sync::RedisEventBusProvider;
use crate::tests::support::redis_support::scripted_redis::ScriptedRedis;
use crate::tests::support::redis_support::scripted_redis::Step;

fn publish_options(url: &str, namespace: &str) -> EventBusConfig {
    let options: ProviderOptions = [
        ("redis.url".into(), url.into()),
        ("redis.namespace".into(), namespace.into()),
        ("redis.max_concurrent_commands".into(), "2".into()),
        ("redis.reserved_settlement_commands".into(), "1".into()),
        ("redis.max_idle_connections".into(), "1".into()),
    ]
    .into();
    EventBusConfig::default().with_provider_options(options)
}

fn publish_message() -> OutboundMessage {
    OutboundMessage::new(
        TopicAddress::new("events").expect("valid topic"),
        EventId::new("single-permit").expect("valid event ID"),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(&b"payload"[..]),
            ContentType::new("application/octet-stream").expect("valid content type"),
            None,
        )),
    )
}

#[test]
fn test_sync_and_async_provider_descriptors_and_factories() {
    let sync_provider = RedisEventBusProvider;
    let async_provider = AsyncRedisEventBusProvider;
    let sync_descriptor = sync_provider.descriptor();
    let async_descriptor = async_provider.descriptor();
    assert_eq!(
        format!("{sync_descriptor:?}"),
        format!("{async_descriptor:?}")
    );

    sync_provider
        .create_configured(&EventBusConfig::default())
        .expect("create lazy sync provider");
    block_on(async_provider.create_configured(&EventBusConfig::default()))
        .expect("create lazy async provider");

    let invalid_options: ProviderOptions = [("redis.unknown".into(), "invalid".into())].into();
    let invalid_config = EventBusConfig::default().with_provider_options(invalid_options);
    assert!(sync_provider.create_configured(&invalid_config).is_err());
    assert!(block_on(async_provider.create_configured(&invalid_config)).is_err());
}

#[test]
fn test_sync_publish_sends_one_encoded_command_with_one_general_slot() {
    let server = ScriptedRedis::start(vec![
        Step::reply("XADD", b"$3\r\n1-0\r\n"),
        Step::reply("XADD", b"$3\r\n2-0\r\n"),
    ])
    .expect("scripted Redis starts");
    let bus = RedisEventBusProvider
        .create_configured(&publish_options(server.url(), "single-permit-sync"))
        .expect("create sync bus");

    for _ in 0..2 {
        crate::wire_fields::reset_encode_count();
        let _ = bus.publish(publish_message()).expect("XADD is accepted");
        assert_eq!(crate::wire_fields::encode_count(), 1);
    }

    let commands = server.finish();
    assert_eq!(commands.len(), 2);
    assert!(
        commands
            .iter()
            .all(|command| command.first().is_some_and(|name| name == "XADD"))
    );
}

#[test]
fn test_async_publish_sends_one_encoded_command_with_one_general_slot() {
    block_on(async {
        let server = ScriptedRedis::start(vec![
            Step::reply("XADD", b"$3\r\n1-0\r\n"),
            Step::reply("XADD", b"$3\r\n2-0\r\n"),
        ])
        .expect("scripted Redis starts");
        let bus = AsyncRedisEventBusProvider
            .create_configured(&publish_options(server.url(), "single-permit-async"))
            .await
            .expect("create async bus");

        for _ in 0..2 {
            crate::wire_fields::reset_encode_count();
            let _ = bus
                .publish(publish_message())
                .await
                .expect("XADD is accepted");
            assert_eq!(crate::wire_fields::encode_count(), 1);
        }

        let commands = server.finish();
        assert_eq!(commands.len(), 2);
        assert!(
            commands
                .iter()
                .all(|command| command.first().is_some_and(|name| name == "XADD"))
        );
    });
}
