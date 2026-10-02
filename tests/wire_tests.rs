// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public byte-budget contracts observed through actual Redis stream writes.

mod support;
mod wire;

#[cfg(feature = "sync")]
use std::any::TypeId;
#[cfg(any(feature = "sync", feature = "async"))]
use std::error::Error;
use std::sync::Arc;
use std::time::SystemTime;

#[cfg(feature = "async")]
use futures_lite::future::block_on;
#[cfg(any(feature = "sync", feature = "async"))]
use qubit_event_bus::EventBusConfig;
#[cfg(any(feature = "sync", feature = "async"))]
use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
#[cfg(any(feature = "sync", feature = "async"))]
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishEffect;
#[cfg(any(feature = "sync", feature = "async"))]
use qubit_event_bus::model::SchemaId;
#[cfg(feature = "sync")]
use qubit_event_bus::model::StartPosition;
#[cfg(feature = "sync")]
use qubit_event_bus::model::SubscriberId;
#[cfg(feature = "sync")]
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::EncodedPayload;
#[cfg(any(feature = "sync", feature = "async"))]
use qubit_event_bus::spi::OrderingKey;
use qubit_event_bus::spi::OutboundMessage;
#[cfg(feature = "sync")]
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
#[cfg(feature = "async")]
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
#[cfg(any(feature = "sync", feature = "async"))]
use qubit_event_bus_redis::naming::stream_key;
#[cfg(feature = "sync")]
use qubit_event_bus_redis::sync::RedisEventBusProvider;
#[cfg(any(feature = "sync", feature = "async"))]
use qubit_event_bus_redis::wire::WireFields;
#[cfg(feature = "sync")]
use qubit_id::Id;
#[cfg(feature = "async")]
use qubit_spi::AsyncServiceProvider;
#[cfg(feature = "sync")]
use qubit_spi::ServiceProvider;
#[cfg(any(feature = "sync", feature = "async"))]
use redis::Client;
#[cfg(any(feature = "sync", feature = "async"))]
use redis::cmd;
#[cfg(any(feature = "sync", feature = "async"))]
use serde_json::to_vec;
#[cfg(any(feature = "sync", feature = "async"))]
use support::redis_server::RedisServer;

#[cfg(any(feature = "sync", feature = "async"))]
type TestResult = Result<(), Box<dyn Error>>;

/// Builds a fixed encoded event from `bytes` and `headers` without I/O;
/// returns the event, panicking only if fixed metadata is invalid.
fn message(bytes: &[u8], headers: Headers) -> OutboundMessage {
    OutboundMessage::new(
        TopicAddress::new("events").unwrap(),
        EventId::new("bounded-event").unwrap(),
        SystemTime::UNIX_EPOCH,
        headers,
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(bytes),
            ContentType::new("application/octet-stream").unwrap(),
            None,
        )),
    )
}

/// Builds settings for `server` and `namespace` with inclusive `payload` and
/// `wire` byte budgets; returns configuration without I/O.
#[cfg(any(feature = "sync", feature = "async"))]
fn config(server: &RedisServer, namespace: &str, payload: usize, wire: usize) -> EventBusConfig {
    let options: ProviderOptions = [
        ("redis.url".into(), server.url().into()),
        ("redis.namespace".into(), namespace.into()),
        ("redis.max_payload_bytes".into(), payload.to_string()),
        ("redis.max_wire_bytes".into(), wire.to_string()),
    ]
    .into();
    EventBusConfig::default().with_provider_options(options)
}

/// Asserts `error` reports non-retryable `expected` size category; panics on
/// any different SPI error or retry policy, without performing I/O.
#[cfg(any(feature = "sync", feature = "async"))]
fn assert_limit(error: SpiError, expected: &str) {
    match error {
        SpiError::Operation { kind, retryable, .. } => {
            assert_eq!(kind, expected);
            assert_eq!(retryable, Some(false));
        }
        SpiError::Publish {
            kind,
            retryable,
            effect,
            ..
        } => {
            assert_eq!(kind, expected);
            assert_eq!(retryable, Some(false));
            assert_eq!(effect, PublishEffect::NotAccepted);
        }
        error => panic!("expected byte-limit operation error, got {error:?}"),
    }
}

/// Reads the stream length for `server` and `namespace` through blocking
/// Redis I/O; returns connection, command, or conversion errors.
#[cfg(any(feature = "sync", feature = "async"))]
fn stream_len(server: &RedisServer, namespace: &str) -> Result<usize, Box<dyn Error>> {
    Ok(cmd("XLEN")
        .arg(stream_key(namespace, "events"))
        .query(&mut Client::open(server.url())?.get_connection()?)?)
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_payload_limit_is_inclusive_and_rejected_publish_writes_nothing() -> TestResult {
    let server = RedisServer::start()?;
    let bus = RedisEventBusProvider
        .create_configured(&config(&server, "size-sync-payload", 3, 4096))
        .map_err(|failure| failure.into_error())?;
    let _ = bus.publish(message(&[0, 128, 255], Headers::new()))?;
    let error = bus
        .publish(message(&[0, 128, 255, 1], Headers::new()))
        .expect_err("max + 1 is rejected");
    assert_limit(error, "payload_too_large");
    assert_eq!(
        stream_len(&server, "size-sync-payload")?,
        1,
        "oversized publish must send no XADD"
    );
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_payload_limit_is_inclusive_and_rejected_publish_writes_nothing() -> TestResult {
    block_on(async {
        let server = RedisServer::start()?;
        let bus = AsyncRedisEventBusProvider
            .create_configured(&config(&server, "size-async-payload", 3, 4096))
            .await
            .map_err(|failure| failure.into_error())?;
        let _ = bus.publish(message(&[0, 128, 255], Headers::new())).await?;
        assert_limit(
            bus.publish(message(&[0, 128, 255, 1], Headers::new()))
                .await
                .expect_err("max + 1 is rejected"),
            "payload_too_large",
        );
        assert_eq!(
            stream_len(&server, "size-async-payload")?,
            1,
            "oversized publish must send no XADD"
        );
        Ok(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_complete_wire_limit_is_inclusive_and_counts_escaped_headers() -> TestResult {
    let server = RedisServer::start()?;
    let headers: Headers = [("quoted".into(), "\\\"中文".repeat(128))].into();
    let outbound = message(&[255], headers.clone());
    let exact = to_vec(&WireFields::from_outbound(&outbound)?)?.len();
    let accepted = RedisEventBusProvider
        .create_configured(&config(&server, "size-sync-exact", 1, exact))
        .map_err(|failure| failure.into_error())?;
    let _ = accepted.publish(outbound)?;
    assert_eq!(stream_len(&server, "size-sync-exact")?, 1);
    let rejected = RedisEventBusProvider
        .create_configured(&config(&server, "size-sync-over", 1, exact - 1))
        .map_err(|failure| failure.into_error())?;
    assert_limit(
        rejected
            .publish(message(&[255], headers))
            .expect_err("wire max + 1 is rejected"),
        "wire_too_large",
    );
    assert_eq!(
        stream_len(&server, "size-sync-over")?,
        0,
        "oversized wire must send no XADD"
    );
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_complete_wire_limit_is_inclusive_and_counts_escaped_headers() -> TestResult {
    block_on(async {
        let server = RedisServer::start()?;
        let headers: Headers = [("quoted".into(), "\\\"中文".repeat(128))].into();
        let outbound = message(&[255], headers.clone());
        let exact = to_vec(&WireFields::from_outbound(&outbound)?)?.len();
        let accepted = AsyncRedisEventBusProvider
            .create_configured(&config(&server, "size-async-exact", 1, exact))
            .await
            .map_err(|failure| failure.into_error())?;
        let _ = accepted.publish(outbound).await?;
        assert_eq!(stream_len(&server, "size-async-exact")?, 1);
        let rejected = AsyncRedisEventBusProvider
            .create_configured(&config(&server, "size-async-over", 1, exact - 1))
            .await
            .map_err(|failure| failure.into_error())?;
        assert_limit(
            rejected
                .publish(message(&[255], headers))
                .await
                .expect_err("wire max + 1 is rejected"),
            "wire_too_large",
        );
        assert_eq!(
            stream_len(&server, "size-async-over")?,
            0,
            "oversized wire must send no XADD"
        );
        Ok(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_subscribe_stream_id_cursor_properties() -> TestResult {
    let server = RedisServer::start()?;
    let bus = RedisEventBusProvider
        .create_configured(&config(&server, "stream-id-properties", 1, 1024))
        .map_err(|failure| failure.into_error())?;
    let request = |index, cursor: &str| {
        SpiSubscriptionRequest::new(
            Id::new(index),
            TopicAddress::new("events").expect("topic"),
            SubscriberId::new(format!("worker-{index}")).expect("subscriber"),
            None,
            SubscriptionDurability::Durable,
            StartPosition::At(cursor.into()),
            ProviderOptions::new(),
            TypeId::of::<Vec<u8>>(),
        )
    };
    for (index, cursor) in ["0-0", "1-0", "0-1", "18446744073709551615-18446744073709551615"]
        .into_iter()
        .enumerate()
    {
        let mut receiver = bus.subscribe(request(index as u64 + 1, cursor))?;
        receiver.close()?;
    }
    for (index, cursor) in [
        "",
        "0",
        "0-",
        "-0",
        "1-2-3",
        "+1-0",
        "1- 0",
        "١-0",
        "18446744073709551616-0",
        "0-18446744073709551616",
    ]
    .into_iter()
    .enumerate()
    {
        assert!(
            bus.subscribe(request(index as u64 + 100, cursor)).is_err(),
            "invalid stream cursor {cursor:?}"
        );
    }
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_oversized_publish_has_zero_xadd_commands() -> TestResult {
    use support::scripted_redis::ScriptedRedis;
    for (payload, wire, bytes, expected) in [
        (1, 4096, vec![0, 1], "payload_too_large"),
        (1, 32, vec![0], "wire_too_large"),
    ] {
        let server = ScriptedRedis::start(Vec::new())?;
        let options: ProviderOptions = [
            ("redis.url".into(), server.url().into()),
            ("redis.max_payload_bytes".into(), payload.to_string()),
            ("redis.max_wire_bytes".into(), wire.to_string()),
        ]
        .into();
        let bus = RedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(options))
            .map_err(|failure| failure.into_error())?;
        assert_limit(
            bus.publish(message(&bytes, Headers::new()))
                .expect_err("oversized message"),
            expected,
        );
        assert!(
            server.finish().is_empty(),
            "size rejection must send exactly zero business commands"
        );
    }
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_oversized_publish_has_zero_xadd_commands() -> TestResult {
    use support::scripted_redis::ScriptedRedis;
    block_on(async {
        for (payload, wire, bytes, expected) in [
            (1, 4096, vec![0, 1], "payload_too_large"),
            (1, 32, vec![0], "wire_too_large"),
        ] {
            let server = ScriptedRedis::start(Vec::new())?;
            let options: ProviderOptions = [
                ("redis.url".into(), server.url().into()),
                ("redis.max_payload_bytes".into(), payload.to_string()),
                ("redis.max_wire_bytes".into(), wire.to_string()),
            ]
            .into();
            let bus = AsyncRedisEventBusProvider
                .create_configured(&EventBusConfig::default().with_provider_options(options))
                .await
                .map_err(|failure| failure.into_error())?;
            assert_limit(
                bus.publish(message(&bytes, Headers::new()))
                    .await
                    .expect_err("oversized message"),
                expected,
            );
            assert!(
                server.finish().is_empty(),
                "size rejection must send exactly zero business commands"
            );
        }
        Ok(())
    })
}

/// Builds one large legal metadata `field` with a one-byte payload; returns
/// an event without I/O, panicking if fixed legal metadata is rejected.
#[cfg(any(feature = "sync", feature = "async"))]
fn long_metadata_message(field: &str) -> OutboundMessage {
    let long = "x".repeat(1024 * 1024);
    let content_type = if field == "content_type" {
        ContentType::new(&format!("application/{long}")).expect("long legal content type")
    } else {
        ContentType::new("application/octet-stream").expect("content type")
    };
    let schema_id = (field == "schema_id").then(|| SchemaId::new(&long).expect("long legal schema"));
    let ordering_key = (field == "ordering_key").then(|| OrderingKey::new(&long).expect("long legal ordering key"));
    OutboundMessage::new(
        TopicAddress::new("events").expect("topic"),
        EventId::new("metadata-limit").expect("id"),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        ordering_key,
        None,
        TransportPayload::Encoded(EncodedPayload::new(Arc::from(&[255][..]), content_type, schema_id)),
    )
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_large_legal_metadata_rejects_before_any_xadd() -> TestResult {
    use support::scripted_redis::ScriptedRedis;
    for field in ["content_type", "schema_id", "ordering_key"] {
        let server = ScriptedRedis::start(Vec::new())?;
        let options: ProviderOptions = [
            ("redis.url".into(), server.url().into()),
            ("redis.max_payload_bytes".into(), "1".into()),
            ("redis.max_wire_bytes".into(), "512".into()),
        ]
        .into();
        let bus = RedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(options))
            .map_err(|failure| failure.into_error())?;
        assert_limit(
            bus.publish(long_metadata_message(field))
                .expect_err("metadata exceeds wire budget"),
            "wire_too_large",
        );
        assert!(
            server.finish().is_empty(),
            "large {field} must send zero business commands"
        );
    }
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_large_legal_metadata_rejects_before_any_xadd() -> TestResult {
    use support::scripted_redis::ScriptedRedis;
    block_on(async {
        for field in ["content_type", "schema_id", "ordering_key"] {
            let server = ScriptedRedis::start(Vec::new())?;
            let options: ProviderOptions = [
                ("redis.url".into(), server.url().into()),
                ("redis.max_payload_bytes".into(), "1".into()),
                ("redis.max_wire_bytes".into(), "512".into()),
            ]
            .into();
            let bus = AsyncRedisEventBusProvider
                .create_configured(&EventBusConfig::default().with_provider_options(options))
                .await
                .map_err(|failure| failure.into_error())?;
            assert_limit(
                bus.publish(long_metadata_message(field))
                    .await
                    .expect_err("metadata exceeds wire budget"),
                "wire_too_large",
            );
            assert!(
                server.finish().is_empty(),
                "large {field} must send zero business commands"
            );
        }
        Ok(())
    })
}
