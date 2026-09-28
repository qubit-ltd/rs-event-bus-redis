// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests stable names and versioned wire-format round trips.

use std::any::TypeId;
use std::sync::Arc;
use std::time::SystemTime;

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::OrderingKey;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
#[cfg(feature = "async")]
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
use qubit_event_bus_redis::config::RedisEventBusConfig;
use qubit_event_bus_redis::naming::group_name;
use qubit_event_bus_redis::naming::stream_key;
#[cfg(feature = "sync")]
use qubit_event_bus_redis::sync::RedisEventBusProvider;
use qubit_event_bus_redis::wire::WireFields;
use qubit_spi::ProviderMetadata;

#[test]
fn test_provider_descriptors_keep_the_stable_redis_streams_id() {
    #[cfg(feature = "sync")]
    assert_eq!(RedisEventBusProvider.descriptor().id().as_str(), "redis-streams");
    #[cfg(feature = "async")]
    assert_eq!(AsyncRedisEventBusProvider.descriptor().id().as_str(), "redis-streams");
}

#[test]
fn test_stream_key_delimits_components_without_collisions() {
    assert_ne!(stream_key("app", "a:b"), stream_key("app:a", "b"));
}

#[test]
fn test_consumer_group_name_is_stable_for_same_identity() {
    assert_eq!(
        group_name("app", "events", "worker", None),
        group_name("app", "events", "worker", None)
    );
    assert_ne!(
        group_name("app", "events", "worker", None),
        group_name("app", "events", "worker", Some("blue"))
    );
}

#[test]
fn test_encoded_payload_round_trips_binary_bytes() -> Result<(), Box<dyn std::error::Error>> {
    let original = vec![0, 1, 127, 128, 255];
    let message = OutboundMessage::new(
        TopicAddress::new("events")?,
        EventId::new("event-1")?,
        SystemTime::now(),
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(original.clone()),
            ContentType::new("application/octet-stream")?,
            None,
        )),
    );
    let fields = WireFields::from_outbound(&message)?;
    let (_, _, _, _, _, TransportPayload::Encoded(payload)) = fields.into_parts(TopicAddress::new("events")?)? else {
        panic!("wire decoder must preserve encoded payloads");
    };
    assert_eq!(payload.bytes(), original);
    Ok(())
}

#[test]
fn test_wire_fields_preserve_optional_metadata_and_timestamp() -> Result<(), Box<dyn std::error::Error>> {
    let message = OutboundMessage::new(
        TopicAddress::new("events")?,
        EventId::new("event-before-epoch")?,
        SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1),
        Headers::new(),
        Some(OrderingKey::new("partition-1").ok_or("valid ordering key was rejected")?),
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(&b"payload"[..]),
            ContentType::new("application/octet-stream")?,
            Some(SchemaId::new("schema-v1")?),
        )),
    );

    let fields = WireFields::from_outbound(&message)?;
    assert_eq!(fields.timestamp_ms, 1_000);
    assert_eq!(fields.ordering_key.as_deref(), Some("partition-1"));
    assert_eq!(fields.schema_id.as_deref(), Some("schema-v1"));

    let (topic, event_id, timestamp, _, ordering_key, TransportPayload::Encoded(payload)) =
        fields.into_parts(TopicAddress::new("events")?)?
    else {
        panic!("wire decoder must preserve encoded payloads");
    };
    assert_eq!(topic.as_str(), "events");
    assert_eq!(event_id.as_str(), "event-before-epoch");
    assert_eq!(timestamp, SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1));
    assert_eq!(
        ordering_key.map(|key| key.as_str().to_owned()).as_deref(),
        Some("partition-1")
    );
    assert_eq!(payload.schema_id().map(|schema| schema.as_str()), Some("schema-v1"));
    Ok(())
}

#[test]
fn test_wire_encoder_rejects_pre_epoch_timestamps() -> Result<(), Box<dyn std::error::Error>> {
    let message = OutboundMessage::new(
        TopicAddress::new("events")?,
        EventId::new("event-before-epoch")?,
        SystemTime::UNIX_EPOCH - std::time::Duration::from_secs(1),
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(&b"payload"[..]),
            ContentType::new("application/octet-stream")?,
            None,
        )),
    );
    assert!(WireFields::from_outbound(&message).is_err());
    Ok(())
}

#[test]
fn test_config_debug_redacts_password_in_url() {
    let error = RedisEventBusConfig::new("redis://user:test-secret@127.0.0.1/", "test").unwrap_err();
    assert!(!error.to_string().contains("test-secret"));
}

#[test]
fn test_unknown_wire_version_is_rejected() {
    let fields = WireFields {
        version: 999,
        event_id: "event-1".into(),
        timestamp_ms: 0,
        headers_json: "{}".into(),
        ordering_key: None,
        content_type: "application/octet-stream".into(),
        schema_id: None,
        payload: Vec::new(),
    };
    assert!(
        fields
            .into_parts(TopicAddress::new("events").expect("valid topic"))
            .is_err()
    );
}

#[test]
fn test_provider_options_validate_boundaries_and_sentinel_pairing() {
    for options in [
        [("redis.url".into(), "not a redis URL".into())].into(),
        [("redis.namespace".into(), "".into())].into(),
        [("redis.namespace".into(), "bad\nnamespace".into())].into(),
        [("redis.max_unsettled_per_subscription".into(), "0".into())].into(),
        [("redis.max_unsettled_per_subscription".into(), "10001".into())].into(),
        [("redis.max_unsettled_per_subscription".into(), "NaN".into())].into(),
        [("redis.max_idle_connections".into(), "0".into())].into(),
        [("redis.max_idle_connections".into(), "65".into())].into(),
        [("redis.recovery_interval_ms".into(), "49".into())].into(),
        [("redis.recovery_interval_ms".into(), "60001".into())].into(),
        [("redis.stream_maxlen_approx".into(), "0".into())].into(),
        [("redis.stream_maxlen_approx".into(), "nope".into())].into(),
        [("redis.claim_min_idle_ms".into(), "forever".into())].into(),
        [("redis.sentinel.nodes".into(), " , ".into())].into(),
        [("redis.sentinel.service_name".into(), "master".into())].into(),
        [("redis.sentinel.nodes".into(), "127.0.0.1:26379".into())].into(),
        [("redis.url".into(), "redis://user:secret@localhost/".into())].into(),
    ] {
        assert!(RedisEventBusConfig::from_provider_options(&options).is_err());
    }
}

#[test]
fn test_provider_options_defaults_and_sentinel_credentials_are_redacted() {
    let defaults = RedisEventBusConfig::from_provider_options(&ProviderOptions::new()).unwrap();
    assert_eq!(defaults.connection_url(), "redis://127.0.0.1/");
    assert_eq!(defaults.namespace(), "qubit");
    assert_eq!(defaults.max_idle_connections(), 8);
    assert_eq!(defaults.recovery_interval_ms(), 1_000);
    assert_eq!(defaults.stream_maxlen_approx(), None);
    let options: ProviderOptions = [
        ("redis.sentinel.nodes".into(), "127.0.0.1:26379, 127.0.0.1:26380".into()),
        ("redis.sentinel.service_name".into(), "primary".into()),
        ("redis.username_env".into(), "REDIS_TEST_MISSING_USER".into()),
    ]
    .into();
    assert!(RedisEventBusConfig::from_provider_options(&options).is_err());
    let options: ProviderOptions = [
        ("redis.sentinel.nodes".into(), "127.0.0.1:26379, 127.0.0.1:26380".into()),
        ("redis.sentinel.service_name".into(), "primary".into()),
    ]
    .into();
    let config = RedisEventBusConfig::from_provider_options(&options).unwrap();
    assert_eq!(config.sentinel_nodes().unwrap().len(), 2);
    assert_eq!(config.sentinel_service(), Some("primary"));
}

#[test]
fn stream_maxlen_approx_is_an_optional_positive_limit() {
    let options: ProviderOptions = [("redis.stream_maxlen_approx".into(), "4096".into())].into();
    let config = RedisEventBusConfig::from_provider_options(&options).unwrap();
    assert_eq!(config.stream_maxlen_approx().map(|value| value.get()), Some(4096));
}

#[test]
fn test_recovery_interval_defaults_and_validates_its_range() {
    let options: ProviderOptions = [("redis.recovery_interval_ms".into(), "50".into())].into();
    let config = RedisEventBusConfig::from_provider_options(&options).expect("50ms is a valid interval");
    assert_eq!(config.recovery_interval_ms(), 50);
    for value in ["49", "60001", "0", "overflow"] {
        let options: ProviderOptions = [("redis.recovery_interval_ms".into(), value.into())].into();
        assert!(RedisEventBusConfig::from_provider_options(&options).is_err());
    }
}

#[test]
fn test_config_default_values_are_explicit() {
    let config = RedisEventBusConfig::default();
    assert_eq!(config.connection_url(), "redis://127.0.0.1/");
    assert_eq!(config.namespace(), "qubit");
    assert_eq!(config.max_idle_connections(), 8);
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_provider_returns_invalid_configuration_without_connecting() {
    use qubit_spi::ServiceProvider;

    let options: ProviderOptions = [("redis.max_idle_connections".into(), "0".into())].into();
    let config = EventBusConfig::default().with_provider_options(options);
    assert!(RedisEventBusProvider.create_configured(&config).is_err());

    let options: ProviderOptions = [
        ("redis.sentinel.nodes".into(), "invalid:port/not-a-db".into()),
        ("redis.sentinel.service_name".into(), "primary".into()),
    ]
    .into();
    let config = EventBusConfig::default().with_provider_options(options);
    assert!(RedisEventBusProvider.create_configured(&config).is_err());
}

#[cfg(feature = "async")]
#[test]
fn test_async_provider_returns_invalid_configuration_without_connecting() {
    use futures_lite::future::block_on;
    use qubit_spi::AsyncServiceProvider;

    let options: ProviderOptions = [("redis.max_idle_connections".into(), "0".into())].into();
    let config = EventBusConfig::default().with_provider_options(options);
    assert!(block_on(AsyncRedisEventBusProvider.create_configured(&config)).is_err());

    let options: ProviderOptions = [
        ("redis.sentinel.nodes".into(), "invalid:port/not-a-db".into()),
        ("redis.sentinel.service_name".into(), "primary".into()),
    ]
    .into();
    let config = EventBusConfig::default().with_provider_options(options);
    assert!(block_on(AsyncRedisEventBusProvider.create_configured(&config)).is_err());
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_subscribe_rejects_invalid_stream_position_before_connecting() -> Result<(), Box<dyn std::error::Error>> {
    use qubit_spi::ServiceProvider;

    let bus = RedisEventBusProvider
        .create_configured(&EventBusConfig::default())
        .map_err(|failure| failure.into_error())?;
    let request = SpiSubscriptionRequest::new(
        qubit_id::Id::new(91_001),
        TopicAddress::new("invalid-position")?,
        SubscriberId::new("invalid-position-worker")?,
        None,
        SubscriptionDurability::Durable,
        StartPosition::At("not-a-stream-id".into()),
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    );
    assert!(bus.subscribe(request).is_err());
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_subscribe_rejects_invalid_stream_position_before_connecting() -> Result<(), Box<dyn std::error::Error>> {
    use futures_lite::future::block_on;
    use qubit_spi::AsyncServiceProvider;

    block_on(async {
        let bus = AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default())
            .await
            .map_err(|failure| failure.into_error())?;
        let request = SpiSubscriptionRequest::new(
            qubit_id::Id::new(91_002),
            TopicAddress::new("invalid-position")?,
            SubscriberId::new("invalid-position-async-worker")?,
            None,
            SubscriptionDurability::Durable,
            StartPosition::At("not-a-stream-id".into()),
            ProviderOptions::new(),
            TypeId::of::<Vec<u8>>(),
        );
        assert!(bus.subscribe(request).await.is_err());
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[test]
fn test_redis_and_sentinel_environment_credentials_stay_redacted() {
    let options: ProviderOptions = [
        ("redis.username_env".into(), "PATH".into()),
        ("redis.password_env".into(), "HOME".into()),
        ("redis.sentinel.nodes".into(), "127.0.0.1:26379".into()),
        ("redis.sentinel.service_name".into(), "primary".into()),
        ("redis.sentinel.username_env".into(), "PATH".into()),
        ("redis.sentinel.password_env".into(), "HOME".into()),
        ("redis.claim_min_idle_ms".into(), "125".into()),
        ("redis.max_unsettled_per_subscription".into(), "8".into()),
    ]
    .into();
    let config = RedisEventBusConfig::from_provider_options(&options).unwrap();
    let debug = format!("{config:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains(&std::env::var("PATH").unwrap()));
    assert!(!debug.contains(&std::env::var("HOME").unwrap()));
}

#[test]
fn test_wire_decoder_rejects_malformed_fields() -> Result<(), Box<dyn std::error::Error>> {
    let valid = WireFields {
        version: 1,
        event_id: "valid-id".into(),
        timestamp_ms: 0,
        headers_json: "{}".into(),
        ordering_key: None,
        content_type: "application/octet-stream".into(),
        schema_id: None,
        payload: vec![],
    };
    let topic = || TopicAddress::new("events").unwrap();
    let mut invalid_id = valid.clone();
    invalid_id.event_id.clear();
    assert!(invalid_id.into_parts(topic()).is_err());
    let mut invalid_headers = valid.clone();
    invalid_headers.headers_json = "[".into();
    assert!(invalid_headers.into_parts(topic()).is_err());
    let mut invalid_type = valid.clone();
    invalid_type.content_type = "not a MIME type".into();
    assert!(invalid_type.into_parts(topic()).is_err());
    let mut invalid_schema = valid;
    invalid_schema.schema_id = Some("bad\nschema".into());
    assert!(invalid_schema.into_parts(topic()).is_err());
    let mut overflowing_timestamp = WireFields {
        version: 1,
        event_id: "timestamp-overflow".into(),
        timestamp_ms: u64::MAX as u128 + 1,
        headers_json: "{}".into(),
        ordering_key: None,
        content_type: "application/octet-stream".into(),
        schema_id: None,
        payload: Vec::new(),
    };
    assert!(overflowing_timestamp.clone().into_parts(topic()).is_err());
    overflowing_timestamp.timestamp_ms = 0;
    overflowing_timestamp.ordering_key = Some(String::new());
    assert!(overflowing_timestamp.into_parts(topic()).is_err());
    Ok(())
}

#[test]
fn test_wire_encoder_rejects_native_payloads() -> Result<(), Box<dyn std::error::Error>> {
    let message = OutboundMessage::new(
        TopicAddress::new("events")?,
        EventId::new("native-event")?,
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        None,
        TransportPayload::Native(Arc::new(7_u8)),
    );
    assert!(WireFields::from_outbound(&message).is_err());
    Ok(())
}
