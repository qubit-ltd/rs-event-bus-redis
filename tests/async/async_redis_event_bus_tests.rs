// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public capabilities and shutdown through lazy provider creation.

use std::any::TypeId;
use std::sync::Arc;
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
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::SubscriptionModes;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
use qubit_id::Id;
use qubit_spi::AsyncServiceProvider;

#[test]
fn test_capabilities_and_shutdown_are_available_without_redis() {
    block_on(async {
        let bus = AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default())
            .await
            .expect("default settings create a lazy Redis SPI");

        assert_eq!(bus.capabilities().payload_modes(), PayloadModes::Encoded);
        assert_eq!(bus.capabilities().subscription_modes(), SubscriptionModes::DURABLE);
        assert_eq!(
            bus.shutdown(ShutdownMode::Immediate).await.expect("shutdown succeeds"),
            ShutdownOutcome::Complete
        );
    });
}

#[test]
fn test_connection_failures_are_returned_for_publish_and_subscribe() {
    block_on(async {
        let options: ProviderOptions = [
            ("redis.url".into(), "redis://127.0.0.1:1/".into()),
            ("redis.namespace".into(), "connection-errors".into()),
        ]
        .into();
        let bus = AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(options))
            .await
            .expect("unreachable endpoint builds a lazy public bus");
        let native_message = OutboundMessage::new(
            TopicAddress::new("native").expect("topic is valid"),
            EventId::new("native-event").expect("event ID is valid"),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            None,
            TransportPayload::Native(Arc::new(7_u8)),
        );
        assert!(bus.publish(native_message).await.is_err());
        let topic = TopicAddress::new("events").expect("topic is valid");
        let message = OutboundMessage::new(
            topic.clone(),
            EventId::new("event-1").expect("event ID is valid"),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            None,
            TransportPayload::Encoded(EncodedPayload::new(
                Arc::from(vec![1_u8]),
                ContentType::new("application/octet-stream").expect("content type is valid"),
                None,
            )),
        );
        let request = SpiSubscriptionRequest::new(
            Id::new(1),
            topic,
            SubscriberId::new("worker").expect("subscriber ID is valid"),
            Some(ConsumerGroup::new("workers").expect("group name is valid")),
            SubscriptionDurability::Durable,
            StartPosition::Earliest,
            ProviderOptions::new(),
            TypeId::of::<Vec<u8>>(),
        );

        assert!(bus.publish(message).await.is_err());
        assert!(bus.subscribe(request).await.is_err());
    });
}
