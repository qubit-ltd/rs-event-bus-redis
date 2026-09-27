// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Synchronous provider factory and SPI implementation.

use std::sync::Arc;

use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::registry::EventBusSpec;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::OrderingCapability;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_event_bus::spi::PublishVisibility;
use qubit_event_bus::spi::ReplayCapability;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderMetadata;
use qubit_spi::ServiceProvider;
use qubit_spi::error::ProviderFailure;

use super::subscription::RedisSubscription;
use crate::client::RedisClient;
use crate::config::RedisEventBusConfig;
use crate::error::RedisProviderError;
use crate::naming::stream_key;
use crate::wire::WireFields;

/// Registered synchronous Redis Streams backend.
pub struct RedisEventBusProvider;

impl ProviderMetadata for RedisEventBusProvider {
    /// Returns the provider's stable SPI registration descriptor.
    fn descriptor(&self) -> ProviderDescriptor {
        qubit_spi::provider_descriptor!("redis-streams")
    }
}

impl ServiceProvider<EventBusSpec> for RedisEventBusProvider {
    /// Creates the Redis backend from facade provider options.
    fn create_configured(
        &self,
        config: &EventBusConfig,
    ) -> Result<Arc<dyn EventBusSpi>, ProviderFailure<qubit_event_bus::EventBusProviderError>> {
        let settings = RedisEventBusConfig::from_event_bus_config(config).map_err(|error| {
            ProviderFailure::invalid_configuration(qubit_event_bus::EventBusProviderError::provider(error))
        })?;
        let client = RedisClient::new(&settings).map_err(|_| {
            ProviderFailure::invalid_configuration(qubit_event_bus::EventBusProviderError::provider(
                RedisProviderError::Configuration("invalid Redis connection configuration"),
            ))
        })?;
        Ok(Arc::new(RedisEventBus {
            client: Arc::new(client),
            settings,
        }))
    }
}

/// A connected configuration and client shared by publisher and subscribers.
pub(crate) struct RedisEventBus {
    client: Arc<RedisClient>,
    settings: RedisEventBusConfig,
}

impl EventBusSpi for RedisEventBus {
    /// Returns the capability declaration shared by both provider modes.
    fn capabilities(&self) -> EventBusCapabilities {
        redis_capabilities()
    }

    /// Appends one encoded event to its Redis stream.
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, qubit_event_bus::error::SpiError> {
        let fields =
            WireFields::from_outbound(&message).map_err(|error| spi_error("publish", Some(message.topic()), error))?;
        let key = stream_key(self.settings.namespace(), message.topic().as_str());
        let payload = serde_json::to_string(&fields).map_err(|_| {
            spi_error(
                "publish",
                Some(message.topic()),
                RedisProviderError::Operation("encode message"),
            )
        })?;
        let mut connection = self.client.get_connection().map_err(|_| {
            spi_error(
                "publish",
                Some(message.topic()),
                RedisProviderError::Operation("connect"),
            )
        })?;
        let message_id: String = redis::cmd("XADD")
            .arg(&key)
            .arg("*")
            .arg("wire")
            .arg(payload)
            .query(&mut connection)
            .map_err(|_| spi_error("publish", Some(message.topic()), RedisProviderError::Operation("XADD")))?;
        Ok(PublishAcknowledgement::Accepted {
            provider_message_id: Some(message_id),
            metadata: Default::default(),
        })
    }

    /// Creates a consumer group and returns its blocking receiver.
    fn subscribe(
        &self,
        request: SpiSubscriptionRequest,
    ) -> Result<Box<dyn EventSubscriptionSpi>, qubit_event_bus::error::SpiError> {
        let key = stream_key(self.settings.namespace(), request.topic().as_str());
        let group = crate::naming::group_name(
            self.settings.namespace(),
            request.topic().as_str(),
            request.subscriber_id().as_str(),
            request.group().map(|value| value.as_str()),
        );
        let start = match request.start_position() {
            qubit_event_bus::model::StartPosition::Earliest => "0-0",
            qubit_event_bus::model::StartPosition::At(id) => id,
            qubit_event_bus::model::StartPosition::New => "$",
            _ => "$",
        };
        let mut connection = self.client.get_connection().map_err(|_| {
            spi_error(
                "subscribe",
                Some(request.topic()),
                RedisProviderError::Operation("connect"),
            )
        })?;
        let result: Result<(), redis::RedisError> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&key)
            .arg(&group)
            .arg(start)
            .arg("MKSTREAM")
            .query(&mut connection);
        if let Err(error) = result
            && error.code() != Some("BUSYGROUP")
        {
            return Err(spi_error(
                "subscribe",
                Some(request.topic()),
                RedisProviderError::Operation("XGROUP CREATE"),
            ));
        }
        let consumer = request.subscription_id().to_string();
        Ok(Box::new(RedisSubscription {
            client: Arc::clone(&self.client),
            key,
            group,
            consumer,
            topic: request.topic().clone(),
            subscription_id: request.subscription_id(),
            closed: false,
            claim_cursor: "0-0".to_owned(),
            claim_min_idle_ms: self.settings.claim_min_idle_ms(),
            max_unsettled: self.settings.max_unsettled_per_subscription(),
            outstanding: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }))
    }

    /// Closes the SPI without acknowledging any outstanding message.
    fn shutdown(&self, _mode: ShutdownMode) -> Result<ShutdownOutcome, qubit_event_bus::error::SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

/// Returns the capabilities supported by the Redis Streams implementation.
pub(crate) const fn redis_capabilities() -> EventBusCapabilities {
    EventBusCapabilities::new(
        PayloadModes::Encoded,
        SettlementCapabilities::AcceptRetryReject,
        OrderingCapability::None,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Durable,
        true,
        ReplayCapability::Position,
        PublishGuarantee::Accepted,
        PublishVisibility::Opaque,
    )
}

/// Wraps an internal failure without preserving potentially secret Redis
/// diagnostics.
pub(crate) fn spi_error(
    operation: &'static str,
    topic: Option<&TopicAddress>,
    source: RedisProviderError,
) -> qubit_event_bus::error::SpiError {
    qubit_event_bus::error::SpiError::Operation {
        provider_id: "redis-streams".into(),
        operation,
        resource: topic.map(|value| value.as_str().into()),
        kind: "redis_error",
        retryable: Some(true),
        source: Box::new(source),
    }
}
