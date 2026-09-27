// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous provider factory and SPI implementation.

use std::sync::Arc;

use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::registry::EventBusSpec;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::OrderingCapability;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_event_bus::spi::PublishVisibility;
use qubit_event_bus::spi::ReplayCapability;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_spi::AsyncServiceProvider;
use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderFuture;
use qubit_spi::ProviderMetadata;
use qubit_spi::error::ProviderFailure;

use super::subscription::AsyncRedisSubscription;
use crate::client::RedisClient;
use crate::config::RedisEventBusConfig;
use crate::error::RedisProviderError;
use crate::naming::stream_key;
use crate::wire::WireFields;

/// Registered runtime-neutral asynchronous Redis Streams backend.
pub struct AsyncRedisEventBusProvider;

impl ProviderMetadata for AsyncRedisEventBusProvider {
    /// Returns the provider's stable SPI registration descriptor.
    fn descriptor(&self) -> ProviderDescriptor {
        qubit_spi::provider_descriptor!("redis-streams")
    }
}

impl AsyncServiceProvider<EventBusSpec> for AsyncRedisEventBusProvider {
    /// Creates an asynchronous Redis backend without selecting an executor.
    fn create_configured<'a>(
        &'a self,
        config: &'a EventBusConfig,
    ) -> ProviderFuture<'a, Result<Arc<dyn AsyncEventBusSpi>, ProviderFailure<qubit_event_bus::EventBusProviderError>>>
    {
        Box::pin(async move {
            let settings = RedisEventBusConfig::from_event_bus_config(config).map_err(|error| {
                ProviderFailure::invalid_configuration(qubit_event_bus::EventBusProviderError::provider(error))
            })?;
            let client = RedisClient::new(&settings).map_err(|_| {
                ProviderFailure::invalid_configuration(qubit_event_bus::EventBusProviderError::provider(
                    RedisProviderError::Configuration("invalid Redis connection configuration"),
                ))
            })?;
            Ok(Arc::new(AsyncRedisEventBus {
                client: Arc::new(client),
                settings,
            }) as Arc<dyn AsyncEventBusSpi>)
        })
    }
}

/// Redis client and namespace shared by asynchronous SPI calls.
struct AsyncRedisEventBus {
    /// Client used to create async multiplexed connections.
    client: Arc<RedisClient>,
    /// Validated non-secret settings.
    settings: RedisEventBusConfig,
}

impl AsyncEventBusSpi for AsyncRedisEventBus {
    /// Returns the capability declaration shared with the synchronous SPI.
    fn capabilities(&self) -> EventBusCapabilities {
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

    /// Publishes an encoded payload using Redis `XADD`.
    fn publish<'a>(
        &'a self,
        message: OutboundMessage,
    ) -> SpiFuture<'a, Result<qubit_event_bus::model::PublishAcknowledgement, qubit_event_bus::SpiError>> {
        Box::pin(async move {
            let topic = message.topic().clone();
            let fields =
                WireFields::from_outbound(&message).map_err(|error| spi_error("publish", Some(&topic), error))?;
            let key = stream_key(self.settings.namespace(), topic.as_str());
            let payload = serde_json::to_string(&fields)
                .map_err(|_| spi_error("publish", Some(&topic), RedisProviderError::Operation("encode message")))?;
            let mut connection = self
                .client
                .get_async_connection()
                .await
                .map_err(|_| spi_error("publish", Some(&topic), RedisProviderError::Operation("connect")))?;
            let message_id: String = redis::cmd("XADD")
                .arg(key)
                .arg("*")
                .arg("wire")
                .arg(payload)
                .query_async(&mut connection)
                .await
                .map_err(|_| spi_error("publish", Some(&topic), RedisProviderError::Operation("XADD")))?;
            Ok(qubit_event_bus::model::PublishAcknowledgement::Accepted {
                provider_message_id: Some(message_id),
                metadata: Default::default(),
            })
        })
    }

    /// Creates a group receiver and lets Redis PEL retain messages across
    /// cancellation.
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, qubit_event_bus::SpiError>> {
        Box::pin(async move {
            let topic = request.topic().clone();
            let key = stream_key(self.settings.namespace(), topic.as_str());
            let group = crate::naming::group_name(
                self.settings.namespace(),
                topic.as_str(),
                request.subscriber_id().as_str(),
                request.group().map(|value| value.as_str()),
            );
            let start = match request.start_position() {
                qubit_event_bus::model::StartPosition::Earliest => "0-0",
                qubit_event_bus::model::StartPosition::At(id) => id.as_ref(),
                _ => "$",
            };
            let mut connection = self
                .client
                .get_async_connection()
                .await
                .map_err(|_| spi_error("subscribe", Some(&topic), RedisProviderError::Operation("connect")))?;
            let result: Result<(), redis::RedisError> = redis::cmd("XGROUP")
                .arg("CREATE")
                .arg(&key)
                .arg(&group)
                .arg(start)
                .arg("MKSTREAM")
                .query_async(&mut connection)
                .await;
            if let Err(error) = result
                && error.code() != Some("BUSYGROUP")
            {
                return Err(spi_error(
                    "subscribe",
                    Some(&topic),
                    RedisProviderError::Operation("XGROUP CREATE"),
                ));
            }
            Ok(Box::new(AsyncRedisSubscription {
                client: Arc::clone(&self.client),
                key,
                group,
                consumer: request.subscription_id().to_string(),
                topic,
                subscription_id: request.subscription_id(),
                closed: false,
                claim_cursor: "0-0".to_owned(),
                claim_min_idle_ms: self.settings.claim_min_idle_ms(),
                max_unsettled: self.settings.max_unsettled_per_subscription(),
                outstanding: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }

    /// Completes shutdown without acknowledging unsettled deliveries.
    fn shutdown<'a>(
        &'a self,
        _mode: ShutdownMode,
    ) -> SpiFuture<'a, Result<ShutdownOutcome, qubit_event_bus::SpiError>> {
        Box::pin(async { Ok(ShutdownOutcome::Complete) })
    }
}

/// Wraps an internal failure without exposing raw Redis diagnostics.
fn spi_error(
    operation: &'static str,
    topic: Option<&TopicAddress>,
    source: RedisProviderError,
) -> qubit_event_bus::SpiError {
    qubit_event_bus::SpiError::Operation {
        provider_id: "redis-streams".into(),
        operation,
        resource: topic.map(|value| value.as_str().into()),
        kind: "redis_error",
        retryable: Some(true),
        source: Box::new(source),
    }
}
