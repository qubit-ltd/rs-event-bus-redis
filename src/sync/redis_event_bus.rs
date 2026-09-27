// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Synchronous Redis Streams operations and secret-safe error conversion.

use std::sync::Arc;
use std::sync::Mutex;

use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriptionDurability;
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
use redis::RedisError;
use redis::cmd;

use super::subscription::Subscription;
use crate::client::Client;
use crate::config::RedisEventBusConfig;
use crate::error::RedisProviderError;
use crate::naming::group_name;
use crate::naming::poison_key;
use crate::naming::stream_key;
use crate::recovery::RecoveryState;
use crate::wire::WireFields;

/// Validated Redis settings and client shared by publishers and subscribers.
pub(super) struct RedisEventBus {
    /// Connection factory used by each blocking SPI call.
    pub(super) client: Arc<Client>,
    /// Settings used to derive keys and receiver limits.
    pub(super) settings: RedisEventBusConfig,
}

impl EventBusSpi for RedisEventBus {
    /// Declares the encoded, durable capabilities available through Redis
    /// Streams.
    ///
    /// # Returns
    ///
    /// The static capability set supported by this backend.
    fn capabilities(&self) -> EventBusCapabilities {
        redis_capabilities()
    }

    /// Appends one encoded event to its stream and returns Redis's stream ID.
    ///
    /// Acceptance means Redis replied successfully to `XADD`; it does not
    /// establish that the record was persisted to disk or processed. If the
    /// connection fails before the reply, the outcome may be unknown.
    ///
    /// # Parameters
    ///
    /// - `message`: Event with an encoded payload and transport metadata.
    ///
    /// # Returns
    ///
    /// An accepted acknowledgement containing the Redis stream ID.
    ///
    /// # Errors
    ///
    /// Returns an SPI error if the payload is not encoded, serialization fails,
    /// a connection cannot be opened, or Redis rejects `XADD`.
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
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
        let message_id: String = cmd("XADD")
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

    /// Creates the group if needed and returns a blocking stream receiver.
    ///
    /// Redis retains an existing group's cursor even if a later request asks
    /// for a different start position. Closing the receiver does not
    /// acknowledge its pending entries.
    ///
    /// # Parameters
    ///
    /// - `request`: Topic, group, start position, and identity for the
    ///   receiver.
    ///
    /// # Returns
    ///
    /// A boxed blocking subscription bound to the requested group.
    ///
    /// # Errors
    ///
    /// Returns an SPI error if Redis cannot connect or create the consumer
    /// group.
    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        let topic = request.topic().clone();
        if request.durability() != SubscriptionDurability::Durable {
            return Err(SpiError::Operation {
                provider_id: "redis-streams".into(),
                operation: "subscribe",
                resource: Some(topic.as_str().into()),
                kind: "unsupported_subscription_durability",
                retryable: Some(false),
                source: Box::new(RedisProviderError::Configuration(
                    "Redis Streams requires durable subscriptions",
                )),
            });
        }
        let key = stream_key(self.settings.namespace(), topic.as_str());
        let group = group_name(
            self.settings.namespace(),
            topic.as_str(),
            request.subscriber_id().as_str(),
            request.group().map(|value| value.as_str()),
        );
        let quarantine = poison_key(self.settings.namespace(), topic.as_str(), &group);
        let start = match request.start_position() {
            StartPosition::Earliest => "0-0",
            StartPosition::At(id) => id,
            StartPosition::New => "$",
            _ => "$",
        };
        if matches!(request.start_position(), StartPosition::At(id) if !valid_stream_id(id)) {
            return Err(SpiError::Operation {
                provider_id: "redis-streams".into(),
                operation: "subscribe",
                resource: Some(topic.as_str().into()),
                kind: "invalid_start_position",
                retryable: Some(false),
                source: Box::new(RedisProviderError::Configuration("invalid Redis stream ID")),
            });
        }
        let mut connection = self
            .client
            .get_connection()
            .map_err(|_| spi_error("subscribe", Some(&topic), RedisProviderError::Operation("connect")))?;
        let result: Result<(), RedisError> = cmd("XGROUP")
            .arg("CREATE")
            .arg(&key)
            .arg(&group)
            .arg(start)
            .arg("MKSTREAM")
            .query(&mut connection);
        let result = if result.is_err() {
            connection.discard();
            let mut retry_connection = self
                .client
                .get_connection()
                .map_err(|_| spi_error("subscribe", Some(&topic), RedisProviderError::Operation("connect")))?;
            cmd("XGROUP")
                .arg("CREATE")
                .arg(&key)
                .arg(&group)
                .arg(start)
                .arg("MKSTREAM")
                .query(&mut retry_connection)
        } else {
            result
        };
        if let Err(error) = result
            && error.code() != Some("BUSYGROUP")
        {
            return Err(spi_error(
                "subscribe",
                Some(&topic),
                RedisProviderError::Operation("XGROUP CREATE"),
            ));
        }
        let consumer = request.subscription_id().to_string();
        Ok(Box::new(Subscription {
            client: Arc::clone(&self.client),
            key,
            group,
            quarantine,
            consumer,
            topic,
            subscription_id: request.subscription_id(),
            closed: false,
            claim_min_idle_ms: self.settings.claim_min_idle_ms(),
            max_unsettled: self.settings.max_unsettled_per_subscription(),
            recovery: Arc::new(Mutex::new(RecoveryState::new())),
        }))
    }

    /// Completes shutdown without acknowledging pending messages.
    ///
    /// # Returns
    ///
    /// `Complete`; any unsettled records remain in the Redis pending entries
    /// list.
    fn shutdown(&self, _mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

fn valid_stream_id(value: &str) -> bool {
    let Some((milliseconds, sequence)) = value.split_once('-') else {
        return false;
    };
    !milliseconds.is_empty()
        && !sequence.is_empty()
        && milliseconds.bytes().all(|byte| byte.is_ascii_digit())
        && sequence.bytes().all(|byte| byte.is_ascii_digit())
}

/// Returns the fixed capability set shared by Redis provider modes.
///
/// # Returns
///
/// Encoded durable delivery with position replay and accept/retry/reject
/// settlement.
pub(super) const fn redis_capabilities() -> EventBusCapabilities {
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

/// Wraps a provider failure without retaining raw Redis diagnostics.
///
/// # Parameters
///
/// - `operation`: Stable SPI operation name.
/// - `topic`: Optional topic identifying the affected stream.
/// - `source`: Sanitized provider error category.
///
/// # Returns
///
/// A retryable SPI operation error without raw Redis diagnostics.
pub(super) fn spi_error(operation: &'static str, topic: Option<&TopicAddress>, source: RedisProviderError) -> SpiError {
    SpiError::Operation {
        provider_id: "redis-streams".into(),
        operation,
        resource: topic.map(|value| value.as_str().into()),
        kind: "redis_error",
        retryable: Some(true),
        source: Box::new(source),
    }
}
