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
use std::time::Duration;

use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishEffect;
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
use qubit_event_bus::spi::SubscriptionModes;
use qubit_event_bus::spi::TopicAddress;
use redis::ConnectionLike;
use redis::RedisError;
use redis::Value;
use redis::cmd;

use super::subscription::Subscription;
use crate::client::Client;
use crate::config::RedisEventBusConfig;
use crate::consumer_identity::new_consumer_name;
use crate::error::RedisProviderError;
use crate::error::invalid_publish_reply;
use crate::error::query_publish_error;
use crate::error::to_publish_error;
use crate::internal::RecoveryState;
use crate::internal::WireLimits;
use crate::naming::group_name;
use crate::naming::poison_key;
use crate::naming::stream_key;
use crate::redis_provider_error::from_redis_error;
use crate::wire_fields::encode_bounded;

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
    #[inline]
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
    /// admission is exhausted, a connection cannot be opened, or Redis rejects
    /// `XADD`. A missing or malformed reply after send returns
    /// outcome-unknown without replay.
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        let payload = encode_bounded(&message, WireLimits::from_config(&self.settings))
            .map_err(|error| to_publish_error(message.topic(), error, PublishEffect::NotAccepted))?;
        let key = stream_key(self.settings.namespace(), message.topic().as_str());
        let mut connection = self
            .client
            .get_connection()
            .map_err(|error| to_publish_error(message.topic(), error, PublishEffect::NotAccepted))?;
        let mut command = cmd("XADD");
        command.arg(&key);
        if let Some(maxlen) = self.settings.stream_maxlen_approx() {
            command.arg("MAXLEN").arg("~").arg(maxlen.get());
        }
        command.arg("*").arg("wire").arg(payload);
        let result = connection.req_command(&command);
        let message_id = match result {
            Ok(Value::BulkString(bytes)) => match String::from_utf8(bytes) {
                Ok(id) if valid_stream_id(&id) && id != "0-0" => id,
                _ => {
                    connection.discard();
                    return Err(invalid_publish_reply(message.topic()));
                }
            },
            Ok(Value::ServerError(error)) => {
                let error: RedisError = error.into();
                return Err(query_publish_error(message.topic(), &error));
            }
            Ok(_) => {
                connection.discard();
                return Err(invalid_publish_reply(message.topic()));
            }
            Err(error) => {
                connection.discard();
                return Err(query_publish_error(message.topic(), &error));
            }
        };
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
        let receiver_permit = self
            .client
            .try_receiver()
            .map_err(|error| spi_error("subscribe", Some(&topic), error))?;
        let receive_connection = self
            .client
            .get_dedicated_connection()
            .map_err(|error| spi_error("subscribe", Some(&topic), error))?;
        let mut connection = self
            .client
            .get_connection()
            .map_err(|error| spi_error("subscribe", Some(&topic), error))?;
        let result: Result<(), RedisError> = cmd("XGROUP")
            .arg("CREATE")
            .arg(&key)
            .arg(&group)
            .arg(start)
            .arg("MKSTREAM")
            .query(&mut connection);
        let result = if result
            .as_ref()
            .is_err_and(|error| error.is_io_error() || error.is_timeout())
        {
            connection.discard();
            drop(connection);
            let mut retry_connection = self
                .client
                .get_connection()
                .map_err(|error| spi_error("subscribe", Some(&topic), error))?;
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
                from_redis_error("subscribe", &error),
            ));
        }
        let consumer = new_consumer_name().map_err(|_| SpiError::Operation {
            provider_id: "redis-streams".into(),
            operation: "subscribe",
            resource: Some(topic.as_str().into()),
            kind: "consumer_identity_unavailable",
            retryable: Some(false),
            source: Box::new(RedisProviderError::Operation("generate consumer identity")),
        })?;
        Ok(Box::new(Subscription {
            client: Arc::clone(&self.client),
            wire_limits: WireLimits::from_config(&self.settings),
            receive_connection: Some(receive_connection),
            receiver_permit: Some(receiver_permit),
            key,
            group,
            quarantine,
            consumer,
            topic,
            subscription_id: request.subscription_id(),
            closed: false,
            claim_min_idle_ms: self.settings.claim_min_idle_ms(),
            recovery_interval: Duration::from_millis(self.settings.recovery_interval_ms() as u64),
            max_unsettled: self.settings.max_unsettled_per_subscription(),
            recovery: Arc::new(Mutex::new(RecoveryState::new())),
        }))
    }

    /// Completes shutdown without acknowledging pending messages.
    ///
    /// # Parameters
    ///
    /// - `_mode`: Facade shutdown request; both modes complete without extra
    ///   Redis I/O.
    ///
    /// # Returns
    ///
    /// `Complete`; any unsettled records remain in the Redis pending entries
    /// list.
    ///
    /// # Errors
    ///
    /// This implementation always succeeds without sending a Redis command.
    #[inline]
    fn shutdown(&self, _mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

/// Validates the two unsigned decimal components of a Redis stream ID.
///
/// # Parameters
///
/// - `value`: Milliseconds and sequence separated by exactly one dash.
///
/// # Returns
///
/// `true` only when both non-empty decimal components fit u64. No Redis I/O is
/// issued.
#[must_use]
fn valid_stream_id(value: &str) -> bool {
    let Some((milliseconds, sequence)) = value.split_once('-') else {
        return false;
    };
    !milliseconds.is_empty()
        && !sequence.is_empty()
        && milliseconds.bytes().all(|byte| byte.is_ascii_digit())
        && sequence.bytes().all(|byte| byte.is_ascii_digit())
        && milliseconds.parse::<u64>().is_ok()
        && sequence.parse::<u64>().is_ok()
}

/// Returns the fixed capability set shared by Redis provider modes.
///
/// # Returns
///
/// Encoded durable delivery with position replay and accept/retry/reject
/// settlement.
#[inline]
pub(super) const fn redis_capabilities() -> EventBusCapabilities {
    EventBusCapabilities::new(
        PayloadModes::Encoded,
        SettlementCapabilities::AcceptRetryReject,
        OrderingCapability::None,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Durable,
        SubscriptionModes::DURABLE,
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
/// A classified SPI operation error without raw Redis diagnostics; retryability
/// reflects both the failure category and whether that operation can safely
/// recover.
#[inline]
pub(super) fn spi_error(operation: &'static str, topic: Option<&TopicAddress>, source: RedisProviderError) -> SpiError {
    crate::error::to_spi_error(operation, topic, source)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use qubit_event_bus::spi::EventSubscriptionSpi;
    use qubit_event_bus::spi::TopicAddress;
    use qubit_id::Id;

    use super::RedisEventBus;
    use crate::client::Client;
    use crate::config::RedisEventBusConfig;
    use crate::internal::RecoveryState;
    use crate::internal::WireLimits;

    #[test]
    fn test_stream_id_validation_rejects_malformed_components() {
        assert!(super::valid_stream_id("123-0"));
        for value in ["", "123", "-0", "123-", "a-0", "1-b", "1-2-3"] {
            assert!(!super::valid_stream_id(value), "accepted malformed ID {value:?}");
        }
    }

    #[test]
    fn test_receiver_connection_failure_is_returned_before_delivery() {
        let settings =
            RedisEventBusConfig::new("redis://127.0.0.1:1/", "connection-errors").expect("valid test configuration");
        let bus = RedisEventBus {
            client: Arc::new(Client::new(&settings).expect("unreachable Redis URL is syntactically valid")),
            settings,
        };
        let mut receiver = super::Subscription {
            client: Arc::clone(&bus.client),
            wire_limits: WireLimits::from_config(&bus.settings),
            receive_connection: None,
            receiver_permit: None,
            key: "connection-errors:events".into(),
            group: "workers".into(),
            quarantine: "connection-errors:quarantine".into(),
            consumer: "worker".into(),
            topic: TopicAddress::new("events").expect("topic is valid"),
            subscription_id: Id::new(2),
            closed: false,
            claim_min_idle_ms: 0,
            recovery_interval: Duration::from_secs(1),
            max_unsettled: 1,
            recovery: Arc::new(Mutex::new(RecoveryState::new())),
        };
        assert!(receiver.receive(Duration::ZERO).is_err());
    }
}
