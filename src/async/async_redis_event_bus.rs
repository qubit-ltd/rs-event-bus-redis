// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runtime-neutral asynchronous Redis Streams operations.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use qubit_event_bus::SpiError;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishEffect;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriptionDurability;
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
use qubit_event_bus::spi::SubscriptionModes;
use redis::RedisError;
use redis::Value;
use redis::cmd;

use super::async_redis_event_bus_provider::spi_error;
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

/// Client and validated settings shared by asynchronous SPI operations.
pub(super) struct AsyncRedisEventBus {
    /// Client that opens executor-neutral multiplexed connections.
    pub(super) client: Arc<Client>,
    /// Non-secret settings used for key and receiver construction.
    pub(super) settings: RedisEventBusConfig,
}

impl AsyncEventBusSpi for AsyncRedisEventBus {
    /// Declares encoded payloads, durable delivery, position replay, and
    /// settlement.
    ///
    /// # Returns
    ///
    /// The static capability set supported by this Redis Streams backend.
    #[inline]
    fn capabilities(&self) -> EventBusCapabilities {
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

    /// Appends an encoded event to the topic stream with Redis `XADD`.
    ///
    /// The returned acknowledgement means Redis accepted the command. It does
    /// not guarantee that the record was fsynced or consumed. A connection loss
    /// before the reply leaves the publish outcome unknown, so a retry may
    /// append a duplicate.
    ///
    /// # Type Parameters
    ///
    /// - `'a`: Lifetime shared by the bus borrow and the returned future.
    /// # Parameters
    ///
    /// - `message`: Event with an encoded payload and its transport metadata.
    ///
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
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async move {
            let topic = message.topic().clone();
            let payload = encode_bounded(&message, WireLimits::from_config(&self.settings))
                .map_err(|error| to_publish_error(&topic, error, PublishEffect::NotAccepted))?;
            let key = stream_key(self.settings.namespace(), topic.as_str());
            let mut connection = self
                .client
                .get_async_connection()
                .await
                .map_err(|error| to_publish_error(&topic, error, PublishEffect::NotAccepted))?;
            let mut command = cmd("XADD");
            command.arg(key);
            if let Some(maxlen) = self.settings.stream_maxlen_approx() {
                command.arg("MAXLEN").arg("~").arg(maxlen.get());
            }
            command.arg("*").arg("wire").arg(payload);
            let result = connection.send_packed_command(&command).await;
            let message_id = match result {
                Ok(Value::BulkString(bytes)) => match String::from_utf8(bytes) {
                    Ok(id) if valid_stream_id(&id) && id != "0-0" => id,
                    _ => {
                        self.client.invalidate_async_connection(connection.generation).await;
                        return Err(invalid_publish_reply(&topic));
                    }
                },
                Ok(Value::ServerError(error)) => {
                    let error: RedisError = error.into();
                    return Err(query_publish_error(&topic, &error));
                }
                Ok(_) => {
                    self.client.invalidate_async_connection(connection.generation).await;
                    return Err(invalid_publish_reply(&topic));
                }
                Err(error) => {
                    self.client.invalidate_async_connection(connection.generation).await;
                    return Err(query_publish_error(&topic, &error));
                }
            };
            Ok(PublishAcknowledgement::Accepted {
                provider_message_id: Some(message_id),
                metadata: Default::default(),
            })
        })
    }

    /// Creates or reuses a consumer group and returns its async receiver.
    ///
    /// A group's cursor is created only when Redis has not seen the group
    /// before; subsequent requests retain the cursor already stored by Redis.
    /// Unsettled entries remain in the pending entries list when a read future
    /// is cancelled or a receiver is closed.
    ///
    /// # Type Parameters
    ///
    /// - `'a`: Lifetime shared by the bus borrow and the returned future.
    /// # Parameters
    ///
    /// - `request`: Topic, group, start position, and identity for the
    ///   receiver.
    ///
    ///
    /// # Returns
    ///
    /// A boxed async subscription bound to the requested group.
    ///
    /// # Errors
    ///
    /// Returns an SPI error if Redis cannot open a connection or create the
    /// requested consumer group.
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async move {
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
                StartPosition::At(id) => id.as_ref(),
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
                .get_async_dedicated_connection()
                .await
                .map_err(|error| spi_error("subscribe", Some(&topic), error))?;
            let mut connection = self
                .client
                .get_async_connection()
                .await
                .map_err(|error| spi_error("subscribe", Some(&topic), error))?;
            let result: Result<(), RedisError> = cmd("XGROUP")
                .arg("CREATE")
                .arg(&key)
                .arg(&group)
                .arg(start)
                .arg("MKSTREAM")
                .query_async(&mut connection)
                .await;
            let result = if result
                .as_ref()
                .is_err_and(|error| error.is_io_error() || error.is_timeout())
            {
                self.client.invalidate_async_connection(connection.generation).await;
                drop(connection);
                let mut retry_connection = self
                    .client
                    .get_async_connection()
                    .await
                    .map_err(|error| spi_error("subscribe", Some(&topic), error))?;
                cmd("XGROUP")
                    .arg("CREATE")
                    .arg(&key)
                    .arg(&group)
                    .arg(start)
                    .arg("MKSTREAM")
                    .query_async(&mut retry_connection)
                    .await
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
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }

    /// Completes shutdown without acknowledging any unsettled delivery.
    ///
    /// # Type Parameters
    ///
    /// - `'a`: Lifetime shared by the bus borrow and the returned future.
    ///
    /// # Parameters
    ///
    /// - `_mode`: Facade shutdown request; this implementation issues no Redis
    ///   command.
    ///
    /// # Returns
    ///
    /// A future resolving to `Complete`; pending records remain in Redis.
    ///
    /// # Errors
    ///
    /// The returned future always resolves successfully; pending PEL state is
    /// untouched.
    #[inline]
    fn shutdown<'a>(&'a self, _mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async { Ok(ShutdownOutcome::Complete) })
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use futures_lite::future::block_on;
    use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
    use qubit_event_bus::spi::TopicAddress;
    use qubit_id::Id;

    use super::AsyncRedisEventBus;
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
        block_on(async {
            let settings = RedisEventBusConfig::new("redis://127.0.0.1:1/", "connection-errors")
                .expect("valid test configuration");
            let bus = AsyncRedisEventBus {
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
            assert!(receiver.receive(Duration::ZERO).await.is_err());
        });
    }
}
