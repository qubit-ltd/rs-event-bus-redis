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

use qubit_event_bus::SpiError;
use qubit_event_bus::model::PublishAcknowledgement;
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
use redis::RedisError;
use redis::cmd;

use super::async_redis_event_bus_provider::spi_error;
use super::subscription::Subscription;
use crate::client::Client;
use crate::config::RedisEventBusConfig;
use crate::error::RedisProviderError;
use crate::naming::group_name;
use crate::naming::poison_key;
use crate::naming::stream_key;
use crate::recovery::RecoveryState;
use crate::wire::WireFields;

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

    /// Appends an encoded event to the topic stream with Redis `XADD`.
    ///
    /// The returned acknowledgement means Redis accepted the command. It does
    /// not guarantee that the record was fsynced or consumed. A connection loss
    /// before the reply leaves the publish outcome unknown, so a retry may
    /// append a duplicate.
    ///
    /// # Parameters
    ///
    /// - `message`: Event with an encoded payload and its transport metadata.
    ///
    /// # Type Parameters
    ///
    /// - `'a`: Lifetime shared by the bus borrow and the returned future.
    ///
    /// # Returns
    ///
    /// An accepted acknowledgement containing the Redis stream ID.
    ///
    /// # Errors
    ///
    /// Returns an SPI error if the payload is not encoded, serialization fails,
    /// a connection cannot be opened, or Redis rejects `XADD`.
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
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
            let result: Result<String, RedisError> = cmd("XADD")
                .arg(key)
                .arg("*")
                .arg("wire")
                .arg(payload)
                .query_async(&mut connection)
                .await;
            let message_id = match result {
                Ok(id) => id,
                Err(_) => {
                    self.client.invalidate_async_connection().await;
                    return Err(spi_error(
                        "publish",
                        Some(&topic),
                        RedisProviderError::Operation("XADD"),
                    ));
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
    /// # Parameters
    ///
    /// - `request`: Topic, group, start position, and identity for the
    ///   receiver.
    ///
    /// # Type Parameters
    ///
    /// - `'a`: Lifetime shared by the bus borrow and the returned future.
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
            let mut connection = self
                .client
                .get_async_connection()
                .await
                .map_err(|_| spi_error("subscribe", Some(&topic), RedisProviderError::Operation("connect")))?;
            let result: Result<(), RedisError> = cmd("XGROUP")
                .arg("CREATE")
                .arg(&key)
                .arg(&group)
                .arg(start)
                .arg("MKSTREAM")
                .query_async(&mut connection)
                .await;
            let result = if result.is_err() {
                self.client.invalidate_async_connection().await;
                let mut retry_connection = self
                    .client
                    .get_async_connection()
                    .await
                    .map_err(|_| spi_error("subscribe", Some(&topic), RedisProviderError::Operation("connect")))?;
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
                    RedisProviderError::Operation("XGROUP CREATE"),
                ));
            }
            Ok(Box::new(Subscription {
                client: Arc::clone(&self.client),
                key,
                group,
                quarantine,
                consumer: request.subscription_id().to_string(),
                topic,
                subscription_id: request.subscription_id(),
                closed: false,
                claim_min_idle_ms: self.settings.claim_min_idle_ms(),
                max_unsettled: self.settings.max_unsettled_per_subscription(),
                recovery: Arc::new(Mutex::new(RecoveryState::new())),
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }

    /// Completes shutdown without acknowledging any unsettled delivery.
    ///
    /// # Returns
    ///
    /// A future resolving to `Complete`; pending records remain in Redis.
    ///
    /// # Type Parameters
    ///
    /// - `'a`: Lifetime shared by the bus borrow and the returned future.
    fn shutdown<'a>(&'a self, _mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async { Ok(ShutdownOutcome::Complete) })
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
