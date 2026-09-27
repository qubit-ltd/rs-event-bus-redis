// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runtime-neutral asynchronous consumer group receiver.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use qubit_event_bus::error::SpiError;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::DeliveryGap;
use qubit_event_bus::spi::InboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::TopicAddress;
use redis::FromRedisValue;
use redis::streams::StreamAutoClaimReply;
use redis::streams::StreamId;
use redis::streams::StreamReadReply;

use crate::error::RedisProviderError;
use crate::wire::WireFields;

/// Redis location and shared settlement state stored in one delivery token.
struct AsyncSettlementState {
    /// Redis stream key.
    stream: String,
    /// Redis consumer group.
    group: String,
    /// Redis message ID.
    message_id: String,
    /// Previously selected disposition, if any.
    disposition: Arc<Mutex<Option<DeliveryDisposition>>>,
    /// Number of unsettled records retained by the receiver.
    outstanding: Arc<AtomicUsize>,
}

/// Asynchronous receiver whose cancelled reads remain in Redis PEL.
pub(crate) struct AsyncRedisSubscription {
    /// Client used to create the command connection.
    pub(crate) client: Arc<crate::client::RedisClient>,
    /// Redis stream key.
    pub(crate) key: String,
    /// Redis group name.
    pub(crate) group: String,
    /// Unique consumer name.
    pub(crate) consumer: String,
    /// Topic preserved for the facade.
    pub(crate) topic: TopicAddress,
    /// Owning facade subscription ID.
    pub(crate) subscription_id: qubit_id::Id,
    /// Whether this receiver has closed.
    pub(crate) closed: bool,
    /// Resume cursor used for bounded claim scans.
    pub(crate) claim_cursor: String,
    /// Minimum idle time before claiming another consumer's pending item.
    pub(crate) claim_min_idle_ms: usize,
    /// Maximum unsettled records before reads pause.
    pub(crate) max_unsettled: usize,
    /// Current unsettled record count shared with tokens.
    pub(crate) outstanding: Arc<AtomicUsize>,
}

impl AsyncEventSubscriptionSpi for AsyncRedisSubscription {
    /// Reads pending deliveries before new group entries using a finite block.
    fn receive<'a>(&'a mut self, timeout: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        Box::pin(async move {
            if self.closed {
                return Ok(ReceiveOutcome::Closed);
            }
            if self.outstanding.load(Ordering::Relaxed) >= self.max_unsettled {
                return Ok(ReceiveOutcome::TimedOut);
            }
            let mut connection = self
                .client
                .get_async_connection()
                .await
                .map_err(|_| spi_error("receive", &self.topic, RedisProviderError::Operation("connect")))?;
            let deadline = Instant::now().checked_add(timeout).unwrap_or_else(Instant::now);
            let claim: StreamAutoClaimReply = redis::cmd("XAUTOCLAIM")
                .arg(&self.key)
                .arg(&self.group)
                .arg(&self.consumer)
                .arg(self.claim_min_idle_ms)
                .arg(&self.claim_cursor)
                .arg("COUNT")
                .arg(1)
                .query_async(&mut connection)
                .await
                .map_err(|_| spi_error("receive", &self.topic, RedisProviderError::Operation("XAUTOCLAIM")))?;
            self.claim_cursor = claim.next_stream_id;
            if !claim.deleted_ids.is_empty() {
                return Ok(ReceiveOutcome::Gap(DeliveryGap::new(
                    "pending Redis stream entries were removed",
                    Some(claim.deleted_ids.len() as u64),
                )));
            }
            if let Some(entry) = claim.claimed.into_iter().next() {
                return decode_entry(self, entry);
            }
            let pending: Option<StreamReadReply> = redis::cmd("XREADGROUP")
                .arg("GROUP")
                .arg(&self.group)
                .arg(&self.consumer)
                .arg("COUNT")
                .arg(1)
                .arg("STREAMS")
                .arg(&self.key)
                .arg("0")
                .query_async(&mut connection)
                .await
                .map_err(|_| spi_error("receive", &self.topic, RedisProviderError::Operation("XREADGROUP")))?;
            if let Some(entry) = pending
                .and_then(|reply| reply.keys.into_iter().next())
                .and_then(|stream| stream.ids.into_iter().next())
            {
                return decode_entry(self, entry);
            }
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                let block_ms = remaining.as_millis().clamp(1, 1_000) as usize;
                let reply: Option<StreamReadReply> = redis::cmd("XREADGROUP")
                    .arg("GROUP")
                    .arg(&self.group)
                    .arg(&self.consumer)
                    .arg("COUNT")
                    .arg(1)
                    .arg("BLOCK")
                    .arg(block_ms)
                    .arg("STREAMS")
                    .arg(&self.key)
                    .arg(">")
                    .query_async(&mut connection)
                    .await
                    .map_err(|_| spi_error("receive", &self.topic, RedisProviderError::Operation("XREADGROUP")))?;
                let entry = reply
                    .and_then(|reply| reply.keys.into_iter().next())
                    .and_then(|stream| stream.ids.into_iter().next());
                if let Some(entry) = entry {
                    return decode_entry(self, entry);
                }
                if Instant::now() >= deadline {
                    return Ok(ReceiveOutcome::TimedOut);
                }
            }
        })
    }

    /// Acknowledges terminal decisions and leaves retry decisions in PEL.
    fn settle<'a>(
        &'a mut self,
        token: &SettlementToken,
        disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        let state = token.downcast_ref::<AsyncSettlementState>().map(|state| {
            (
                state.stream.clone(),
                state.group.clone(),
                state.message_id.clone(),
                Arc::clone(&state.disposition),
                Arc::clone(&state.outstanding),
            )
        });
        let belongs = token.belongs_to(self.subscription_id);
        let topic = self.topic.clone();
        let client = Arc::clone(&self.client);
        Box::pin(async move {
            if !belongs {
                return Err(invalid_token("token belongs to another subscription", &topic));
            }
            let Some((stream, group, message_id, applied, outstanding)) = state else {
                return Err(invalid_token("token type is not recognized", &topic));
            };
            {
                let current = applied
                    .lock()
                    .map_err(|_| spi_error("settle", &topic, RedisProviderError::Operation("settlement lock")))?;
                if let Some(previous) = *current {
                    return if previous == disposition {
                        Ok(())
                    } else {
                        Err(invalid_token("token already has a different disposition", &topic))
                    };
                }
            }
            if disposition != DeliveryDisposition::Retry {
                let mut connection = client
                    .get_async_connection()
                    .await
                    .map_err(|_| spi_error("settle", &topic, RedisProviderError::Operation("connect")))?;
                let _: usize = redis::cmd("XACK")
                    .arg(stream)
                    .arg(group)
                    .arg(message_id)
                    .query_async(&mut connection)
                    .await
                    .map_err(|_| spi_error("settle", &topic, RedisProviderError::Operation("XACK")))?;
            }
            {
                let mut applied = applied
                    .lock()
                    .map_err(|_| spi_error("settle", &topic, RedisProviderError::Operation("settlement lock")))?;
                if applied.is_none() {
                    *applied = Some(disposition);
                    outstanding.fetch_sub(1, Ordering::Relaxed);
                }
            }
            Ok(())
        })
    }

    /// Closes the receiver without implicitly acknowledging pending records.
    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        Box::pin(async move {
            self.closed = true;
            Ok(())
        })
    }
}

/// Converts one stream record into an inbound transport message.
fn decode_entry(subscription: &AsyncRedisSubscription, entry: StreamId) -> Result<ReceiveOutcome, SpiError> {
    let value = entry.map.get("wire").ok_or_else(|| {
        spi_error(
            "receive",
            &subscription.topic,
            RedisProviderError::Operation("missing wire field"),
        )
    })?;
    let encoded: String = String::from_redis_value(value).map_err(|_| {
        spi_error(
            "receive",
            &subscription.topic,
            RedisProviderError::Operation("invalid wire field"),
        )
    })?;
    let fields: WireFields = serde_json::from_str(&encoded).map_err(|_| {
        spi_error(
            "receive",
            &subscription.topic,
            RedisProviderError::Operation("decode message"),
        )
    })?;
    let (topic, event_id, timestamp, headers, ordering_key, payload) = fields
        .into_parts(subscription.topic.clone())
        .map_err(|error| spi_error("receive", &subscription.topic, error))?;
    let settlement = SettlementToken::new(
        subscription.subscription_id,
        AsyncSettlementState {
            stream: subscription.key.clone(),
            group: subscription.group.clone(),
            message_id: entry.id,
            disposition: Arc::new(Mutex::new(None)),
            outstanding: Arc::clone(&subscription.outstanding),
        },
    );
    subscription.outstanding.fetch_add(1, Ordering::Relaxed);
    Ok(ReceiveOutcome::Message(InboundMessage::new(
        topic,
        event_id,
        timestamp,
        headers,
        ordering_key,
        payload,
        Some(settlement),
        Default::default(),
    )))
}

/// Converts an internal failure into a secret-safe SPI error.
fn spi_error(operation: &'static str, topic: &TopicAddress, source: RedisProviderError) -> SpiError {
    SpiError::Operation {
        provider_id: "redis-streams".into(),
        operation,
        resource: Some(topic.as_str().into()),
        kind: "redis_error",
        retryable: Some(true),
        source: Box::new(source),
    }
}

/// Creates an invalid settlement-token SPI error.
fn invalid_token(reason: &'static str, topic: &TopicAddress) -> SpiError {
    SpiError::InvalidSettlementToken {
        provider_id: "redis-streams".into(),
        operation: "settle",
        resource: Some(topic.as_str().into()),
        reason,
        retryable: Some(false),
        source: Box::new(std::io::Error::other("invalid settlement token")),
    }
}
