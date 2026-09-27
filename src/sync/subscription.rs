// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Synchronous Redis Streams consumer group receiver.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use qubit_event_bus::error::SpiError;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::DeliveryGap;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::InboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::TopicAddress;
use qubit_id::Id;
use redis::FromRedisValue;
use redis::cmd;
use redis::streams::StreamAutoClaimReply;
use redis::streams::StreamId;
use redis::streams::StreamReadReply;

use super::internal::SettlementState;
use super::redis_event_bus::spi_error;
use crate::client::Client;
use crate::error::RedisProviderError;
use crate::wire::WireFields;

/// One blocking consumer owned by a facade subscription.
pub(crate) struct Subscription {
    /// Shared factory used to create a blocking connection per operation.
    pub(crate) client: Arc<Client>,
    /// Redis stream key.
    pub(crate) key: String,
    /// Consumer group name.
    pub(crate) group: String,
    /// Unique Redis consumer name.
    pub(crate) consumer: String,
    /// Original typed topic address.
    pub(crate) topic: TopicAddress,
    /// Facade subscription identifier.
    pub(crate) subscription_id: Id,
    /// Whether this receiver is closed.
    pub(crate) closed: bool,
    /// Resume cursor used so each `XAUTOCLAIM` call scans one pending entry.
    pub(crate) claim_cursor: String,
    /// Minimum pending idle milliseconds before another consumer may claim it.
    pub(crate) claim_min_idle_ms: usize,
    /// Maximum unsettled record count before reads pause.
    pub(crate) max_unsettled: usize,
    /// In-flight count shared with tokens and updated atomically across
    /// callers.
    pub(crate) outstanding: Arc<AtomicUsize>,
}

impl EventSubscriptionSpi for Subscription {
    /// Reads a pending item first, then a new group item with a finite block.
    ///
    /// The call scans reclaimable entries, this consumer's pending entries, and
    /// then new group entries. Closing the receiver does not acknowledge a
    /// record, so pending deliveries remain recoverable. Redis block intervals
    /// are capped to let the call observe the requested deadline.
    ///
    /// # Parameters
    ///
    /// - `timeout`: Maximum wait for a new stream entry.
    ///
    /// # Returns
    ///
    /// A message, a gap for removed pending entries, a timeout, or `Closed`.
    ///
    /// # Errors
    ///
    /// Returns an SPI operation error when a Redis command fails or a record
    /// cannot be decoded.
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError> {
        if self.closed {
            return Ok(ReceiveOutcome::Closed);
        }
        if self.outstanding.load(Ordering::Relaxed) >= self.max_unsettled {
            return Ok(ReceiveOutcome::TimedOut);
        }
        let mut connection = self
            .client
            .get_connection()
            .map_err(|_| spi_error("receive", Some(&self.topic), RedisProviderError::Operation("connect")))?;
        let deadline = Instant::now().checked_add(timeout).unwrap_or_else(Instant::now);
        let claim: StreamAutoClaimReply = cmd("XAUTOCLAIM")
            .arg(&self.key)
            .arg(&self.group)
            .arg(&self.consumer)
            .arg(self.claim_min_idle_ms)
            .arg(&self.claim_cursor)
            .arg("COUNT")
            .arg(1)
            .query(&mut connection)
            .map_err(|_| {
                spi_error(
                    "receive",
                    Some(&self.topic),
                    RedisProviderError::Operation("XAUTOCLAIM"),
                )
            })?;
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
        let pending: Option<StreamReadReply> = cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&self.group)
            .arg(&self.consumer)
            .arg("COUNT")
            .arg(1)
            .arg("STREAMS")
            .arg(&self.key)
            .arg("0")
            .query(&mut connection)
            .map_err(|_| {
                spi_error(
                    "receive",
                    Some(&self.topic),
                    RedisProviderError::Operation("XREADGROUP"),
                )
            })?;
        if let Some(entry) = pending
            .and_then(|reply| reply.keys.into_iter().next())
            .and_then(|stream| stream.ids.into_iter().next())
        {
            return decode_entry(self, entry);
        }
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let block_ms = remaining.as_millis().clamp(1, 1_000) as usize;
            let reply: Option<StreamReadReply> = cmd("XREADGROUP")
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
                .query(&mut connection)
                .map_err(|_| {
                    spi_error(
                        "receive",
                        Some(&self.topic),
                        RedisProviderError::Operation("XREADGROUP"),
                    )
                })?;
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
    }

    /// Acknowledges terminal dispositions and leaves retries in the PEL.
    ///
    /// Accept and Reject issue `XACK`; Retry only releases the receiver's local
    /// in-flight slot and leaves the Redis entry pending. Repeating the same
    /// disposition succeeds, but a different disposition for an applied token
    /// is rejected.
    ///
    /// # Parameters
    ///
    /// - `token`: Settlement token produced by this receiver.
    /// - `disposition`: Terminal action or retry decision to apply.
    ///
    /// # Returns
    ///
    /// Success after the requested disposition is recorded.
    ///
    /// # Errors
    ///
    /// Returns an invalid-token error for foreign, unknown, or conflicting
    /// tokens, or an operation error if Redis settlement fails.
    fn settle(&mut self, token: &SettlementToken, disposition: DeliveryDisposition) -> Result<(), SpiError> {
        if !token.belongs_to(self.subscription_id) {
            return Err(invalid_token("token belongs to another subscription", &self.topic));
        }
        let state = token
            .downcast_ref::<SettlementState>()
            .ok_or_else(|| invalid_token("token type is not recognized", &self.topic))?;
        let mut applied = state.disposition.lock().map_err(|_| {
            spi_error(
                "settle",
                Some(&self.topic),
                RedisProviderError::Operation("settlement lock"),
            )
        })?;
        if let Some(previous) = *applied {
            return if previous == disposition {
                Ok(())
            } else {
                Err(invalid_token("token already has a different disposition", &self.topic))
            };
        }
        if disposition == DeliveryDisposition::Retry {
            *applied = Some(disposition);
            state.outstanding.fetch_sub(1, Ordering::Relaxed);
            return Ok(());
        }
        let mut connection = self
            .client
            .get_connection()
            .map_err(|_| spi_error("settle", Some(&self.topic), RedisProviderError::Operation("connect")))?;
        let _: usize = cmd("XACK")
            .arg(&state.stream)
            .arg(&state.group)
            .arg(&state.message_id)
            .query(&mut connection)
            .map_err(|_| spi_error("settle", Some(&self.topic), RedisProviderError::Operation("XACK")))?;
        *applied = Some(disposition);
        state.outstanding.fetch_sub(1, Ordering::Relaxed);
        Ok(())
    }

    /// Closes the receiver without acknowledging pending messages.
    ///
    /// # Returns
    ///
    /// Success after subsequent receive calls begin returning `Closed`.
    fn close(&mut self) -> Result<(), SpiError> {
        self.closed = true;
        Ok(())
    }
}

/// Decodes one Redis record and tracks its unsettled delivery slot.
///
/// Missing or malformed wire data is surfaced as a secret-safe operation
/// error; this helper does not delete or acknowledge the record on failure.
///
/// # Parameters
///
/// - `subscription`: Receiver supplying the topic, group, and in-flight
///   counter.
/// - `entry`: Stream entry returned by a read or claim command.
///
/// # Returns
///
/// A message outcome containing a token bound to this receiver.
///
/// # Errors
///
/// Returns an operation error when the wire field is absent, malformed, or
/// cannot be converted to typed event metadata.
fn decode_entry(subscription: &Subscription, entry: StreamId) -> Result<ReceiveOutcome, SpiError> {
    let Some(value) = entry.map.get("wire") else {
        return Err(spi_error(
            "receive",
            Some(&subscription.topic),
            RedisProviderError::Operation("missing wire field"),
        ));
    };
    let payload: String = String::from_redis_value(value).map_err(|_| {
        spi_error(
            "receive",
            Some(&subscription.topic),
            RedisProviderError::Operation("invalid wire field"),
        )
    })?;
    let fields: WireFields = serde_json::from_str(&payload).map_err(|_| {
        spi_error(
            "receive",
            Some(&subscription.topic),
            RedisProviderError::Operation("decode message"),
        )
    })?;
    let (topic, event_id, timestamp, headers, ordering_key, payload) = fields
        .into_parts(subscription.topic.clone())
        .map_err(|error| spi_error("receive", Some(&subscription.topic), error))?;
    let settlement = SettlementToken::new(
        subscription.subscription_id,
        SettlementState {
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

/// Describes a settlement token that this receiver cannot apply.
///
/// # Parameters
///
/// - `reason`: Stable explanation suitable for the caller.
/// - `topic`: Topic associated with the receiver rejecting the token.
///
/// # Returns
///
/// A non-retryable invalid-settlement-token error.
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
