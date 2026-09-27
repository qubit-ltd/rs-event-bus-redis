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
use redis::FromRedisValue;
use redis::streams::StreamAutoClaimReply;
use redis::streams::StreamId;
use redis::streams::StreamReadReply;

use super::redis_event_bus_provider::spi_error;
use crate::error::RedisProviderError;
use crate::wire::WireFields;

/// Redis coordinates and settlement status carried by a delivery token.
pub(crate) struct SettlementState {
    /// Redis stream key.
    stream: String,
    /// Consumer group name.
    group: String,
    /// Redis message ID.
    message_id: String,
    /// Final state applied through this token.
    disposition: Arc<Mutex<Option<DeliveryDisposition>>>,
    /// Number of currently unsettled records owned by the receiver.
    outstanding: Arc<AtomicUsize>,
}

/// One blocking consumer owned by a facade subscription.
pub(crate) struct RedisSubscription {
    /// Client used to create a dedicated blocking connection.
    pub(crate) client: std::sync::Arc<crate::client::RedisClient>,
    /// Redis stream key.
    pub(crate) key: String,
    /// Consumer group name.
    pub(crate) group: String,
    /// Unique Redis consumer name.
    pub(crate) consumer: String,
    /// Original typed topic address.
    pub(crate) topic: TopicAddress,
    /// Facade subscription identifier.
    pub(crate) subscription_id: qubit_id::Id,
    /// Whether this receiver is closed.
    pub(crate) closed: bool,
    /// Resume cursor for bounded `XAUTOCLAIM` scans.
    pub(crate) claim_cursor: String,
    /// Pending-message idle duration before another consumer can claim it.
    pub(crate) claim_min_idle_ms: usize,
    /// Maximum unsettled record count before reads pause.
    pub(crate) max_unsettled: usize,
    /// Current unsettled record count shared with delivery tokens.
    pub(crate) outstanding: Arc<AtomicUsize>,
}

impl EventSubscriptionSpi for RedisSubscription {
    /// Reads a pending item first, then a new group item with a finite block.
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
        let claim: StreamAutoClaimReply = redis::cmd("XAUTOCLAIM")
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
        let pending: Option<StreamReadReply> = redis::cmd("XREADGROUP")
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
        let _: usize = redis::cmd("XACK")
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
    fn close(&mut self) -> Result<(), SpiError> {
        self.closed = true;
        Ok(())
    }
}

/// Converts one Redis record to the transport-neutral inbound form.
fn decode_entry(subscription: &RedisSubscription, entry: StreamId) -> Result<ReceiveOutcome, SpiError> {
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

/// Builds the structured invalid-token SPI error.
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
