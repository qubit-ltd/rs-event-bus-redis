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
use redis::ConnectionLike;
use redis::FromRedisValue;
use redis::cmd;
use redis::streams::StreamAutoClaimReply;
use redis::streams::StreamId;
use redis::streams::StreamReadReply;

use super::internal::SettlementState;
use super::redis_event_bus::spi_error;
use crate::client::Client;
use crate::client::PooledConnection;
use crate::error::RedisProviderError;
use crate::poison::PoisonOutcome;
use crate::poison::PoisonReason;
use crate::poison::quarantine;
use crate::recovery::RecoveryState;
use crate::wire::WireFields;

/// One blocking consumer owned by a facade subscription.
pub(crate) struct Subscription {
    /// Shared blocking connection factory.
    pub(crate) client: Arc<Client>,
    /// Dedicated connection reused between completed receives.
    pub(crate) receive_connection: Option<PooledConnection>,
    /// Redis stream key.
    pub(crate) key: String,
    /// Consumer group name.
    pub(crate) group: String,
    /// Quarantine stream associated with this topic and group.
    pub(crate) quarantine: String,
    /// Unique Redis consumer name.
    pub(crate) consumer: String,
    /// Original typed topic address.
    pub(crate) topic: TopicAddress,
    /// Facade subscription identifier.
    pub(crate) subscription_id: Id,
    /// Whether this receiver is closed.
    pub(crate) closed: bool,
    /// Minimum pending idle milliseconds before another consumer may claim it.
    pub(crate) claim_min_idle_ms: usize,
    /// Maximum unsettled record count before reads pause.
    pub(crate) max_unsettled: usize,
    /// Local active-delivery set and recovery cursors shared with tokens.
    pub(crate) recovery: Arc<Mutex<RecoveryState>>,
}

impl EventSubscriptionSpi for Subscription {
    /// Recovers pending work without redelivering locally active entries.
    ///
    /// The call scans reclaimable entries, this consumer's pending entries, and
    /// then new group entries. Pending IDs already held by local handlers are
    /// skipped. Malformed entries move atomically to the group quarantine
    /// stream. Closing the receiver does not acknowledge valid pending work.
    ///
    /// # Parameters
    ///
    /// - `timeout`: Maximum wait for a new stream entry.
    ///
    /// # Returns
    ///
    /// A message, a gap for removed or quarantined entries, a timeout, or
    /// `Closed`.
    ///
    /// # Errors
    ///
    /// Returns an SPI operation error when a Redis command or quarantine
    /// transfer fails.
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError> {
        if self.closed {
            return Ok(ReceiveOutcome::Closed);
        }
        if self
            .recovery
            .lock()
            .map_err(|_| {
                spi_error(
                    "receive",
                    Some(&self.topic),
                    RedisProviderError::Operation("recovery lock"),
                )
            })?
            .active_len()
            >= self.max_unsettled
        {
            return Ok(ReceiveOutcome::TimedOut);
        }
        let mut connection = match self.receive_connection.take() {
            Some(connection) => connection,
            None => self
                .client
                .get_dedicated_connection()
                .map_err(|_| spi_error("receive", Some(&self.topic), RedisProviderError::Operation("connect")))?,
        };
        let result = (|| {
            let deadline = Instant::now().checked_add(timeout).unwrap_or_else(Instant::now);
            let scan_limit = self.max_unsettled.saturating_add(2);
            for _ in 0..scan_limit {
                let cursor = self
                    .recovery
                    .lock()
                    .map_err(|_| {
                        spi_error(
                            "receive",
                            Some(&self.topic),
                            RedisProviderError::Operation("recovery lock"),
                        )
                    })?
                    .claim_cursor()
                    .to_owned();
                let claim: StreamAutoClaimReply = cmd("XAUTOCLAIM")
                    .arg(&self.key)
                    .arg(&self.group)
                    .arg(&self.consumer)
                    .arg(self.claim_min_idle_ms)
                    .arg(cursor)
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
                let at_end = claim.next_stream_id == "0-0";
                let claimed = claim.claimed.into_iter().next();
                self.recovery
                    .lock()
                    .map_err(|_| {
                        spi_error(
                            "receive",
                            Some(&self.topic),
                            RedisProviderError::Operation("recovery lock"),
                        )
                    })?
                    .set_claim_cursor(claim.next_stream_id);
                if !claim.deleted_ids.is_empty() {
                    return Ok(ReceiveOutcome::Gap(DeliveryGap::new(
                        "pending Redis stream entries were removed",
                        Some(claim.deleted_ids.len() as u64),
                    )));
                }
                if let Some(entry) = claimed {
                    let available = self
                        .recovery
                        .lock()
                        .map_err(|_| {
                            spi_error(
                                "receive",
                                Some(&self.topic),
                                RedisProviderError::Operation("recovery lock"),
                            )
                        })?
                        .can_deliver(&entry.id);
                    if available && let Some(outcome) = read_entry(self, &mut connection, entry)? {
                        return Ok(outcome);
                    }
                }
                if at_end {
                    break;
                }
            }

            for _ in 0..scan_limit {
                let cursor = self
                    .recovery
                    .lock()
                    .map_err(|_| {
                        spi_error(
                            "receive",
                            Some(&self.topic),
                            RedisProviderError::Operation("recovery lock"),
                        )
                    })?
                    .pending_cursor()
                    .to_owned();
                let pending: Option<StreamReadReply> = cmd("XREADGROUP")
                    .arg("GROUP")
                    .arg(&self.group)
                    .arg(&self.consumer)
                    .arg("COUNT")
                    .arg(1)
                    .arg("STREAMS")
                    .arg(&self.key)
                    .arg(&cursor)
                    .query(&mut connection)
                    .map_err(|_| {
                        spi_error(
                            "receive",
                            Some(&self.topic),
                            RedisProviderError::Operation("XREADGROUP"),
                        )
                    })?;
                let entry = pending
                    .and_then(|reply| reply.keys.into_iter().next())
                    .and_then(|stream| stream.ids.into_iter().next());
                let Some(entry) = entry else {
                    self.recovery
                        .lock()
                        .map_err(|_| {
                            spi_error(
                                "receive",
                                Some(&self.topic),
                                RedisProviderError::Operation("recovery lock"),
                            )
                        })?
                        .reset_pending_scan();
                    break;
                };
                let id = entry.id.clone();
                self.recovery
                    .lock()
                    .map_err(|_| {
                        spi_error(
                            "receive",
                            Some(&self.topic),
                            RedisProviderError::Operation("recovery lock"),
                        )
                    })?
                    .set_pending_cursor(id.clone());
                let available = self
                    .recovery
                    .lock()
                    .map_err(|_| {
                        spi_error(
                            "receive",
                            Some(&self.topic),
                            RedisProviderError::Operation("recovery lock"),
                        )
                    })?
                    .can_deliver(&id);
                if available && let Some(outcome) = read_entry(self, &mut connection, entry)? {
                    return Ok(outcome);
                }
            }

            if timeout.is_zero() {
                let reply: Option<StreamReadReply> = cmd("XREADGROUP")
                    .arg("GROUP")
                    .arg(&self.group)
                    .arg(&self.consumer)
                    .arg("COUNT")
                    .arg(1)
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
                if let Some(entry) = reply
                    .and_then(|reply| reply.keys.into_iter().next())
                    .and_then(|stream| stream.ids.into_iter().next())
                    && let Some(outcome) = read_entry(self, &mut connection, entry)?
                {
                    return Ok(outcome);
                }
                return Ok(ReceiveOutcome::TimedOut);
            }
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Ok(ReceiveOutcome::TimedOut);
                }
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
                    if let Some(outcome) = read_entry(self, &mut connection, entry)? {
                        return Ok(outcome);
                    }
                    continue;
                }
                if Instant::now() >= deadline {
                    return Ok(ReceiveOutcome::TimedOut);
                }
            }
        })();
        if result.is_ok() {
            self.receive_connection = Some(connection);
        }
        result
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
            state
                .recovery
                .lock()
                .map_err(|_| {
                    spi_error(
                        "settle",
                        Some(&self.topic),
                        RedisProviderError::Operation("recovery lock"),
                    )
                })?
                .mark_retry(&state.message_id);
            *applied = Some(disposition);
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
        state
            .recovery
            .lock()
            .map_err(|_| {
                spi_error(
                    "settle",
                    Some(&self.topic),
                    RedisProviderError::Operation("recovery lock"),
                )
            })?
            .mark_terminal(&state.message_id);
        *applied = Some(disposition);
        Ok(())
    }

    /// Closes the receiver without acknowledging pending messages.
    ///
    /// # Returns
    ///
    /// Success after subsequent receive calls begin returning `Closed`.
    fn close(&mut self) -> Result<(), SpiError> {
        self.closed = true;
        self.receive_connection.take();
        Ok(())
    }
}

/// Decodes one Redis record into an inbound message without settling it.
///
/// # Parameters
///
/// - `subscription`: Receiver supplying topic, group, and shared recovery
///   state.
/// - `entry`: Stream entry returned by a read or claim command.
///
/// # Returns
///
/// An inbound message containing a token bound to this receiver.
///
/// # Errors
///
/// Returns a stable reason when the wire field or event metadata is malformed.
fn decode_entry(subscription: &Subscription, entry: StreamId) -> Result<InboundMessage, PoisonReason> {
    let Some(value) = entry.map.get("wire") else {
        return Err(PoisonReason::MissingWire);
    };
    let payload: String = String::from_redis_value(value).map_err(|_| PoisonReason::InvalidWireField)?;
    let fields: WireFields = serde_json::from_str(&payload).map_err(|_| PoisonReason::InvalidJson)?;
    let (topic, event_id, timestamp, headers, ordering_key, payload) =
        fields.into_parts(subscription.topic.clone()).map_err(|error| {
            if matches!(error, RedisProviderError::UnsupportedWireVersion) {
                PoisonReason::UnsupportedVersion
            } else {
                PoisonReason::InvalidEventMetadata
            }
        })?;
    let settlement = SettlementToken::new(
        subscription.subscription_id,
        SettlementState {
            stream: subscription.key.clone(),
            group: subscription.group.clone(),
            message_id: entry.id,
            disposition: Arc::new(Mutex::new(None)),
            recovery: Arc::clone(&subscription.recovery),
        },
    );
    Ok(InboundMessage::new(
        topic,
        event_id,
        timestamp,
        headers,
        ordering_key,
        payload,
        Some(settlement),
        Default::default(),
    ))
}

/// Decodes a record or transfers a deterministic decode failure to quarantine.
///
/// # Parameters
///
/// - `subscription`: Receiver whose consumer group owns the entry.
/// - `connection`: Redis connection used for the transfer script.
/// - `entry`: Stream entry returned by a pending or new-message read.
///
/// # Returns
///
/// `Some` contains a message or gap; `None` means another consumer took
/// ownership and the caller should continue scanning.
///
/// # Errors
///
/// Returns an SPI error if Redis cannot execute the quarantine script or the
/// receiver's recovery state cannot be updated.
fn read_entry(
    subscription: &Subscription,
    connection: &mut impl ConnectionLike,
    entry: StreamId,
) -> Result<Option<ReceiveOutcome>, SpiError> {
    let id = entry.id.clone();
    match decode_entry(subscription, entry) {
        Ok(message) => {
            let marked = subscription
                .recovery
                .lock()
                .map_err(|_| {
                    spi_error(
                        "receive",
                        Some(&subscription.topic),
                        RedisProviderError::Operation("recovery lock"),
                    )
                })?
                .mark_delivered(id, subscription.max_unsettled);
            if !marked {
                return Err(spi_error(
                    "receive",
                    Some(&subscription.topic),
                    RedisProviderError::Operation("active delivery limit"),
                ));
            }
            Ok(Some(ReceiveOutcome::Message(message)))
        }
        Err(reason) => {
            let outcome = quarantine(
                connection,
                &subscription.key,
                &subscription.quarantine,
                &subscription.group,
                &subscription.consumer,
                &id,
                reason,
            )
            .map_err(|_| {
                spi_error(
                    "receive",
                    Some(&subscription.topic),
                    RedisProviderError::Operation("quarantine"),
                )
            })?;
            match outcome {
                PoisonOutcome::Quarantined => Ok(Some(ReceiveOutcome::Gap(DeliveryGap::new(
                    "malformed Redis stream entry was quarantined",
                    Some(1),
                )))),
                PoisonOutcome::SourceGone => Ok(Some(ReceiveOutcome::Gap(DeliveryGap::new(
                    "malformed pending Redis stream entry was removed",
                    Some(1),
                )))),
                PoisonOutcome::OwnershipChanged => Ok(None),
            }
        }
    }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use qubit_event_bus::model::StartPosition;
    use qubit_event_bus::spi::DeliveryDisposition;
    use qubit_event_bus::spi::EventSubscriptionSpi;
    use qubit_event_bus::spi::SettlementToken;
    use qubit_id::Id;

    use super::Subscription;
    use crate::client::Client;
    use crate::config::RedisEventBusConfig;
    use crate::recovery::RecoveryState;

    fn subscription() -> Subscription {
        let settings = RedisEventBusConfig::default();
        Subscription {
            client: Arc::new(Client::new(&settings).expect("default Redis client configuration is valid")),
            receive_connection: None,
            key: "unit-test-stream".into(),
            group: "unit-test-group".into(),
            quarantine: "unit-test-quarantine".into(),
            consumer: "unit-test-consumer".into(),
            topic: qubit_event_bus::spi::TopicAddress::new("unit-test-topic").expect("topic is valid"),
            subscription_id: Id::new(1),
            closed: false,
            claim_min_idle_ms: 0,
            max_unsettled: 1,
            recovery: Arc::new(Mutex::new(RecoveryState::new())),
        }
    }

    #[test]
    fn receive_reports_a_poisoned_recovery_lock() {
        let mut subscription = subscription();
        let recovery = Arc::clone(&subscription.recovery);
        let _ = std::thread::spawn(move || {
            let _guard = recovery.lock().expect("recovery lock is initially healthy");
            panic!("poison recovery lock for error-path coverage");
        })
        .join();
        let error = match subscription.receive(std::time::Duration::ZERO) {
            Ok(_) => panic!("poisoned recovery lock should return an error"),
            Err(error) => error,
        };
        assert!(matches!(error, qubit_event_bus::error::SpiError::Operation { .. }));
    }

    #[test]
    fn settle_rejects_an_unrecognized_token_state() {
        let mut subscription = subscription();
        let token = SettlementToken::new(subscription.subscription_id, StartPosition::New);
        assert!(subscription.settle(&token, DeliveryDisposition::Accept).is_err());
    }
}
