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
use redis::Value;
use redis::cmd;
use redis::streams::StreamId;
use redis::streams::StreamRangeReply;
use redis::streams::StreamReadReply;

use super::internal::SettlementState;
use super::redis_event_bus::spi_error;
use crate::client::Client;
use crate::client::PooledConnection;
use crate::error::RedisProviderError;
use crate::poison::DecodeFailure;
use crate::poison::PoisonOutcome;
use crate::poison::PoisonReason;
use crate::poison::quarantine;
use crate::recovery::RecoveryScanBudget;
use crate::recovery::RecoveryScanStage;
use crate::recovery::RecoveryState;
use crate::stream_protocol::parse_auto_claim;
use crate::stream_protocol::parse_pending_entries;
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
    /// Minimum delay between recovery rounds during one long receive.
    pub(crate) recovery_interval: Duration,
    /// Maximum unsettled record count before reads pause.
    pub(crate) max_unsettled: usize,
    /// Local active-delivery set and recovery cursors shared with tokens.
    pub(crate) recovery: Arc<Mutex<RecoveryState>>,
}

/// Locks shared state and maps poisoning to a stable SPI error.
///
/// # Parameters
///
/// - `state`: Mutex protecting receiver or settlement state.
/// - `topic`: Topic associated with the SPI operation.
/// - `operation`: SPI operation that requested the lock.
/// - `kind`: Sanitized state category included in the error.
///
/// # Returns
///
/// A guard for the shared state.
///
/// # Errors
///
/// Returns a secret-safe operation error when another thread poisoned the lock.
fn lock_state<'a, T>(
    state: &'a Mutex<T>,
    topic: &TopicAddress,
    operation: &'static str,
    kind: &'static str,
) -> Result<std::sync::MutexGuard<'a, T>, SpiError> {
    state
        .lock()
        .map_err(|_| spi_error(operation, Some(topic), RedisProviderError::Operation(kind)))
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
        if lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?.active_len() >= self.max_unsettled {
            return Ok(ReceiveOutcome::TimedOut);
        }
        let started = Instant::now();
        let mut connection = match self.receive_connection.take() {
            Some(connection) => connection,
            None => self
                .client
                .get_dedicated_connection()
                .map_err(|_| spi_error("receive", Some(&self.topic), RedisProviderError::Operation("connect")))?,
        };
        let result = (|| {
            let mut budget = RecoveryScanBudget::new(timeout, started, self.recovery_interval)
                .map_err(|error| spi_error("receive", Some(&self.topic), error))?;
            'receive: loop {
                if let Some(entry) =
                    lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?.take_deferred_claim()
                    && lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?.can_deliver(&entry.id)
                    && let Some(outcome) = read_entry(self, &mut connection, entry)?
                {
                    return Ok(outcome);
                }
                while budget.take_recovery_command(RecoveryScanStage::Claim, Instant::now()) {
                    let cursor = lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?
                        .claim_cursor()
                        .to_owned();
                    let raw_claim: Value = cmd("XAUTOCLAIM")
                        .arg(&self.key)
                        .arg(&self.group)
                        .arg(&self.consumer)
                        .arg(self.claim_min_idle_ms)
                        .arg(&cursor)
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
                    let (claim, has_missing_entries) = parse_auto_claim(raw_claim).map_err(|_| {
                        spi_error(
                            "receive",
                            Some(&self.topic),
                            RedisProviderError::Operation("XAUTOCLAIM"),
                        )
                    })?;
                    let at_end = claim.next_stream_id == "0-0";
                    let claimed = claim.claimed.into_iter().next();
                    lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?
                        .set_claim_cursor(claim.next_stream_id);
                    let mut deleted_count = claim.deleted_ids.len() as u64;
                    if has_missing_entries {
                        let pending_reply: Value = cmd("XPENDING")
                            .arg(&self.key)
                            .arg(&self.group)
                            .arg(&cursor)
                            .arg("+")
                            .arg(16)
                            .query(&mut connection)
                            .map_err(|_| {
                                spi_error("receive", Some(&self.topic), RedisProviderError::Operation("XPENDING"))
                            })?;
                        let pending_rows = parse_pending_entries(pending_reply).map_err(|_| {
                            spi_error("receive", Some(&self.topic), RedisProviderError::Operation("XPENDING"))
                        })?;
                        for (id, owner, idle_ms) in pending_rows {
                            if idle_ms < self.claim_min_idle_ms as u64 {
                                continue;
                            }
                            let rows: StreamRangeReply = cmd("XRANGE")
                                .arg(&self.key)
                                .arg(&id)
                                .arg(&id)
                                .query(&mut connection)
                                .map_err(|_| {
                                    spi_error("receive", Some(&self.topic), RedisProviderError::Operation("XRANGE"))
                                })?;
                            if rows.ids.is_empty() {
                                match quarantine(
                                    &mut connection,
                                    &self.key,
                                    &self.quarantine,
                                    &self.group,
                                    &owner,
                                    &id,
                                    PoisonReason::MissingWire,
                                )
                                .map_err(|_| {
                                    spi_error(
                                        "receive",
                                        Some(&self.topic),
                                        RedisProviderError::Operation("XACK tombstone"),
                                    )
                                })? {
                                    PoisonOutcome::TombstoneCleared => deleted_count += 1,
                                    PoisonOutcome::SourceGone | PoisonOutcome::OwnershipChanged => {}
                                    PoisonOutcome::Quarantined => {}
                                }
                            }
                        }
                    }
                    if deleted_count > 0 {
                        if let Some(entry) = claimed {
                            lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?.defer_claim(entry);
                        }
                        return Ok(ReceiveOutcome::Gap(DeliveryGap::new(
                            "pending Redis stream entries were removed",
                            Some(deleted_count),
                        )));
                    }
                    if let Some(entry) = claimed {
                        let available =
                            lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?.can_deliver(&entry.id);
                        if available && let Some(outcome) = read_entry(self, &mut connection, entry)? {
                            return Ok(outcome);
                        }
                    }
                    if at_end {
                        break;
                    }
                }

                while budget.take_recovery_command(RecoveryScanStage::Pending, Instant::now()) {
                    let cursor = lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?
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
                        lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?.reset_pending_scan();
                        break;
                    };
                    let id = entry.id.clone();
                    lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?.set_pending_cursor(id.clone());
                    let available =
                        lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?.can_deliver(&id);
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
                    let now = Instant::now();
                    if budget.recovery_due(now) {
                        budget.start_recovery_round(now);
                        continue 'receive;
                    }
                    let Some(block_interval) = budget.block_interval(Instant::now()) else {
                        return Ok(ReceiveOutcome::TimedOut);
                    };
                    if !budget.can_read_new(Instant::now()) {
                        return Ok(ReceiveOutcome::TimedOut);
                    }
                    let block_ms = block_interval.as_millis().clamp(1, 1_000) as usize;
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
                    if !budget.can_read_new(Instant::now()) {
                        return Ok(ReceiveOutcome::TimedOut);
                    }
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
        let mut applied = lock_state(&state.disposition, &self.topic, "settle", "settlement lock")?;
        if let Some(previous) = *applied {
            return if previous == disposition {
                Ok(())
            } else {
                Err(invalid_token("token already has a different disposition", &self.topic))
            };
        }
        if disposition == DeliveryDisposition::Retry {
            lock_state(&state.recovery, &self.topic, "settle", "recovery lock")?.mark_retry(&state.message_id);
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
        lock_state(&state.recovery, &self.topic, "settle", "recovery lock")?.mark_terminal(&state.message_id);
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
fn decode_entry(subscription: &Subscription, entry: StreamId) -> Result<InboundMessage, DecodeFailure> {
    let Some(value) = entry.map.get("wire") else {
        return Err(DecodeFailure::Poison(PoisonReason::MissingWire));
    };
    let payload: String =
        String::from_redis_value(value).map_err(|_| DecodeFailure::Poison(PoisonReason::InvalidWireField))?;
    let fields = WireFields::decode_wire(&payload).map_err(|error| {
        if matches!(error, RedisProviderError::UnsupportedWireVersion) {
            DecodeFailure::UnsupportedVersion
        } else {
            DecodeFailure::Poison(PoisonReason::InvalidJson)
        }
    })?;
    let (topic, event_id, timestamp, headers, ordering_key, payload) =
        fields.into_parts(subscription.topic.clone()).map_err(|error| {
            if matches!(error, RedisProviderError::UnsupportedWireVersion) {
                DecodeFailure::UnsupportedVersion
            } else {
                DecodeFailure::Poison(PoisonReason::InvalidEventMetadata)
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
            let marked = lock_state(&subscription.recovery, &subscription.topic, "receive", "recovery lock")?
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
        Err(DecodeFailure::UnsupportedVersion) => Err(SpiError::Operation {
            provider_id: "redis-streams".into(),
            operation: "receive",
            resource: Some(subscription.topic.as_str().into()),
            kind: "unsupported_wire_version",
            retryable: Some(false),
            source: Box::new(RedisProviderError::UnsupportedWireVersion),
        }),
        Err(DecodeFailure::Poison(reason)) => {
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
                PoisonOutcome::TombstoneCleared => Ok(Some(ReceiveOutcome::Gap(DeliveryGap::new(
                    "deleted Redis stream entry was removed from the pending list",
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
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use qubit_event_bus::model::StartPosition;
    use qubit_event_bus::spi::DeliveryDisposition;
    use qubit_event_bus::spi::EventSubscriptionSpi;
    use qubit_event_bus::spi::SettlementToken;
    use qubit_id::Id;
    use redis::Value;
    use redis::streams::StreamId;

    use super::Subscription;
    use super::decode_entry;
    use crate::client::Client;
    use crate::config::RedisEventBusConfig;
    use crate::recovery::RecoveryState;
    use crate::wire::WireFields;

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
            recovery_interval: Duration::from_secs(1),
            max_unsettled: 1,
            recovery: Arc::new(Mutex::new(RecoveryState::new())),
        }
    }

    fn stream_entry(wire: Option<Value>) -> StreamId {
        let mut map = HashMap::new();
        if let Some(wire) = wire {
            map.insert("wire".into(), wire);
        }
        StreamId { id: "1-0".into(), map }
    }

    #[test]
    fn decode_entry_reports_each_malformed_wire_category() {
        let subscription = subscription();
        let cases = [
            (
                stream_entry(None),
                crate::poison::DecodeFailure::Poison(crate::poison::PoisonReason::MissingWire),
            ),
            (
                stream_entry(Some(Value::Nil)),
                crate::poison::DecodeFailure::Poison(crate::poison::PoisonReason::InvalidWireField),
            ),
            (
                stream_entry(Some(Value::BulkString(b"{".to_vec()))),
                crate::poison::DecodeFailure::Poison(crate::poison::PoisonReason::InvalidJson),
            ),
            (
                stream_entry(Some(Value::BulkString(
                    serde_json::to_vec(&WireFields {
                        version: 1,
                        event_id: "".into(),
                        timestamp_ms: 0,
                        headers_json: "{}".into(),
                        ordering_key: None,
                        content_type: "application/octet-stream".into(),
                        schema_id: None,
                        payload: Vec::new(),
                    })
                    .expect("wire fields serialize"),
                ))),
                crate::poison::DecodeFailure::Poison(crate::poison::PoisonReason::InvalidEventMetadata),
            ),
        ];

        for (entry, expected) in cases {
            let error = match decode_entry(&subscription, entry) {
                Ok(_) => panic!("invalid wire should be rejected"),
                Err(error) => error,
            };
            assert_eq!(error, expected);
        }
        let unsupported = stream_entry(Some(Value::BulkString(br#"{"version":99}"#.to_vec())));
        let error = match decode_entry(&subscription, unsupported) {
            Ok(_) => panic!("unsupported wire version should be rejected"),
            Err(error) => error,
        };
        assert_eq!(error, crate::poison::DecodeFailure::UnsupportedVersion);
    }

    #[test]
    fn decode_entry_builds_a_settlement_token_for_valid_wire_data() {
        let subscription = subscription();
        let entry = stream_entry(Some(Value::BulkString(
            serde_json::to_vec(&WireFields {
                version: 1,
                event_id: "event-1".into(),
                timestamp_ms: 42,
                headers_json: "{}".into(),
                ordering_key: None,
                content_type: "application/octet-stream".into(),
                schema_id: None,
                payload: vec![1, 2, 3],
            })
            .expect("wire fields serialize"),
        )));

        let message = decode_entry(&subscription, entry).expect("valid wire is decoded");
        assert_eq!(message.id().as_str(), "event-1");
        assert!(message.settlement().is_some());
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

    #[test]
    fn settle_reports_a_poisoned_disposition_lock() {
        let mut subscription = subscription();
        let disposition = Arc::new(Mutex::new(None));
        let poisoned = Arc::clone(&disposition);
        let _ = std::thread::spawn(move || {
            let _guard = poisoned.lock().expect("disposition lock is initially healthy");
            panic!("poison disposition lock for error-path coverage");
        })
        .join();
        let token = SettlementToken::new(
            subscription.subscription_id,
            super::super::internal::SettlementState {
                stream: subscription.key.clone(),
                group: subscription.group.clone(),
                message_id: "1-0".into(),
                disposition,
                recovery: Arc::clone(&subscription.recovery),
            },
        );

        assert!(subscription.settle(&token, DeliveryDisposition::Accept).is_err());
    }

    #[test]
    fn close_makes_future_receives_return_closed() {
        let mut subscription = subscription();
        subscription.close().expect("close succeeds");
        assert!(matches!(
            subscription.receive(std::time::Duration::ZERO),
            Ok(qubit_event_bus::spi::ReceiveOutcome::Closed)
        ));
    }

    #[test]
    fn retry_settlement_is_idempotent_and_rejects_a_conflicting_disposition() {
        let mut subscription = subscription();
        let recovery = Arc::clone(&subscription.recovery);
        recovery
            .lock()
            .expect("recovery lock is healthy")
            .mark_delivered("1-0".into(), 1);
        let token = SettlementToken::new(
            subscription.subscription_id,
            super::super::internal::SettlementState {
                stream: subscription.key.clone(),
                group: subscription.group.clone(),
                message_id: "1-0".into(),
                disposition: Arc::new(Mutex::new(None)),
                recovery,
            },
        );

        subscription
            .settle(&token, DeliveryDisposition::Retry)
            .expect("retry succeeds without a Redis acknowledgement");
        subscription
            .settle(&token, DeliveryDisposition::Retry)
            .expect("repeating retry is idempotent");
        assert!(subscription.settle(&token, DeliveryDisposition::Accept).is_err());
    }
}
