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
use qubit_id::Id;
use redis::Value;
use redis::aio::MultiplexedConnection;
use redis::cmd;
use redis::streams::StreamId;
use redis::streams::StreamRangeReply;
use redis::streams::StreamReadReply;

use crate::client::Client;
use crate::error::RedisProviderError;
use crate::error::from_redis_error as classified_spi_error;
use crate::internal::DecodeFailure;
use crate::internal::PoisonOutcome;
use crate::internal::PoisonReason;
use crate::internal::ReceiveAction;
use crate::internal::ReceiveDriver;
use crate::internal::ReceiveReply;
use crate::internal::RecoveryState;
use crate::internal::SettlementAction;
use crate::internal::SettlementState;
use crate::internal::decode_entry as decode_wire_entry;
use crate::internal::read_group_command;
use crate::internal::settlement_action;
use crate::poison::quarantine_async;
use crate::stream_protocol::parse_auto_claim;
use crate::stream_protocol::parse_pending_entries;

/// Asynchronous receiver whose cancelled reads remain in Redis PEL.
pub(crate) struct Subscription {
    /// Shared async connection factory.
    pub(crate) client: Arc<Client>,
    /// Receiver-only connection reused after completed receives. Cancellation
    /// leaves this empty so a connection with an in-flight reply is discarded.
    pub(crate) receive_connection: Option<MultiplexedConnection>,
    /// Redis stream key.
    pub(crate) key: String,
    /// Redis group name.
    pub(crate) group: String,
    /// Group-specific malformed-record quarantine stream.
    pub(crate) quarantine: String,
    /// Unique consumer name.
    pub(crate) consumer: String,
    /// Topic preserved for the facade.
    pub(crate) topic: TopicAddress,
    /// Owning facade subscription ID.
    pub(crate) subscription_id: Id,
    /// Whether this receiver has closed.
    pub(crate) closed: bool,
    /// Minimum idle milliseconds before claiming another consumer's pending
    /// item.
    pub(crate) claim_min_idle_ms: usize,
    /// Minimum delay between recovery rounds during one long receive.
    pub(crate) recovery_interval: Duration,
    /// Maximum unsettled records before reads pause.
    pub(crate) max_unsettled: usize,
    /// Validated limits checked before decoding one record.
    pub(crate) wire_limits: crate::wire_limits::WireLimits,
    /// Active-delivery registry and recovery cursors shared with tokens.
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
        .map_err(|_| spi_error(operation, topic, RedisProviderError::Operation(kind)))
}

impl AsyncEventSubscriptionSpi for Subscription {
    /// Reads pending deliveries before new group entries using a finite block.
    ///
    /// Cancellation leaves any Redis-delivered item in the pending entries
    /// list. The next call first scans reclaimable entries, then this
    /// consumer's pending entries, and finally waits for new group entries
    /// up to `timeout`.
    ///
    /// # Parameters
    ///
    /// - `timeout`: Maximum wait for a new entry; Redis blocking intervals are
    ///   capped so the call can observe the deadline.
    ///
    /// # Type Parameters
    ///
    /// - `'a`: Lifetime shared by the mutable receiver borrow and future.
    ///
    /// # Returns
    ///
    /// A message, a gap for removed pending entries, a timeout, or `Closed`.
    ///
    /// # Errors
    ///
    /// Returns an SPI operation error when a Redis command fails or a stream
    /// record cannot be decoded.
    fn receive<'a>(&'a mut self, timeout: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        Box::pin(async move {
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
                    .get_async_dedicated_connection()
                    .await
                    .map_err(|error| classified_spi_error("receive", Some(&self.topic), &error))?,
            };
            let result = async {
                let mut driver = ReceiveDriver::new(timeout, started, self.recovery_interval)
                    .map_err(|error| spi_error("receive", &self.topic, error))?;
                'receive: loop {
                    let deferred = {
                        let mut recovery = lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?;
                        recovery
                            .take_deferred_claim()
                            .filter(|entry| recovery.can_deliver(&entry.id))
                    };
                    if let Some(entry) = deferred
                        && let Some(outcome) = read_entry(self, &mut connection, entry, &mut driver).await?
                    {
                        return Ok(outcome);
                    }
                    while matches!(driver.next_action(Instant::now()), ReceiveAction::Claim) {
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
                            .query_async(&mut connection)
                            .await
                            .map_err(|error| classified_spi_error("receive", Some(&self.topic), &error))?;
                        let (claim, has_missing_entries) = parse_auto_claim(raw_claim).map_err(|_| {
                            spi_error("receive", &self.topic, RedisProviderError::Operation("XAUTOCLAIM"))
                        })?;
                        let at_end = claim.next_stream_id == "0-0";
                        driver.reply(if at_end {
                            ReceiveReply::ClaimAtEnd
                        } else {
                            ReceiveReply::ClaimHasMore
                        });
                        lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?
                            .set_claim_cursor(claim.next_stream_id);
                        let mut deleted_count = claim.deleted_ids.len() as u64;
                        if has_missing_entries && driver.budget_mut().take_tombstone_probe(Instant::now()) {
                            let tombstone_cursor = lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?
                                .tombstone_cursor()
                                .to_owned();
                            let pending_start = if tombstone_cursor == "0-0" {
                                "-".to_owned()
                            } else {
                                format!("({tombstone_cursor}")
                            };
                            let pending_reply: Value = cmd("XPENDING")
                                .arg(&self.key)
                                .arg(&self.group)
                                .arg(pending_start)
                                .arg("+")
                                .arg(4)
                                .query_async(&mut connection)
                                .await
                                .map_err(|error| classified_spi_error("receive", Some(&self.topic), &error))?;
                            let pending_rows = parse_pending_entries(pending_reply).map_err(|_| {
                                spi_error("receive", &self.topic, RedisProviderError::Operation("XPENDING"))
                            })?;
                            let pending_row_count = pending_rows.len();
                            let mut scan_complete = true;
                            for (id, owner, idle_ms) in pending_rows {
                                if idle_ms < self.claim_min_idle_ms as u64 {
                                    lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?
                                        .set_tombstone_cursor(id);
                                    continue;
                                }
                                if !driver.budget_mut().take_tombstone_range(Instant::now()) {
                                    scan_complete = false;
                                    break;
                                }
                                let rows: StreamRangeReply = cmd("XRANGE")
                                    .arg(&self.key)
                                    .arg(&id)
                                    .arg(&id)
                                    .query_async(&mut connection)
                                    .await
                                    .map_err(|error| classified_spi_error("receive", Some(&self.topic), &error))?;
                                if rows.ids.is_empty() {
                                    if !driver.budget_mut().take_maintenance_evaluation(Instant::now()) {
                                        scan_complete = false;
                                        break;
                                    }
                                    match quarantine_async(
                                        &mut connection,
                                        &self.key,
                                        &self.quarantine,
                                        &self.group,
                                        &owner,
                                        &id,
                                        PoisonReason::MissingWire,
                                    )
                                    .await
                                    .map_err(|error| classified_spi_error("receive", Some(&self.topic), &error))?
                                    {
                                        PoisonOutcome::TombstoneCleared => deleted_count += 1,
                                        PoisonOutcome::SourceGone | PoisonOutcome::OwnershipChanged => {}
                                        PoisonOutcome::Quarantined => {}
                                    }
                                }
                                lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?
                                    .set_tombstone_cursor(id);
                            }
                            if scan_complete && pending_row_count < 4 {
                                lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?
                                    .reset_tombstone_cursor();
                            }
                        }
                        if deleted_count > 0 {
                            if let Some(entry) = claim.claimed.into_iter().next() {
                                lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?.defer_claim(entry);
                            }
                            return Ok(ReceiveOutcome::Gap(DeliveryGap::new(
                                "pending Redis stream entries were removed",
                                Some(deleted_count),
                            )));
                        }
                        if let Some(entry) = claim.claimed.into_iter().next()
                            && lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?
                                .can_deliver(&entry.id)
                            && let Some(outcome) = read_entry(self, &mut connection, entry, &mut driver).await?
                        {
                            return Ok(outcome);
                        }
                        if at_end {
                            break;
                        }
                    }
                    while matches!(driver.next_action(Instant::now()), ReceiveAction::Pending) {
                        let cursor = lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?
                            .pending_cursor()
                            .to_owned();
                        let pending: Option<StreamReadReply> =
                            read_group_command(&self.group, &self.consumer, &self.key, &cursor, None)
                                .query_async(&mut connection)
                                .await
                                .map_err(|error| classified_spi_error("receive", Some(&self.topic), &error))?;
                        if let Some(entry) = pending
                            .and_then(|reply| reply.keys.into_iter().next())
                            .and_then(|stream| stream.ids.into_iter().next())
                        {
                            driver.reply(ReceiveReply::PendingEntry);
                            let id = entry.id.clone();
                            lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?
                                .set_pending_cursor(id.clone());
                            if lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?.can_deliver(&id)
                                && let Some(outcome) = read_entry(self, &mut connection, entry, &mut driver).await?
                            {
                                return Ok(outcome);
                            }
                        } else {
                            driver.reply(ReceiveReply::PendingEmpty);
                            lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?.reset_pending_scan();
                            break;
                        }
                    }
                    loop {
                        let block_ms = match driver.next_action(Instant::now()) {
                            ReceiveAction::ReadNew { block_ms } => block_ms,
                            ReceiveAction::Claim => continue 'receive,
                            ReceiveAction::TimedOut => return Ok(ReceiveOutcome::TimedOut),
                            ReceiveAction::Pending => unreachable!("pending stage was already scanned"),
                        };
                        let reply: Option<StreamReadReply> =
                            read_group_command(&self.group, &self.consumer, &self.key, ">", block_ms)
                                .query_async(&mut connection)
                                .await
                                .map_err(|error| classified_spi_error("receive", Some(&self.topic), &error))?;
                        let entry = reply
                            .and_then(|reply| reply.keys.into_iter().next())
                            .and_then(|stream| stream.ids.into_iter().next());
                        driver.reply(if entry.is_some() {
                            ReceiveReply::NewEntry
                        } else {
                            ReceiveReply::NewEmpty
                        });
                        if let Some(entry) = entry
                            && let Some(outcome) = read_entry(self, &mut connection, entry, &mut driver).await?
                        {
                            return Ok(outcome);
                        }
                    }
                }
            }
            .await;
            if result.is_ok() {
                self.receive_connection = Some(connection);
            }
            result
        })
    }

    /// Acknowledges terminal decisions and leaves retry decisions in PEL.
    ///
    /// Accept and Reject issue `XACK`; Retry only releases the receiver's local
    /// in-flight slot, leaving the Redis entry available for redelivery.
    /// Applying the same disposition again succeeds, while changing an
    /// applied disposition is rejected.
    ///
    /// # Parameters
    ///
    /// - `token`: Settlement token produced by this receiver.
    /// - `disposition`: Terminal action or retry decision to apply.
    ///
    /// # Type Parameters
    ///
    /// - `'a`: Lifetime shared by the mutable receiver borrow and future.
    ///
    /// # Returns
    ///
    /// Success after the requested disposition is recorded.
    ///
    /// # Errors
    ///
    /// Returns an invalid-token error for foreign, unknown, or conflicting
    /// tokens, or an operation error if Redis settlement fails.
    fn settle<'a>(
        &'a mut self,
        token: &SettlementToken,
        disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        let state = token.downcast_ref::<SettlementState>().map(|state| {
            (
                state.stream.clone(),
                state.group.clone(),
                state.message_id.clone(),
                Arc::clone(&state.disposition),
                Arc::clone(&state.recovery),
            )
        });
        let belongs = token.belongs_to(self.subscription_id);
        let topic = self.topic.clone();
        let client = Arc::clone(&self.client);
        Box::pin(async move {
            if !belongs {
                return Err(invalid_token("token belongs to another subscription", &topic));
            }
            let Some((stream, group, message_id, applied, recovery)) = state else {
                return Err(invalid_token("token type is not recognized", &topic));
            };
            if !Arc::ptr_eq(&recovery, &self.recovery) {
                return Err(invalid_token("token belongs to another receiver", &topic));
            }
            let action = {
                let current = lock_state(&applied, &topic, "settle", "settlement lock")?;
                settlement_action(*current, disposition)
                    .map_err(|()| invalid_token("token already has a different disposition", &topic))?
            };
            if matches!(action, SettlementAction::AlreadyApplied) {
                return Ok(());
            }
            if matches!(action, SettlementAction::Acknowledge) {
                let mut connection = client
                    .get_async_connection()
                    .await
                    .map_err(|error| classified_spi_error("settle", Some(&topic), &error))?;
                let result: Result<usize, redis::RedisError> = cmd("XACK")
                    .arg(stream)
                    .arg(group)
                    .arg(&message_id)
                    .query_async(&mut connection)
                    .await;
                if let Err(error) = result {
                    client.invalidate_async_connection().await;
                    return Err(classified_spi_error("settle", Some(&topic), &error));
                }
            }
            if matches!(action, SettlementAction::Retry) {
                lock_state(&recovery, &topic, "settle", "recovery lock")?.mark_retry(&message_id);
            } else {
                lock_state(&recovery, &topic, "settle", "recovery lock")?.mark_terminal(&message_id);
            }
            {
                let mut applied = lock_state(&applied, &topic, "settle", "settlement lock")?;
                if applied.is_none() {
                    *applied = Some(disposition);
                }
            }
            Ok(())
        })
    }

    /// Closes the receiver without implicitly acknowledging pending records.
    ///
    /// # Returns
    ///
    /// Success after future receive calls begin returning `Closed`.
    ///
    /// # Type Parameters
    ///
    /// - `'a`: Lifetime shared by the mutable receiver borrow and future.
    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        Box::pin(async move {
            self.closed = true;
            self.receive_connection.take();
            Ok(())
        })
    }
}

/// Decodes one Redis stream record and tracks its unsettled delivery slot.
///
/// Missing or malformed wire data is surfaced as a secret-safe operation
/// error; the helper does not delete or acknowledge the record on failure.
///
/// # Parameters
///
/// - `subscription`: Receiver supplying the topic, group, and in-flight
///   counter.
/// - `entry`: Stream entry returned by a read or claim command.
///
/// # Returns
///
/// A message outcome containing a token bound to this subscription.
///
/// # Errors
///
/// Returns an operation error when the wire field is absent, malformed, or
/// cannot be converted to typed event metadata.
async fn read_entry(
    subscription: &Subscription,
    connection: &mut redis::aio::MultiplexedConnection,
    entry: StreamId,
    driver: &mut ReceiveDriver,
) -> Result<Option<ReceiveOutcome>, SpiError> {
    let id = entry.id.clone();
    let message = match decode_entry(subscription, entry) {
        Ok(message) => message,
        Err(DecodeFailure::LimitExceeded) => {
            return Err(spi_error(
                "receive",
                &subscription.topic,
                RedisProviderError::LimitExceeded,
            ));
        }
        Err(DecodeFailure::UnsupportedVersion) => {
            return Err(SpiError::Operation {
                provider_id: "redis-streams".into(),
                operation: "receive",
                resource: Some(subscription.topic.as_str().into()),
                kind: "unsupported_wire_version",
                retryable: Some(false),
                source: Box::new(RedisProviderError::UnsupportedWireVersion),
            });
        }
        Err(DecodeFailure::Poison(reason)) => {
            if !driver.budget_mut().take_maintenance_evaluation(Instant::now()) {
                lock_state(&subscription.recovery, &subscription.topic, "receive", "recovery lock")?
                    .reset_pending_scan();
                return Ok(Some(ReceiveOutcome::TimedOut));
            }
            let outcome = quarantine_async(
                connection,
                &subscription.key,
                &subscription.quarantine,
                &subscription.group,
                &subscription.consumer,
                &id,
                reason,
            )
            .await
            .map_err(|error| classified_spi_error("receive", Some(&subscription.topic), &error))?;
            return Ok(match outcome {
                PoisonOutcome::Quarantined => Some(ReceiveOutcome::Gap(DeliveryGap::new(
                    "malformed Redis stream entry was quarantined",
                    Some(1),
                ))),
                PoisonOutcome::SourceGone => Some(ReceiveOutcome::Gap(DeliveryGap::new(
                    "malformed pending Redis stream entry was removed",
                    Some(1),
                ))),
                PoisonOutcome::TombstoneCleared => Some(ReceiveOutcome::Gap(DeliveryGap::new(
                    "deleted Redis stream entry was removed from the pending list",
                    Some(1),
                ))),
                PoisonOutcome::OwnershipChanged => None,
            });
        }
    };
    let marked = lock_state(&subscription.recovery, &subscription.topic, "receive", "recovery lock")?
        .mark_delivered(id, subscription.max_unsettled);
    if !marked {
        return Err(spi_error(
            "receive",
            &subscription.topic,
            RedisProviderError::Operation("active delivery limit"),
        ));
    }
    Ok(Some(ReceiveOutcome::Message(message)))
}

/// Decodes one Redis record into a message with a receiver-bound settlement
/// token.
///
/// # Parameters
///
/// - `subscription`: Receiver supplying the topic, group, and recovery state.
/// - `entry`: Stream entry returned by a read or claim command.
///
/// # Returns
///
/// The decoded inbound message.
///
/// # Errors
///
/// Returns the deterministic poison reason for malformed wire data.
fn decode_entry(subscription: &Subscription, entry: StreamId) -> Result<InboundMessage, DecodeFailure> {
    decode_wire_entry(
        subscription.subscription_id,
        &subscription.topic,
        &subscription.key,
        &subscription.group,
        &subscription.recovery,
        entry,
        subscription.wire_limits,
    )
}

/// Converts a provider failure into a retryable, secret-safe SPI error.
///
/// # Parameters
///
/// - `operation`: Stable SPI operation name.
/// - `topic`: Topic whose Redis stream is involved.
/// - `source`: Sanitized provider error category.
///
/// # Returns
///
/// An SPI operation error without raw Redis diagnostics.
fn spi_error(operation: &'static str, topic: &TopicAddress, source: RedisProviderError) -> SpiError {
    crate::error::to_spi_error(operation, Some(topic), source)
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
    use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
    use qubit_event_bus::spi::DeliveryDisposition;
    use qubit_event_bus::spi::SettlementToken;
    use qubit_id::Id;
    use redis::Value;
    use redis::streams::StreamId;

    use super::Subscription;
    use super::decode_entry;
    use crate::client::Client;
    use crate::config::RedisEventBusConfig;
    use crate::internal::RecoveryState;
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
            wire_limits: crate::wire_limits::WireLimits::default(),
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
                crate::internal::DecodeFailure::Poison(crate::internal::PoisonReason::MissingWire),
            ),
            (
                stream_entry(Some(Value::Nil)),
                crate::internal::DecodeFailure::Poison(crate::internal::PoisonReason::InvalidWireField),
            ),
            (
                stream_entry(Some(Value::BulkString(b"{".to_vec()))),
                crate::internal::DecodeFailure::Poison(crate::internal::PoisonReason::InvalidJson),
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
                crate::internal::DecodeFailure::Poison(crate::internal::PoisonReason::InvalidEventMetadata),
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
        assert_eq!(error, crate::internal::DecodeFailure::UnsupportedVersion);
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
        futures_lite::future::block_on(async {
            let mut subscription = subscription();
            let recovery = Arc::clone(&subscription.recovery);
            let _ = std::thread::spawn(move || {
                let _guard = recovery.lock().expect("recovery lock is initially healthy");
                panic!("poison recovery lock for error-path coverage");
            })
            .join();
            let error = match subscription.receive(std::time::Duration::ZERO).await {
                Ok(_) => panic!("poisoned recovery lock should return an error"),
                Err(error) => error,
            };
            assert!(matches!(error, qubit_event_bus::error::SpiError::Operation { .. }));
        });
    }

    #[test]
    fn settle_rejects_an_unrecognized_token_state() {
        futures_lite::future::block_on(async {
            let mut subscription = subscription();
            let token = SettlementToken::new(subscription.subscription_id, StartPosition::New);
            assert!(subscription.settle(&token, DeliveryDisposition::Accept).await.is_err());
        });
    }

    #[test]
    fn settle_rejects_a_token_from_a_different_receiver_with_the_same_id() {
        futures_lite::future::block_on(async {
            let mut subscription = subscription();
            let token = SettlementToken::new(
                subscription.subscription_id,
                crate::internal::SettlementState {
                    stream: subscription.key.clone(),
                    group: subscription.group.clone(),
                    message_id: "1-0".into(),
                    disposition: Arc::new(Mutex::new(None)),
                    recovery: Arc::new(Mutex::new(RecoveryState::new())),
                },
            );
            let error = subscription
                .settle(&token, DeliveryDisposition::Retry)
                .await
                .expect_err("foreign receiver state must be rejected");
            assert!(matches!(
                error,
                qubit_event_bus::error::SpiError::InvalidSettlementToken { .. }
            ));
        });
    }

    #[test]
    fn settle_reports_a_poisoned_disposition_lock() {
        futures_lite::future::block_on(async {
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
                crate::internal::SettlementState {
                    stream: subscription.key.clone(),
                    group: subscription.group.clone(),
                    message_id: "1-0".into(),
                    disposition,
                    recovery: Arc::clone(&subscription.recovery),
                },
            );

            assert!(subscription.settle(&token, DeliveryDisposition::Accept).await.is_err());
        });
    }

    #[test]
    fn close_makes_future_receives_return_closed() {
        futures_lite::future::block_on(async {
            let mut subscription = subscription();
            subscription.close().await.expect("close succeeds");
            assert!(matches!(
                subscription.receive(std::time::Duration::ZERO).await,
                Ok(qubit_event_bus::spi::ReceiveOutcome::Closed)
            ));
        });
    }

    #[test]
    fn retry_settlement_is_idempotent_and_rejects_a_conflicting_disposition() {
        futures_lite::future::block_on(async {
            let mut subscription = subscription();
            let recovery = Arc::clone(&subscription.recovery);
            recovery
                .lock()
                .expect("recovery lock is healthy")
                .mark_delivered("1-0".into(), 1);
            let token = SettlementToken::new(
                subscription.subscription_id,
                crate::internal::SettlementState {
                    stream: subscription.key.clone(),
                    group: subscription.group.clone(),
                    message_id: "1-0".into(),
                    disposition: Arc::new(Mutex::new(None)),
                    recovery,
                },
            );

            subscription
                .settle(&token, DeliveryDisposition::Retry)
                .await
                .expect("retry succeeds without a Redis acknowledgement");
            subscription
                .settle(&token, DeliveryDisposition::Retry)
                .await
                .expect("repeating retry is idempotent");
            assert!(subscription.settle(&token, DeliveryDisposition::Accept).await.is_err());
        });
    }
}
