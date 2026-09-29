// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Synchronous Redis Streams consumer group receiver.

/// Controls short-command admission and bounded replies on dedicated receiver
/// sockets.
#[path = "subscription/internal/receive_command.rs"]
mod receive_command;

use std::io::Error as IoError;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
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
use redis::RedisError;
use redis::Value;
use redis::cmd;
use redis::streams::StreamId;

use self::receive_command::ReceiveCommand;
use super::redis_event_bus::spi_error;
use crate::client::Client;
use crate::client::PooledConnection;
use crate::client::ReceiverPermit;
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
use crate::internal::SettlementProgress;
use crate::internal::SettlementState;
use crate::internal::WireLimits;
use crate::internal::decode_entry as decode_wire_entry;
use crate::internal::read_group_command;
use crate::poison::quarantine;
use crate::stream_protocol::parse_auto_claim;
use crate::stream_protocol::parse_pending_entries;
use crate::stream_protocol::parse_range;
use crate::stream_protocol::parse_read_group;

/// One blocking consumer owned by a facade subscription.
pub(crate) struct Subscription {
    /// Shared blocking connection factory.
    pub(crate) client: Arc<Client>,
    /// Inclusive byte budgets applied before wire parsing and payload delivery.
    pub(crate) wire_limits: WireLimits,
    /// Dedicated connection reused between completed receives.
    pub(crate) receive_connection: Option<PooledConnection>,
    /// Receiver admission released on close/drop, independent of tokens.
    pub(crate) receiver_permit: Option<ReceiverPermit>,
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
/// # Type Parameters
///
/// - `'a`: Lifetime of the borrowed mutex retained by the returned guard.
/// - `T`: Protected receiver or settlement bookkeeping.
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
) -> Result<MutexGuard<'a, T>, SpiError> {
    state
        .lock()
        .map_err(|_| spi_error(operation, Some(topic), RedisProviderError::Operation(kind)))
}

impl EventSubscriptionSpi for Subscription {
    /// Recovers pending work without redelivering locally active entries.
    ///
    /// The call scans reclaimable entries, this consumer's pending entries, and
    /// then new group entries. Pending IDs already held by local handlers are
    /// skipped. Malformed entries are copied to the group quarantine stream
    /// and acknowledged by one serialized Lua invocation. Lua does not roll
    /// back prior writes on failure, and a lost reply leaves the transfer
    /// outcome unknown. Closing the receiver does not acknowledge valid work.
    ///
    /// # Parameters
    ///
    /// - `timeout`: Scheduling wait for new entries, zero for bounded
    ///   non-blocking phase reads, or Duration::MAX for repeated finite BLOCK
    ///   intervals. Individual I/O waits can extend beyond this scheduling
    ///   deadline.
    ///
    /// # Returns
    ///
    /// A message, a gap for removed or quarantined entries, a timeout, or
    /// `Closed`.
    ///
    /// # Errors
    ///
    /// Returns a classified SPI error for admission, locks, connection/setup,
    /// command/protocol failure, or unsupported wire version. Unknown read
    /// outcomes recover through later PEL scans; an uncertain quarantine is
    /// not transparently replayed.
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
                .map_err(|error| spi_error("receive", Some(&self.topic), error))?,
        };
        let result = (|| {
            let mut driver = ReceiveDriver::new(timeout, started, self.recovery_interval)
                .map_err(|error| spi_error("receive", Some(&self.topic), error))?;
            loop {
                // Release the guard before read_entry acquires recovery state again.
                let deferred = {
                    let mut recovery = lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?;
                    recovery
                        .take_deferred_claim()
                        .filter(|entry| recovery.can_deliver(&entry.id))
                };
                if let Some(entry) = deferred
                    && let Some(outcome) = read_entry(self, &mut connection, entry, &mut driver)?
                {
                    return Ok(outcome);
                }
                let short_timeout = self.client.command_timeout();
                connection
                    .set_read_timeout(Some(short_timeout))
                    .map_err(|error| classified_spi_error("receive", Some(&self.topic), &error))?;
                connection
                    .set_write_timeout(Some(short_timeout))
                    .map_err(|error| classified_spi_error("receive", Some(&self.topic), &error))?;
                match driver.next_action(Instant::now()) {
                    ReceiveAction::Claim => {
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
                            .query_receive(&mut connection, &self.client, true)
                            .map_err(|error| spi_error("receive", Some(&self.topic), error))?;
                        let (claim, has_missing_entries) = parse_auto_claim(raw_claim).map_err(|_| {
                            spi_error(
                                "receive",
                                Some(&self.topic),
                                RedisProviderError::OutcomeUnknown { operation: "receive" },
                            )
                        })?;
                        let at_end = claim.next_stream_id == "0-0";
                        driver.reply(if at_end {
                            ReceiveReply::ClaimAtEnd
                        } else {
                            ReceiveReply::ClaimHasMore
                        });
                        let claimed = claim.claimed.into_iter().next();
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
                                .query_receive(&mut connection, &self.client, true)
                                .map_err(|error| spi_error("receive", Some(&self.topic), error))?;
                            let pending_rows = parse_pending_entries(pending_reply).map_err(|_| {
                                spi_error(
                                    "receive",
                                    Some(&self.topic),
                                    RedisProviderError::OutcomeUnknown { operation: "receive" },
                                )
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
                                let raw_rows: Value = cmd("XRANGE")
                                    .arg(&self.key)
                                    .arg(&id)
                                    .arg(&id)
                                    .query_receive(&mut connection, &self.client, true)
                                    .map_err(|error| spi_error("receive", Some(&self.topic), error))?;
                                let rows = parse_range(raw_rows).map_err(|_| {
                                    spi_error(
                                        "receive",
                                        Some(&self.topic),
                                        RedisProviderError::OutcomeUnknown { operation: "receive" },
                                    )
                                })?;
                                if rows.ids.is_empty() {
                                    if !driver.budget_mut().take_maintenance_evaluation(Instant::now()) {
                                        scan_complete = false;
                                        break;
                                    }
                                    let _permit = self
                                        .client
                                        .try_command()
                                        .map_err(|error| spi_error("receive", Some(&self.topic), error))?;
                                    let timeout = self.client.command_timeout();
                                    connection
                                        .set_read_timeout(Some(timeout))
                                        .map_err(|error| classified_spi_error("receive", Some(&self.topic), &error))?;
                                    connection
                                        .set_write_timeout(Some(timeout))
                                        .map_err(|error| classified_spi_error("receive", Some(&self.topic), &error))?;
                                    match quarantine(
                                        &mut connection,
                                        &self.key,
                                        &self.quarantine,
                                        &self.group,
                                        &owner,
                                        &id,
                                        PoisonReason::MissingWire,
                                    )
                                    .map_err(|_error| {
                                        spi_error(
                                            "receive",
                                            Some(&self.topic),
                                            RedisProviderError::OutcomeUnknown {
                                                operation: "quarantine",
                                            },
                                        )
                                    })? {
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
                            if let Some(entry) = claimed {
                                lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?.defer_claim(entry);
                            }
                            return Ok(ReceiveOutcome::Gap(DeliveryGap::new(
                                "pending Redis stream entries were removed",
                                Some(deleted_count),
                            )));
                        }
                        if let Some(entry) = claimed {
                            let available = lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?
                                .can_deliver(&entry.id);
                            if available && let Some(outcome) = read_entry(self, &mut connection, entry, &mut driver)? {
                                return Ok(outcome);
                            }
                        }
                    }

                    ReceiveAction::Pending => {
                        let cursor = lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?
                            .pending_cursor()
                            .to_owned();
                        let raw_pending: Value =
                            read_group_command(&self.group, &self.consumer, &self.key, &cursor, None)
                                .query_receive(&mut connection, &self.client, true)
                                .map_err(|error| spi_error("receive", Some(&self.topic), error))?;
                        let pending = parse_read_group(raw_pending).map_err(|_| {
                            spi_error(
                                "receive",
                                Some(&self.topic),
                                RedisProviderError::OutcomeUnknown { operation: "receive" },
                            )
                        })?;
                        let entry = pending
                            .and_then(|reply| reply.keys.into_iter().next())
                            .and_then(|stream| stream.ids.into_iter().next());
                        let Some(entry) = entry else {
                            driver.reply(ReceiveReply::PendingEmpty);
                            lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?.reset_pending_scan();
                            continue;
                        };
                        driver.reply(ReceiveReply::PendingEntry);
                        let id = entry.id.clone();
                        lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?
                            .set_pending_cursor(id.clone());
                        let available =
                            lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?.can_deliver(&id);
                        if available && let Some(outcome) = read_entry(self, &mut connection, entry, &mut driver)? {
                            return Ok(outcome);
                        }
                    }

                    ReceiveAction::ReadNew { block_ms } => {
                        let response_timeout = self
                            .client
                            .response_timeout(block_ms.map(|ms| ms.clamp(1, 1_000)))
                            .map_err(|error| spi_error("receive", Some(&self.topic), error))?;
                        connection
                            .set_read_timeout(Some(response_timeout))
                            .map_err(|error| classified_spi_error("receive", Some(&self.topic), &error))?;
                        connection
                            .set_write_timeout(Some(response_timeout))
                            .map_err(|error| classified_spi_error("receive", Some(&self.topic), &error))?;
                        let raw_reply: Value =
                            read_group_command(&self.group, &self.consumer, &self.key, ">", block_ms)
                                .query_receive(&mut connection, &self.client, block_ms.is_none())
                                .map_err(|error| spi_error("receive", Some(&self.topic), error))?;
                        let reply = parse_read_group(raw_reply).map_err(|_| {
                            spi_error(
                                "receive",
                                Some(&self.topic),
                                RedisProviderError::OutcomeUnknown { operation: "receive" },
                            )
                        })?;
                        let entry = reply
                            .and_then(|reply| reply.keys.into_iter().next())
                            .and_then(|stream| stream.ids.into_iter().next());
                        driver.reply(if entry.is_some() {
                            ReceiveReply::NewEntry
                        } else {
                            ReceiveReply::NewEmpty
                        });
                        if let Some(entry) = entry
                            && let Some(outcome) = read_entry(self, &mut connection, entry, &mut driver)?
                        {
                            return Ok(outcome);
                        }
                    }
                    ReceiveAction::TimedOut => return Ok(ReceiveOutcome::TimedOut),
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
    /// disposition succeeds. Once XACK may have been sent, its intent remains
    /// fixed across reply loss; conflicting dispositions are rejected.
    ///
    /// # Parameters
    ///
    /// - `token`: Settlement token produced by this receiver.
    /// - `disposition`: Terminal action or retry decision to apply.
    ///
    /// # Returns
    ///
    /// Success after local active-slot release and Applied bookkeeping.
    /// Terminal decisions require a valid XACK reply (0 or 1); Retry commits
    /// locally without acknowledging Redis. Repeating the applied intent is
    /// idempotent, while unknown prior ACK intent remains fixed.
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
        if !Arc::ptr_eq(&state.recovery, &self.recovery) {
            return Err(invalid_token("token belongs to another receiver", &self.topic));
        }
        let action = {
            let progress = lock_state(&state.progress, &self.topic, "settle", "settlement lock")?;
            progress
                .action(disposition)
                .map_err(|()| invalid_token("token already has a different disposition", &self.topic))?
        };
        if action == SettlementAction::AlreadyApplied {
            return Ok(());
        }
        if action != SettlementAction::ApplyRetry {
            let mut connection = self
                .client
                .get_connection()
                .map_err(|error| spi_error("settle", Some(&self.topic), error))?;
            // The connection is ready: any following failure may follow a sent XACK.
            *lock_state(&state.progress, &self.topic, "settle", "settlement lock")? =
                SettlementProgress::AckPending(disposition);
            let mut command = cmd("XACK");
            command.arg(&state.stream).arg(&state.group).arg(&state.message_id);
            // Preserve nested errors as malformed replies rather than rejecting XACK.
            let result = connection.req_command(&command);
            match result {
                Ok(Value::Int(0 | 1)) => {}
                Ok(Value::ServerError(error)) => {
                    let error: RedisError = error.into();
                    if action == SettlementAction::PrepareAck {
                        *lock_state(&state.progress, &self.topic, "settle", "settlement lock")? =
                            SettlementProgress::Open;
                    }
                    return Err(classified_spi_error("settle", Some(&self.topic), &error));
                }
                Ok(_) | Err(_) => {
                    connection.discard();
                    return Err(spi_error(
                        "settle",
                        Some(&self.topic),
                        RedisProviderError::OutcomeUnknown { operation: "settle" },
                    ));
                }
            }
        }
        state
            .commit(disposition)
            .map_err(|label| spi_error("settle", Some(&self.topic), RedisProviderError::Operation(label)))?;
        Ok(())
    }

    /// Closes the receiver without acknowledging pending messages.
    ///
    /// # Returns
    ///
    /// Success after subsequent receive calls return `Closed`, the dedicated
    /// transport is dropped, and receiver admission is released. Tokens retain
    /// their settlement bookkeeping but no receiver permit. No Redis command
    /// is issued and this implementation cannot return an error.
    fn close(&mut self) -> Result<(), SpiError> {
        self.closed = true;
        self.receive_connection.take();
        self.receiver_permit.take();
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
/// Returns a deterministic poison category for malformed fields, invalid
/// metadata, or byte-budget rejection; unknown numeric versions remain
/// unsupported without ACK.
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

/// Decodes a record or transfers a deterministic decode failure to quarantine.
///
/// # Parameters
///
/// - `subscription`: Receiver whose consumer group owns the entry.
/// - `connection`: Redis connection used for the transfer script.
/// - `entry`: Stream entry returned by a pending or new-message read.
/// - `driver`: Receive budget used to reserve poison maintenance.
///
/// # Returns
///
/// `Some` contains a message, gap, or TimedOut after maintenance quota
/// exhaustion; `None` means another consumer owns the poison record and the
/// caller should continue scanning. Isolation can copy wire data and ACK the
/// PEL entry; Redis Lua does not roll back partial writes.
///
/// # Errors
///
/// Returns unsupported-version without ACK, active-state/lock failure,
/// admission exhaustion, or uncertain quarantine execution. The caller must
/// inspect PEL state on a later receive rather than transparently replaying an
/// uncertain script.
fn read_entry(
    subscription: &Subscription,
    connection: &mut PooledConnection,
    entry: StreamId,
    driver: &mut ReceiveDriver,
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
        Err(DecodeFailure::LimitExceeded) => Err(spi_error(
            "receive",
            Some(&subscription.topic),
            RedisProviderError::LimitExceeded,
        )),
        Err(DecodeFailure::UnsupportedVersion) => Err(SpiError::Operation {
            provider_id: "redis-streams".into(),
            operation: "receive",
            resource: Some(subscription.topic.as_str().into()),
            kind: "unsupported_wire_version",
            retryable: Some(false),
            source: Box::new(RedisProviderError::UnsupportedWireVersion),
        }),
        Err(DecodeFailure::Poison(reason)) => {
            if !driver.budget_mut().take_maintenance_evaluation(Instant::now()) {
                lock_state(&subscription.recovery, &subscription.topic, "receive", "recovery lock")?
                    .reset_pending_scan();
                return Ok(Some(ReceiveOutcome::TimedOut));
            }
            let _permit = subscription
                .client
                .try_command()
                .map_err(|error| spi_error("receive", Some(&subscription.topic), error))?;
            let timeout = subscription.client.command_timeout();
            connection
                .set_read_timeout(Some(timeout))
                .map_err(|error| classified_spi_error("receive", Some(&subscription.topic), &error))?;
            connection
                .set_write_timeout(Some(timeout))
                .map_err(|error| classified_spi_error("receive", Some(&subscription.topic), &error))?;
            let outcome = quarantine(
                connection,
                &subscription.key,
                &subscription.quarantine,
                &subscription.group,
                &subscription.consumer,
                &id,
                reason,
            )
            .map_err(|_error| {
                spi_error(
                    "receive",
                    Some(&subscription.topic),
                    RedisProviderError::OutcomeUnknown {
                        operation: "quarantine",
                    },
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
        source: Box::new(IoError::other("invalid settlement token")),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::error::Error;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::thread::scope;
    use std::thread::spawn;
    use std::time::Duration;

    use qubit_event_bus::error::SpiError;
    use qubit_event_bus::spi::DeliveryDisposition;
    use qubit_event_bus::spi::EventSubscriptionSpi;
    use qubit_event_bus::spi::SettlementToken;
    use qubit_event_bus::spi::TopicAddress;
    use qubit_id::Id;
    use redis::Client as RedisClient;
    use redis::Value;
    use redis::cmd;
    use redis::streams::StreamId;
    use serde_json::to_vec;

    use super::Subscription;
    use super::decode_entry;
    use crate::client::Client;
    use crate::config::RedisEventBusConfig;
    use crate::error::RedisProviderError;
    use crate::internal::RecoveryState;
    use crate::internal::SettlementProgress;
    use crate::internal::SettlementState;
    use crate::tests::support::redis_support::controlled_redis::proxy::ControlledRedis;
    use crate::tests::support::redis_support::redis_server::RedisServer;
    use crate::wire::WireFields;

    /// Builds synthetic receiver state without opening Redis so private failure
    /// paths are observable.
    fn subscription() -> Subscription {
        let settings = RedisEventBusConfig::default();
        Subscription {
            client: Arc::new(Client::new(&settings).expect("default Redis client configuration is valid")),
            wire_limits: crate::internal::WireLimits::from_config(&settings),
            receive_connection: None,
            receiver_permit: None,
            key: "unit-test-stream".into(),
            group: "unit-test-group".into(),
            quarantine: "unit-test-quarantine".into(),
            consumer: "unit-test-consumer".into(),
            topic: TopicAddress::new("unit-test-topic").expect("topic is valid"),
            subscription_id: Id::new(1),
            closed: false,
            claim_min_idle_ms: 0,
            recovery_interval: Duration::from_secs(1),
            max_unsettled: 1,
            recovery: Arc::new(Mutex::new(RecoveryState::new())),
        }
    }

    /// Constructs a fixed-ID RESP record with a present wire value or a
    /// missing-field fixture.
    fn stream_entry(wire: Option<Value>) -> StreamId {
        let mut map = HashMap::new();
        if let Some(wire) = wire {
            map.insert("wire".into(), wire);
        }
        StreamId { id: "1-0".into(), map }
    }

    #[test]
    fn test_decode_entry_reports_each_malformed_wire_category() {
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
                    to_vec(&WireFields {
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
    fn test_decode_entry_builds_a_settlement_token_for_valid_wire_data() {
        let subscription = subscription();
        let entry = stream_entry(Some(Value::BulkString(
            to_vec(&WireFields {
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
    fn test_receive_reports_a_poisoned_recovery_lock() {
        let mut subscription = subscription();
        let recovery = Arc::clone(&subscription.recovery);
        let _ = spawn(move || {
            let _guard = recovery.lock().expect("recovery lock is initially healthy");
            panic!("poison recovery lock for error-path coverage");
        })
        .join();
        let error = match subscription.receive(Duration::ZERO) {
            Ok(_) => panic!("poisoned recovery lock should return an error"),
            Err(error) => error,
        };
        assert!(matches!(error, SpiError::Operation { .. }));
    }

    #[test]
    fn test_settle_rejects_a_token_from_a_different_receiver_with_the_same_id() {
        let mut subscription = subscription();
        let token = SettlementToken::new(
            subscription.subscription_id,
            SettlementState {
                stream: subscription.key.clone(),
                group: subscription.group.clone(),
                message_id: "1-0".into(),
                progress: Arc::new(Mutex::new(SettlementProgress::Open)),
                recovery: Arc::new(Mutex::new(RecoveryState::new())),
            },
        );
        let error = subscription
            .settle(&token, DeliveryDisposition::Retry)
            .expect_err("foreign receiver state must be rejected");
        assert!(matches!(error, SpiError::InvalidSettlementToken { .. }));
    }

    #[test]
    fn test_settle_reports_a_poisoned_disposition_lock() {
        let mut subscription = subscription();
        let progress = Arc::new(Mutex::new(SettlementProgress::Open));
        let poisoned = Arc::clone(&progress);
        let _ = spawn(move || {
            let _guard = poisoned.lock().expect("disposition lock is initially healthy");
            panic!("poison disposition lock for error-path coverage");
        })
        .join();
        let token = SettlementToken::new(
            subscription.subscription_id,
            SettlementState {
                stream: subscription.key.clone(),
                group: subscription.group.clone(),
                message_id: "1-0".into(),
                progress,
                recovery: Arc::clone(&subscription.recovery),
            },
        );

        assert!(subscription.settle(&token, DeliveryDisposition::Accept).is_err());
    }

    #[test]
    fn test_settle_connection_acquisition_failure_preserves_open_intent() {
        let mut subscription = subscription();
        let settings = RedisEventBusConfig::new("redis://127.0.0.1:1/", "offline").expect("offline endpoint is valid");
        subscription.client = Arc::new(Client::new(&settings).expect("offline client builds"));
        let progress = Arc::new(Mutex::new(SettlementProgress::Open));
        let token = SettlementToken::new(
            subscription.subscription_id,
            SettlementState {
                stream: subscription.key.clone(),
                group: subscription.group.clone(),
                message_id: "1-0".into(),
                progress: Arc::clone(&progress),
                recovery: Arc::clone(&subscription.recovery),
            },
        );
        assert!(subscription.settle(&token, DeliveryDisposition::Accept).is_err());
        assert_eq!(*progress.lock().expect("progress healthy"), SettlementProgress::Open);
        subscription
            .settle(&token, DeliveryDisposition::Retry)
            .expect("Retry remains available before any XACK is sent");
    }
    /// Injects a private bookkeeping-lock fault after Redis acknowledges the
    /// real PEL entry. The adapter must return its safe local commit error
    /// and retain terminal intent.
    #[test]
    fn test_settle_applied_xack_local_commit_failure_preserves_intent() -> Result<(), Box<dyn Error>> {
        let server = RedisServer::start()?;
        let proxy = ControlledRedis::start(server.url())?;
        let mut subscription = subscription();
        let settings = RedisEventBusConfig::new(&proxy.url(), "local-commit-fault")?;
        subscription.client = Arc::new(Client::new(&settings)?);
        let mut observer = RedisClient::open(server.url())?.get_connection()?;
        cmd("XADD")
            .arg(&subscription.key)
            .arg("1-0")
            .arg("fixture")
            .arg("commit-fault")
            .query::<String>(&mut observer)?;
        cmd("XGROUP")
            .arg("CREATE")
            .arg(&subscription.key)
            .arg(&subscription.group)
            .arg("0")
            .query::<()>(&mut observer)?;
        cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&subscription.group)
            .arg(&subscription.consumer)
            .arg("STREAMS")
            .arg(&subscription.key)
            .arg(">")
            .query::<Value>(&mut observer)?;
        let initial: Vec<Value> = cmd("XPENDING")
            .arg(&subscription.key)
            .arg(&subscription.group)
            .arg("-")
            .arg("+")
            .arg(10)
            .query(&mut observer)?;
        assert_eq!(initial.len(), 1, "fixture begins with a real pending Redis record");
        let recovery = Arc::clone(&subscription.recovery);
        recovery
            .lock()
            .expect("recovery initially healthy")
            .mark_delivered("1-0".into(), 1);
        let progress = Arc::new(Mutex::new(SettlementProgress::Open));
        let token = SettlementToken::new(
            subscription.subscription_id,
            SettlementState {
                stream: subscription.key.clone(),
                group: subscription.group.clone(),
                message_id: "1-0".into(),
                progress: Arc::clone(&progress),
                recovery: Arc::clone(&recovery),
            },
        );
        let key = subscription.key.clone();
        let group = subscription.group.clone();
        let gate = proxy.pause_after_reply("XACK");

        let (result, token) = scope(|scope| {
            let receiver = &mut subscription;
            let worker = scope.spawn(move || {
                let result = receiver.settle(&token, DeliveryDisposition::Accept);
                (result, token)
            });
            let reached = gate.wait_until_reached(Duration::from_secs(3));
            if !reached {
                gate.release();
            }
            assert!(reached, "applied XACK reply gate must be reached");

            let remaining: Vec<Value> = cmd("XPENDING")
                .arg(&key)
                .arg(&group)
                .arg("-")
                .arg("+")
                .arg(10)
                .query(&mut observer)
                .expect("observe the real PEL");
            assert!(
                remaining.is_empty(),
                "Redis applied XACK before local commit fault injection"
            );
            assert_eq!(
                *progress.lock().expect("progress healthy"),
                SettlementProgress::AckPending(DeliveryDisposition::Accept)
            );
            let poisoned = Arc::clone(&recovery);
            // Internal fault injection targets local bookkeeping after real Redis ACK.
            assert!(
                spawn(move || {
                    let _guard = poisoned.lock().expect("recovery initially healthy");
                    panic!("inject poisoned recovery between applied XACK and local commit");
                })
                .join()
                .is_err()
            );
            gate.release();
            worker.join().expect("settlement worker completes without panic")
        });

        let error = result.expect_err("applied XACK must not falsely report a successful local commit");
        let SpiError::Operation {
            operation,
            kind,
            retryable,
            source,
            ..
        } = error
        else {
            panic!("local commit fault must map to a sanitized operation error");
        };
        assert_eq!(operation, "settle");
        assert_eq!(kind, "redis_error");
        assert_eq!(retryable, None);
        assert!(matches!(
            source.downcast_ref::<RedisProviderError>(),
            Some(RedisProviderError::Operation("recovery lock"))
        ));
        assert_eq!(
            *progress.lock().expect("progress remains healthy"),
            SettlementProgress::AckPending(DeliveryDisposition::Accept)
        );
        assert_eq!(
            recovery
                .lock()
                .err()
                .expect("recovery remains poisoned")
                .into_inner()
                .active_len(),
            1
        );
        for conflict in [DeliveryDisposition::Retry, DeliveryDisposition::Reject] {
            assert!(matches!(
                subscription.settle(&token, conflict),
                Err(SpiError::InvalidSettlementToken {
                    retryable: Some(false),
                    ..
                })
            ));
        }
        recovery.clear_poison();
        // The PEL was cleared above: this identical retry obtains XACK=0.
        subscription.settle(&token, DeliveryDisposition::Accept)?;
        assert_eq!(
            *progress.lock().expect("progress healthy"),
            SettlementProgress::Applied(DeliveryDisposition::Accept)
        );
        assert_eq!(recovery.lock().expect("recovery repaired").active_len(), 0);
        subscription.settle(&token, DeliveryDisposition::Accept)?;
        Ok(())
    }
}
