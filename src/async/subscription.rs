// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runtime-neutral asynchronous consumer group receiver.

/// Controls bounded replies on dedicated receiver sockets.
#[path = "subscription/internal/receive_command.rs"]
mod receive_command;

use std::io::Error as IoError;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
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
use redis::RedisError;
use redis::Value;
use redis::aio::MultiplexedConnection;
use redis::cmd;
use redis::streams::StreamId;

use self::receive_command::ReceiveCommand;
use crate::client::Client;
use crate::client::CommandClass;
use crate::client::ReceiverPermit;
use crate::diagnostics::RedisDiagnosticCounter;
use crate::error::RedisProviderError;
use crate::error::from_redis_error as classified_spi_error;
use crate::internal::DecodeFailure;
use crate::internal::PoisonOutcome;
use crate::internal::PoisonReason;
use crate::internal::ReceiveAction;
use crate::internal::ReceiveDriver;
use crate::internal::ReceiveReply;
use crate::internal::RecoveryGuard;
use crate::internal::RecoveryState;
use crate::internal::SettlementAction;
use crate::internal::SettlementProgress;
use crate::internal::SettlementState;
use crate::internal::WireLimits;
use crate::internal::decode_entry as decode_wire_entry;
use crate::internal::read_group_command;
use crate::poison::quarantine_async;
use crate::stream_protocol::parse_auto_claim;
use crate::stream_protocol::parse_pending_entries;
use crate::stream_protocol::parse_range;
use crate::stream_protocol::parse_read_group;
use crate::stream_protocol::trusted_attempt;

/// Asynchronous receiver whose cancelled reads remain in Redis PEL.
pub(crate) struct Subscription {
    /// Shared async connection factory.
    pub(crate) client: Arc<Client>,
    /// Inclusive byte budgets applied before wire parsing and payload delivery.
    pub(crate) wire_limits: WireLimits,
    /// Receiver-only connection reused after completed receives. Cancellation
    /// leaves this empty so a connection with an in-flight reply is discarded.
    pub(crate) receive_connection: Option<MultiplexedConnection>,
    /// Receiver admission released on close/drop, independent of tokens.
    pub(crate) receiver_permit: Option<ReceiverPermit>,
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
    /// Minimum delay between complete recovery rounds across receive calls.
    pub(crate) recovery_interval: Duration,
    /// Maximum unsettled records before reads pause.
    pub(crate) max_unsettled: usize,
    /// Active-delivery registry and recovery cursors shared with tokens.
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
        .map_err(|_| spi_error(operation, topic, RedisProviderError::Operation(kind)))
}

impl AsyncEventSubscriptionSpi for Subscription {
    /// Reads pending deliveries before new group entries using a finite block.
    ///
    /// Normal calls before the next recovery deadline read new entries
    /// directly. Cancellation leaves any Redis-delivered item in the pending
    /// entries list and forces the next call to scan reclaimable and
    /// own-pending entries before waiting for new group entries.
    ///
    /// # Parameters
    ///
    /// - `timeout`: Scheduling wait for new entries, zero for one non-blocking
    ///   read per phase, or Duration::MAX for repeated finite BLOCK intervals.
    ///   Socket/response waits are bounded separately and can extend past this
    ///   deadline.
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
    /// Returns a classified SPI error for admission, locks, connection/setup,
    /// command/protocol failures, or unsupported wire version. Cancellation
    /// can leave command execution unknown; later receives recover via PEL.
    fn receive<'a>(&'a mut self, timeout: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        Box::pin(async move {
            if self.closed {
                return Ok(ReceiveOutcome::Closed);
            }
            if lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?.active_len() >= self.max_unsettled {
                return Ok(ReceiveOutcome::TimedOut);
            }
            let started = Instant::now();
            let (initial_recovery_due, next_recovery_at, recovery_generation, resume_pending) = {
                let recovery = lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?;
                let (due, next, generation) = recovery.recovery_schedule(started);
                (due, next, generation, recovery.claim_phase_complete())
            };
            let mut recovery_guard = RecoveryGuard::new(Arc::clone(&self.recovery));
            let mut connection = match self.receive_connection.take() {
                Some(connection) => connection,
                None => self
                    .client
                    .get_async_dedicated_connection()
                    .await
                    .map_err(|error| spi_error("receive", &self.topic, error))?,
            };
            let mut connection_reusable = true;
            let result: Result<ReceiveOutcome, SpiError> = async {
                let mut driver = ReceiveDriver::new(
                    timeout,
                    started,
                    self.recovery_interval,
                    initial_recovery_due,
                    next_recovery_at,
                )
                .map_err(|error| spi_error("receive", &self.topic, error))?;
                if initial_recovery_due && resume_pending {
                    driver.resume_pending();
                }
                loop {
                    let deferred = {
                        let mut recovery = lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?;
                        recovery
                            .take_deferred_claim()
                            .filter(|entry| recovery.can_deliver(&entry.id))
                    };
                    if let Some(entry) = deferred
                        && let Some(outcome) = read_entry(
                            self,
                            &mut connection,
                            entry,
                            true,
                            &mut connection_reusable,
                            &mut driver,
                        )
                        .await?
                    {
                        return Ok(outcome);
                    }
                    connection.set_response_timeout(self.client.command_timeout());
                    match driver.next_action(Instant::now()) {
                        ReceiveAction::Claim => {
                            lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?.begin_recovery();
                            let cursor = lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?
                                .claim_cursor()
                                .to_owned();
                            if let Some(diagnostics) = self.client.diagnostics() {
                                diagnostics.increment(RedisDiagnosticCounter::RecoveryClaimCommands);
                            }
                            let raw_claim: Value = cmd("XAUTOCLAIM")
                                .arg(&self.key)
                                .arg(&self.group)
                                .arg(&self.consumer)
                                .arg(self.claim_min_idle_ms)
                                .arg(&cursor)
                                .arg("COUNT")
                                .arg(1)
                                .query_receive(&mut connection)
                                .await
                                .map_err(|error| spi_error("receive", &self.topic, error))?;
                            let (claim, has_missing_entries) = parse_auto_claim(raw_claim).map_err(|_| {
                                spi_error(
                                    "receive",
                                    &self.topic,
                                    RedisProviderError::OutcomeUnknown { operation: "receive" },
                                )
                            })?;
                            let at_end = claim.next_stream_id == "0-0";
                            driver.reply(if at_end {
                                ReceiveReply::ClaimAtEnd
                            } else {
                                ReceiveReply::ClaimHasMore
                            });
                            {
                                let mut recovery = lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?;
                                recovery.set_claim_cursor(claim.next_stream_id);
                                if at_end {
                                    recovery.mark_claim_complete(recovery_generation);
                                }
                            }
                            let mut deleted_count = claim.deleted_ids.len() as u64;
                            if has_missing_entries {
                                deleted_count += scan_missing_tombstones(self, &mut connection, &mut driver).await?;
                            }
                            if deleted_count > 0 {
                                if let Some(entry) = claim.claimed.into_iter().next() {
                                    lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?
                                        .defer_claim(entry);
                                }
                                return Ok(ReceiveOutcome::Gap(DeliveryGap::new(
                                    "pending Redis stream entries were removed",
                                    Some(deleted_count),
                                )));
                            }
                            if let Some(entry) = claim.claimed.into_iter().next()
                                && lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?
                                    .can_deliver(&entry.id)
                                && let Some(outcome) = read_entry(
                                    self,
                                    &mut connection,
                                    entry,
                                    true,
                                    &mut connection_reusable,
                                    &mut driver,
                                )
                                .await?
                            {
                                return Ok(outcome);
                            }
                        }
                        ReceiveAction::Pending => {
                            let cursor = lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?
                                .pending_cursor()
                                .to_owned();
                            let raw_pending: Value =
                                read_group_command(&self.group, &self.consumer, &self.key, &cursor, None)
                                    .query_receive(&mut connection)
                                    .await
                                    .map_err(|error| spi_error("receive", &self.topic, error))?;
                            let pending = parse_read_group(raw_pending).map_err(|_| {
                                spi_error(
                                    "receive",
                                    &self.topic,
                                    RedisProviderError::OutcomeUnknown { operation: "receive" },
                                )
                            })?;
                            if let Some(entry) = pending
                                .and_then(|reply| reply.keys.into_iter().next())
                                .and_then(|stream| stream.ids.into_iter().next())
                            {
                                driver.reply(ReceiveReply::PendingEntry);
                                let id = entry.id.clone();
                                lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?
                                    .set_pending_cursor(id.clone());
                                if lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?.can_deliver(&id)
                                    && let Some(outcome) = read_entry(
                                        self,
                                        &mut connection,
                                        entry,
                                        true,
                                        &mut connection_reusable,
                                        &mut driver,
                                    )
                                    .await?
                                {
                                    return Ok(outcome);
                                }
                            } else {
                                driver.reply(ReceiveReply::PendingEmpty);
                                let next = driver.complete_recovery_round(Instant::now());
                                let mut recovery = lock_state(&self.recovery, &self.topic, "receive", "recovery lock")?;
                                recovery.reset_pending_scan();
                                if let Some(next) = next {
                                    recovery.complete_recovery_at(next, recovery_generation);
                                }
                                continue;
                            }
                        }
                        ReceiveAction::ReadNew { block_ms } => {
                            let response_timeout = self
                                .client
                                .response_timeout(block_ms.map(|ms| ms.clamp(1, 1_000)))
                                .map_err(|error| spi_error("receive", &self.topic, error))?;
                            connection.set_response_timeout(response_timeout);
                            let raw_reply: Value =
                                read_group_command(&self.group, &self.consumer, &self.key, ">", block_ms)
                                    .query_receive(&mut connection)
                                    .await
                                    .map_err(|error| spi_error("receive", &self.topic, error))?;
                            let reply = parse_read_group(raw_reply).map_err(|_| {
                                spi_error(
                                    "receive",
                                    &self.topic,
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
                                && let Some(outcome) = read_entry(
                                    self,
                                    &mut connection,
                                    entry,
                                    false,
                                    &mut connection_reusable,
                                    &mut driver,
                                )
                                .await?
                            {
                                return Ok(match outcome {
                                    ReceiveOutcome::Message(message) => {
                                        ReceiveOutcome::Message(message.with_provider_attempt(NonZeroU32::MIN))
                                    }
                                    other => other,
                                });
                            }
                        }
                        ReceiveAction::TimedOut => return Ok(ReceiveOutcome::TimedOut),
                    }
                }
            }
            .await;
            if result.is_ok() && connection_reusable {
                recovery_guard.disarm();
                self.receive_connection = Some(connection);
            }
            if result
                .as_ref()
                .is_err_and(|error| error.kind() == "receive_response_too_large")
            {
                self.closed = true;
                self.receive_connection.take();
            }
            if let Some(diagnostics) = self.client.diagnostics() {
                match &result {
                    Ok(ReceiveOutcome::Gap(_)) => diagnostics.increment(RedisDiagnosticCounter::DeliveryGaps),
                    Err(error) if error.kind() == "outcome_unknown" => {
                        diagnostics.increment(RedisDiagnosticCounter::ReceiveUnknown);
                    }
                    _ => {}
                }
            }
            result
        })
    }

    /// Acknowledges terminal decisions and leaves retry decisions in PEL.
    ///
    /// Accept and Reject issue `XACK`; Retry only releases the receiver's local
    /// in-flight slot, leaving the Redis entry available for redelivery.
    /// Applying the same disposition again succeeds. Once XACK may have been
    /// sent, cancellation or reply loss retains its intent and rejects
    /// conflicts.
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
    /// Success after local active-slot release and Applied bookkeeping.
    /// Terminal decisions require a valid XACK reply (0 or 1); Retry commits
    /// locally without acknowledging Redis. Repeating the applied intent is
    /// idempotent, while unknown prior ACK intent remains fixed.
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
        let state = token.downcast_ref::<SettlementState>().cloned();
        let belongs = token.belongs_to(self.subscription_id);
        let topic = self.topic.clone();
        let client = Arc::clone(&self.client);
        Box::pin(async move {
            if !belongs {
                return Err(invalid_token("token belongs to another subscription", &topic));
            }
            let Some(state) = state else {
                return Err(invalid_token("token type is not recognized", &topic));
            };
            if !Arc::ptr_eq(&state.recovery, &self.recovery) {
                return Err(invalid_token("token belongs to another receiver", &topic));
            }
            let action = {
                let current = lock_state(&state.progress, &topic, "settle", "settlement lock")?;
                current
                    .action(disposition)
                    .map_err(|()| invalid_token("token already has a different disposition", &topic))?
            };
            if action == SettlementAction::AlreadyApplied {
                return Ok(());
            }
            if action != SettlementAction::ApplyRetry {
                let mut connection = client
                    .get_async_connection(CommandClass::Settlement)
                    .await
                    .map_err(|error| spi_error("settle", &topic, error))?;
                // No await separates fixing intent from first polling the XACK I/O.
                *lock_state(&state.progress, &topic, "settle", "settlement lock")? =
                    SettlementProgress::AckPending(disposition);
                let mut command = cmd("XACK");
                command.arg(&state.stream).arg(&state.group).arg(&state.message_id);
                // Preserve nested errors as malformed replies rather than rejecting XACK.
                let result = connection.send_packed_command(&command).await;
                match result {
                    Ok(Value::Int(0 | 1)) => {}
                    Ok(Value::ServerError(error)) => {
                        let error: RedisError = error.into();
                        if action == SettlementAction::PrepareAck {
                            *lock_state(&state.progress, &topic, "settle", "settlement lock")? =
                                SettlementProgress::Open;
                        }
                        client.invalidate_async_connection(connection.generation).await;
                        return Err(classified_spi_error("settle", Some(&topic), &error));
                    }
                    Ok(_) | Err(_) => {
                        client.invalidate_async_connection(connection.generation).await;
                        if let Some(diagnostics) = client.diagnostics() {
                            diagnostics.increment(RedisDiagnosticCounter::SettlementUnknown);
                        }
                        return Err(spi_error(
                            "settle",
                            &topic,
                            RedisProviderError::OutcomeUnknown { operation: "settle" },
                        ));
                    }
                }
            }
            // A successful reply and both local mutations share one no-await boundary.
            state
                .commit(disposition)
                .map_err(|label| spi_error("settle", &topic, RedisProviderError::Operation(label)))?;
            Ok(())
        })
    }

    /// Closes the receiver without implicitly acknowledging pending records.
    ///
    /// # Returns
    ///
    /// Success once polled: future receives return `Closed`, the dedicated
    /// transport is dropped, and receiver admission is released. Tokens retain
    /// settlement bookkeeping without retaining receiver admission. No Redis
    /// command is issued and this future cannot return an error.
    ///
    /// # Type Parameters
    ///
    /// - `'a`: Lifetime shared by the mutable receiver borrow and future.
    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        Box::pin(async move {
            self.closed = true;
            self.receive_connection.take();
            self.receiver_permit.take();
            Ok(())
        })
    }
}

/// Scans pending IDs Redis reports as deleted and clears their PEL tombstones.
///
/// # Parameters
///
/// - `subscription`: Receiver supplying Redis keys, recovery cursors, and
///   command admission.
/// - `connection`: Dedicated socket used for the bounded pending/range queries.
/// - `driver`: Receive budget controlling whether this probe and its commands
///   may run.
///
/// # Returns
///
/// The number of deleted pending entries cleared from the consumer group's
/// pending list. Returns zero when no probe budget is available.
///
/// # Errors
///
/// Returns an operation error for exhausted command admission and an uncertain
/// receive/quarantine outcome for malformed or failed Redis replies.
async fn scan_missing_tombstones(
    subscription: &Subscription,
    connection: &mut MultiplexedConnection,
    driver: &mut ReceiveDriver,
) -> Result<u64, SpiError> {
    if !driver.budget_mut().take_tombstone_probe(Instant::now()) {
        return Ok(0);
    }
    let mut deleted_count = 0;
    let tombstone_cursor = lock_state(&subscription.recovery, &subscription.topic, "receive", "recovery lock")?
        .tombstone_cursor()
        .to_owned();
    let pending_start = if tombstone_cursor == "0-0" {
        "-".to_owned()
    } else {
        format!("({tombstone_cursor}")
    };
    let pending_reply: Value = cmd("XPENDING")
        .arg(&subscription.key)
        .arg(&subscription.group)
        .arg(pending_start)
        .arg("+")
        .arg(4)
        .query_receive(connection)
        .await
        .map_err(|error| spi_error("receive", &subscription.topic, error))?;
    let pending_rows = parse_pending_entries(pending_reply).map_err(|_| {
        spi_error(
            "receive",
            &subscription.topic,
            RedisProviderError::OutcomeUnknown { operation: "receive" },
        )
    })?;
    let pending_row_count = pending_rows.len();
    let mut scan_complete = true;
    for pending_entry in pending_rows {
        let id = pending_entry.id;
        let owner = pending_entry.owner;
        if pending_entry.idle_ms < subscription.claim_min_idle_ms as u64 {
            lock_state(&subscription.recovery, &subscription.topic, "receive", "recovery lock")?
                .set_tombstone_cursor(id);
            continue;
        }
        if !driver.budget_mut().take_tombstone_range(Instant::now()) {
            scan_complete = false;
            break;
        }
        let raw_rows: Value = cmd("XRANGE")
            .arg(&subscription.key)
            .arg(&id)
            .arg(&id)
            .query_receive(connection)
            .await
            .map_err(|error| spi_error("receive", &subscription.topic, error))?;
        let rows = parse_range(raw_rows).map_err(|_| {
            spi_error(
                "receive",
                &subscription.topic,
                RedisProviderError::OutcomeUnknown { operation: "receive" },
            )
        })?;
        if rows.ids.is_empty() {
            if !driver.budget_mut().take_maintenance_evaluation(Instant::now()) {
                scan_complete = false;
                break;
            }
            connection.set_response_timeout(subscription.client.command_timeout());
            match quarantine_async(
                connection,
                &subscription.key,
                &subscription.quarantine,
                &subscription.group,
                &owner,
                &id,
                PoisonReason::MissingWire,
            )
            .await
            .map_err(|_error| {
                spi_error(
                    "receive",
                    &subscription.topic,
                    RedisProviderError::OutcomeUnknown {
                        operation: "quarantine",
                    },
                )
            })? {
                PoisonOutcome::TombstoneCleared => deleted_count += 1,
                PoisonOutcome::SourceGone | PoisonOutcome::OwnershipChanged => {}
                PoisonOutcome::Quarantined => {
                    if let Some(diagnostics) = subscription.client.diagnostics() {
                        diagnostics.increment(RedisDiagnosticCounter::QuarantineSucceeded);
                    }
                }
            }
        }
        lock_state(&subscription.recovery, &subscription.topic, "receive", "recovery lock")?.set_tombstone_cursor(id);
    }
    if scan_complete && pending_row_count < 4 {
        lock_state(&subscription.recovery, &subscription.topic, "receive", "recovery lock")?.reset_tombstone_cursor();
    }
    Ok(deleted_count)
}

/// Decodes one entry and reserves its local slot or quarantines deterministic
/// poison.
///
/// # Parameters
///
/// - `subscription`: Receiver supplying coordinates, limits, ownership, and
///   active state.
/// - `connection`: Dedicated transport used for controlled quarantine I/O.
/// - `entry`: Owned stream entry returned by a successful read or claim.
/// - `driver`: Current receive budget used to reserve poison maintenance.
///
/// # Returns
///
/// `Some` contains a decoded message, a quarantine/removal gap, or TimedOut
/// when maintenance allowance is exhausted. `None` means another consumer owns
/// the poison entry and scanning should continue. Successful poison isolation
/// can copy the wire and XACK its PEL entry; Redis Lua does not roll back
/// partial writes.
///
/// # Errors
///
/// Returns unsupported-version without ACK, active-state/lock failure,
/// admission exhaustion, or an uncertain quarantine outcome. Cancellation can
/// conceal completed Redis script writes; no transparent quarantine replay is
/// attempted.
async fn read_entry(
    subscription: &Subscription,
    connection: &mut MultiplexedConnection,
    entry: StreamId,
    recovered: bool,
    connection_reusable: &mut bool,
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
            connection.set_response_timeout(subscription.client.command_timeout());
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
            .map_err(|_error| {
                spi_error(
                    "receive",
                    &subscription.topic,
                    RedisProviderError::OutcomeUnknown {
                        operation: "quarantine",
                    },
                )
            })?;
            return Ok(match outcome {
                PoisonOutcome::Quarantined => {
                    if let Some(diagnostics) = subscription.client.diagnostics() {
                        diagnostics.increment(RedisDiagnosticCounter::QuarantineSucceeded);
                    }
                    Some(ReceiveOutcome::Gap(DeliveryGap::new(
                        "malformed Redis stream entry was quarantined",
                        Some(1),
                    )))
                }
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
    let provider_attempt = if recovered {
        connection.set_response_timeout(subscription.client.command_timeout());
        let pending: Result<Value, _> = cmd("XPENDING")
            .arg(&subscription.key)
            .arg(&subscription.group)
            .arg(&id)
            .arg(&id)
            .arg(1)
            .query_receive(connection)
            .await;
        match pending {
            Ok(value) => match parse_pending_entries(value) {
                Ok(rows) => (rows.len() == 1)
                    .then(|| rows.into_iter().next())
                    .flatten()
                    .and_then(|row| trusted_attempt(&row, &id, &subscription.consumer)),
                Err(_) => {
                    *connection_reusable = false;
                    None
                }
            },
            Err(_) => {
                *connection_reusable = false;
                None
            }
        }
    } else {
        None
    };
    let message = if let Some(attempt) = provider_attempt {
        message.with_provider_attempt(attempt)
    } else {
        message
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

/// Classifies a provider failure into a secret-safe SPI error.
///
/// Retryability follows the failure category and operation context, including
/// constrained same-intent ACK retry and unknown publish/quarantine replay
/// limits.
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
        source: Box::new(IoError::other("invalid settlement token")),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::error::Error;
    use std::future::pending;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::thread::spawn;
    use std::time::Duration;

    use futures_lite::future::block_on;
    use futures_lite::future::race;
    use qubit_event_bus::error::SpiError;
    use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
    use qubit_event_bus::spi::DeliveryDisposition;
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
        assert!(
            message.settlement().is_some(),
            "decoded messages must carry a settlement token"
        );
    }

    #[test]
    fn test_receive_reports_a_poisoned_recovery_lock() {
        block_on(async {
            let mut subscription = subscription();
            let recovery = Arc::clone(&subscription.recovery);
            let _ = spawn(move || {
                let _guard = recovery.lock().expect("recovery lock is initially healthy");
                panic!("poison recovery lock for error-path coverage");
            })
            .join();
            let error = match subscription.receive(Duration::ZERO).await {
                Ok(_) => panic!("poisoned recovery lock should return an error"),
                Err(error) => error,
            };
            assert!(matches!(error, SpiError::Operation { .. }));
        });
    }

    #[test]
    fn test_settle_rejects_a_token_from_a_different_receiver_with_the_same_id() {
        block_on(async {
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
                .await
                .expect_err("foreign receiver state must be rejected");
            assert!(matches!(error, SpiError::InvalidSettlementToken { .. }));
        });
    }

    #[test]
    fn test_settle_reports_a_poisoned_disposition_lock() {
        block_on(async {
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

            assert!(
                subscription.settle(&token, DeliveryDisposition::Accept).await.is_err(),
                "settlement must fail when the disposition mutex is poisoned"
            );
        });
    }

    #[test]
    fn test_settle_connection_acquisition_failure_preserves_open_intent() {
        block_on(async {
            let mut subscription = subscription();
            let settings =
                RedisEventBusConfig::new("redis://127.0.0.1:1/", "offline").expect("offline endpoint is valid");
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
            assert!(
                subscription.settle(&token, DeliveryDisposition::Accept).await.is_err(),
                "settlement must report connection acquisition failure before fixing intent"
            );
            assert_eq!(*progress.lock().expect("progress healthy"), SettlementProgress::Open);
            subscription
                .settle(&token, DeliveryDisposition::Retry)
                .await
                .expect("Retry remains available before any XACK is sent");
        });
    }
    /// Injects a private bookkeeping-lock fault after Redis acknowledges the
    /// real PEL entry. The adapter must return its safe local commit error
    /// and retain terminal intent.
    #[test]
    fn test_settle_applied_xack_local_commit_failure_preserves_intent() -> Result<(), Box<dyn Error>> {
        block_on(async {
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

            let result = race(subscription.settle(&token, DeliveryDisposition::Accept), async {
                gate.wait_applied().await;

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
                pending::<Result<(), SpiError>>().await
            })
            .await;

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
                    subscription.settle(&token, conflict).await,
                    Err(SpiError::InvalidSettlementToken {
                        retryable: Some(false),
                        ..
                    })
                ));
            }
            recovery.clear_poison();
            // The PEL was cleared above: this identical retry obtains XACK=0.
            subscription.settle(&token, DeliveryDisposition::Accept).await?;
            assert_eq!(
                *progress.lock().expect("progress healthy"),
                SettlementProgress::Applied(DeliveryDisposition::Accept)
            );
            assert_eq!(recovery.lock().expect("recovery repaired").active_len(), 0);
            subscription.settle(&token, DeliveryDisposition::Accept).await?;
            Ok(())
        })
    }
}
