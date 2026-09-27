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
use redis::FromRedisValue;
use redis::aio::MultiplexedConnection;
use redis::cmd;
use redis::streams::StreamAutoClaimReply;
use redis::streams::StreamId;
use redis::streams::StreamReadReply;

use super::internal::AsyncSettlementState;
use crate::client::Client;
use crate::error::RedisProviderError;
use crate::poison::PoisonOutcome;
use crate::poison::PoisonReason;
use crate::poison::quarantine_async;
use crate::recovery::RecoveryState;
use crate::wire::WireFields;

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
    /// Maximum unsettled records before reads pause.
    pub(crate) max_unsettled: usize,
    /// Active-delivery registry and recovery cursors shared with tokens.
    pub(crate) recovery: Arc<Mutex<RecoveryState>>,
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
            if self
                .recovery
                .lock()
                .map_err(|_| spi_error("receive", &self.topic, RedisProviderError::Operation("recovery lock")))?
                .active_len()
                >= self.max_unsettled
            {
                return Ok(ReceiveOutcome::TimedOut);
            }
            let mut connection = match self.receive_connection.take() {
                Some(connection) => connection,
                None => self
                    .client
                    .get_async_dedicated_connection()
                    .await
                    .map_err(|_| spi_error("receive", &self.topic, RedisProviderError::Operation("connect")))?,
            };
            let result = async {
                let deadline = Instant::now().checked_add(timeout).unwrap_or_else(Instant::now);
                let scan_limit = self.max_unsettled.saturating_add(2);
                for _ in 0..scan_limit {
                    let cursor = self
                        .recovery
                        .lock()
                        .map_err(|_| spi_error("receive", &self.topic, RedisProviderError::Operation("recovery lock")))?
                        .claim_cursor()
                        .to_owned();
                    let claim: StreamAutoClaimReply = cmd("XAUTOCLAIM")
                        .arg(&self.key)
                        .arg(&self.group)
                        .arg(&self.consumer)
                        .arg(self.claim_min_idle_ms)
                        .arg(&cursor)
                        .arg("COUNT")
                        .arg(1)
                        .query_async(&mut connection)
                        .await
                        .map_err(|_| spi_error("receive", &self.topic, RedisProviderError::Operation("XAUTOCLAIM")))?;
                    let at_end = claim.next_stream_id == "0-0";
                    self.recovery
                        .lock()
                        .map_err(|_| spi_error("receive", &self.topic, RedisProviderError::Operation("recovery lock")))?
                        .set_claim_cursor(claim.next_stream_id);
                    if !claim.deleted_ids.is_empty() {
                        return Ok(ReceiveOutcome::Gap(DeliveryGap::new(
                            "pending Redis stream entries were removed",
                            Some(claim.deleted_ids.len() as u64),
                        )));
                    }
                    if let Some(entry) = claim.claimed.into_iter().next()
                        && self
                            .recovery
                            .lock()
                            .map_err(|_| {
                                spi_error("receive", &self.topic, RedisProviderError::Operation("recovery lock"))
                            })?
                            .can_deliver(&entry.id)
                        && let Some(outcome) = read_entry(self, &mut connection, entry).await?
                    {
                        return Ok(outcome);
                    }
                    if at_end {
                        break;
                    }
                }
                for _ in 0..scan_limit {
                    let cursor = self
                        .recovery
                        .lock()
                        .map_err(|_| spi_error("receive", &self.topic, RedisProviderError::Operation("recovery lock")))?
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
                        .query_async(&mut connection)
                        .await
                        .map_err(|_| spi_error("receive", &self.topic, RedisProviderError::Operation("XREADGROUP")))?;
                    if let Some(entry) = pending
                        .and_then(|reply| reply.keys.into_iter().next())
                        .and_then(|stream| stream.ids.into_iter().next())
                    {
                        let id = entry.id.clone();
                        self.recovery
                            .lock()
                            .map_err(|_| {
                                spi_error("receive", &self.topic, RedisProviderError::Operation("recovery lock"))
                            })?
                            .set_pending_cursor(id.clone());
                        if self
                            .recovery
                            .lock()
                            .map_err(|_| {
                                spi_error("receive", &self.topic, RedisProviderError::Operation("recovery lock"))
                            })?
                            .can_deliver(&id)
                            && let Some(outcome) = read_entry(self, &mut connection, entry).await?
                        {
                            return Ok(outcome);
                        }
                    } else {
                        self.recovery
                            .lock()
                            .map_err(|_| {
                                spi_error("receive", &self.topic, RedisProviderError::Operation("recovery lock"))
                            })?
                            .reset_pending_scan();
                        break;
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
                        .query_async(&mut connection)
                        .await
                        .map_err(|_| spi_error("receive", &self.topic, RedisProviderError::Operation("XREADGROUP")))?;
                    if let Some(entry) = reply
                        .and_then(|r| r.keys.into_iter().next())
                        .and_then(|s| s.ids.into_iter().next())
                        && let Some(outcome) = read_entry(self, &mut connection, entry).await?
                    {
                        return Ok(outcome);
                    }
                    return Ok(ReceiveOutcome::TimedOut);
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
                        .query_async(&mut connection)
                        .await
                        .map_err(|_| spi_error("receive", &self.topic, RedisProviderError::Operation("XREADGROUP")))?;
                    let entry = reply
                        .and_then(|reply| reply.keys.into_iter().next())
                        .and_then(|stream| stream.ids.into_iter().next());
                    if let Some(entry) = entry
                        && let Some(outcome) = read_entry(self, &mut connection, entry).await?
                    {
                        return Ok(outcome);
                    }
                    if Instant::now() >= deadline {
                        return Ok(ReceiveOutcome::TimedOut);
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
        let state = token.downcast_ref::<AsyncSettlementState>().map(|state| {
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
                let result: Result<usize, redis::RedisError> = cmd("XACK")
                    .arg(stream)
                    .arg(group)
                    .arg(&message_id)
                    .query_async(&mut connection)
                    .await;
                if result.is_err() {
                    client.invalidate_async_connection().await;
                    return Err(spi_error("settle", &topic, RedisProviderError::Operation("XACK")));
                }
            }
            recovery
                .lock()
                .map_err(|_| spi_error("settle", &topic, RedisProviderError::Operation("recovery lock")))?
                .mark_terminal(&message_id);
            if disposition == DeliveryDisposition::Retry {
                recovery
                    .lock()
                    .map_err(|_| spi_error("settle", &topic, RedisProviderError::Operation("recovery lock")))?
                    .mark_retry(&message_id);
            }
            {
                let mut applied = applied
                    .lock()
                    .map_err(|_| spi_error("settle", &topic, RedisProviderError::Operation("settlement lock")))?;
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
) -> Result<Option<ReceiveOutcome>, SpiError> {
    let id = entry.id.clone();
    let decoded = (|| {
        let Some(value) = entry.map.get("wire") else {
            return Err(PoisonReason::MissingWire);
        };
        let encoded: String = String::from_redis_value(value).map_err(|_| PoisonReason::InvalidWireField)?;
        let fields: WireFields = serde_json::from_str(&encoded).map_err(|_| PoisonReason::InvalidJson)?;
        let (topic, event_id, timestamp, headers, ordering_key, payload) =
            fields.into_parts(subscription.topic.clone()).map_err(|error| {
                if matches!(error, RedisProviderError::UnsupportedWireVersion) {
                    PoisonReason::UnsupportedVersion
                } else {
                    PoisonReason::InvalidEventMetadata
                }
            })?;
        Ok((topic, event_id, timestamp, headers, ordering_key, payload))
    })();
    let (topic, event_id, timestamp, headers, ordering_key, payload) = match decoded {
        Ok(parts) => parts,
        Err(reason) => {
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
            .map_err(|_| {
                spi_error(
                    "receive",
                    &subscription.topic,
                    RedisProviderError::Operation("quarantine"),
                )
            })?;
            return Ok(match outcome {
                PoisonOutcome::Quarantined => Some(ReceiveOutcome::Gap(DeliveryGap::new(
                    "malformed Redis stream entry was quarantined",
                    Some(1),
                ))),
                PoisonOutcome::SourceGone => Some(ReceiveOutcome::Gap(DeliveryGap::new(
                    "malformed pending Redis stream entry was removed",
                    Some(1),
                ))),
                PoisonOutcome::OwnershipChanged => None,
            });
        }
    };
    let settlement = SettlementToken::new(
        subscription.subscription_id,
        AsyncSettlementState {
            stream: subscription.key.clone(),
            group: subscription.group.clone(),
            message_id: entry.id,
            disposition: Arc::new(Mutex::new(None)),
            recovery: Arc::clone(&subscription.recovery),
        },
    );
    let marked = subscription
        .recovery
        .lock()
        .map_err(|_| {
            spi_error(
                "receive",
                &subscription.topic,
                RedisProviderError::Operation("recovery lock"),
            )
        })?
        .mark_delivered(id, subscription.max_unsettled);
    if !marked {
        return Err(spi_error(
            "receive",
            &subscription.topic,
            RedisProviderError::Operation("active delivery limit"),
        ));
    }
    Ok(Some(ReceiveOutcome::Message(InboundMessage::new(
        topic,
        event_id,
        timestamp,
        headers,
        ordering_key,
        payload,
        Some(settlement),
        Default::default(),
    ))))
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
    SpiError::Operation {
        provider_id: "redis-streams".into(),
        operation,
        resource: Some(topic.as_str().into()),
        kind: "redis_error",
        retryable: Some(true),
        source: Box::new(source),
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
    use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
    use qubit_event_bus::spi::DeliveryDisposition;
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
}
