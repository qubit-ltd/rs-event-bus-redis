// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal counter state associated with one SPI lifetime.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use super::RedisDiagnosticCounter;
use crate::client::ResourceBudget;
use crate::diagnostics::RedisProviderMode;
use crate::diagnostics::RedisProviderSnapshot;
use crate::diagnostics::internal::directory;
use crate::error::RedisProviderError;

/// Atomics and the existing shared resource budget for one live SPI.
#[cfg(any(feature = "sync", feature = "async"))]
pub(crate) struct RedisDiagnosticsState {
    /// Process-unique ID assigned before registration.
    pub(super) instance_id: u64,
    /// Mode attached by the creating provider.
    mode: RedisProviderMode,
    /// Validated namespace, retained without endpoint settings.
    namespace: String,
    /// Existing admission budget shared with the Client and active permits.
    budget: Arc<ResourceBudget>,
    /// General or settlement command admission rejections.
    command_rejections: AtomicU64,
    /// Receiver admission rejections.
    receiver_rejections: AtomicU64,
    /// Provider-level connection open attempts.
    connection_attempts: AtomicU64,
    /// Provider-level connection open failures.
    connection_failures: AtomicU64,
    /// Publishes confirmed accepted by Redis.
    publish_accepted: AtomicU64,
    /// Publishes with an unknown outcome.
    publish_unknown: AtomicU64,
    /// Receive calls with an unknown outcome.
    receive_unknown: AtomicU64,
    /// Settlement calls with an unknown outcome.
    settlement_unknown: AtomicU64,
    /// Recovery XAUTOCLAIM commands sent.
    recovery_claim_commands: AtomicU64,
    /// Confirmed quarantine copies.
    quarantine_succeeded: AtomicU64,
    /// Delivery gaps returned to callers.
    delivery_gaps: AtomicU64,
}

#[cfg(any(feature = "sync", feature = "async"))]
impl RedisDiagnosticsState {
    /// Registers a new instance without keeping it alive in the directory.
    ///
    /// Returns a creation error if the process-wide ID space is exhausted.
    pub(crate) fn register(
        mode: RedisProviderMode,
        namespace: &str,
        budget: Arc<ResourceBudget>,
    ) -> Result<Arc<Self>, RedisProviderError> {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let instance_id = loop {
            let current = NEXT_ID.load(Ordering::Relaxed);
            let Some(next) = current.checked_add(1) else {
                return Err(RedisProviderError::Operation(
                    "Redis diagnostics instance IDs exhausted",
                ));
            };
            if NEXT_ID
                .compare_exchange(current, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break current;
            }
        };
        let state = Arc::new(Self {
            instance_id,
            mode,
            namespace: namespace.to_owned(),
            budget,
            command_rejections: AtomicU64::new(0),
            receiver_rejections: AtomicU64::new(0),
            connection_attempts: AtomicU64::new(0),
            connection_failures: AtomicU64::new(0),
            publish_accepted: AtomicU64::new(0),
            publish_unknown: AtomicU64::new(0),
            receive_unknown: AtomicU64::new(0),
            settlement_unknown: AtomicU64::new(0),
            recovery_claim_commands: AtomicU64::new(0),
            quarantine_succeeded: AtomicU64::new(0),
            delivery_gaps: AtomicU64::new(0),
        });
        directory()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .register(&state);
        Ok(state)
    }

    /// Saturatingly increments one monotonic counter without taking the
    /// directory lock.
    pub(crate) fn increment(&self, field: RedisDiagnosticCounter) {
        let counter = match field {
            RedisDiagnosticCounter::CommandRejections => &self.command_rejections,
            RedisDiagnosticCounter::ReceiverRejections => &self.receiver_rejections,
            RedisDiagnosticCounter::ConnectionAttempts => &self.connection_attempts,
            RedisDiagnosticCounter::ConnectionFailures => &self.connection_failures,
            RedisDiagnosticCounter::PublishAccepted => &self.publish_accepted,
            RedisDiagnosticCounter::PublishUnknown => &self.publish_unknown,
            RedisDiagnosticCounter::ReceiveUnknown => &self.receive_unknown,
            RedisDiagnosticCounter::SettlementUnknown => &self.settlement_unknown,
            RedisDiagnosticCounter::RecoveryClaimCommands => &self.recovery_claim_commands,
            RedisDiagnosticCounter::QuarantineSucceeded => &self.quarantine_succeeded,
            RedisDiagnosticCounter::DeliveryGaps => &self.delivery_gaps,
        };
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| Some(n.saturating_add(1)));
    }

    /// Reads independent atomic values into a secret-safe public snapshot.
    pub(in crate::diagnostics) fn snapshot(&self) -> RedisProviderSnapshot {
        RedisProviderSnapshot {
            instance_id: self.instance_id,
            mode: self.mode,
            namespace: self.namespace.clone(),
            general_in_flight: self.budget.general_in_flight(),
            settlement_in_flight: self.budget.settlement_in_flight(),
            active_receivers: self.budget.active_receivers(),
            command_rejections: self.command_rejections.load(Ordering::Relaxed),
            receiver_rejections: self.receiver_rejections.load(Ordering::Relaxed),
            connection_attempts: self.connection_attempts.load(Ordering::Relaxed),
            connection_failures: self.connection_failures.load(Ordering::Relaxed),
            publish_accepted: self.publish_accepted.load(Ordering::Relaxed),
            publish_unknown: self.publish_unknown.load(Ordering::Relaxed),
            receive_unknown: self.receive_unknown.load(Ordering::Relaxed),
            settlement_unknown: self.settlement_unknown.load(Ordering::Relaxed),
            recovery_claim_commands: self.recovery_claim_commands.load(Ordering::Relaxed),
            quarantine_succeeded: self.quarantine_succeeded.load(Ordering::Relaxed),
            delivery_gaps: self.delivery_gaps.load(Ordering::Relaxed),
        }
    }
}
