// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Process-local diagnostics for live Redis provider instances.

#[cfg(any(feature = "sync", feature = "async"))]
use std::sync::Arc;
#[cfg(any(feature = "sync", feature = "async"))]
use std::sync::Mutex;
#[cfg(any(feature = "sync", feature = "async"))]
use std::sync::OnceLock;
#[cfg(any(feature = "sync", feature = "async"))]
use std::sync::Weak;
#[cfg(any(feature = "sync", feature = "async"))]
use std::sync::atomic::AtomicU64;
#[cfg(any(feature = "sync", feature = "async"))]
use std::sync::atomic::Ordering;

#[cfg(any(feature = "sync", feature = "async"))]
use crate::client::ResourceBudget;
#[cfg(any(feature = "sync", feature = "async"))]
use crate::error::RedisProviderError;

/// Provider execution mode for a live Redis SPI instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisProviderMode {
    /// Synchronous Redis SPI.
    Sync,
    /// Runtime-neutral asynchronous Redis SPI.
    Async,
}

/// Point-in-time, process-local values for one live Redis SPI.
///
/// Each field is read independently, so the snapshot is not an atomic view
/// across counters. It contains no endpoint, credentials, payload, or raw
/// Redis error.
#[derive(Clone, Debug)]
pub struct RedisProviderSnapshot {
    /// Process-unique ID assigned to this SPI instance.
    instance_id: u64,
    /// Synchronous or asynchronous provider mode.
    mode: RedisProviderMode,
    /// Configured scope used to derive Redis keys.
    namespace: String,
    /// Currently occupied general command slots.
    general_in_flight: u64,
    /// Currently occupied reserved settlement slots.
    settlement_in_flight: u64,
    /// Currently retained receiver leases.
    active_receivers: u64,
    /// General or settlement command admission rejections.
    command_rejections: u64,
    /// Receiver admission rejections.
    receiver_rejections: u64,
    /// Provider-level connection open attempts.
    connection_attempts: u64,
    /// Provider-level connection open failures.
    connection_failures: u64,
    /// Publishes confirmed accepted by Redis.
    publish_accepted: u64,
    /// Publishes with an unknown outcome.
    publish_unknown: u64,
    /// Receive calls with an unknown outcome.
    receive_unknown: u64,
    /// Settlement calls with an unknown outcome.
    settlement_unknown: u64,
    /// Recovery XAUTOCLAIM commands sent.
    recovery_claim_commands: u64,
    /// Confirmed quarantine copies.
    quarantine_succeeded: u64,
    /// Delivery gaps returned to callers.
    delivery_gaps: u64,
}

impl RedisProviderSnapshot {
    /// Returns the process-unique ID assigned at successful SPI creation.
    #[must_use]
    #[inline]
    pub fn instance_id(&self) -> u64 {
        self.instance_id
    }

    /// Returns the provider mode.
    #[must_use]
    #[inline]
    pub fn mode(&self) -> RedisProviderMode {
        self.mode
    }

    /// Returns the configured namespace, without Redis endpoint details.
    #[must_use]
    #[inline]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Returns occupied general command slots, including general-lane settlements.
    #[must_use]
    #[inline]
    pub fn general_in_flight(&self) -> u64 {
        self.general_in_flight
    }

    /// Returns occupied reserved settlement slots.
    #[must_use]
    #[inline]
    pub fn settlement_in_flight(&self) -> u64 {
        self.settlement_in_flight
    }

    /// Returns active receiver leases.
    #[must_use]
    #[inline]
    pub fn active_receivers(&self) -> u64 {
        self.active_receivers
    }

    /// Returns command admission rejections.
    #[must_use]
    #[inline]
    pub fn command_rejections(&self) -> u64 {
        self.command_rejections
    }

    /// Returns receiver admission rejections.
    #[must_use]
    #[inline]
    pub fn receiver_rejections(&self) -> u64 {
        self.receiver_rejections
    }

    /// Returns actual provider connection open attempts.
    #[must_use]
    #[inline]
    pub fn connection_attempts(&self) -> u64 {
        self.connection_attempts
    }

    /// Returns failed provider connection open attempts.
    #[must_use]
    #[inline]
    pub fn connection_failures(&self) -> u64 {
        self.connection_failures
    }

    /// Returns publishes confirmed accepted by Redis.
    #[must_use]
    #[inline]
    pub fn publish_accepted(&self) -> u64 {
        self.publish_accepted
    }

    /// Returns publishes with an unknown Redis outcome.
    #[must_use]
    #[inline]
    pub fn publish_unknown(&self) -> u64 {
        self.publish_unknown
    }

    /// Returns receive calls that reported an unknown outcome.
    #[must_use]
    #[inline]
    pub fn receive_unknown(&self) -> u64 {
        self.receive_unknown
    }

    /// Returns settlement calls that reported an unknown outcome.
    #[must_use]
    #[inline]
    pub fn settlement_unknown(&self) -> u64 {
        self.settlement_unknown
    }

    /// Returns XAUTOCLAIM commands sent during recovery.
    #[must_use]
    #[inline]
    pub fn recovery_claim_commands(&self) -> u64 {
        self.recovery_claim_commands
    }

    /// Returns confirmed quarantine copies.
    #[must_use]
    #[inline]
    pub fn quarantine_succeeded(&self) -> u64 {
        self.quarantine_succeeded
    }

    /// Returns gaps actually returned to the facade.
    #[must_use]
    #[inline]
    pub fn delivery_gaps(&self) -> u64 {
        self.delivery_gaps
    }
}

/// Reads diagnostics for Redis SPI instances still alive in this process.
pub struct RedisProviderDiagnostics;

impl RedisProviderDiagnostics {
    /// Returns snapshots of live instances ordered by increasing instance ID.
    ///
    /// The directory lock covers only weak-reference upgrades and removal of
    /// expired entries. Counter reads happen after releasing it. With neither
    /// provider feature enabled, this returns an empty vector.
    #[must_use]
    pub fn snapshots() -> Vec<RedisProviderSnapshot> {
        #[cfg(any(feature = "sync", feature = "async"))]
        {
            let live = {
                let mut directory = directory()
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let mut live = Vec::with_capacity(directory.len());
                directory.retain(|weak| {
                    if let Some(state) = weak.upgrade() {
                        live.push(state);
                        true
                    } else {
                        false
                    }
                });
                live
            };
            let mut snapshots: Vec<_> = live.iter().map(|state| state.snapshot()).collect();
            snapshots.sort_unstable_by_key(RedisProviderSnapshot::instance_id);
            snapshots
        }
        #[cfg(not(any(feature = "sync", feature = "async")))]
        {
            Vec::new()
        }
    }
}

/// Counter selected by an operation outcome or admission boundary.
#[cfg(any(feature = "sync", feature = "async"))]
#[derive(Clone, Copy, Debug)]
#[allow(dead_code)] // T2 and T3 attach the counter update sites.
pub(crate) enum RedisDiagnosticCounter {
    CommandRejections,
    ReceiverRejections,
    ConnectionAttempts,
    ConnectionFailures,
    PublishAccepted,
    PublishUnknown,
    ReceiveUnknown,
    SettlementUnknown,
    RecoveryClaimCommands,
    QuarantineSucceeded,
    DeliveryGaps,
}

/// Atomics and the existing shared resource budget for one live SPI.
#[cfg(any(feature = "sync", feature = "async"))]
pub(crate) struct RedisDiagnosticsState {
    /// Process-unique ID assigned before registration.
    instance_id: u64,
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
            .push(Arc::downgrade(&state));
        Ok(state)
    }

    /// Saturatingly increments one monotonic counter without taking the directory lock.
    #[allow(dead_code)] // T2 and T3 attach the counter update sites.
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
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
            Some(n.saturating_add(1))
        });
    }

    /// Reads independent atomic values into a secret-safe public snapshot.
    fn snapshot(&self) -> RedisProviderSnapshot {
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

/// Lazily creates the process-wide directory; it owns only weak references.
#[cfg(any(feature = "sync", feature = "async"))]
fn directory() -> &'static Mutex<Vec<Weak<RedisDiagnosticsState>>> {
    static DIRECTORY: OnceLock<Mutex<Vec<Weak<RedisDiagnosticsState>>>> = OnceLock::new();
    DIRECTORY.get_or_init(|| Mutex::new(Vec::new()))
}
