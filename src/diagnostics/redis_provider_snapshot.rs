// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Point-in-time values collected from a live provider.

use super::RedisProviderMode;

/// Point-in-time, process-local values for one live Redis SPI.
///
/// Each field is read independently, so the snapshot is not an atomic view
/// across counters. It contains no endpoint, credentials, payload, or raw
/// Redis error.
///
/// # Examples
///
/// ```
/// use qubit_event_bus_redis::diagnostics::RedisProviderSnapshot;
///
/// fn report(snapshot: &RedisProviderSnapshot) {
///     let occupied = snapshot.general_in_flight() + snapshot.settlement_in_flight();
///     println!("{}: {occupied}", snapshot.namespace());
/// }
/// ```
#[derive(Clone, Debug)]
pub struct RedisProviderSnapshot {
    /// Process-unique ID assigned to this SPI instance.
    pub(in crate::diagnostics) instance_id: u64,
    /// Synchronous or asynchronous provider mode.
    pub(in crate::diagnostics) mode: RedisProviderMode,
    /// Configured scope used to derive Redis keys.
    pub(in crate::diagnostics) namespace: String,
    /// Currently occupied general command slots.
    pub(in crate::diagnostics) general_in_flight: u64,
    /// Currently occupied reserved settlement slots.
    pub(in crate::diagnostics) settlement_in_flight: u64,
    /// Currently retained receiver leases.
    pub(in crate::diagnostics) active_receivers: u64,
    /// General or settlement command admission rejections.
    pub(in crate::diagnostics) command_rejections: u64,
    /// Receiver admission rejections.
    pub(in crate::diagnostics) receiver_rejections: u64,
    /// Provider-level connection open attempts.
    pub(in crate::diagnostics) connection_attempts: u64,
    /// Provider-level connection open failures.
    pub(in crate::diagnostics) connection_failures: u64,
    /// Publishes confirmed accepted by Redis.
    pub(in crate::diagnostics) publish_accepted: u64,
    /// Publishes with an unknown outcome.
    pub(in crate::diagnostics) publish_unknown: u64,
    /// Receive calls with an unknown outcome.
    pub(in crate::diagnostics) receive_unknown: u64,
    /// Settlement calls with an unknown outcome.
    pub(in crate::diagnostics) settlement_unknown: u64,
    /// Recovery XAUTOCLAIM commands sent.
    pub(in crate::diagnostics) recovery_claim_commands: u64,
    /// Confirmed quarantine copies.
    pub(in crate::diagnostics) quarantine_succeeded: u64,
    /// Delivery gaps returned to callers.
    pub(in crate::diagnostics) delivery_gaps: u64,
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

    /// Returns occupied general command slots, including general-lane
    /// settlements.
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
