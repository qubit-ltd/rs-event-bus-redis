// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Entry point for collecting process-local provider diagnostics.

use super::RedisProviderSnapshot;
#[cfg(any(feature = "sync", feature = "async"))]
use super::internal::directory;

/// Reads diagnostics for Redis SPI instances still alive in this process.
///
/// # Examples
///
/// ```
/// use qubit_event_bus_redis::diagnostics::RedisProviderDiagnostics;
///
/// for snapshot in RedisProviderDiagnostics::snapshots() {
///     assert!(snapshot.instance_id() > 0);
///     let occupied = snapshot.general_in_flight() + snapshot.settlement_in_flight();
///     println!("{}: {occupied}", snapshot.namespace());
/// }
/// ```
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
                directory()
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .live()
            };
            let mut snapshots: Vec<RedisProviderSnapshot> =
                live.iter().map(|state| state.snapshot()).collect();
            snapshots.sort_unstable_by_key(RedisProviderSnapshot::instance_id);
            snapshots
        }
        #[cfg(not(any(feature = "sync", feature = "async")))]
        {
            Vec::new()
        }
    }
}
