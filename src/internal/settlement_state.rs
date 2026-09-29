// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared Redis settlement metadata retained by synchronous and asynchronous
//! tokens.

use std::sync::Arc;
use std::sync::Mutex;

use qubit_event_bus::spi::DeliveryDisposition;

use super::recovery_state::RecoveryState;
use super::settlement_progress::SettlementProgress;

/// Redis coordinates and shared settlement bookkeeping carried by one token.
#[derive(Clone)]
pub(crate) struct SettlementState {
    /// Stream containing the pending event.
    pub(crate) stream: String,
    /// Consumer group that owns the pending entry.
    pub(crate) group: String,
    /// Redis stream ID used by `XACK`.
    pub(crate) message_id: String,
    /// Fixed acknowledgement intent and locally committed disposition.
    pub(crate) progress: Arc<Mutex<SettlementProgress>>,
    /// Per-subscription active delivery registry released after settlement.
    pub(crate) recovery: Arc<Mutex<RecoveryState>>,
}

impl SettlementState {
    /// Commits `disposition` and releases its active slot without I/O.
    ///
    /// Locks progress before recovery, acquiring both before mutating either.
    /// The caller must have validated this disposition and, for terminal
    /// outcomes, received a valid XACK reply. Neither lock spans an await.
    ///
    /// # Parameters
    ///
    /// - `disposition`: The permitted disposition to commit locally.
    ///
    /// # Returns
    ///
    /// Success once the active slot is released and progress becomes Applied.
    ///
    /// # Errors
    ///
    /// Returns a stable lock label when poisoning prevents the full commit.
    /// Neither state changes; pending acknowledgement intent remains fixed
    /// so only the identical terminal decision may retry.
    pub(crate) fn commit(&self, disposition: DeliveryDisposition) -> Result<(), &'static str> {
        let mut progress = self.progress.lock().map_err(|_| "settlement lock")?;
        let mut recovery = self.recovery.lock().map_err(|_| "recovery lock")?;
        if disposition == DeliveryDisposition::Retry {
            recovery.mark_retry(&self.message_id);
        } else {
            recovery.mark_terminal(&self.message_id);
        }
        *progress = SettlementProgress::Applied(disposition);
        Ok(())
    }
}
