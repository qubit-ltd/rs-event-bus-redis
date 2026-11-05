// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Cancellation guard for per-subscription recovery state.

use std::sync::Arc;
use std::sync::Mutex;

use super::recovery_state::RecoveryState;

/// Marks recovery due if an in-progress receive is cancelled or unwinds.
pub(crate) struct RecoveryGuard {
    /// Shared receiver state; this guard never retains its mutex across I/O.
    state: Arc<Mutex<RecoveryState>>,
    /// Cleared only after a receive returns normally.
    armed: bool,
}

impl RecoveryGuard {
    /// Arms cancellation recovery after a receive future has actually been
    /// polled. The guard owns an `Arc`, not a mutex lock.
    ///
    /// # Parameters
    ///
    /// - `state`: Shared state to mark due if this receive is cancelled.
    pub(crate) fn new(state: Arc<Mutex<RecoveryState>>) -> Self {
        Self { state, armed: true }
    }

    /// Prevents normal completion from forcing an extra recovery round.
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RecoveryGuard {
    /// Conservatively forces the next scan after cancellation or unwind.
    fn drop(&mut self) {
        if self.armed
            && let Ok(mut state) = self.state.lock()
        {
            state.force_recovery();
        }
    }
}
