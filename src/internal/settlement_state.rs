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

/// Redis coordinates and shared settlement bookkeeping carried by one token.
pub(crate) struct SettlementState {
    /// Stream containing the pending event.
    pub(crate) stream: String,
    /// Consumer group that owns the pending entry.
    pub(crate) group: String,
    /// Redis stream ID used by `XACK`.
    pub(crate) message_id: String,
    /// Final disposition applied through this token.
    pub(crate) disposition: Arc<Mutex<Option<DeliveryDisposition>>>,
    /// Per-subscription active delivery registry released after settlement.
    pub(crate) recovery: Arc<Mutex<RecoveryState>>,
}
