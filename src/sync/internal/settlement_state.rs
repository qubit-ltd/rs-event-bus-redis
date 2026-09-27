// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Settlement metadata retained by synchronous delivery tokens.

use std::sync::Arc;
use std::sync::Mutex;

use qubit_event_bus::spi::DeliveryDisposition;

use crate::recovery::RecoveryState;

/// Redis coordinates and settlement bookkeeping carried by one delivery token.
pub(in crate::sync) struct SettlementState {
    /// Stream containing the pending event.
    pub(in crate::sync) stream: String,
    /// Consumer group that owns the pending entry.
    pub(in crate::sync) group: String,
    /// Redis stream ID used by `XACK`.
    pub(in crate::sync) message_id: String,
    /// Final disposition applied through this token.
    pub(in crate::sync) disposition: Arc<Mutex<Option<DeliveryDisposition>>>,
    /// Per-subscription active delivery registry released after settlement.
    pub(in crate::sync) recovery: Arc<Mutex<RecoveryState>>,
}
