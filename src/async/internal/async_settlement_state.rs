// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Settlement metadata retained by asynchronous delivery tokens.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;

use qubit_event_bus::spi::DeliveryDisposition;

/// Redis coordinates and shared settlement bookkeeping carried by one token.
pub(in crate::r#async) struct AsyncSettlementState {
    /// Stream containing the pending event.
    pub(in crate::r#async) stream: String,
    /// Consumer group that owns the pending entry.
    pub(in crate::r#async) group: String,
    /// Redis stream ID used by `XACK`.
    pub(in crate::r#async) message_id: String,
    /// Disposition already applied, shared by cloned token references.
    pub(in crate::r#async) disposition: Arc<Mutex<Option<DeliveryDisposition>>>,
    /// Receiver's in-flight count, decremented after successful settlement.
    pub(in crate::r#async) outstanding: Arc<AtomicUsize>,
}
