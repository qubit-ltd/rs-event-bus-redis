// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! One detailed pending Redis delivery row.

/// One row returned by the detailed XPENDING command.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PendingEntry {
    /// Redis stream entry ID.
    pub(crate) id: String,
    /// Consumer that currently owns the pending delivery.
    pub(crate) owner: String,
    /// Idle duration reported by Redis, in milliseconds.
    pub(crate) idle_ms: u64,
    /// Number of times Redis delivered this entry.
    pub(crate) times_delivered: u64,
}
