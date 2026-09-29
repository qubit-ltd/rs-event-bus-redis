// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Confirmed quarantine script outcomes; errors may hide partial Lua writes.

/// Confirmed result of checking and transferring a malformed group delivery.
///
/// Redis prevents concurrent command interleaving during the script but does
/// not roll back writes on failure. An unconfirmed or failed script is reported
/// separately as an unknown outcome rather than represented by this enum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PoisonOutcome {
    /// Entry is recorded in quarantine and acknowledged from its original
    /// group.
    Quarantined,
    /// A missing source entry was removed from the consumer group's PEL.
    TombstoneCleared,
    /// Another consumer owns the pending entry now.
    OwnershipChanged,
    /// Entry is no longer pending or has already been removed from the stream.
    SourceGone,
}
