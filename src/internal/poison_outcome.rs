// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Confirmed quarantine script outcomes; errors may hide partial Lua writes.

use qubit_event_bus::spi::DeliveryGap;
use qubit_event_bus::spi::ReceiveOutcome;

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

impl PoisonOutcome {
    /// Maps a confirmed poison script result to its public receive outcome.
    ///
    /// Ownership changes return `None` because this consumer did not confirm
    /// a gap. This method performs no I/O and does not update diagnostics.
    #[must_use]
    pub(crate) fn receive_outcome(self) -> Option<ReceiveOutcome> {
        let (message, count) = match self {
            Self::Quarantined => ("malformed Redis stream entry was quarantined", Some(1)),
            Self::SourceGone => ("malformed pending Redis stream entry was removed", Some(1)),
            Self::TombstoneCleared => (
                "deleted Redis stream entry was removed from the pending list",
                Some(1),
            ),
            Self::OwnershipChanged => return None,
        };
        Some(ReceiveOutcome::Gap(DeliveryGap::new(message, count)))
    }
}
