// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Settlement intent retained across unknown acknowledgement outcomes.

use qubit_event_bus::spi::DeliveryDisposition;

use super::settlement_action::SettlementAction;

/// Local settlement progress for one receiver-bound token.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SettlementProgress {
    /// No acknowledgement may have applied and no disposition is committed.
    Open,
    /// A terminal acknowledgement may have applied; only its intent may repeat.
    AckPending(
        /// Terminal intent whose prior XACK may have applied; only this intent
        /// may repeat.
        DeliveryDisposition,
    ),
    /// Local bookkeeping committed this disposition; terminal outcomes also
    /// have a valid XACK reply, while Retry commits without Redis I/O.
    Applied(
        /// Disposition already committed to local active-slot bookkeeping.
        DeliveryDisposition,
    ),
}

impl SettlementProgress {
    /// Selects the next operation without changing progress or issuing I/O.
    ///
    /// # Parameters
    ///
    /// - `requested`: The disposition requested for this token.
    ///
    /// # Returns
    ///
    /// The acknowledgement preparation, identical acknowledgement retry,
    /// local Retry commit, or already-applied action permitted by this state.
    ///
    /// # Errors
    ///
    /// Returns `Err(())` when `requested` conflicts with a pending or applied
    /// disposition. Connection acquisition must precede fixing a new intent.
    #[inline]
    pub(crate) fn action(self, requested: DeliveryDisposition) -> Result<SettlementAction, ()> {
        match self {
            Self::Applied(previous) if previous == requested => {
                Ok(SettlementAction::AlreadyApplied)
            }
            Self::AckPending(previous) if previous == requested => Ok(SettlementAction::RepeatAck),
            Self::Applied(_) | Self::AckPending(_) => Err(()),
            Self::Open if requested == DeliveryDisposition::Retry => {
                Ok(SettlementAction::ApplyRetry)
            }
            Self::Open => Ok(SettlementAction::PrepareAck),
        }
    }
}
