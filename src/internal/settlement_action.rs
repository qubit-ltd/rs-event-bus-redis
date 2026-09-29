// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Pure disposition decisions shared by both receiver adapters.

/// Next operation selected without changing local state or issuing I/O.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SettlementAction {
    /// The identical disposition is already committed.
    AlreadyApplied,
    /// Obtain a connection before fixing a new terminal acknowledgement intent.
    PrepareAck,
    /// Repeat the previously fixed acknowledgement intent.
    RepeatAck,
    /// Release local ownership without issuing XACK.
    ApplyRetry,
}
