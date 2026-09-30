// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Operations selected by the receive driver.

/// Redis operation selected by the common receive state machine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub(crate) enum ReceiveAction {
    /// Recover work owned by another consumer.
    Claim,
    /// Continue reading this consumer's pending entries.
    Pending,
    /// Read new group entries, optionally blocking for this many milliseconds.
    ReadNew {
        /// Bounded Redis block duration, or `None` for a zero-timeout read.
        block_ms: Option<usize>,
    },
    /// The receive deadline expired or its zero-timeout reads completed.
    TimedOut,
}
