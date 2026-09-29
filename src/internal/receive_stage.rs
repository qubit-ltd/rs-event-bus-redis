// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Current phase of one receive dispatch sequence.

/// Recovery or new-message phase currently scheduled by the receive driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReceiveStage {
    /// Recover another consumer's pending work.
    Claim,
    /// Scan this consumer's pending work.
    Pending,
    /// Read previously undelivered work.
    ReadNew,
    /// Stop scheduling commands for this receive.
    TimedOut,
}
