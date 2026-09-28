// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Per-subscription pending-entry cursors and active-delivery tracking.

/// Which Redis recovery scan is consuming a receive-call command budget.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryScanStage {
    /// Scan pending entries eligible for claim by this consumer.
    Claim,
    /// Revisit pending entries already owned by this consumer.
    Pending,
}
