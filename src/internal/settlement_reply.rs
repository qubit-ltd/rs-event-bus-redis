// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Response classes returned by the atomic settlement script.

/// Reply classes returned by the atomic settlement script.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SettlementReply {
    /// Redis acknowledged the entry or found it already absent.
    Completed,
    /// The pending entry is currently owned by another consumer.
    OwnershipChanged,
}
