// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Completed receive command classifications.

/// Meaning of a completed Redis receive command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub(crate) enum ReceiveReply {
    /// Claim cursor reached the end of its scan.
    ClaimAtEnd,
    /// Claim returned another page to scan.
    ClaimHasMore,
    /// The pending scan returned one entry.
    PendingEntry,
    /// The pending scan has no more entries.
    PendingEmpty,
    /// A read for new entries returned one entry.
    NewEntry,
    /// A read for new entries returned no entries.
    NewEmpty,
}
