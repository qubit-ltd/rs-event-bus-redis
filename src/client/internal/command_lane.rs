// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Admission lane retained by one command permit.

/// Counter lane owned by a short-command permit.
#[derive(Clone, Copy, Debug)]
pub(super) enum CommandLane {
    /// Ordinary short Redis operations.
    General,
    /// Terminal acknowledgements using reserved capacity.
    ReservedSettlement,
}
