// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Admission priority requested for a short Redis command.

/// Classifies a short Redis command by its admission priority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommandClass {
    /// Publishing, subscription setup, and other ordinary short commands.
    General,
    /// Terminal acknowledgement; may use a reserved or general slot.
    Settlement,
}
