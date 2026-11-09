// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Async short-command connection role used for cache selection and
//! invalidation.

use super::command_class::CommandClass;

/// Identifies the standalone async connection cache used by a short command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AsyncConnectionRole {
    /// Publish, subscription setup, and other ordinary commands.
    General,
    /// Terminal settlement EVAL commands.
    Settlement,
}

/// Converts a command classification into its standalone connection-cache role.
impl From<CommandClass> for AsyncConnectionRole {
    /// Returns the cache role selected by `value`.
    ///
    /// # Parameters
    ///
    /// - `value`: Command classification to map.
    fn from(value: CommandClass) -> Self {
        match value {
            CommandClass::General => Self::General,
            CommandClass::Settlement => Self::Settlement,
        }
    }
}
