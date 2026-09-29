// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Wire version discovery without building an untyped JSON value tree.

use serde::Deserialize;

/// Deserializes only the protocol selector and ignores unrelated JSON fields.
#[derive(Deserialize)]
pub(super) struct VersionProbe {
    /// Numeric protocol selector; unknown positive versions remain unsupported.
    pub(super) version: u64,
}
