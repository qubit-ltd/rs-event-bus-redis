// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Serde deserializer seed that discards a recursively checked JSON value.

use serde::Deserialize;
use serde::Deserializer;

use super::skip_visitor::SkipVisitor;

/// Sink value returned after recursively validating and discarding one JSON
/// value.
pub(super) struct Skip;
impl<'de> Deserialize<'de> for Skip {
    /// Consumes one value without retaining it; malformed or deeply nested
    /// input remains an error.
    ///
    /// # Parameters
    ///
    /// - `deserializer`: Source that provides a single JSON value.
    ///
    /// # Returns
    ///
    /// A zero-sized sink after the complete value has been consumed.
    ///
    /// # Errors
    ///
    /// Returns the deserializer error for malformed input or recursion-limit
    /// violations.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(SkipVisitor)
    }
}
