// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Passes inclusive wire limits into the version-one field visitor.

use std::cell::Cell;

use serde::Deserializer;
use serde::de::DeserializeSeed;

use super::fields_visitor::FieldsVisitor;
use crate::wire_fields::WireFields;
use crate::wire_limits::WireLimits;

/// Supplies decode limits and a shared rejection marker to the field visitor.
///
/// The marker lets the caller distinguish a byte-budget violation from other
/// deserialization errors without retaining an error string.
///
/// # Type Parameters
///
/// - `'a`: Lifetime of the shared rejection marker used during decoding.
pub(super) struct FieldsSeed<'a> {
    /// Inclusive byte limits applied to decoded wire fields.
    pub(super) limits: WireLimits,
    /// Set when a bounded field exceeds its configured budget.
    pub(super) exceeded: &'a Cell<bool>,
}
impl<'de> DeserializeSeed<'de> for FieldsSeed<'_> {
    /// The decoded version-one wire record.
    type Value = WireFields;
    /// Constructs fields directly while sharing a typed limit-failure marker.
    ///
    /// # Type Parameters
    ///
    /// - `'de`: Lifetime of data borrowed from the deserializer.
    /// - `D`: Deserializer implementation supplying the wire object.
    ///
    /// # Parameters
    ///
    /// - `self`: Seed carrying limits and the shared rejection marker.
    /// - `deserializer`: Source containing the top-level wire object.
    ///
    /// # Returns
    ///
    /// Parsed version-one fields with decoded components kept within limits.
    ///
    /// # Errors
    ///
    /// Returns syntax, missing/duplicate-field, type, or configured size-limit
    /// errors.
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<WireFields, D::Error> {
        deserializer.deserialize_map(FieldsVisitor(self))
    }
}
