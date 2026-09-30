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

/// Supplies the limits and shared rejection marker to the field visitor.
pub(super) struct FieldsSeed<'a> {
    /// Inclusive byte limits applied to decoded wire fields.
    pub(super) limits: WireLimits,
    /// Set when a bounded field exceeds its configured budget.
    pub(super) exceeded: &'a Cell<bool>,
}
impl<'de> DeserializeSeed<'de> for FieldsSeed<'_> {
    type Value = WireFields;
    /// Constructs fields directly while sharing a typed limit-failure marker.
    ///
    /// # Parameters
    ///
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
