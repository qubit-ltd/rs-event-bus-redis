// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounds decoding of wire header strings.

use std::cell::Cell;

use serde::Deserializer;
use serde::de::DeserializeSeed;

use super::string_visitor::StringVisitor;

/// Deserializes a string while enforcing its byte limit.
pub(super) struct StringSeed<'a> {
    /// Maximum decoded UTF-8 byte length.
    pub(super) limit: usize,
    /// Records whether the configured limit was exceeded.
    pub(super) exceeded: &'a Cell<bool>,
}

impl<'de> DeserializeSeed<'de> for StringSeed<'_> {
    type Value = String;

    /// Decodes a JSON string through the bounded string visitor.
    ///
    /// # Type Parameters
    ///
    /// - `'de`: Lifetime of data borrowed from the deserializer.
    /// - `D`: Deserializer implementation.
    ///
    /// # Parameters
    ///
    /// - `self`: Seed carrying the byte limit and exceeded flag.
    /// - `deserializer`: Source JSON deserializer.
    ///
    /// # Returns
    ///
    /// The decoded owned string when it is within the configured limit.
    ///
    /// # Errors
    ///
    /// Returns a deserialization error for non-string or oversized input.
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<String, D::Error> {
        deserializer.deserialize_str(StringVisitor(self))
    }
}
