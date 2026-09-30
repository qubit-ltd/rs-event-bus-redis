// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounds decoding of wire payload byte arrays.

use std::cell::Cell;

use serde::Deserializer;
use serde::de::DeserializeSeed;

use super::payload_visitor::PayloadVisitor;

/// Deserializes a byte array while enforcing its length limit.
pub(super) struct PayloadSeed<'a> {
    /// Maximum number of decoded bytes.
    pub(super) limit: usize,
    /// Records whether the configured limit was exceeded.
    pub(super) exceeded: &'a Cell<bool>,
}

impl<'de> DeserializeSeed<'de> for PayloadSeed<'_> {
    type Value = Vec<u8>;

    /// Decodes a JSON sequence through the bounded payload visitor.
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
    /// The decoded byte vector when it is within the configured limit.
    ///
    /// # Errors
    ///
    /// Returns a deserialization error for non-sequence or oversized input.
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Vec<u8>, D::Error> {
        deserializer.deserialize_seq(PayloadVisitor(self))
    }
}
