// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Validates and collects bounded wire payload bytes.

use std::fmt;

use serde::de;
use serde::de::SeqAccess;
use serde::de::Visitor;

use super::payload_seed::PayloadSeed;

/// Collects bytes after checking the configured length limit.
pub(super) struct PayloadVisitor<'a>(
    /// Seed containing the configured limit and overflow flag.
    pub(super) PayloadSeed<'a>,
);

impl<'de> Visitor<'de> for PayloadVisitor<'_> {
    type Value = Vec<u8>;

    /// Describes the expected bounded byte array input.
    ///
    /// # Parameters
    ///
    /// - `self`: Visitor used for the current byte array.
    /// - `f`: Formatter receiving the input description.
    ///
    /// # Returns
    ///
    /// Success after writing the expected input description.
    ///
    /// # Errors
    ///
    /// Returns formatting failure from the supplied formatter.
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a bounded byte array")
    }

    /// Rejects excessive hints and checks every push before extending the
    /// output vector.
    ///
    /// # Type Parameters
    ///
    /// - `'de`: Lifetime of data borrowed from the deserializer.
    /// - `A`: Sequence access implementation supplied by the deserializer.
    ///
    /// # Parameters
    ///
    /// - `self`: Visitor carrying the limit and overflow flag.
    /// - `seq`: Sequence of integer byte values to consume.
    ///
    /// # Returns
    ///
    /// The decoded byte vector when all values fit within the limit.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid byte, malformed sequence, or limit
    /// violation.
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<u8>, A::Error> {
        if seq.size_hint().is_some_and(|size| size > self.0.limit) {
            self.0.exceeded.set(true);
            return Err(de::Error::custom("payload limit"));
        }
        let mut bytes = Vec::new();
        while let Some(byte) = seq.next_element::<u8>()? {
            if bytes.len() == self.0.limit {
                self.0.exceeded.set(true);
                return Err(de::Error::custom("payload limit"));
            }
            bytes.push(byte);
        }
        Ok(bytes)
    }
}
