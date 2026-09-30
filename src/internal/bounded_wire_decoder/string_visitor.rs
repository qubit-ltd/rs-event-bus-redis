// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Validates and collects bounded wire header strings.

use std::fmt;

use serde::de;
use serde::de::Visitor;

use super::string_seed::StringSeed;

/// Collects a string after checking its decoded byte length.
pub(super) struct StringVisitor<'a>(
    /// Seed containing the configured limit and overflow flag.
    pub(super) StringSeed<'a>,
);

impl<'de> Visitor<'de> for StringVisitor<'_> {
    type Value = String;

    /// Describes the expected bounded string input.
    ///
    /// # Parameters
    ///
    /// - `self`: Visitor used for the current string.
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
        f.write_str("a bounded headers string")
    }

    /// Checks unescaped byte length before cloning the parser's string view.
    ///
    /// # Type Parameters
    ///
    /// - `E`: Deserializer error implementation.
    ///
    /// # Parameters
    ///
    /// - `self`: Visitor carrying the limit and overflow flag.
    /// - `value`: Decoded string view provided by the deserializer.
    ///
    /// # Returns
    ///
    /// An owned copy when its byte length is within the limit.
    ///
    /// # Errors
    ///
    /// Returns a custom deserialization error when the limit is exceeded.
    fn visit_str<E: de::Error>(self, value: &str) -> Result<String, E> {
        if value.len() > self.0.limit {
            self.0.exceeded.set(true);
            return Err(E::custom("headers limit"));
        }
        Ok(value.to_owned())
    }
}
