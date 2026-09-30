// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Serde visitor that recursively consumes JSON without an intermediate tree.

use std::fmt;

use serde::de;
use serde::de::MapAccess;
use serde::de::SeqAccess;
use serde::de::Visitor;

use super::skip::Skip;

/// Recursively consumes JSON values without retaining their contents.
pub(super) struct SkipVisitor;
impl<'de> Visitor<'de> for SkipVisitor {
    type Value = Skip;
    /// Describes any valid JSON value accepted by this discard visitor.
    ///
    /// # Parameters
    ///
    /// - `f`: Formatter receiving the expected input description.
    ///
    /// # Returns
    ///
    /// Success after writing the JSON-value description.
    ///
    /// # Errors
    ///
    /// Returns formatting failure from the supplied formatter.
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON value")
    }
    /// Discards a boolean without allocating or validating nested data.
    ///
    /// # Type Parameters
    ///
    /// - `E`: Error type selected by the deserializer.
    ///
    /// # Parameters
    ///
    /// - `value`: Boolean value to discard.
    ///
    /// # Returns
    ///
    /// The zero-sized sink value.
    ///
    /// # Errors
    ///
    /// Never returns an error for a scalar value.
    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Skip, E> {
        let _ = value;
        Ok(Skip)
    }
    /// Discards a signed integer without allocating or validating nested data.
    ///
    /// # Type Parameters
    ///
    /// - `E`: Error type selected by the deserializer.
    ///
    /// # Parameters
    ///
    /// - `value`: Signed integer to discard.
    ///
    /// # Returns
    ///
    /// The zero-sized sink value.
    ///
    /// # Errors
    ///
    /// Never returns an error for a scalar value.
    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Skip, E> {
        let _ = value;
        Ok(Skip)
    }
    /// Discards an unsigned integer without allocating or validating nested
    /// data.
    ///
    /// # Type Parameters
    ///
    /// - `E`: Error type selected by the deserializer.
    ///
    /// # Parameters
    ///
    /// - `value`: Unsigned integer to discard.
    ///
    /// # Returns
    ///
    /// The zero-sized sink value.
    ///
    /// # Errors
    ///
    /// Never returns an error for a scalar value.
    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Skip, E> {
        let _ = value;
        Ok(Skip)
    }
    /// Discards a floating point number without retaining it.
    ///
    /// # Type Parameters
    ///
    /// - `E`: Error type selected by the deserializer.
    ///
    /// # Parameters
    ///
    /// - `value`: Floating point value to discard.
    ///
    /// # Returns
    ///
    /// The zero-sized sink value.
    ///
    /// # Errors
    ///
    /// Never returns an error for a scalar value.
    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Skip, E> {
        let _ = value;
        Ok(Skip)
    }
    /// Discards a borrowed string without copying its contents.
    ///
    /// # Type Parameters
    ///
    /// - `E`: Error type selected by the deserializer.
    ///
    /// # Parameters
    ///
    /// - `value`: Borrowed string to discard.
    ///
    /// # Returns
    ///
    /// The zero-sized sink value.
    ///
    /// # Errors
    ///
    /// Never returns an error for a scalar value.
    fn visit_str<E: de::Error>(self, value: &str) -> Result<Skip, E> {
        let _ = value;
        Ok(Skip)
    }
    /// Discards a null value.
    ///
    /// # Type Parameters
    ///
    /// - `E`: Error type selected by the deserializer.
    ///
    /// # Returns
    ///
    /// The zero-sized sink value.
    ///
    /// # Errors
    ///
    /// Never returns an error for a scalar value.
    fn visit_unit<E: de::Error>(self) -> Result<Skip, E> {
        Ok(Skip)
    }
    /// Recursively consumes array elements without building an intermediate
    /// tree.
    ///
    /// # Type Parameters
    ///
    /// - `'de`: Lifetime of data borrowed from the deserializer.
    /// - `A`: Sequence access implementation supplied by the deserializer.
    ///
    /// # Parameters
    ///
    /// - `seq`: Access to the array elements to consume.
    ///
    /// # Returns
    ///
    /// The zero-sized sink after every array element is consumed.
    ///
    /// # Errors
    ///
    /// Propagates malformed nested values and deserializer recursion-limit
    /// failures.
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Skip, A::Error> {
        while seq.next_element::<Skip>()?.is_some() {}
        Ok(Skip)
    }
    /// Recursively consumes object values without retaining keys or values.
    ///
    /// # Type Parameters
    ///
    /// - `'de`: Lifetime of data borrowed from the deserializer.
    /// - `A`: Map access implementation supplied by the deserializer.
    ///
    /// # Parameters
    ///
    /// - `map`: Access to the object keys and values to consume.
    ///
    /// # Returns
    ///
    /// The zero-sized sink after every object entry is consumed.
    ///
    /// # Errors
    ///
    /// Propagates malformed nested values and deserializer recursion-limit
    /// failures.
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Skip, A::Error> {
        while map.next_key::<de::IgnoredAny>()?.is_some() {
            map.next_value::<Skip>()?;
        }
        Ok(Skip)
    }
}
