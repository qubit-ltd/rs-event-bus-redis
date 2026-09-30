// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Selects the wire version while validating unknown members.

use std::fmt;

use serde::de;
use serde::de::MapAccess;
use serde::de::Visitor;

use super::skip::Skip;

/// Reads the version field while recursively validating unknown JSON members.
pub(super) struct VersionVisitor;
impl<'de> Visitor<'de> for VersionVisitor {
    type Value = u64;
    /// Requires the versioned top-level object.
    ///
    /// # Parameters
    ///
    /// - `f`: Formatter receiving the required input description.
    ///
    /// # Returns
    ///
    /// Success after writing the versioned-object description.
    ///
    /// # Errors
    ///
    /// Returns formatting failure from the supplied formatter.
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a versioned wire object")
    }
    /// Extracts exactly one numeric version while validating the remaining
    /// JSON.
    ///
    /// # Type Parameters
    ///
    /// - `'de`: Lifetime of data borrowed from the deserializer.
    /// - `A`: Map access implementation supplied by the deserializer.
    ///
    /// # Parameters
    ///
    /// - `map`: Top-level object map to inspect.
    ///
    /// # Returns
    ///
    /// The required numeric version after all members are consumed.
    ///
    /// # Errors
    ///
    /// Returns errors for duplicate/missing/non-numeric versions or malformed
    /// unknown fields.
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<u64, A::Error> {
        let mut version = None;
        while let Some(key) = map.next_key::<String>()? {
            if key == "version" {
                if version.is_some() {
                    return Err(de::Error::duplicate_field("version"));
                }
                version = Some(map.next_value::<u64>()?);
            } else {
                map.next_value::<Skip>()?;
            }
        }
        version.ok_or_else(|| de::Error::missing_field("version"))
    }
}
