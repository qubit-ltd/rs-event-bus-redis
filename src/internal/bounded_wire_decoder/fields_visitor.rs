// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Validates and constructs the version-one field map without an intermediate
//! tree.

use std::fmt;

use serde::de;
use serde::de::MapAccess;
use serde::de::Visitor;

use super::fields_seed::FieldsSeed;
use super::payload_seed::PayloadSeed;
use super::skip::Skip;
use super::string_seed::StringSeed;
use crate::wire_fields::WireFields;

/// Validates required/duplicate fields and constructs the version-one record.
pub(super) struct FieldsVisitor<'a>(
    /// Decoder limits and the shared exceeded flag.
    pub(super) FieldsSeed<'a>,
);
impl<'de> Visitor<'de> for FieldsVisitor<'_> {
    type Value = WireFields;
    /// Describes the supported version-one field layout.
    ///
    /// # Parameters
    ///
    /// - `f`: Formatter receiving the expected-layout description.
    ///
    /// # Returns
    ///
    /// Success after writing the version-one field layout.
    ///
    /// # Errors
    ///
    /// Returns formatting failure from the supplied formatter.
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("version 1 wire fields")
    }
    /// Validates duplicate/required fields and bounds decoded components during
    /// construction.
    ///
    /// # Type Parameters
    ///
    /// - `'de`: Lifetime of data borrowed from the deserializer.
    /// - `A`: Map access implementation supplied by the deserializer.
    ///
    /// # Parameters
    ///
    /// - `map`: Top-level object map containing wire fields.
    ///
    /// # Returns
    ///
    /// A `WireFields` value after each required field is validated.
    ///
    /// # Errors
    ///
    /// Returns malformed type, duplicate/missing field, or configured component
    /// limit errors.
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<WireFields, A::Error> {
        let mut version = None;
        let mut event_id = None;
        let mut timestamp_ms = None;
        let mut headers_json = None;
        let mut ordering_key: Option<Option<String>> = None;
        let mut content_type = None;
        let mut schema_id: Option<Option<String>> = None;
        let mut payload = None;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "version" => {
                    duplicate(&version, "version")?;
                    version = Some(map.next_value()?);
                }
                "event_id" => {
                    duplicate(&event_id, "event_id")?;
                    event_id = Some(map.next_value()?);
                }
                "timestamp_ms" => {
                    duplicate(&timestamp_ms, "timestamp_ms")?;
                    timestamp_ms = Some(map.next_value()?);
                }
                "headers_json" => {
                    duplicate(&headers_json, "headers_json")?;
                    headers_json = Some(map.next_value_seed(StringSeed {
                        limit: self.0.limits.headers,
                        exceeded: self.0.exceeded,
                    })?);
                }
                "ordering_key" => {
                    duplicate(&ordering_key, "ordering_key")?;
                    ordering_key = Some(map.next_value()?);
                }
                "content_type" => {
                    duplicate(&content_type, "content_type")?;
                    content_type = Some(map.next_value()?);
                }
                "schema_id" => {
                    duplicate(&schema_id, "schema_id")?;
                    schema_id = Some(map.next_value()?);
                }
                "payload" => {
                    duplicate(&payload, "payload")?;
                    payload = Some(map.next_value_seed(PayloadSeed {
                        limit: self.0.limits.payload,
                        exceeded: self.0.exceeded,
                    })?);
                }
                _ => {
                    map.next_value::<Skip>()?;
                }
            }
        }
        Ok(WireFields {
            version: required(version, "version")?,
            event_id: required(event_id, "event_id")?,
            timestamp_ms: required(timestamp_ms, "timestamp_ms")?,
            headers_json: required(headers_json, "headers_json")?,
            ordering_key: ordering_key.unwrap_or(None),
            content_type: required(content_type, "content_type")?,
            schema_id: schema_id.unwrap_or(None),
            payload: required(payload, "payload")?,
        })
    }
}

/// Rejects duplicate fields without exposing input text.
///
/// # Type Parameters
///
/// - `T`: Stored field value type.
/// - `E`: Deserializer error type.
///
/// # Parameters
///
/// - `value`: Current value for the field, if already encountered.
/// - `field`: Static field name used in serde's duplicate-field error.
///
/// # Returns
///
/// Success when the field has not already appeared.
///
/// # Errors
///
/// Returns a duplicate-field error when a prior value exists.
fn duplicate<T, E: de::Error>(value: &Option<T>, field: &'static str) -> Result<(), E> {
    if value.is_some() {
        Err(E::duplicate_field(field))
    } else {
        Ok(())
    }
}
/// Extracts one required field, preserving serde's missing-field diagnostics
/// internally.
///
/// # Type Parameters
///
/// - `T`: Required field value type.
/// - `E`: Deserializer error type.
///
/// # Parameters
///
/// - `value`: Parsed field value, if present.
/// - `field`: Static field name used in serde's missing-field error.
///
/// # Returns
///
/// The parsed field value when present.
///
/// # Errors
///
/// Returns a missing-field error when no value was parsed.
fn required<T, E: de::Error>(value: Option<T>, field: &'static str) -> Result<T, E> {
    value.ok_or_else(|| E::missing_field(field))
}
