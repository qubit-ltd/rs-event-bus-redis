// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Two-pass version selection and bounded typed wire construction, without a
//! JSON AST.

use std::cell::Cell;
use std::fmt;

use serde::Deserialize;
use serde::Deserializer;
use serde::de;
use serde::de::DeserializeSeed;
use serde::de::MapAccess;
use serde::de::SeqAccess;
use serde::de::Visitor;

use crate::error::RedisProviderError;
use crate::wire_fields::WireFields;
use crate::wire_limits::WireLimits;

/// Parses version first, preserving future records without interpreting their
/// layout. Returns a sanitized malformed/version/limit error. Limits do not cap
/// the client's RESP frame allocation.
pub(crate) fn decode(encoded: &[u8], limits: WireLimits) -> Result<WireFields, RedisProviderError> {
    if encoded.len() > limits.wire {
        return Err(RedisProviderError::LimitExceeded);
    }
    let malformed = || RedisProviderError::Operation("decode wire JSON");
    let mut version_parser = serde_json::Deserializer::from_slice(encoded);
    let version = version_parser
        .deserialize_map(VersionVisitor)
        .map_err(|_| malformed())?;
    version_parser.end().map_err(|_| malformed())?;
    if version != 1 {
        return Err(RedisProviderError::UnsupportedWireVersion);
    }
    let exceeded = Cell::new(false);
    let mut fields_parser = serde_json::Deserializer::from_slice(encoded);
    let result = FieldsSeed {
        limits,
        exceeded: &exceeded,
    }
    .deserialize(&mut fields_parser);
    let fields = result.map_err(|_| {
        if exceeded.get() {
            RedisProviderError::LimitExceeded
        } else {
            malformed()
        }
    })?;
    fields_parser.end().map_err(|_| malformed())?;
    Ok(fields)
}

/// Discards values recursively through serde's visitor entry points, retaining
/// its depth protection. serde_json's IgnoredAny bypasses that protection, so
/// nested values use this sink instead.
struct Skip;
impl<'de> Deserialize<'de> for Skip {
    /// Consumes one value without retaining it; malformed/deep input stays an
    /// error.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(SkipVisitor)
    }
}
struct SkipVisitor;
impl<'de> Visitor<'de> for SkipVisitor {
    type Value = Skip;
    /// Describes any valid JSON value.
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON value")
    }
    /// Drops a boolean.
    fn visit_bool<E: de::Error>(self, _: bool) -> Result<Skip, E> {
        Ok(Skip)
    }
    /// Drops a signed number.
    fn visit_i64<E: de::Error>(self, _: i64) -> Result<Skip, E> {
        Ok(Skip)
    }
    /// Drops an unsigned number.
    fn visit_u64<E: de::Error>(self, _: u64) -> Result<Skip, E> {
        Ok(Skip)
    }
    /// Drops a floating point number.
    fn visit_f64<E: de::Error>(self, _: f64) -> Result<Skip, E> {
        Ok(Skip)
    }
    /// Drops a decoded string without copying it.
    fn visit_str<E: de::Error>(self, _: &str) -> Result<Skip, E> {
        Ok(Skip)
    }
    /// Drops null.
    fn visit_unit<E: de::Error>(self) -> Result<Skip, E> {
        Ok(Skip)
    }
    /// Recursively consumes array elements without an intermediate tree.
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Skip, A::Error> {
        while seq.next_element::<Skip>()?.is_some() {}
        Ok(Skip)
    }
    /// Recursively consumes object values without retaining keys or values.
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Skip, A::Error> {
        while map.next_key::<de::IgnoredAny>()?.is_some() {
            map.next_value::<Skip>()?;
        }
        Ok(Skip)
    }
}

struct VersionVisitor;
impl<'de> Visitor<'de> for VersionVisitor {
    type Value = u64;
    /// Requires the versioned top-level object.
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a versioned wire object")
    }
    /// Extracts exactly one numeric version while validating the remaining
    /// JSON.
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

struct FieldsSeed<'a> {
    limits: WireLimits,
    exceeded: &'a Cell<bool>,
}
impl<'de> DeserializeSeed<'de> for FieldsSeed<'_> {
    type Value = WireFields;
    /// Constructs fields directly while sharing a typed limit-failure marker.
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<WireFields, D::Error> {
        deserializer.deserialize_map(FieldsVisitor(self))
    }
}
struct FieldsVisitor<'a>(FieldsSeed<'a>);
impl<'de> Visitor<'de> for FieldsVisitor<'_> {
    type Value = WireFields;
    /// Describes the supported version-one field layout.
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("version 1 wire fields")
    }
    /// Validates duplicate/required fields and bounds decoded components during
    /// construction.
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
fn duplicate<T, E: de::Error>(value: &Option<T>, field: &'static str) -> Result<(), E> {
    if value.is_some() {
        Err(E::duplicate_field(field))
    } else {
        Ok(())
    }
}
/// Extracts one required field, preserving serde's missing-field diagnostics
/// internally.
fn required<T, E: de::Error>(value: Option<T>, field: &'static str) -> Result<T, E> {
    value.ok_or_else(|| E::missing_field(field))
}
struct StringSeed<'a> {
    limit: usize,
    exceeded: &'a Cell<bool>,
}
impl<'de> DeserializeSeed<'de> for StringSeed<'_> {
    type Value = String;
    /// Bounds decoded UTF-8 bytes before allocating the owned header string.
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<String, D::Error> {
        deserializer.deserialize_str(StringVisitor(self))
    }
}
struct StringVisitor<'a>(StringSeed<'a>);
impl<'de> Visitor<'de> for StringVisitor<'_> {
    type Value = String;
    /// Requires a JSON string.
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a bounded headers string")
    }
    /// Checks unescaped byte length before cloning the parser's string view.
    fn visit_str<E: de::Error>(self, value: &str) -> Result<String, E> {
        if value.len() > self.0.limit {
            self.0.exceeded.set(true);
            return Err(E::custom("headers limit"));
        }
        Ok(value.to_owned())
    }
}
struct PayloadSeed<'a> {
    limit: usize,
    exceeded: &'a Cell<bool>,
}
impl<'de> DeserializeSeed<'de> for PayloadSeed<'_> {
    type Value = Vec<u8>;
    /// Reads integer bytes directly, rejecting over-limit lengths before
    /// pushing.
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Vec<u8>, D::Error> {
        deserializer.deserialize_seq(PayloadVisitor(self))
    }
}
struct PayloadVisitor<'a>(PayloadSeed<'a>);
impl<'de> Visitor<'de> for PayloadVisitor<'_> {
    type Value = Vec<u8>;
    /// Requires the version-one byte array representation.
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a bounded byte array")
    }
    /// Rejects excessive hints and checks every push before extending the
    /// output vector.
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
