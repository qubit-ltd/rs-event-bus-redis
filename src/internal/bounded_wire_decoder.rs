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

use serde::Deserializer;
use serde::de::DeserializeSeed;

/// Deserializes the bounded version-one wire fields.
#[path = "bounded_wire_decoder/fields_seed.rs"]
mod fields_seed;
/// Validates and constructs the version-one wire field map.
#[path = "bounded_wire_decoder/fields_visitor.rs"]
mod fields_visitor;
/// Deserializes bounded wire payload bytes.
#[path = "bounded_wire_decoder/payload_seed.rs"]
mod payload_seed;
/// Validates bounded wire payload bytes.
#[path = "bounded_wire_decoder/payload_visitor.rs"]
mod payload_visitor;
/// Discards JSON values while retaining serde's recursion protection.
#[path = "bounded_wire_decoder/skip.rs"]
mod skip;
/// Recursively consumes JSON values without building an intermediate tree.
#[path = "bounded_wire_decoder/skip_visitor.rs"]
mod skip_visitor;
#[path = "bounded_wire_decoder/string_seed.rs"]
mod string_seed;
/// Validates bounded wire header strings.
#[path = "bounded_wire_decoder/string_visitor.rs"]
mod string_visitor;
/// Selects the wire version without interpreting version-specific fields.
#[path = "bounded_wire_decoder/version_visitor.rs"]
mod version_visitor;

use self::fields_seed::FieldsSeed;
use self::version_visitor::VersionVisitor;
use crate::error::RedisProviderError;
use crate::wire_fields::WireFields;
use crate::wire_limits::WireLimits;

/// Parses version first, preserving future records without interpreting their
/// layout. Limits do not cap the client's RESP frame allocation.
///
/// # Parameters
///
/// - `encoded`: Serialized wire record to validate and decode.
/// - `limits`: Maximum wire, header, and payload sizes.
///
/// # Returns
///
/// The validated version-one wire fields.
///
/// # Errors
///
/// Returns a sanitized malformed JSON, unsupported version, or limit error.
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
