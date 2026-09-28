// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Stable reason recorded for malformed stream entries.

/// Stable reason stored beside malformed wire data without exposing it in
/// errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PoisonReason {
    /// Stream entry does not contain the `wire` field.
    MissingWire,
    /// Stream entry's `wire` value is not a byte string.
    InvalidWireField,
    /// The wire field is not valid JSON.
    InvalidJson,
    /// A decoded field cannot construct the required event metadata.
    InvalidEventMetadata,
}

impl PoisonReason {
    /// Returns the stable, secret-free reason stored in the quarantine stream.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::MissingWire => "missing_wire",
            Self::InvalidWireField => "invalid_wire_field",
            Self::InvalidJson => "invalid_json",
            Self::InvalidEventMetadata => "invalid_event_metadata",
        }
    }
}
