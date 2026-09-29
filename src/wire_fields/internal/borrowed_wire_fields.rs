// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Borrowed version 1 serialization without copying caller-owned metadata.

use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::OutboundMessage;
use serde::Serialize;

/// Version 1 field order and byte-array payload using borrowed event
/// components.
///
/// # Type Parameters
///
/// - `'a`: Shared lifetime of the outbound metadata, encoded bytes, and bounded
///   header JSON.
#[derive(Serialize)]
pub(super) struct BorrowedWireFields<'a> {
    /// Fixed supported protocol version.
    version: u32,
    /// Caller-owned stable event identifier.
    event_id: &'a str,
    /// Validated milliseconds since the Unix epoch.
    timestamp_ms: u128,
    /// Serialized headers already bounded by the wire sink.
    headers_json: &'a str,
    /// Caller-owned optional ordering metadata.
    ordering_key: Option<&'a str>,
    /// Caller-owned content type whose length has no facade cap.
    content_type: &'a str,
    /// Caller-owned optional schema identifier.
    schema_id: Option<&'a str>,
    /// Encoded caller-owned bytes already checked against the payload budget.
    payload: &'a [u8],
}

impl<'a> BorrowedWireFields<'a> {
    /// Borrows event fields for bounded serialization without additional
    /// copies.
    ///
    /// # Type Parameters
    ///
    /// - `'a`: Lifetime for all borrowed strings and bytes retained until
    ///   serialization completes.
    ///
    /// # Parameters
    ///
    /// - `message`: Event supplying the identifier and optional ordering key.
    /// - `payload`: Validated encoded payload and its content/schema metadata.
    /// - `headers_json`: Headers serialized within the configured wire budget.
    /// - `timestamp_ms`: Timestamp already validated against the Unix epoch.
    ///
    /// # Returns
    ///
    /// A version 1 representation borrowing all strings and payload bytes. No
    /// allocation, Redis command, or other external I/O is performed.
    #[must_use]
    #[inline]
    pub(super) fn new(
        message: &'a OutboundMessage,
        payload: &'a EncodedPayload,
        headers_json: &'a str,
        timestamp_ms: u128,
    ) -> Self {
        Self {
            version: 1,
            event_id: message.id().as_str(),
            timestamp_ms,
            headers_json,
            ordering_key: message.ordering_key().map(|key| key.as_str()),
            content_type: payload.content_type().as_str(),
            schema_id: payload.schema_id().map(|schema| schema.as_str()),
            payload: payload.bytes(),
        }
    }
}

#[cfg(test)]
#[path = "../../tests/wire_fields/internal/borrowed_wire_fields_tests.rs"]
mod tests;
