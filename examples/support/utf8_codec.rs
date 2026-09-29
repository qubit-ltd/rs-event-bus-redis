// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! A small UTF-8 codec used by the facade examples.

use std::sync::Arc;

use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::CodecError;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::spi::EncodedPayload;

/// Encodes owned strings as UTF-8 bytes.
pub(crate) struct Utf8Codec(pub(crate) ContentType);

impl EventCodec<String> for Utf8Codec {
    /// Returns the stored UTF-8 content type without allocation or I/O.
    fn content_type(&self) -> &ContentType {
        &self.0
    }

    /// Returns `None`, because this example codec has no schema identifier.
    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }

    /// Copies UTF-8 bytes from `value`; returns the encoded bytes and never
    /// errors.
    fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from(value.as_bytes()))
    }

    /// Decodes `bytes` as UTF-8; returns the owned string or a contextual codec
    /// error for invalid UTF-8, without performing I/O.
    fn decode(&self, payload: &EncodedPayload) -> Result<String, CodecError> {
        String::from_utf8(payload.bytes().to_vec()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}
