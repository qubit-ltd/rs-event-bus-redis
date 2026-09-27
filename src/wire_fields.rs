// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Versioned byte-safe Redis stream message fields.

use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use qubit_event_bus::model::EventId;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use serde::Deserialize;
use serde::Serialize;

use crate::error::RedisProviderError;

type WireMessageParts = (
    TopicAddress,
    EventId,
    SystemTime,
    qubit_event_bus::model::Headers,
    Option<qubit_event_bus::spi::OrderingKey>,
    TransportPayload,
);

/// Fields encoded into one Redis Stream record.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WireFields {
    /// Current wire version.
    pub version: u32,
    /// Event identifier.
    pub event_id: String,
    /// Milliseconds since Unix epoch.
    pub timestamp_ms: u128,
    /// String headers encoded as JSON.
    pub headers_json: String,
    /// Optional ordering key.
    pub ordering_key: Option<String>,
    /// Content type string.
    pub content_type: String,
    /// Optional schema identifier.
    pub schema_id: Option<String>,
    /// Payload bytes encoded using base64-free JSON integer arrays.
    pub payload: Vec<u8>,
}

impl WireFields {
    /// Encodes an event, requiring a portable encoded payload.
    pub fn from_outbound(message: &OutboundMessage) -> Result<Self, RedisProviderError> {
        let TransportPayload::Encoded(payload) = message.payload() else {
            return Err(RedisProviderError::Configuration("encoded payload required"));
        };
        let timestamp_ms = message
            .timestamp()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        Ok(Self {
            version: 1,
            event_id: message.id().as_str().to_owned(),
            timestamp_ms,
            headers_json: serde_json::to_string(message.headers())
                .map_err(|_| RedisProviderError::Operation("encode headers"))?,
            ordering_key: message.ordering_key().map(|key| key.as_str().to_owned()),
            content_type: payload.content_type().as_str().to_owned(),
            schema_id: payload.schema_id().map(|schema| schema.as_str().to_owned()),
            payload: payload.bytes().to_vec(),
        })
    }

    /// Converts stored fields back to a typed stream message.
    pub fn into_parts(self, topic: TopicAddress) -> Result<WireMessageParts, RedisProviderError> {
        if self.version != 1 {
            return Err(RedisProviderError::UnsupportedWireVersion);
        }
        let id = EventId::new(self.event_id.as_str()).map_err(|_| RedisProviderError::Operation("decode event ID"))?;
        let timestamp = UNIX_EPOCH + std::time::Duration::from_millis(self.timestamp_ms as u64);
        let headers =
            serde_json::from_str(&self.headers_json).map_err(|_| RedisProviderError::Operation("decode headers"))?;
        let content_type = qubit_event_bus::model::ContentType::new(self.content_type.as_str())
            .map_err(|_| RedisProviderError::Operation("decode content type"))?;
        let schema_id = self
            .schema_id
            .as_deref()
            .map(qubit_event_bus::model::SchemaId::new)
            .transpose()
            .map_err(|_| RedisProviderError::Operation("decode schema ID"))?;
        let payload = EncodedPayload::new(Arc::from(self.payload), content_type, schema_id);
        let ordering_key = self
            .ordering_key
            .as_deref()
            .and_then(qubit_event_bus::spi::OrderingKey::new);
        Ok((
            topic,
            id,
            timestamp,
            headers,
            ordering_key,
            TransportPayload::Encoded(payload),
        ))
    }
}
