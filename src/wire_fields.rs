// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Versioned byte-safe Redis stream message fields.

use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::OrderingKey;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use serde::Deserialize;
use serde::Serialize;

use crate::error::RedisProviderError;

/// Decoded transport components returned by [`WireFields::into_parts`].
type WireMessageParts = (
    TopicAddress,
    EventId,
    SystemTime,
    Headers,
    Option<OrderingKey>,
    TransportPayload,
);

/// Versioned event fields stored in one Redis Stream record.
///
/// The payload remains a byte vector, while headers are represented as JSON so
/// the record can be round-tripped independently of a Rust process. Version 1
/// carries encoded payloads and treats an unknown version as a protocol error.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use std::time::SystemTime;
///
/// use qubit_event_bus::model::ContentType;
/// use qubit_event_bus::model::EventId;
/// use qubit_event_bus::model::Headers;
/// use qubit_event_bus::spi::EncodedPayload;
/// use qubit_event_bus::spi::OutboundMessage;
/// use qubit_event_bus::spi::TopicAddress;
/// use qubit_event_bus::spi::TransportPayload;
/// use qubit_event_bus_redis::wire::WireFields;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let message = OutboundMessage::new(
///     TopicAddress::new("orders.created")?,
///     EventId::new("evt-42")?,
///     SystemTime::UNIX_EPOCH,
///     Headers::new(),
///     None,
///     None,
///     TransportPayload::Encoded(EncodedPayload::new(
///         Arc::from(&b"order-42"[..]),
///         ContentType::new("text/plain")?,
///         None,
///     )),
/// );
/// let fields = WireFields::from_outbound(&message)?;
/// assert_eq!(fields.version, 1);
/// assert_eq!(fields.payload, b"order-42");
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WireFields {
    /// Protocol version used to select the field decoder.
    pub version: u32,
    /// Stable event identifier preserved across transport boundaries.
    pub event_id: String,
    /// Event timestamp as whole milliseconds since the Unix epoch.
    pub timestamp_ms: u128,
    /// Serialized string-valued event headers.
    pub headers_json: String,
    /// Optional ordering metadata; Redis does not enforce ordering by this key.
    pub ordering_key: Option<String>,
    /// MIME-like content type used by the facade codec registry.
    pub content_type: String,
    /// Optional schema identifier supplied by the encoder.
    pub schema_id: Option<String>,
    /// Encoded payload bytes, serialized as JSON integer values.
    pub payload: Vec<u8>,
}

impl WireFields {
    /// Parses the protocol version before applying the version 1 field layout.
    ///
    /// # Parameters
    ///
    /// - `encoded`: JSON wire value stored in the Redis stream.
    ///
    /// # Returns
    ///
    /// The decoded version 1 fields.
    ///
    /// # Errors
    ///
    /// Returns `UnsupportedWireVersion` for a valid numeric version other than
    /// 1 and an operation error for malformed JSON or version 1 fields.
    #[cfg(any(feature = "sync", feature = "async"))]
    pub(crate) fn decode_wire(
        encoded: &[u8],
        limits: crate::wire_limits::WireLimits,
    ) -> Result<Self, RedisProviderError> {
        crate::bounded_wire_decoder::decode(encoded, limits)
    }

    /// Converts an outbound event into the version 1 Redis wire representation.
    ///
    /// Native in-process payloads cannot be persisted and are rejected. The
    /// timestamp is clamped to the Unix epoch when it predates that epoch.
    ///
    /// # Parameters
    ///
    /// - `message`: Event whose encoded payload and metadata should be stored.
    ///
    /// # Returns
    ///
    /// Wire fields containing a copy of the payload bytes and event metadata.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for a native payload, a resource-limit
    /// error before copying an oversized payload or producing oversized
    /// headers, or an operation error if headers cannot be serialized.
    pub fn from_outbound(message: &OutboundMessage) -> Result<Self, RedisProviderError> {
        Self::from_outbound_with_limits(message, crate::wire_limits::WireLimits::default())
    }

    /// Builds wire fields, checking the payload before copying and capping
    /// headers output. Returns a sanitized limit/configuration/encoding
    /// error before Redis IO.
    pub(crate) fn from_outbound_with_limits(
        message: &OutboundMessage,
        limits: crate::wire_limits::WireLimits,
    ) -> Result<Self, RedisProviderError> {
        let TransportPayload::Encoded(payload) = message.payload() else {
            return Err(RedisProviderError::Configuration("encoded payload required"));
        };
        if payload.bytes().len() > limits.payload {
            return Err(RedisProviderError::LimitExceeded);
        }
        let timestamp_ms = message
            .timestamp()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| RedisProviderError::Configuration("timestamp precedes the Unix epoch"))?
            .as_millis();
        Ok(Self {
            version: 1,
            event_id: message.id().as_str().to_owned(),
            timestamp_ms,
            headers_json: crate::capped_writer::to_capped_json(message.headers(), limits.headers)?,
            ordering_key: message.ordering_key().map(|key| key.as_str().to_owned()),
            content_type: payload.content_type().as_str().to_owned(),
            schema_id: payload.schema_id().map(|schema| schema.as_str().to_owned()),
            payload: payload.bytes().to_vec(),
        })
    }

    /// Decodes version 1 fields into the typed components used by a stream
    /// receiver.
    ///
    /// The topic is supplied by the consumer group because it is not duplicated
    /// in each record. Invalid identifiers, headers, content types, and schema
    /// identifiers fail decoding; timestamps are interpreted as milliseconds
    /// after the Unix epoch and must fit the platform's `SystemTime` range.
    /// Invalid optional ordering keys fail decoding instead of being discarded.
    ///
    /// # Parameters
    ///
    /// - `topic`: Typed topic associated with the Redis stream being consumed.
    ///
    /// # Returns
    ///
    /// Topic, event ID, timestamp, headers, optional ordering key, and encoded
    /// payload in transport order.
    ///
    /// # Errors
    ///
    /// Returns `UnsupportedWireVersion` for unknown versions and an operation
    /// error when a stored field is malformed.
    pub fn into_parts(self, topic: TopicAddress) -> Result<WireMessageParts, RedisProviderError> {
        if self.version != 1 {
            return Err(RedisProviderError::UnsupportedWireVersion);
        }
        let id = EventId::new(self.event_id.as_str()).map_err(|_| RedisProviderError::Operation("decode event ID"))?;
        let timestamp_ms = u64::try_from(self.timestamp_ms)
            .map_err(|_| RedisProviderError::Configuration("timestamp_ms exceeds the supported range"))?;
        let timestamp =
            UNIX_EPOCH
                .checked_add(Duration::from_millis(timestamp_ms))
                .ok_or(RedisProviderError::Configuration(
                    "timestamp_ms exceeds the platform range",
                ))?;
        let headers =
            serde_json::from_str(&self.headers_json).map_err(|_| RedisProviderError::Operation("decode headers"))?;
        let content_type = ContentType::new(self.content_type.as_str())
            .map_err(|_| RedisProviderError::Operation("decode content type"))?;
        let schema_id = self
            .schema_id
            .as_deref()
            .map(SchemaId::new)
            .transpose()
            .map_err(|_| RedisProviderError::Operation("decode schema ID"))?;
        let payload = EncodedPayload::new(Arc::from(self.payload), content_type, schema_id);
        let ordering_key = match self.ordering_key.as_deref() {
            Some(value) => {
                Some(OrderingKey::new(value).ok_or(RedisProviderError::Configuration("invalid ordering key"))?)
            }
            None => None,
        };
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
