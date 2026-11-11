// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Versioned byte-safe Redis stream message fields.

mod internal;

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
use serde_json::from_str;
use serde_json::to_string;

#[cfg(all(test, any(feature = "sync", feature = "async")))]
thread_local! {
    static ENCODE_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(all(test, any(feature = "sync", feature = "async")))]
type EncodeGateRelease = std::sync::Arc<std::sync::Mutex<std::sync::mpsc::Receiver<()>>>;

#[cfg(all(test, any(feature = "sync", feature = "async")))]
type EncodeGate = Option<(
    String,
    std::sync::mpsc::Sender<()>,
    EncodeGateRelease,
    std::sync::Arc<std::sync::atomic::AtomicBool>,
)>;

#[cfg(all(test, any(feature = "sync", feature = "async")))]
static ENCODE_GATE: std::sync::OnceLock<std::sync::Mutex<EncodeGate>> = std::sync::OnceLock::new();

#[cfg(all(test, any(feature = "sync", feature = "async")))]
static ENCODE_GATE_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(all(test, any(feature = "sync", feature = "async")))]
pub(crate) fn reset_encode_count() {
    ENCODE_COUNT.set(0);
}

#[cfg(all(test, any(feature = "sync", feature = "async")))]
pub(crate) fn encode_count() -> usize {
    ENCODE_COUNT.get()
}

#[cfg(all(test, any(feature = "sync", feature = "async")))]
pub(crate) fn install_encode_gate(
    event_id: &str,
    entered: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
) {
    *ENCODE_GATE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("encoding gate lock") = Some((
        event_id.to_owned(),
        entered,
        std::sync::Arc::new(std::sync::Mutex::new(release)),
        std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
    ));
}

#[cfg(all(test, any(feature = "sync", feature = "async")))]
pub(crate) fn clear_encode_gate() {
    *ENCODE_GATE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("encoding gate lock") = None;
}

#[cfg(all(test, any(feature = "sync", feature = "async")))]
pub(crate) fn encode_gate_test_lock() -> std::sync::MutexGuard<'static, ()> {
    ENCODE_GATE_TEST_LOCK
        .lock()
        .expect("encoding gate test lock")
}

#[cfg(all(test, any(feature = "sync", feature = "async")))]
fn wait_at_encode_gate(message: &OutboundMessage) {
    let gate = ENCODE_GATE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("encoding gate lock")
        .as_ref()
        .filter(|(event_id, _, _, used)| {
            event_id == message.id().as_str()
                && !used.swap(true, std::sync::atomic::Ordering::AcqRel)
        })
        .map(|(_, entered, release, _)| (entered.clone(), std::sync::Arc::clone(release)));
    if let Some((entered, release)) = gate {
        let _ = entered.send(());
        let _ = release
            .lock()
            .expect("encoding gate release lock")
            .recv_timeout(std::time::Duration::from_secs(10));
    }
}

#[cfg(any(feature = "sync", feature = "async"))]
use self::internal::BorrowedWireFields;
#[cfg(any(feature = "sync", feature = "async"))]
use self::internal::BoundedWriter;
use crate::error::RedisProviderError;
#[cfg(any(feature = "sync", feature = "async"))]
use crate::internal::WireLimits;

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
/// This is the concrete Rust layout for version 1 Redis stream fields. The
/// payload is a byte vector and headers are represented as JSON. Its derived
/// [`Deserialize`] implementation requires this layout directly; it does not
/// perform the provider's version-first classification or enforce configured
/// wire, payload, or header size limits. The provider's internal decoder first
/// validates JSON and reads the version, then applies bounded version 1 field
/// decoding. A complete version 1-shaped value with an unknown version can be
/// deserialized as this type, but [`WireFields::into_parts`] rejects it. An
/// arbitrary future layout need not deserialize as `WireFields` at all.
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
    /// Converts an outbound event into the version 1 Redis wire representation.
    ///
    /// Native in-process payloads cannot be persisted and are rejected. The
    /// timestamp must be at or after the Unix epoch.
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
    /// Returns a configuration error for a native payload or a timestamp before
    /// the Unix epoch, and an operation error if headers cannot be serialized.
    /// This public compatibility constructor does not enforce provider size
    /// budgets.
    pub fn from_outbound(message: &OutboundMessage) -> Result<Self, RedisProviderError> {
        let TransportPayload::Encoded(payload) = message.payload() else {
            return Err(RedisProviderError::Configuration(
                "encoded payload required",
            ));
        };
        let timestamp_ms = message
            .timestamp()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| RedisProviderError::Configuration("timestamp precedes the Unix epoch"))?
            .as_millis();
        Ok(Self {
            version: 1,
            event_id: message.id().as_str().to_owned(),
            timestamp_ms,
            headers_json: to_string(message.headers())
                .map_err(|_| RedisProviderError::Operation("encode headers"))?,
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
    /// payload in transport order. The ordering key is `Some` only when
    /// supplied; `None` carries no ordering metadata. Missing schema
    /// metadata stays absent.
    ///
    /// # Errors
    ///
    /// Returns `UnsupportedWireVersion` for unknown versions, a configuration
    /// error for timestamps outside u64/platform range or an invalid ordering
    /// key, and an operation error for malformed event ID, headers, content
    /// type, or schema ID.
    pub fn into_parts(self, topic: TopicAddress) -> Result<WireMessageParts, RedisProviderError> {
        if self.version != 1 {
            return Err(RedisProviderError::UnsupportedWireVersion);
        }
        let id = EventId::new(self.event_id.as_str())
            .map_err(|_| RedisProviderError::Operation("decode event ID"))?;
        let timestamp_ms = u64::try_from(self.timestamp_ms).map_err(|_| {
            RedisProviderError::Configuration("timestamp_ms exceeds the supported range")
        })?;
        let timestamp = UNIX_EPOCH
            .checked_add(Duration::from_millis(timestamp_ms))
            .ok_or(RedisProviderError::Configuration(
                "timestamp_ms exceeds the platform range",
            ))?;
        let headers = from_str(&self.headers_json)
            .map_err(|_| RedisProviderError::Operation("decode headers"))?;
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
            Some(value) => Some(
                OrderingKey::new(value)
                    .ok_or(RedisProviderError::Configuration("invalid ordering key"))?,
            ),
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

    /// Parses the protocol version before applying the version 1 field layout.
    ///
    /// # Parameters
    ///
    /// - `encoded`: JSON wire value stored in the Redis stream.
    /// - `limits`: Inclusive complete wire and decoded payload byte budgets.
    ///
    /// # Returns
    ///
    /// The decoded version 1 fields.
    ///
    /// # Errors
    ///
    /// Returns `UnsupportedWireVersion` only after the complete JSON document
    /// and Serde's recursion guard have passed for a numeric version other
    /// than 1. A size error is returned when wire/payload bytes exceed their
    /// budget, and an operation error for malformed JSON or version 1 fields.
    /// Version 1 field validation runs only after version selection.
    /// Decoding performs no external I/O.
    #[cfg(any(feature = "sync", feature = "async"))]
    pub(crate) fn decode_wire(
        encoded: &str,
        limits: WireLimits,
    ) -> Result<Self, RedisProviderError> {
        crate::bounded_wire_decoder::decode(encoded.as_bytes(), limits)
    }
}

/// Encodes a provider message within validated payload and wire budgets.
///
/// # Parameters
///
/// - `message`: Outbound message containing an encoded payload.
/// - `limits`: Inclusive budgets copied from validated provider settings.
///
/// # Returns
///
/// The complete version 1 JSON wire string.
///
/// # Errors
///
/// Returns `PayloadTooLarge` before processing oversized payload bytes,
/// `WireTooLarge` as soon as headers or the final representation exceed their
/// budget, or a stable configuration/serialization error for invalid metadata.
/// All caller-owned metadata and payload bytes are borrowed; only the bounded
/// header and complete wire strings are allocated. No Redis command or other
/// external I/O is performed.
#[cfg(any(feature = "sync", feature = "async"))]
pub(crate) fn encode_bounded(
    message: &OutboundMessage,
    limits: WireLimits,
) -> Result<String, RedisProviderError> {
    #[cfg(all(test, any(feature = "sync", feature = "async")))]
    ENCODE_COUNT.set(ENCODE_COUNT.get() + 1);
    #[cfg(all(test, any(feature = "sync", feature = "async")))]
    wait_at_encode_gate(message);
    let TransportPayload::Encoded(payload) = message.payload() else {
        return Err(RedisProviderError::Configuration(
            "encoded payload required",
        ));
    };
    limits.check_payload(payload.bytes().len())?;
    let headers_json =
        BoundedWriter::new(limits.headers.min(limits.wire)).serialize(message.headers())?;
    let timestamp_ms = message
        .timestamp()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RedisProviderError::Configuration("timestamp precedes the Unix epoch"))?
        .as_millis();
    let fields = BorrowedWireFields::new(message, payload, &headers_json, timestamp_ms);
    BoundedWriter::new(limits.wire).serialize(&fields)
}

/// Checks the encoded-payload variant and byte length without wire encoding.
///
/// This keeps cheap input validation ahead of command admission while leaving
/// all header and complete-wire checks to `encode_bounded`.
///
/// # Parameters
///
/// - `message`: Outbound event whose payload variant and raw length are
///   checked.
/// - `limits`: Provider payload limit applied before wire encoding.
///
/// # Errors
///
/// Returns an error when the message is not encoded or its payload exceeds the
/// configured limit.
#[cfg(any(feature = "sync", feature = "async"))]
pub(crate) fn check_publish_payload(
    message: &OutboundMessage,
    limits: WireLimits,
) -> Result<(), RedisProviderError> {
    let TransportPayload::Encoded(payload) = message.payload() else {
        return Err(RedisProviderError::Configuration(
            "encoded payload required",
        ));
    };
    limits.check_payload(payload.bytes().len())
}
