// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Deterministic properties of persisted version 1 payloads and metadata.

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::OrderingKey;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_event_bus_redis::wire::WireFields;
use serde_json::from_slice;
use serde_json::to_vec;

use crate::message;

#[test]
fn test_binary_payload_json_round_trip_preserves_every_byte() {
    for length in [0, 1, 2, 7, 127, 256, 1024] {
        let bytes: Vec<u8> = (0..length).map(|index| (index % 256) as u8).collect();
        let fields = WireFields::from_outbound(&message(&bytes, Headers::new())).unwrap();
        let encoded = to_vec(&fields).unwrap();
        let decoded: WireFields = from_slice(&encoded).unwrap();
        let (_, _, _, _, _, TransportPayload::Encoded(payload)) =
            decoded.into_parts(TopicAddress::new("events").unwrap()).unwrap()
        else {
            panic!("encoded payload must stay encoded");
        };
        assert_eq!(payload.bytes(), bytes, "round trip length {length}");
    }
}

#[test]
fn test_invalid_event_identifiers_are_never_accepted_from_wire() {
    for id in ["", " ", "bad\nid", "bad\0id"] {
        let mut fields = WireFields::from_outbound(&message(&[], Headers::new())).unwrap();
        fields.event_id = id.into();
        assert!(
            fields.into_parts(TopicAddress::new("events").unwrap()).is_err(),
            "invalid ID {id:?}"
        );
    }
}

#[test]
fn test_encoded_payload_round_trips_binary_bytes() -> Result<(), Box<dyn Error>> {
    let original = vec![0, 1, 127, 128, 255];
    let message = OutboundMessage::new(
        TopicAddress::new("events")?,
        EventId::new("event-1")?,
        SystemTime::now(),
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(original.clone()),
            ContentType::new("application/octet-stream")?,
            None,
        )),
    );
    let fields = WireFields::from_outbound(&message)?;
    let (_, _, _, _, _, TransportPayload::Encoded(payload)) = fields.into_parts(TopicAddress::new("events")?)? else {
        panic!("wire decoder must preserve encoded payloads");
    };
    assert_eq!(payload.bytes(), original);
    Ok(())
}

#[test]
fn test_wire_fields_preserve_optional_metadata_and_timestamp() -> Result<(), Box<dyn Error>> {
    let message = OutboundMessage::new(
        TopicAddress::new("events")?,
        EventId::new("event-before-epoch")?,
        SystemTime::UNIX_EPOCH + Duration::from_secs(1),
        Headers::new(),
        Some(OrderingKey::new("partition-1").ok_or("valid ordering key was rejected")?),
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(&b"payload"[..]),
            ContentType::new("application/octet-stream")?,
            Some(SchemaId::new("schema-v1")?),
        )),
    );

    let fields = WireFields::from_outbound(&message)?;
    assert_eq!(fields.timestamp_ms, 1_000);
    assert_eq!(fields.ordering_key.as_deref(), Some("partition-1"));
    assert_eq!(fields.schema_id.as_deref(), Some("schema-v1"));

    let (topic, event_id, timestamp, _, ordering_key, TransportPayload::Encoded(payload)) =
        fields.into_parts(TopicAddress::new("events")?)?
    else {
        panic!("wire decoder must preserve encoded payloads");
    };
    assert_eq!(topic.as_str(), "events");
    assert_eq!(event_id.as_str(), "event-before-epoch");
    assert_eq!(timestamp, SystemTime::UNIX_EPOCH + Duration::from_secs(1));
    assert_eq!(
        ordering_key.map(|key| key.as_str().to_owned()).as_deref(),
        Some("partition-1")
    );
    assert_eq!(payload.schema_id().map(|schema| schema.as_str()), Some("schema-v1"));
    Ok(())
}

#[test]
fn test_wire_encoder_rejects_pre_epoch_timestamps() -> Result<(), Box<dyn Error>> {
    let message = OutboundMessage::new(
        TopicAddress::new("events")?,
        EventId::new("event-before-epoch")?,
        SystemTime::UNIX_EPOCH - Duration::from_secs(1),
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(&b"payload"[..]),
            ContentType::new("application/octet-stream")?,
            None,
        )),
    );
    assert!(WireFields::from_outbound(&message).is_err());
    Ok(())
}

#[test]
fn test_unknown_wire_version_is_rejected() {
    let fields = WireFields {
        version: 999,
        event_id: "event-1".into(),
        timestamp_ms: 0,
        headers_json: "{}".into(),
        ordering_key: None,
        content_type: "application/octet-stream".into(),
        schema_id: None,
        payload: Vec::new(),
    };
    assert!(
        fields
            .into_parts(TopicAddress::new("events").expect("valid topic"))
            .is_err()
    );
}

#[test]
fn test_wire_decoder_rejects_malformed_fields() -> Result<(), Box<dyn Error>> {
    let valid = WireFields {
        version: 1,
        event_id: "valid-id".into(),
        timestamp_ms: 0,
        headers_json: "{}".into(),
        ordering_key: None,
        content_type: "application/octet-stream".into(),
        schema_id: None,
        payload: vec![],
    };
    let topic = || TopicAddress::new("events").unwrap();
    let mut invalid_id = valid.clone();
    invalid_id.event_id.clear();
    assert!(invalid_id.into_parts(topic()).is_err());
    let mut invalid_headers = valid.clone();
    invalid_headers.headers_json = "[".into();
    assert!(invalid_headers.into_parts(topic()).is_err());
    let mut invalid_type = valid.clone();
    invalid_type.content_type = "not a MIME type".into();
    assert!(invalid_type.into_parts(topic()).is_err());
    let mut invalid_schema = valid;
    invalid_schema.schema_id = Some("bad\nschema".into());
    assert!(invalid_schema.into_parts(topic()).is_err());
    let mut overflowing_timestamp = WireFields {
        version: 1,
        event_id: "timestamp-overflow".into(),
        timestamp_ms: u64::MAX as u128 + 1,
        headers_json: "{}".into(),
        ordering_key: None,
        content_type: "application/octet-stream".into(),
        schema_id: None,
        payload: Vec::new(),
    };
    assert!(overflowing_timestamp.clone().into_parts(topic()).is_err());
    overflowing_timestamp.timestamp_ms = 0;
    overflowing_timestamp.ordering_key = Some(String::new());
    assert!(overflowing_timestamp.into_parts(topic()).is_err());
    Ok(())
}

#[test]
fn test_wire_encoder_rejects_native_payloads() -> Result<(), Box<dyn Error>> {
    let message = OutboundMessage::new(
        TopicAddress::new("events")?,
        EventId::new("native-event")?,
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        None,
        TransportPayload::Native(Arc::new(7_u8)),
    );
    assert!(WireFields::from_outbound(&message).is_err());
    Ok(())
}
