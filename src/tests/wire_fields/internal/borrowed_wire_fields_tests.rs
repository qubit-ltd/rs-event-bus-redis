// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Borrowing and wire-compatibility contracts for bounded serialization.

use std::sync::Arc;
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
use serde_json::to_string;
use serde_json::to_vec;

use super::BorrowedWireFields;
use crate::wire_fields::WireFields;

/// Builds Unicode metadata, escaped headers, optional IDs, and a binary payload
/// without I/O; fixed valid metadata must construct successfully.
fn message() -> OutboundMessage {
    OutboundMessage::new(
        TopicAddress::new("events").expect("topic"),
        EventId::new("borrowed-event").expect("id"),
        SystemTime::UNIX_EPOCH,
        [("quoted".into(), "中文\\\"".into())].into(),
        OrderingKey::new("order-中文"),
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(&[0, 128, 255][..]),
            ContentType::new("application/octet-stream").expect("content type"),
            Some(SchemaId::new("schema-中文").expect("schema")),
        )),
    )
}

#[test]
fn test_borrowed_wire_fields_reuses_every_caller_owned_buffer() {
    let outbound = message();
    let TransportPayload::Encoded(payload) = outbound.payload() else {
        panic!("encoded payload");
    };
    let headers = to_string(outbound.headers()).expect("serialize headers");
    let fields = BorrowedWireFields::new(&outbound, payload, &headers, 0);
    assert_eq!(fields.event_id.as_ptr(), outbound.id().as_str().as_ptr());
    assert_eq!(fields.headers_json.as_ptr(), headers.as_ptr());
    assert_eq!(
        fields.ordering_key.expect("key").as_ptr(),
        outbound.ordering_key().expect("key").as_str().as_ptr()
    );
    assert_eq!(
        fields.content_type.as_ptr(),
        payload.content_type().as_str().as_ptr()
    );
    assert_eq!(
        fields.schema_id.expect("schema").as_ptr(),
        payload.schema_id().expect("schema").as_str().as_ptr()
    );
    assert_eq!(fields.payload.as_ptr(), payload.bytes().as_ptr());
}

#[test]
fn test_borrowed_wire_fields_matches_public_v1_json_byte_for_byte() {
    for optional in [true, false] {
        let outbound = if optional {
            message()
        } else {
            OutboundMessage::new(
                TopicAddress::new("events").expect("topic"),
                EventId::new("plain").expect("id"),
                SystemTime::UNIX_EPOCH,
                Headers::new(),
                None,
                None,
                TransportPayload::Encoded(EncodedPayload::new(
                    Arc::from(&[0, 128, 255][..]),
                    ContentType::new("application/octet-stream").expect("content type"),
                    None,
                )),
            )
        };
        let TransportPayload::Encoded(payload) = outbound.payload() else {
            panic!("encoded payload");
        };
        let headers = to_string(outbound.headers()).expect("serialize headers");
        let borrowed = BorrowedWireFields::new(&outbound, payload, &headers, 0);
        let owned = WireFields::from_outbound(&outbound).expect("public version 1 fields");
        assert_eq!(
            to_vec(&borrowed).expect("borrowed wire"),
            to_vec(&owned).expect("owned wire")
        );
    }
}
