// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Stable secret-free categories used by poison quarantine.

use std::sync::Arc;
use std::sync::Mutex;

use qubit_event_bus::spi::TopicAddress;
use qubit_id::Id;
use redis::Value;
use redis::streams::StreamId;

use crate::internal::DecodeFailure;
use crate::internal::PoisonReason;
use crate::internal::RecoveryState;
use crate::internal::WireLimits;
use crate::internal::decode_entry;

#[test]
fn test_poison_reasons_have_stable_secret_free_names() {
    assert_eq!(PoisonReason::MissingWire.as_str(), "missing_wire");
    assert_eq!(
        PoisonReason::InvalidWireField.as_str(),
        "invalid_wire_field"
    );
    assert_eq!(PoisonReason::InvalidJson.as_str(), "invalid_json");
    assert_eq!(
        PoisonReason::InvalidEventMetadata.as_str(),
        "invalid_event_metadata"
    );
}

fn decode_wire(
    wire: Vec<u8>,
    limits: WireLimits,
) -> Result<qubit_event_bus::spi::InboundMessage, DecodeFailure> {
    let mut entry = StreamId::default();
    entry.id = "1-0".into();
    entry.map = [("wire".into(), Value::BulkString(wire))].into();
    decode_entry(
        Id::new(1),
        &TopicAddress::new("events").expect("valid topic"),
        "events",
        "workers",
        &Arc::new(Mutex::new(RecoveryState::new())),
        entry,
        limits,
    )
}

#[test]
fn test_future_layout_depth_failure_classifies_as_poison() {
    let limits = WireLimits {
        wire: 4096,
        payload: 1,
        headers: 2,
    };
    let shallow = br#"{"version":2,"future":0}"#.to_vec();
    assert!(matches!(
        decode_wire(shallow, limits),
        Err(DecodeFailure::UnsupportedVersion)
    ));

    let deep = format!(
        "{{\"version\":2,\"future\":{}0{}}}",
        "[".repeat(129),
        "]".repeat(129)
    )
    .into_bytes();
    assert!(matches!(
        decode_wire(deep, limits),
        Err(DecodeFailure::Poison(PoisonReason::InvalidJson))
    ));

    let valid_v1 = br#"{"version":1,"event_id":"event","timestamp_ms":0,"headers_json":"{}","content_type":"text/plain","payload":[]}"#.to_vec();
    assert!(decode_wire(valid_v1.clone(), limits).is_ok());
    assert!(matches!(
        decode_wire(
            valid_v1.clone(),
            WireLimits {
                wire: valid_v1.len() - 1,
                payload: 1,
                headers: 2,
            }
        ),
        Err(DecodeFailure::LimitExceeded)
    ));
}
