// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Actual decoder depth and allocation boundaries.

use crate::bounded_wire_decoder::decode;
use crate::error::RedisProviderError;
use crate::wire_limits::WireLimits;

#[test]
fn skipping_future_layout_keeps_json_recursion_guard() {
    let limits = WireLimits {
        wire: 4096,
        payload: 1,
        headers: 1,
    };
    let shallow = format!("{{\"version\":2,\"future\":{}0{}}}", "[".repeat(16), "]".repeat(16));
    assert!(matches!(
        decode(shallow.as_bytes(), limits),
        Err(RedisProviderError::UnsupportedWireVersion)
    ));
    let deep = format!("{{\"version\":2,\"future\":{}0{}}}", "[".repeat(129), "]".repeat(129));
    assert!(
        matches!(decode(deep.as_bytes(), limits), Err(RedisProviderError::Operation(_))),
        "skipping nested values must not bypass serde_json's depth protection"
    );
}

#[test]
fn bounded_decoder_rejects_oversize_headers_before_inner_json_validation() {
    let wire = br#"{"version":1,"event_id":"event","timestamp_ms":0,"headers_json":"not JSON","content_type":"text/plain","payload":[]}"#;
    assert!(matches!(
        decode(
            wire,
            WireLimits {
                wire: wire.len(),
                payload: 1,
                headers: 2
            }
        ),
        Err(RedisProviderError::LimitExceeded)
    ));
}

#[test]
fn bounded_decoder_preserves_exact_lengths_and_rejects_each_limit_plus_one() {
    let wire = br#"{"version":1,"event_id":"event","timestamp_ms":0,"headers_json":"{}","content_type":"text/plain","payload":[0,255]}"#;
    let fields = decode(
        wire,
        WireLimits {
            wire: wire.len(),
            payload: 2,
            headers: 2,
        },
    )
    .unwrap();
    assert_eq!(fields.payload, [0, 255]);
    assert_eq!(fields.headers_json, "{}");
    for limits in [
        WireLimits {
            wire: wire.len() - 1,
            payload: 2,
            headers: 2,
        },
        WireLimits {
            wire: wire.len(),
            payload: 1,
            headers: 2,
        },
        WireLimits {
            wire: wire.len(),
            payload: 2,
            headers: 1,
        },
    ] {
        assert!(matches!(decode(wire, limits), Err(RedisProviderError::LimitExceeded)));
    }
}

#[test]
fn duplicate_version_and_invalid_bytes_are_malformed() {
    let limits = WireLimits {
        wire: 4096,
        payload: 2,
        headers: 2,
    };
    assert!(matches!(
        decode(br#"{"version":1,"version":2}"#, limits),
        Err(RedisProviderError::Operation(_))
    ));
    let invalid = br#"{"version":1,"event_id":"event","timestamp_ms":0,"headers_json":"{}","content_type":"text/plain","payload":[256]}"#;
    assert!(matches!(decode(invalid, limits), Err(RedisProviderError::Operation(_))));
}

#[test]
fn mixed_extension_fields_preserve_version_one_transport_fields() {
    let wire = br#"{"extension":{"enabled":true,"offset":-7,"ratio":1.25,"nested":[false,null,{"label":"future metadata"}]},"version":1,"event_id":"event","timestamp_ms":17,"headers_json":"{}","content_type":"text/plain","payload":[0,255]}"#;
    let fields = decode(
        wire,
        WireLimits {
            wire: wire.len(),
            payload: 2,
            headers: 2,
        },
    )
    .unwrap();
    assert_eq!(fields.version, 1);
    assert_eq!(fields.event_id, "event");
    assert_eq!(fields.timestamp_ms, 17);
    assert_eq!(fields.content_type, "text/plain");
    assert_eq!(fields.headers_json, "{}");
    assert_eq!(fields.payload, [0, 255]);
    assert!(fields.ordering_key.is_none());
    assert!(fields.schema_id.is_none());
}

#[test]
fn valid_future_layout_is_unsupported_without_applying_version_one_field_types() {
    let wire = br#"{"payload":{"compressed":true,"offset":-7,"ratio":1.25,"blocks":[null,{"bytes":"future representation"}]},"headers_json":{"future":false},"version":2}"#;
    assert!(matches!(
        decode(
            wire,
            WireLimits {
                wire: wire.len(),
                payload: 1,
                headers: 1
            }
        ),
        Err(RedisProviderError::UnsupportedWireVersion)
    ));
}

#[test]
fn incomplete_version_one_records_are_malformed_instead_of_defaulted() {
    let complete = r#"{"version":1,"event_id":"event","timestamp_ms":0,"headers_json":"{}","content_type":"text/plain","payload":[0,255]}"#;
    for missing in [
        "version",
        "event_id",
        "timestamp_ms",
        "headers_json",
        "content_type",
        "payload",
    ] {
        let mut fixture: serde_json::Value = serde_json::from_str(complete).unwrap();
        fixture.as_object_mut().unwrap().remove(missing);
        let wire = serde_json::to_vec(&fixture).unwrap();
        assert!(
            matches!(
                decode(
                    &wire,
                    WireLimits {
                        wire: wire.len(),
                        payload: 2,
                        headers: 2
                    }
                ),
                Err(RedisProviderError::Operation(_))
            ),
            "required field {missing} cannot be silently defaulted"
        );
    }
}

#[test]
fn wrong_json_types_and_trailing_values_are_malformed() {
    let limits = WireLimits {
        wire: 4096,
        payload: 2,
        headers: 2,
    };
    for wire in [
        br#"[]"#.as_slice(),
        br#"{"version":"1"}"#.as_slice(),
        br#"{"version":1,"event_id":"event","timestamp_ms":0,"headers_json":{},"content_type":"text/plain","payload":[]}"#.as_slice(),
        br#"{"version":1,"event_id":"event","timestamp_ms":0,"headers_json":"{}","content_type":"text/plain","payload":"not bytes"}"#.as_slice(),
        br#"{"version":2} {"version":1}"#.as_slice(),
    ] {
        assert!(matches!(decode(wire, limits), Err(RedisProviderError::Operation(_))), "invalid wire must not become an unsupported-version or limit error");
    }
}
