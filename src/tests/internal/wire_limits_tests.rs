// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal size-budget classification, independent of transport behavior.

use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;

use crate::config::RedisEventBusConfig;
use crate::error::RedisProviderError;
use crate::internal::WireLimits;
use crate::wire_fields::encode_bounded;

/// Builds metadata whose precedence over payload size can be observed.
fn message(payload: &[u8], headers: Headers, timestamp: SystemTime) -> OutboundMessage {
    OutboundMessage::new(
        TopicAddress::new("events").expect("topic"),
        EventId::new("limit-event").expect("id"),
        timestamp,
        headers,
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(payload),
            ContentType::new("application/octet-stream").expect("content type"),
            None,
        )),
    )
}

#[test]
fn test_wire_limits_payload_rejection_precedes_header_encoding_and_timestamp_validation() {
    let headers = [("large".into(), "x".repeat(4096))].into();
    let outbound = message(
        &[0, 1],
        headers,
        SystemTime::UNIX_EPOCH - Duration::from_secs(1),
    );
    assert!(matches!(
        encode_bounded(
            &outbound,
            WireLimits {
                payload: 1,
                wire: 8,
                headers: 8
            }
        ),
        Err(RedisProviderError::PayloadTooLarge)
    ));
}

#[test]
fn test_wire_limits_headers_are_bounded_before_wire_fields_are_constructed() {
    let headers = [("large".into(), "x".repeat(4096))].into();
    let outbound = message(
        &[],
        headers,
        SystemTime::UNIX_EPOCH - Duration::from_secs(1),
    );
    assert!(matches!(
        encode_bounded(
            &outbound,
            WireLimits {
                payload: 1,
                wire: 32,
                headers: 32
            }
        ),
        Err(RedisProviderError::WireTooLarge)
    ));
}

#[test]
fn test_wire_limits_exact_boundaries() {
    let limits = WireLimits::from_config(&RedisEventBusConfig::default());
    assert!(limits.check_payload(limits.payload).is_ok());
    assert!(matches!(
        limits.check_payload(limits.payload + 1),
        Err(RedisProviderError::PayloadTooLarge)
    ));
    assert!(limits.check_wire(limits.wire).is_ok());
    assert!(matches!(
        limits.check_wire(limits.wire + 1),
        Err(RedisProviderError::WireTooLarge)
    ));
}
