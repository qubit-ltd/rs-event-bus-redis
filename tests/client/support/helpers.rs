// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared public-SPI inputs and sanitized error assertions for client tests.

use std::sync::Arc;
use std::time::SystemTime;

use qubit_event_bus::SpiError;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishEffect;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;

/// Uses finite 200ms transport waits and configurable admission caps.
pub(crate) fn options(url: &str, cap: usize) -> ProviderOptions {
    [
        ("redis.url".into(), url.into()),
        ("redis.namespace".into(), "transport-tests".into()),
        ("redis.connect_timeout_ms".into(), "200".into()),
        ("redis.command_timeout_ms".into(), "200".into()),
        ("redis.max_concurrent_commands".into(), cap.to_string()),
        ("redis.max_idle_connections".into(), "1".into()),
        ("redis.max_active_receivers".into(), "1".into()),
    ]
    .into()
}
/// Constructs an encoded public SPI message.
pub(crate) fn message() -> OutboundMessage {
    OutboundMessage::new(
        TopicAddress::new("events").expect("topic"),
        EventId::new("id").expect("event"),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(vec![1_u8]),
            ContentType::new("application/octet-stream").expect("type"),
            None,
        )),
    )
}
/// Asserts stable sanitized SPI kind and retry semantics.
pub(crate) fn assert_error(error: SpiError, expected_kind: &str, expected_retryable: bool) {
    match error {
        SpiError::Operation {
            kind, retryable, ..
        } => {
            assert_eq!(kind, expected_kind);
            assert_eq!(retryable, Some(expected_retryable));
        }
        SpiError::Publish {
            kind,
            retryable,
            effect,
            ..
        } => {
            assert_eq!(kind, expected_kind);
            assert_eq!(retryable, Some(expected_retryable));
            let expected_effect = if expected_kind == "outcome_unknown" {
                PublishEffect::MayHaveBeenAccepted
            } else {
                PublishEffect::NotAccepted
            };
            assert_eq!(effect, expected_effect);
        }
        other => panic!("unexpected error: {other}"),
    }
}
