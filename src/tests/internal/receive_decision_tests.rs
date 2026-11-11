// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared receive decision mapping tests.

use qubit_event_bus::error::SpiError;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::TopicAddress;

use crate::internal::DecodeFailure;
use crate::internal::PoisonOutcome;
use crate::internal::PoisonReason;
use crate::redis_provider_error::RedisProviderError;

#[test]
fn test_receive_error_maps_decode_failures_to_the_existing_spi_errors() {
    let topic = TopicAddress::new("events").expect("valid topic");
    assert!(
        DecodeFailure::Poison(PoisonReason::InvalidJson)
            .receive_error(&topic)
            .is_none()
    );

    let SpiError::Operation {
        provider_id,
        operation,
        resource,
        kind,
        retryable,
        source,
    } = DecodeFailure::LimitExceeded
        .receive_error(&topic)
        .expect("limit error")
    else {
        panic!("expected operation error");
    };
    assert_eq!(&*provider_id, "redis-streams");
    assert_eq!(operation, "receive");
    assert_eq!(resource.as_deref(), Some("events"));
    assert_eq!(kind, "receive_limit_exceeded");
    assert_eq!(retryable, Some(false));
    assert!(
        source
            .downcast_ref::<RedisProviderError>()
            .is_some_and(|error| matches!(error, RedisProviderError::LimitExceeded))
    );

    let SpiError::Operation {
        provider_id,
        operation,
        resource,
        kind,
        retryable,
        source,
    } = DecodeFailure::UnsupportedVersion
        .receive_error(&topic)
        .expect("version error")
    else {
        panic!("expected operation error");
    };
    assert_eq!(&*provider_id, "redis-streams");
    assert_eq!(operation, "receive");
    assert_eq!(resource.as_deref(), Some("events"));
    assert_eq!(kind, "unsupported_wire_version");
    assert_eq!(retryable, Some(false));
    assert!(
        source
            .downcast_ref::<RedisProviderError>()
            .is_some_and(|error| matches!(error, RedisProviderError::UnsupportedWireVersion))
    );
}

#[test]
fn test_receive_outcome_preserves_gaps_without_inventing_ownership_gaps() {
    for (outcome, expected) in [
        (
            PoisonOutcome::Quarantined,
            "malformed Redis stream entry was quarantined",
        ),
        (
            PoisonOutcome::SourceGone,
            "malformed pending Redis stream entry was removed",
        ),
        (
            PoisonOutcome::TombstoneCleared,
            "deleted Redis stream entry was removed from the pending list",
        ),
    ] {
        let Some(ReceiveOutcome::Gap(gap)) = outcome.receive_outcome() else {
            panic!("expected gap");
        };
        assert_eq!(&*gap.reason, expected);
        assert_eq!(gap.missed, Some(1));
    }
    assert!(PoisonOutcome::OwnershipChanged.receive_outcome().is_none());
}
