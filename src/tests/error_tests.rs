// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! SPI failure categories preserve operation-specific retryability.

use qubit_event_bus::error::SpiError;

use crate::error::RedisProviderError;
use crate::error::to_spi_error;

#[test]
fn test_error_retryability_context() {
    for (operation, retryable) in [
        ("publish", false),
        ("settle", true),
        ("receive", true),
        ("quarantine", false),
    ] {
        let error = to_spi_error(operation, None, RedisProviderError::OutcomeUnknown { operation });
        assert!(
            matches!(error, SpiError::Operation { kind: "outcome_unknown", retryable: Some(value), .. } if value == retryable)
        );
    }
    for (source, expected_kind, expected_retryable) in [
        (
            RedisProviderError::ResourceLimit { resource: "commands" },
            "resource_limit",
            true,
        ),
        (RedisProviderError::PayloadTooLarge, "payload_too_large", false),
        (RedisProviderError::WireTooLarge, "wire_too_large", false),
    ] {
        assert!(
            matches!(to_spi_error("publish", None, source), SpiError::Operation { kind, retryable: Some(value), .. } if kind == expected_kind && value == expected_retryable)
        );
    }
}
