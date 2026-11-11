// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public stable, secret-safe size and outcome diagnostics.

use qubit_event_bus_redis::error::RedisProviderError;

#[test]
fn test_provider_error_new_categories_display_and_debug() {
    for (error, expected) in [
        (
            RedisProviderError::OutcomeUnknown {
                operation: "publish",
            },
            "Redis operation outcome unknown (publish)",
        ),
        (
            RedisProviderError::ResourceLimit {
                resource: "commands",
            },
            "Redis resource limit reached (commands)",
        ),
        (
            RedisProviderError::PayloadTooLarge,
            "Redis encoded payload exceeds the configured byte limit",
        ),
        (
            RedisProviderError::WireTooLarge,
            "Redis wire exceeds the configured byte limit",
        ),
    ] {
        assert_eq!(error.to_string(), expected);
        assert!(!format!("{error:?}").contains("secret-value"));
    }
}
