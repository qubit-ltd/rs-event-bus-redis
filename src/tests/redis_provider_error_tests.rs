// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Stable Redis client error classification without retained secrets.

use redis::ErrorKind;
use redis::RedisError;

use crate::error::RedisProviderError;
use crate::redis_provider_error::from_redis_error;

#[test]
fn test_redis_error_categories_are_stable_and_secret_safe() {
    for (error_kind, expected_kind, expected_retryable) in [
        (
            ErrorKind::AuthenticationFailed,
            "authentication",
            Some(false),
        ),
        (ErrorKind::TypeError, "wrong_type", Some(false)),
        (ErrorKind::IoError, "transport", Some(true)),
        (ErrorKind::Moved, "unsupported_topology", Some(false)),
        (ErrorKind::ExtensionError, "redis_error", None),
    ] {
        let error = RedisError::from((error_kind, "password=secret"));
        let provider = from_redis_error("publish", &error);
        let rendered = provider.to_string();
        assert!(!rendered.contains("secret"));
        assert!(matches!(
            provider,
            RedisProviderError::Transport {
                kind,
                retryable,
                ..
            } if kind == expected_kind && retryable == expected_retryable
        ));
    }
}
