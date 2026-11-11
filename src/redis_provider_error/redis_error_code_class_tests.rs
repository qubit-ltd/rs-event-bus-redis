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
use redis::ServerErrorKind;

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
        (ErrorKind::UnexpectedReturnType, "wrong_type", Some(false)),
        (ErrorKind::InvalidClientConfig, "configuration", Some(false)),
        (ErrorKind::EmptySentinelList, "configuration", Some(false)),
        (
            ErrorKind::Server(ServerErrorKind::BusyLoading),
            "temporarily_unavailable",
            Some(true),
        ),
        (
            ErrorKind::Server(ServerErrorKind::TryAgain),
            "temporarily_unavailable",
            Some(true),
        ),
        (
            ErrorKind::Server(ServerErrorKind::MasterDown),
            "temporarily_unavailable",
            Some(true),
        ),
        (
            ErrorKind::Server(ServerErrorKind::ReadOnly),
            "temporarily_unavailable",
            Some(true),
        ),
        (ErrorKind::Io, "transport", Some(true)),
        (ErrorKind::Parse, "protocol", Some(false)),
        (ErrorKind::RESP3NotSupported, "protocol", Some(false)),
        (
            ErrorKind::Server(ServerErrorKind::Moved),
            "unsupported_topology",
            Some(false),
        ),
        (
            ErrorKind::Server(ServerErrorKind::Ask),
            "unsupported_topology",
            Some(false),
        ),
        (
            ErrorKind::Server(ServerErrorKind::CrossSlot),
            "unsupported_topology",
            Some(false),
        ),
        (
            ErrorKind::Server(ServerErrorKind::ClusterDown),
            "unsupported_topology",
            Some(false),
        ),
        (
            ErrorKind::ClusterConnectionNotFound,
            "unsupported_topology",
            Some(false),
        ),
        (ErrorKind::Extension, "redis_error", None),
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
