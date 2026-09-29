// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public Redis provider errors.

#[cfg(any(feature = "sync", feature = "async"))]
use qubit_event_bus::error::SpiError;
#[cfg(any(feature = "sync", feature = "async"))]
use qubit_event_bus::spi::TopicAddress;
#[cfg(any(feature = "sync", feature = "async"))]
use redis::RedisError;

pub use crate::redis_provider_error::RedisProviderError;
#[cfg(any(feature = "sync", feature = "async"))]
use crate::redis_provider_error::from_redis_error as classify_redis_error;

/// Converts a sanitized provider failure into the facade's stable SPI shape.
#[cfg(any(feature = "sync", feature = "async"))]
pub(crate) fn to_spi_error(
    operation: &'static str,
    topic: Option<&TopicAddress>,
    source: RedisProviderError,
) -> SpiError {
    let (kind, retryable) = match &source {
        RedisProviderError::Configuration(_) => ("configuration", Some(false)),
        RedisProviderError::UnsupportedWireVersion => ("unsupported_wire_version", Some(false)),
        RedisProviderError::Transport { kind, retryable, .. } => (*kind, *retryable),
        RedisProviderError::Operation(_) => ("redis_error", None),
    };
    SpiError::Operation {
        provider_id: "redis-streams".into(),
        operation,
        resource: topic.map(|value| value.as_str().into()),
        kind,
        retryable,
        source: Box::new(source),
    }
}

/// Converts a Redis client failure using stable kind and retryability rules.
#[cfg(any(feature = "sync", feature = "async"))]
pub(crate) fn from_redis_error(operation: &'static str, topic: Option<&TopicAddress>, source: &RedisError) -> SpiError {
    to_spi_error(operation, topic, classify_redis_error(operation, source))
}

#[cfg(all(test, any(feature = "sync", feature = "async")))]
mod tests {
    use redis::ErrorKind;
    use redis::RedisError;

    use crate::redis_provider_error::from_redis_error;

    #[test]
    fn redis_error_categories_are_stable_and_secret_safe() {
        for (error_kind, expected_kind, expected_retryable) in [
            (ErrorKind::AuthenticationFailed, "authentication", Some(false)),
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
                super::RedisProviderError::Transport {
                    kind,
                    retryable,
                    ..
                } if kind == expected_kind && retryable == expected_retryable
            ));
        }
    }
}
