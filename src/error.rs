// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public Redis provider errors.

pub use crate::redis_provider_error::RedisProviderError;

/// Converts a sanitized provider failure into the facade's stable SPI shape.
pub(crate) fn to_spi_error(
    operation: &'static str,
    topic: Option<&qubit_event_bus::spi::TopicAddress>,
    source: RedisProviderError,
) -> qubit_event_bus::error::SpiError {
    let (kind, retryable) = match &source {
        RedisProviderError::Configuration(_) => ("configuration", Some(false)),
        RedisProviderError::UnsupportedWireVersion => ("unsupported_wire_version", Some(false)),
        RedisProviderError::Transport { kind, retryable, .. } => (*kind, *retryable),
        RedisProviderError::Operation(_) => ("redis_error", Some(true)),
    };
    qubit_event_bus::error::SpiError::Operation {
        provider_id: "redis-streams".into(),
        operation,
        resource: topic.map(|value| value.as_str().into()),
        kind,
        retryable,
        source: Box::new(source),
    }
}

#[cfg(test)]
mod tests {
    use redis::ErrorKind;

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
            let error = redis::RedisError::from((error_kind, "password=secret"));
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
