// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Redis provider errors with secret-safe formatting.

/// Redis provider errors with secret-safe formatting.
///
/// Redis client diagnostics can contain connection details, so this type
/// exposes stable operation categories instead of the raw Redis error text.
///
/// # Examples
///
/// ```
/// use qubit_event_bus_redis::error::RedisProviderError;
///
/// let error = RedisProviderError::Configuration("invalid redis.url");
/// assert_eq!(error.to_string(), "invalid Redis provider configuration: invalid redis.url");
/// ```
#[must_use]
#[derive(Debug, thiserror::Error)]
pub enum RedisProviderError {
    /// A required option is missing or a supplied setting violates validation.
    #[error("invalid Redis provider configuration: {0}")]
    Configuration(&'static str),
    /// A Redis operation failed and its raw client diagnostic was omitted.
    #[error("Redis operation failed ({0})")]
    Operation(&'static str),
    /// A Redis client operation failed with a stable, secret-safe category.
    #[error("Redis operation {operation} failed ({kind})")]
    Transport {
        /// Stable SPI operation name.
        operation: &'static str,
        /// Stable Redis error category.
        kind: &'static str,
        /// Retry policy when the Redis error kind is known.
        retryable: Option<bool>,
    },
    /// A stream record uses a wire version this implementation does not decode.
    #[error("unsupported Redis event wire version")]
    UnsupportedWireVersion,
}

/// Classifies a Redis error without retaining its diagnostic details.
#[cfg(any(feature = "sync", feature = "async"))]
pub(crate) fn from_redis_error(operation: &'static str, error: &redis::RedisError) -> RedisProviderError {
    use redis::ErrorKind;

    let code_class = match error.code().unwrap_or_default() {
        "NOAUTH" | "WRONGPASS" | "NOPERM" => Some(("authentication", Some(false))),
        "WRONGTYPE" => Some(("wrong_type", Some(false))),
        "OOM" => Some(("out_of_memory", Some(false))),
        "LOADING" | "TRYAGAIN" | "MASTERDOWN" | "READONLY" => Some(("temporarily_unavailable", Some(true))),
        "MOVED" | "ASK" | "CROSSSLOT" | "CLUSTERDOWN" => Some(("unsupported_topology", Some(false))),
        _ => None,
    };
    let (kind, retryable) = code_class.unwrap_or_else(|| match error.kind() {
        ErrorKind::AuthenticationFailed => ("authentication", Some(false)),
        ErrorKind::TypeError => ("wrong_type", Some(false)),
        ErrorKind::InvalidClientConfig | ErrorKind::EmptySentinelList => ("configuration", Some(false)),
        ErrorKind::BusyLoadingError | ErrorKind::TryAgain | ErrorKind::MasterDown | ErrorKind::ReadOnly => {
            ("temporarily_unavailable", Some(true))
        }
        ErrorKind::IoError => ("transport", Some(true)),
        ErrorKind::ParseError | ErrorKind::RESP3NotSupported => ("protocol", Some(false)),
        ErrorKind::Moved
        | ErrorKind::Ask
        | ErrorKind::CrossSlot
        | ErrorKind::ClusterDown
        | ErrorKind::ClusterConnectionNotFound => ("unsupported_topology", Some(false)),
        _ => ("redis_error", None),
    });
    RedisProviderError::Transport {
        operation,
        kind,
        retryable,
    }
}
