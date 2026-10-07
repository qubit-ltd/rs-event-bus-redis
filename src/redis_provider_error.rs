// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Redis provider errors with secret-safe formatting.

#[cfg(any(feature = "sync", feature = "async"))]
use redis::ErrorKind;
#[cfg(any(feature = "sync", feature = "async"))]
use redis::RedisError;
use thiserror::Error;

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
#[derive(Debug, Error)]
pub enum RedisProviderError {
    /// A required option is missing or a supplied setting violates validation.
    #[error("invalid Redis provider configuration: {0}")]
    Configuration(
        /// Static validation label that never contains endpoint or credential
        /// values.
        &'static str,
    ),
    /// A Redis operation failed and its raw client diagnostic was omitted.
    #[error("Redis operation failed ({0})")]
    Operation(
        /// Static operation label without raw Redis diagnostics or message
        /// bytes.
        &'static str,
    ),
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
    /// A command may have executed but no valid reply confirmed its result.
    #[error("Redis operation outcome unknown ({operation})")]
    OutcomeUnknown {
        /// Stable operation name, without connection or message data.
        operation: &'static str,
    },
    /// A bounded resource admission limit was reached before sending a command.
    #[error("Redis resource limit reached ({resource})")]
    ResourceLimit {
        /// Stable resource category, without user-supplied data.
        resource: &'static str,
    },
    /// Encoded payload exceeds the configured raw byte limit.
    #[error("Redis encoded payload exceeds the configured byte limit")]
    PayloadTooLarge,
    /// Serialized wire exceeds the configured JSON byte limit.
    #[error("Redis wire exceeds the configured byte limit")]
    WireTooLarge,
    /// A stream record uses a wire version this implementation does not decode.
    #[error("unsupported Redis event wire version")]
    UnsupportedWireVersion,
    /// A wire, payload, or headers component exceeded a provider resource
    /// limit.
    #[error("Redis event resource limit exceeded")]
    LimitExceeded,
}

/// Classifies a Redis error without retaining its diagnostic details.
///
/// # Parameters
///
/// - `operation`: Static operation category attached to the sanitized failure.
/// - `error`: Redis client error inspected by stable code and kind only.
///
/// # Returns
///
/// A transport category and retryability hint without raw endpoint, ACL, or
/// wire data.
#[cfg(any(feature = "sync", feature = "async"))]
pub(crate) fn from_redis_error(operation: &'static str, error: &RedisError) -> RedisProviderError {
    let code_class = match error.code().unwrap_or_default() {
        "NOAUTH" | "WRONGPASS" | "NOPERM" => Some(("authentication", Some(false))),
        "WRONGTYPE" => Some(("wrong_type", Some(false))),
        "OOM" => Some(("out_of_memory", Some(false))),
        "LOADING" | "TRYAGAIN" | "MASTERDOWN" | "READONLY" => {
            Some(("temporarily_unavailable", Some(true)))
        }
        "MOVED" | "ASK" | "CROSSSLOT" | "CLUSTERDOWN" => {
            Some(("unsupported_topology", Some(false)))
        }
        _ => None,
    };
    let (kind, retryable) = code_class.unwrap_or_else(|| match error.kind() {
        ErrorKind::AuthenticationFailed => ("authentication", Some(false)),
        ErrorKind::TypeError => ("wrong_type", Some(false)),
        ErrorKind::InvalidClientConfig | ErrorKind::EmptySentinelList => {
            ("configuration", Some(false))
        }
        ErrorKind::BusyLoadingError
        | ErrorKind::TryAgain
        | ErrorKind::MasterDown
        | ErrorKind::ReadOnly => ("temporarily_unavailable", Some(true)),
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
