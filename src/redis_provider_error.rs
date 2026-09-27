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
    /// A stream record uses a wire version this implementation does not decode.
    #[error("unsupported Redis event wire version")]
    UnsupportedWireVersion,
}
