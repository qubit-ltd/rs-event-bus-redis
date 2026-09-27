// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Redis provider errors with secret-safe formatting.

/// Configuration and message protocol failures.
#[derive(Debug, thiserror::Error)]
pub enum RedisProviderError {
    /// Provider options were absent or invalid.
    #[error("invalid Redis provider configuration: {0}")]
    Configuration(&'static str),
    /// A Redis operation failed. Its raw detail is intentionally omitted.
    #[error("Redis operation failed ({0})")]
    Operation(&'static str),
    /// The stream message uses a wire format this crate cannot decode.
    #[error("unsupported Redis event wire version")]
    UnsupportedWireVersion,
}
