// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider execution mode exposed by diagnostics.

/// Provider execution mode for a live Redis SPI instance.
///
/// # Examples
///
/// ```
/// use qubit_event_bus_redis::diagnostics::RedisProviderMode;
///
/// fn mode_name(mode: RedisProviderMode) -> &'static str {
///     match mode {
///         RedisProviderMode::Sync => "sync",
///         RedisProviderMode::Async => "async",
///     }
/// }
///
/// assert_eq!(mode_name(RedisProviderMode::Sync), "sync");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisProviderMode {
    /// Synchronous Redis SPI.
    Sync,
    /// Runtime-neutral asynchronous Redis SPI.
    Async,
}
