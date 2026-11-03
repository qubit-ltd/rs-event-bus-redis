// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Process-local diagnostics for live Redis provider instances.
//!
//! Counter values reset with each SPI instance. Instance IDs are unique only
//! within the current process. Snapshots are non-atomic views collected from
//! live instances and do not include Redis pending-entry-list state.

mod redis_provider_diagnostics;
mod redis_provider_mode;
mod redis_provider_snapshot;

#[cfg(any(feature = "sync", feature = "async"))]
mod internal;

#[cfg(any(feature = "sync", feature = "async"))]
pub(crate) use internal::RedisDiagnosticCounter;
#[cfg(any(feature = "sync", feature = "async"))]
pub(crate) use internal::RedisDiagnosticsState;
pub use redis_provider_diagnostics::RedisProviderDiagnostics;
pub use redis_provider_mode::RedisProviderMode;
pub use redis_provider_snapshot::RedisProviderSnapshot;
