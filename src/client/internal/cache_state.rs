// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Published async connection and generation protected by the cache mutex.

use redis::aio::MultiplexedConnection;

/// Retains the monotonic generation when a failed connection is invalidated.
pub(super) struct CacheState {
    /// Identity assigned to the last successfully published connection.
    pub(super) generation: u64,
    /// `Some` is the current shared transport; `None` requires bounded cold
    /// setup.
    pub(super) connection: Option<MultiplexedConnection>,
}
