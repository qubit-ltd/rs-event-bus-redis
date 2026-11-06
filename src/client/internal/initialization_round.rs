// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Result gate for one host-polled cold initialization round.

use async_lock::Mutex;
use redis::aio::MultiplexedConnection;

use crate::error::RedisProviderError;

/// Serializes initialization and retains one terminal result for its waiters.
pub(super) struct InitializationRound {
    /// `None` until one joined caller completes or rejects initialization.
    pub(super) gate: Mutex<Option<Result<(u64, MultiplexedConnection), RedisProviderError>>>,
}
