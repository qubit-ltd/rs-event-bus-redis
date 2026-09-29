// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded idle connection storage for synchronous Redis commands.

#[cfg(feature = "sync")]
use std::sync::Mutex as StdMutex;

#[cfg(feature = "sync")]
use redis::Connection;

/// Stores only idle synchronous short-command connections.
///
/// This retention cap is separate from the independently bounded receiver
/// admission budget; dedicated receiver sockets never enter this pool.
#[cfg(feature = "sync")]
pub(crate) struct SyncConnectionPool {
    /// Connections available for short commands.
    pub(crate) idle: StdMutex<Vec<Connection>>,
    /// Maximum number of idle connections retained.
    pub(crate) max_idle: usize,
}

#[cfg(feature = "sync")]
impl SyncConnectionPool {
    /// Creates an empty idle pool with the configured retention limit.
    ///
    /// # Parameters
    ///
    /// - `max_idle`: Positive validated cap for retained short-command sockets.
    ///
    /// # Returns
    ///
    /// Empty storage; no connection is opened.
    #[must_use]
    #[inline]
    pub(crate) fn new(max_idle: usize) -> Self {
        Self {
            idle: StdMutex::new(Vec::new()),
            max_idle,
        }
    }
}
