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

/// Stores only idle synchronous connections; active receiver handles are
/// unbounded by this limit.
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
    pub(crate) fn new(max_idle: usize) -> Self {
        Self {
            idle: StdMutex::new(Vec::new()),
            max_idle,
        }
    }
}
