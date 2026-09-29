// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded synchronous connection reuse.

#[cfg(feature = "sync")]
use std::sync::Arc;

#[cfg(feature = "sync")]
use redis::Connection;
#[cfg(feature = "sync")]
use redis::ConnectionLike;

#[cfg(feature = "sync")]
use super::sync_connection_pool::SyncConnectionPool;

#[cfg(feature = "sync")]
pub(crate) struct PooledConnection {
    connection: Option<Connection>,
    pool: Option<Arc<SyncConnectionPool>>,
}

#[cfg(feature = "sync")]
impl PooledConnection {
    pub(crate) fn new(connection: Connection, pool: Option<Arc<SyncConnectionPool>>) -> Self {
        Self {
            connection: Some(connection),
            pool,
        }
    }

    pub(crate) fn discard(&mut self) {
        self.pool = None;
    }
}

#[cfg(feature = "sync")]
impl std::ops::Deref for PooledConnection {
    type Target = Connection;
    fn deref(&self) -> &Self::Target {
        self.connection.as_ref().expect("pooled connection is present")
    }
}

#[cfg(feature = "sync")]
impl std::ops::DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.connection.as_mut().expect("pooled connection is present")
    }
}

#[cfg(feature = "sync")]
impl Drop for PooledConnection {
    fn drop(&mut self) {
        let (Some(pool), Some(connection)) = (&self.pool, self.connection.take()) else {
            return;
        };
        if connection.is_open()
            && let Ok(mut idle) = pool.idle.lock()
            && idle.len() < pool.max_idle
        {
            idle.push(connection);
        }
    }
}
