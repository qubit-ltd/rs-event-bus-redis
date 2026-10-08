// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Single-flight async connection publication and generation-safe invalidation.
use std::future::Future;

use async_lock::Mutex;
use redis::aio::MultiplexedConnection;

use super::cache_state::CacheState;
use crate::error::RedisProviderError;

/// Stores a published generation; the async lock spans cold initialization.
pub(crate) struct AsyncConnectionCache {
    /// Async mutex held through cold setup so only one initializer publishes.
    state: Mutex<CacheState>,
}
impl AsyncConnectionCache {
    /// Creates an empty cache without opening a Redis connection.
    ///
    /// # Returns
    ///
    /// An unpublished cache at generation zero. Initial setup is deferred until
    /// polled.
    #[must_use]
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(CacheState {
                generation: 0,
                connection: None,
            }),
        }
    }
    /// Returns the current lease identity, initializing once under the async
    /// lock. Cancellation drops the initializer and leaves an empty cache
    /// unchanged. `F` is the host-polled connection initializer; its error
    /// is returned without publication. Returns an internal error before
    /// polling `F` if generation increment would overflow. The async mutex,
    /// never a standard mutex, spans I/O.
    ///
    /// # Type Parameters
    ///
    /// - `F`: Host-polled connection initializer with a sanitized failure type.
    ///
    /// # Parameters
    ///
    /// - `connect`: Cold initializer, polled only when no connection is
    ///   published.
    ///
    /// # Returns
    ///
    /// The current generation and a clone of its multiplexed connection.
    ///
    /// # Errors
    ///
    /// Returns the initializer failure without publication, or generation
    /// overflow before I/O.
    pub(crate) async fn get_or_connect<F>(
        &self,
        connect: F,
    ) -> Result<(u64, MultiplexedConnection), RedisProviderError>
    where
        F: Future<Output = Result<MultiplexedConnection, RedisProviderError>>,
    {
        let mut state = self.state.lock().await;
        if let Some(connection) = &state.connection {
            return Ok((state.generation, connection.clone()));
        }
        let generation = state
            .generation
            .checked_add(1)
            .ok_or(RedisProviderError::Operation(
                "connection generation overflow",
            ))?;
        let connection = connect.await?;
        state.connection = Some(connection.clone());
        state.generation = generation;
        Ok((generation, connection))
    }
    /// Clears only the connection used by the failing lease, never a
    /// replacement.
    ///
    /// # Parameters
    ///
    /// - `generation`: Identity of the failed lease. A stale identity leaves
    ///   state intact.
    ///
    /// Waits only for the async mutex; no Redis command is issued.
    pub(crate) async fn invalidate_if_current(&self, generation: u64) {
        let mut state = self.state.lock().await;
        if state.generation == generation {
            state.connection = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use async_lock::Mutex;
    use futures_lite::future::block_on;
    use futures_lite::future::poll_once;
    use redis::aio::MultiplexedConnection;

    use super::AsyncConnectionCache;
    use super::CacheState;
    use crate::error::RedisProviderError;

    #[test]
    fn test_generation_overflow_rejects_without_polling_initializer() {
        let cache = AsyncConnectionCache {
            state: Mutex::new(CacheState {
                generation: u64::MAX,
                connection: None,
            }),
        };
        block_on(async {
            let mut initialize =
                Box::pin(cache.get_or_connect(pending::<
                    Result<MultiplexedConnection, RedisProviderError>,
                >()));
            let ready = poll_once(&mut initialize).await;
            assert!(
                matches!(
                    ready,
                    Some(Err(RedisProviderError::Operation(
                        "connection generation overflow"
                    )))
                ),
                "overflow must reject before polling the pending initializer"
            );
            let state = cache.state.lock().await;
            assert_eq!(state.generation, u64::MAX);
            assert!(state.connection.is_none());
        });
    }
}
