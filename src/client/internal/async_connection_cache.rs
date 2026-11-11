// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Single-flight async connection publication and generation-safe invalidation.
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;

use async_lock::Mutex as AsyncMutex;
use redis::aio::MultiplexedConnection;

use super::cache_state::CacheState;
use super::initialization_round::InitializationRound;
use crate::error::RedisProviderError;

/// Stores a published generation and registers host-polled cold rounds.
pub(crate) struct AsyncConnectionCache {
    /// Short synchronous directory lock; never held across an await or I/O.
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
                current_round: None,
            }),
        }
    }
    /// Returns the current lease identity, sharing one host-polled initializer
    /// and its terminal result with callers in the same cold round.
    /// Cancellation releases the round gate so another joined caller can
    /// take over. `F` is never spawned and is not polled when a terminal
    /// result is already available. Generation overflow is reported before
    /// polling `F`.
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
        let round = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| RedisProviderError::Operation("connection cache lock poisoned"))?;
            if let Some(connection) = &state.connection {
                return Ok((state.generation, connection.clone()));
            }
            match state
                .current_round
                .as_ref()
                .and_then(std::sync::Weak::upgrade)
            {
                Some(round) => round,
                None => {
                    let round = Arc::new(InitializationRound {
                        gate: AsyncMutex::new(None),
                    });
                    state.current_round = Some(Arc::downgrade(&round));
                    round
                }
            }
        };

        let mut terminal = round.gate.lock().await;
        if let Some(result) = terminal.as_ref() {
            return result.clone();
        }
        let generation = {
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => {
                    let result = Err(RedisProviderError::Operation(
                        "connection cache lock poisoned",
                    ));
                    *terminal = Some(result.clone());
                    return result;
                }
            };
            match state.generation.checked_add(1) {
                Some(generation) => generation,
                None => {
                    if state
                        .current_round
                        .as_ref()
                        .and_then(std::sync::Weak::upgrade)
                        .is_some_and(|current| Arc::ptr_eq(&current, &round))
                    {
                        state.current_round = None;
                    }
                    let result = Err(RedisProviderError::Operation(
                        "connection generation overflow",
                    ));
                    *terminal = Some(result.clone());
                    return result;
                }
            }
        };

        let result = match connect.await {
            Ok(connection) => Ok((generation, connection)),
            Err(error) => Err(error),
        };
        let mut state = self
            .state
            .lock()
            .map_err(|_| RedisProviderError::Operation("connection cache lock poisoned"))?;
        if let Ok((generation, connection)) = &result {
            state.generation = *generation;
            state.connection = Some(connection.clone());
        }
        *terminal = Some(result.clone());
        if state
            .current_round
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .is_some_and(|current| Arc::ptr_eq(&current, &round))
        {
            state.current_round = None;
        }
        result
    }
    /// Clears only the connection used by the failing lease, never a
    /// replacement.
    ///
    /// # Parameters
    ///
    /// - `generation`: Identity of the failed lease. A stale identity leaves
    ///   state intact.
    ///
    /// Acquires only the short directory mutex; no Redis command is issued.
    pub(crate) fn invalidate_if_current(&self, generation: u64) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.generation == generation {
            state.connection = None;
        }
    }

    #[cfg(all(test, feature = "sync", feature = "async"))]
    pub(crate) fn published_generation(&self) -> Option<u64> {
        let state = self.state.lock().ok()?;
        state.connection.as_ref().map(|_| state.generation)
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::future::poll_fn;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Poll;
    use std::task::Waker;

    use futures_lite::future::block_on;
    use futures_lite::future::poll_once;
    use redis::aio::MultiplexedConnection;

    use super::AsyncConnectionCache;
    use super::CacheState;
    use crate::error::RedisProviderError;

    #[test]
    fn test_cold_failure_is_shared_with_same_round() {
        let cache = Arc::new(AsyncConnectionCache::new());
        let polls = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new((AtomicBool::new(false), StdMutex::new(Vec::<Waker>::new())));
        let mut calls = Vec::new();
        for _ in 0..8 {
            let cache = Arc::clone(&cache);
            let polls = Arc::clone(&polls);
            let gate = Arc::clone(&gate);
            calls.push(Box::pin(async move {
                cache
                    .get_or_connect(async move {
                        polls.fetch_add(1, Ordering::SeqCst);
                        poll_fn(|context| {
                            if gate.0.load(Ordering::SeqCst) {
                                Poll::Ready(())
                            } else {
                                gate.1
                                    .lock()
                                    .expect("gate waiters")
                                    .push(context.waker().clone());
                                Poll::Pending
                            }
                        })
                        .await;
                        Err(RedisProviderError::Transport {
                            operation: "connect",
                            kind: "transport",
                            retryable: Some(true),
                        })
                    })
                    .await
            }));
        }
        for call in &mut calls {
            assert!(
                block_on(poll_once(call.as_mut())).is_none(),
                "every same-round caller must register before the gate opens"
            );
        }
        drop(calls.pop().expect("one cancelled waiter"));
        gate.0.store(true, Ordering::SeqCst);
        for waker in std::mem::take(&mut *gate.1.lock().expect("gate waiters")) {
            waker.wake();
        }
        for call in calls {
            let error = block_on(call).expect_err("shared initialization failure");
            assert!(matches!(
                error,
                RedisProviderError::Transport {
                    operation: "connect",
                    kind: "transport",
                    retryable: Some(true),
                }
            ));
        }
        assert_eq!(polls.load(Ordering::SeqCst), 1, "one initializer per round");
        let error = block_on(cache.get_or_connect(async {
            polls.fetch_add(1, Ordering::SeqCst);
            Err(RedisProviderError::Transport {
                operation: "connect",
                kind: "recovered_transport",
                retryable: Some(true),
            })
        }))
        .expect_err("a later call starts a fresh round");
        assert!(matches!(
            error,
            RedisProviderError::Transport {
                kind: "recovered_transport",
                ..
            }
        ));
        assert_eq!(
            polls.load(Ordering::SeqCst),
            2,
            "completed failure is not cached"
        );
    }

    #[test]
    fn test_cancelled_initializer_is_taken_over_by_joined_caller() {
        let cache = Arc::new(AsyncConnectionCache::new());
        let polls = Arc::new(AtomicUsize::new(0));
        let leader = {
            let cache = Arc::clone(&cache);
            let polls = Arc::clone(&polls);
            Box::pin(async move {
                cache
                    .get_or_connect(async move {
                        polls.fetch_add(1, Ordering::SeqCst);
                        pending::<Result<MultiplexedConnection, RedisProviderError>>().await
                    })
                    .await
            })
        };
        let follower = {
            let cache = Arc::clone(&cache);
            let polls = Arc::clone(&polls);
            Box::pin(async move {
                cache
                    .get_or_connect(async move {
                        polls.fetch_add(1, Ordering::SeqCst);
                        Err(RedisProviderError::Transport {
                            operation: "connect",
                            kind: "transport",
                            retryable: Some(true),
                        })
                    })
                    .await
            })
        };
        let mut leader = leader;
        let mut follower = follower;
        assert!(block_on(poll_once(leader.as_mut())).is_none());
        assert!(block_on(poll_once(follower.as_mut())).is_none());
        assert_eq!(
            polls.load(Ordering::SeqCst),
            1,
            "only the leader initialized"
        );
        drop(leader);
        let result = block_on(follower).expect_err("joined caller takes over");
        assert!(matches!(result, RedisProviderError::Transport { .. }));
        assert_eq!(
            polls.load(Ordering::SeqCst),
            2,
            "takeover polls its own initializer"
        );
    }

    #[test]
    fn test_all_cancelled_callers_allow_a_fresh_round() {
        let cache = Arc::new(AsyncConnectionCache::new());
        let polls = Arc::new(AtomicUsize::new(0));
        let mut first = {
            let cache = Arc::clone(&cache);
            let polls = Arc::clone(&polls);
            Box::pin(async move {
                cache
                    .get_or_connect(async move {
                        polls.fetch_add(1, Ordering::SeqCst);
                        pending::<Result<MultiplexedConnection, RedisProviderError>>().await
                    })
                    .await
            })
        };
        assert!(block_on(poll_once(first.as_mut())).is_none());
        drop(first);
        let result = block_on(cache.get_or_connect(async {
            polls.fetch_add(1, Ordering::SeqCst);
            Err(RedisProviderError::Transport {
                operation: "connect",
                kind: "transport",
                retryable: Some(true),
            })
        }));
        assert!(matches!(result, Err(RedisProviderError::Transport { .. })));
        assert_eq!(
            polls.load(Ordering::SeqCst),
            2,
            "the fresh round polls its initializer"
        );
    }

    #[test]
    fn test_generation_overflow_rejects_without_polling_initializer() {
        let cache = AsyncConnectionCache {
            state: std::sync::Mutex::new(CacheState {
                generation: u64::MAX,
                connection: None,
                current_round: None,
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
            let state = cache.state.lock().expect("cache state");
            assert_eq!(state.generation, u64::MAX);
            assert!(state.connection.is_none());
            assert!(
                state.current_round.is_none(),
                "overflow round is complete and retryable"
            );
        });
    }
}
