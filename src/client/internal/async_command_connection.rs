// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Generation-tagged async connection lease holding command admission.
use std::sync::Arc;

use redis::Cmd;
use redis::Pipeline;
use redis::RedisFuture;
use redis::RedisResult;
use redis::Value;
use redis::aio::ConnectionLike;
use redis::aio::MultiplexedConnection;

use super::async_connection_cache::AsyncConnectionCache;
use super::async_connection_role::AsyncConnectionRole;
use super::command_permit::CommandPermit;
/// Owns admission and one connection generation until completion or
/// cancellation.
pub(crate) struct AsyncCommandConnection {
    /// Cache role carried with this lease so an error invalidates only the
    /// matching command channel.
    pub(crate) role: AsyncConnectionRole,
    /// Cache generation used to reject stale invalidation; Sentinel leases use
    /// zero.
    pub(crate) generation: u64,
    /// Multiplexed transport configured with finite short-command waiting
    /// budgets.
    pub(crate) connection: MultiplexedConnection,
    /// Standalone cache whose generation this lease may retire if a sent
    /// request is abandoned. Sentinel transports are not cached.
    pub(crate) cache: Option<Arc<AsyncConnectionCache>>,
    /// Whether a command request was polled through this lease.
    pub(crate) request_started: bool,
    /// Whether that request returned a response or error.
    pub(crate) request_complete: bool,
    /// Non-cloneable admission retained for the lifetime of this operation
    /// lease.
    pub(crate) _permit: CommandPermit,
}
impl AsyncCommandConnection {
    /// Sends one raw command, preserving top-level and nested Redis errors.
    ///
    /// # Parameters
    ///
    /// - `command`: Packed request whose raw reply must be classified by the
    ///   caller.
    ///
    /// # Returns
    ///
    /// The owned RESP reply while command admission remains held.
    ///
    /// # Errors
    ///
    /// Returns Redis transport/protocol failures. Cancellation can leave
    /// execution unknown.
    pub(crate) async fn send_packed_command(&mut self, command: &Cmd) -> RedisResult<Value> {
        self.request_complete = false;
        self.request_started = true;
        let result = self.connection.send_packed_command(command).await;
        self.request_complete = true;
        result
    }
}
impl Drop for AsyncCommandConnection {
    /// Retires the cached generation when its request future is dropped before
    /// the transport returns a result. Dropping this lease then releases its
    /// multiplexed connection clone; stale generations cannot clear a newer
    /// cache publication.
    fn drop(&mut self) {
        if self.request_started
            && !self.request_complete
            && let Some(cache) = &self.cache
        {
            cache.invalidate_if_current(self.generation);
        }
    }
}
impl ConnectionLike for AsyncCommandConnection {
    /// Delegates `cmd` while the operation permit remains held.
    ///
    /// # Type Parameters
    ///
    /// - `'a`: Borrow lifetime shared by this lease, command, and returned
    ///   future.
    ///
    /// # Parameters
    ///
    /// - `cmd`: One Redis command executed when the host polls the future.
    ///
    /// # Returns
    ///
    /// A future yielding the owned reply; Redis setup, transport, and protocol
    /// errors propagate. The command and lease remain borrowed for `'a`; the
    /// host executor drives I/O.
    ///
    /// # Errors
    ///
    /// The returned future propagates Redis transport, protocol, and command
    /// errors.
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        Box::pin(async move {
            self.request_complete = false;
            self.request_started = true;
            let result = self.connection.req_packed_command(cmd).await;
            self.request_complete = true;
            result
        })
    }
    /// Delegates `cmd`, skipping `offset` replies and returning at most
    /// `count`.
    ///
    /// # Type Parameters
    ///
    /// - `'a`: Borrow lifetime shared by this lease, pipeline, and returned
    ///   future.
    ///
    /// # Parameters
    ///
    /// - `cmd`: Packed pipeline executed when the host polls the future.
    /// - `offset`: Number of leading replies omitted from the returned batch.
    /// - `count`: Number of replies to retain after the offset.
    ///
    /// # Returns
    ///
    /// A host-polled future yielding owned replies, or Redis transport/protocol
    /// errors. The command and lease remain borrowed for `'a` while
    /// admission is retained.
    ///
    /// # Errors
    ///
    /// The returned future propagates Redis transport, protocol, and command
    /// errors.
    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        Box::pin(async move {
            self.request_complete = false;
            self.request_started = true;
            let result = self
                .connection
                .req_packed_commands(cmd, offset, count)
                .await;
            self.request_complete = true;
            result
        })
    }
    /// Returns the configured database without I/O or allocation.
    ///
    /// # Returns
    ///
    /// The database index recorded during connection setup.
    #[inline]
    fn get_db(&self) -> i64 {
        self.connection.get_db()
    }
}
