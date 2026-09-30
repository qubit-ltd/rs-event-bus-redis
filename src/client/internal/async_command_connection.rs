// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Generation-tagged async connection lease holding command admission.
use redis::Cmd;
use redis::Pipeline;
use redis::RedisFuture;
use redis::RedisResult;
use redis::Value;
use redis::aio::ConnectionLike;
use redis::aio::MultiplexedConnection;

use super::command_permit::CommandPermit;
/// Owns admission and one connection generation until completion or
/// cancellation.
pub(crate) struct AsyncCommandConnection {
    /// Cache generation used to reject stale invalidation; Sentinel leases use
    /// zero.
    pub(crate) generation: u64,
    /// Multiplexed transport configured with finite short-command waiting
    /// budgets.
    pub(crate) connection: MultiplexedConnection,
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
        self.connection.send_packed_command(command).await
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
        self.connection.req_packed_command(cmd)
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
        self.connection.req_packed_commands(cmd, offset, count)
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
