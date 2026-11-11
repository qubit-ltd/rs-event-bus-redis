// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded synchronous connection reuse.

use std::sync::Arc;
use std::time::Duration;

use redis::Connection;
use redis::ConnectionLike;
use redis::RedisResult;
use redis::Value;
use redis::cmd;

use super::command_permit::CommandPermit;
use super::sync_connection_pool::SyncConnectionPool;

/// Owns one blocking socket and optionally returns healthy short-command
/// leases.
///
/// Dedicated receiver sockets have no idle pool or short-command permit here;
/// their receiver admission is retained by the subscription itself.
pub(crate) struct PooledConnection {
    /// Socket remains present until Drop takes ownership for idle retention.
    connection: Option<Connection>,
    /// `Some` allows healthy idle retention; `None` forces socket destruction.
    pool: Option<Arc<SyncConnectionPool>>,
    /// Short-command admission, or `None` for a dedicated receiver connection.
    _permit: Option<CommandPermit>,
}

#[cfg(feature = "sync")]
impl PooledConnection {
    /// Wraps an already initialized socket without performing network I/O.
    ///
    /// # Parameters
    ///
    /// - `connection`: Socket with controlled setup and command waits.
    /// - `pool`: Idle-retention destination, or `None` for a dedicated socket.
    /// - `permit`: Acquired short-command slot, or `None` for a receiver
    ///   socket.
    ///
    /// # Returns
    ///
    /// A lease that releases admission and disposes or retains the socket on
    /// drop.
    #[must_use]
    #[inline]
    pub(crate) fn new(
        connection: Connection,
        pool: Option<Arc<SyncConnectionPool>>,
        permit: Option<CommandPermit>,
    ) -> Self {
        Self {
            connection: Some(connection),
            pool,
            _permit: permit,
        }
    }

    /// Sets the socket read wait to `timeout`; `None` clears that wait.
    ///
    /// # Parameters
    ///
    /// - `timeout`: `Some` sets the finite socket wait; `None` removes it.
    ///
    /// # Returns
    ///
    /// Success after socket configuration, without sending a Redis command.
    ///
    /// # Errors
    ///
    /// Returns a Redis I/O error if the underlying socket option cannot be
    /// applied.
    ///
    /// # Panics
    ///
    /// Panics only if the live-lease socket invariant has been violated.
    pub(crate) fn set_read_timeout(&self, timeout: Option<Duration>) -> RedisResult<()> {
        self.connection
            .as_ref()
            .expect("pooled connection present")
            .set_read_timeout(timeout)
    }
    /// Sets the socket write wait to `timeout`; `None` clears that wait.
    ///
    /// # Parameters
    ///
    /// - `timeout`: `Some` sets the finite socket wait; `None` removes it.
    ///
    /// # Returns
    ///
    /// Success after socket configuration, without sending a Redis command.
    ///
    /// # Errors
    ///
    /// Returns a Redis I/O error if the underlying socket option cannot be
    /// applied.
    ///
    /// # Panics
    ///
    /// Panics only if the live-lease socket invariant has been violated.
    pub(crate) fn set_write_timeout(&self, timeout: Option<Duration>) -> RedisResult<()> {
        self.connection
            .as_ref()
            .expect("pooled connection present")
            .set_write_timeout(timeout)
    }
    /// Prevents idle retention after uncertain transport or protocol results.
    ///
    /// The socket stays alive until this lease is dropped; no new I/O is
    /// issued.
    #[inline]
    pub(crate) fn discard(&mut self) {
        self.pool = None;
    }
}

#[cfg(feature = "sync")]
impl Drop for PooledConnection {
    /// Returns an open, undiscarded socket if the idle pool lock and cap permit
    /// it.
    ///
    /// Otherwise destroys the socket. Admission is released with the permit
    /// field; no network command is sent and a poisoned pool cannot block
    /// resource release.
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

#[cfg(feature = "sync")]
impl ConnectionLike for PooledConnection {
    /// Sends `command` and discards the lease on any returned Redis error.
    ///
    /// # Parameters
    ///
    /// - `command`: Packed bytes for one Redis command.
    ///
    /// # Returns
    ///
    /// The owned RESP reply. Raw top-level server errors remain values for
    /// caller classification.
    ///
    /// # Errors
    ///
    /// Returns bounded socket I/O or protocol failures without retaining the
    /// socket.
    ///
    /// # Panics
    ///
    /// Panics only if the live-lease socket invariant has been violated.
    fn req_packed_command(&mut self, command: &[u8]) -> RedisResult<Value> {
        let result = self
            .connection
            .as_mut()
            .expect("pooled connection present")
            .req_packed_command(command);
        if result.is_err() {
            self.discard();
        }
        result
    }
    /// Sends `command`, skipping `offset` replies and collecting `count`
    /// replies.
    ///
    /// # Parameters
    ///
    /// - `command`: Packed pipeline bytes sent once on this socket.
    /// - `offset`: Number of leading replies to omit from the result.
    /// - `count`: Number of replies to retain after the offset.
    ///
    /// # Returns
    ///
    /// Owned pipeline replies after blocking I/O with the configured socket
    /// waits.
    ///
    /// # Errors
    ///
    /// Returns Redis command, I/O, or protocol failures and prevents idle
    /// retention.
    ///
    /// # Panics
    ///
    /// Panics only if the live-lease socket invariant has been violated.
    fn req_packed_commands(
        &mut self,
        command: &[u8],
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>> {
        let result = self
            .connection
            .as_mut()
            .expect("pooled connection present")
            .req_packed_commands(command, offset, count);
        if result.is_err() {
            self.discard();
        }
        result
    }
    /// Returns the configured database index without I/O or allocation.
    ///
    /// # Returns
    ///
    /// The database index recorded during connection setup.
    ///
    /// # Panics
    ///
    /// Panics only if the live-lease socket invariant has been violated.
    #[inline]
    fn get_db(&self) -> i64 {
        self.connection
            .as_ref()
            .expect("pooled connection present")
            .get_db()
    }
    /// Sends a blocking PING using the currently configured socket waits.
    ///
    /// # Returns
    ///
    /// `true` only for a PONG reply; other replies and failures discard the
    /// lease.
    ///
    /// # Panics
    ///
    /// Panics only if the live-lease socket invariant has been violated.
    fn check_connection(&mut self) -> bool {
        let healthy = matches!(self.req_command(&cmd("PING")), Ok(Value::SimpleString(pong)) if pong == "PONG");
        if !healthy {
            self.discard();
        }
        healthy
    }
    /// Returns the transport open flag without network I/O.
    ///
    /// This flag alone does not establish protocol alignment; earlier errors
    /// discard the lease.
    ///
    /// # Returns
    ///
    /// The underlying socket open flag without testing server liveness.
    ///
    /// # Panics
    ///
    /// Panics only if the live-lease socket invariant has been violated.
    #[inline]
    fn is_open(&self) -> bool {
        self.connection
            .as_ref()
            .expect("pooled connection present")
            .is_open()
    }
}
