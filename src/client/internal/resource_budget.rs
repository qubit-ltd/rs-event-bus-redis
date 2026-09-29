// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared fail-fast admission for application operations and receivers.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use super::command_permit::CommandPermit;
use super::receiver_permit::ReceiverPermit;
use crate::error::RedisProviderError;

/// Counts application operations; cancelled Redis requests may still complete.
pub(crate) struct ResourceBudget {
    /// Application short operations currently admitted, including awaited async
    /// I/O.
    pub(super) commands: AtomicUsize,
    /// Receiver leases currently retained through setup, receive, close, or
    /// drop.
    pub(super) receivers: AtomicUsize,
    /// Inclusive cap for concurrently admitted short operations.
    max_commands: usize,
    /// Inclusive cap for active receiver leases, separate from unsettled
    /// tokens.
    max_receivers: usize,
}
impl ResourceBudget {
    /// Creates empty admission counters with validated finite caps.
    ///
    /// # Parameters
    ///
    /// - `max_commands`: Positive validated short-operation cap.
    /// - `max_receivers`: Positive validated active-receiver cap.
    ///
    /// # Returns
    ///
    /// Zeroed counters; no queue, connection, or network work is created.
    #[must_use]
    #[inline]
    pub(crate) fn new(max_commands: usize, max_receivers: usize) -> Self {
        Self {
            commands: AtomicUsize::new(0),
            receivers: AtomicUsize::new(0),
            max_commands,
            max_receivers,
        }
    }
    /// Acquires a short-operation slot without waiting or issuing I/O.
    ///
    /// # Returns
    ///
    /// A permit retaining this shared budget until operation completion or
    /// cancellation.
    ///
    /// # Errors
    ///
    /// Returns `ResourceLimit { resource: "commands" }` when admission is full.
    pub(crate) fn try_command(self: &Arc<Self>) -> Result<CommandPermit, RedisProviderError> {
        acquire(&self.commands, self.max_commands, "commands")?;
        Ok(CommandPermit {
            budget: Arc::clone(self),
        })
    }
    /// Acquires a receiver slot immediately; setup failures release it through
    /// Drop.
    ///
    /// # Returns
    ///
    /// A permit retained until setup failure, receiver close, or receiver drop.
    ///
    /// # Errors
    ///
    /// Returns `ResourceLimit { resource: "receivers" }` when admission is
    /// full.
    pub(crate) fn try_receiver(self: &Arc<Self>) -> Result<ReceiverPermit, RedisProviderError> {
        acquire(&self.receivers, self.max_receivers, "receivers")?;
        Ok(ReceiverPermit {
            budget: Arc::clone(self),
        })
    }
}
/// Atomically reserves one slot without a waiting queue or exceeding `limit`.
///
/// # Parameters
///
/// - `counter`: Shared number of live permits.
/// - `limit`: Positive validated admission cap.
/// - `resource`: Static error category for the exhausted counter.
///
/// # Returns
///
/// Success after exactly one increment; the caller must create its RAII permit.
///
/// # Errors
///
/// Returns resource exhaustion without changing the counter when it is full.
fn acquire(counter: &AtomicUsize, limit: usize, resource: &'static str) -> Result<(), RedisProviderError> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (current < limit).then_some(current + 1)
        })
        .map(|_| ())
        .map_err(|_| RedisProviderError::ResourceLimit { resource })
}
