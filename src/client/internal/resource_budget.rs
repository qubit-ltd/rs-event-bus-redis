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

use super::command_permit::CommandLane;
use super::command_permit::CommandPermit;
use super::receiver_permit::ReceiverPermit;
use crate::error::RedisProviderError;

/// Classifies a short Redis command by its admission priority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommandClass {
    /// Publishing, subscription setup, and other ordinary short commands.
    General,
    /// Terminal acknowledgement; may use a reserved or general slot.
    Settlement,
}

/// Counts application operations; cancelled Redis requests may still complete.
pub(crate) struct ResourceBudget {
    /// General-lane operations currently admitted, including awaited async I/O.
    pub(super) commands: AtomicUsize,
    /// Terminal acknowledgements admitted from the reserved lane.
    pub(super) reserved_settlements: AtomicUsize,
    /// Receiver leases currently retained through setup, receive, close, or
    /// drop.
    pub(super) receivers: AtomicUsize,
    /// Inclusive cap for ordinary short operations.
    max_commands: usize,
    /// Inclusive cap reserved for terminal acknowledgements.
    max_reserved_settlements: usize,
    /// Inclusive cap for active receiver leases, separate from unsettled
    /// tokens.
    max_receivers: usize,
}
impl ResourceBudget {
    /// Reads occupied general-lane slots, including general-lane settlements.
    #[must_use]
    pub(crate) fn general_in_flight(&self) -> u64 {
        u64::try_from(self.commands.load(Ordering::Relaxed)).unwrap_or(u64::MAX)
    }

    /// Reads occupied reserved settlement slots.
    #[must_use]
    pub(crate) fn settlement_in_flight(&self) -> u64 {
        u64::try_from(self.reserved_settlements.load(Ordering::Relaxed)).unwrap_or(u64::MAX)
    }

    /// Reads active receiver leases.
    #[must_use]
    pub(crate) fn active_receivers(&self) -> u64 {
        u64::try_from(self.receivers.load(Ordering::Relaxed)).unwrap_or(u64::MAX)
    }

    /// Creates empty admission counters with validated finite caps.
    ///
    /// # Parameters
    ///
    /// - `max_commands`: Validated total short-operation cap.
    /// - `reserved_settlements`: Slots reserved for terminal acknowledgements.
    /// - `max_receivers`: Positive validated active-receiver cap.
    ///
    /// # Returns
    ///
    /// Zeroed counters; no queue, connection, or network work is created.
    #[must_use]
    #[inline]
    pub(crate) fn new(max_commands: usize, reserved_settlements: usize, max_receivers: usize) -> Self {
        Self {
            commands: AtomicUsize::new(0),
            reserved_settlements: AtomicUsize::new(0),
            receivers: AtomicUsize::new(0),
            max_commands: max_commands - reserved_settlements,
            max_reserved_settlements: reserved_settlements,
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
    pub(crate) fn try_command(self: &Arc<Self>, class: CommandClass) -> Result<CommandPermit, RedisProviderError> {
        let lane = match class {
            CommandClass::General => {
                acquire(&self.commands, self.max_commands, "commands")?;
                CommandLane::General
            }
            CommandClass::Settlement => {
                if acquire(&self.reserved_settlements, self.max_reserved_settlements, "commands").is_ok() {
                    CommandLane::ReservedSettlement
                } else {
                    acquire(&self.commands, self.max_commands, "commands")?;
                    CommandLane::General
                }
            }
        };
        Ok(CommandPermit {
            budget: Arc::clone(self),
            lane,
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

#[cfg(test)]
mod tests {
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::atomic::Ordering;
    use std::thread;

    use super::CommandClass;
    use super::ResourceBudget;
    use crate::error::RedisProviderError;

    /// Verifies that general work cannot consume capacity reserved for ACKs.
    #[test]
    fn test_reserved_settlement_lane_survives_general_saturation() {
        let budget = Arc::new(ResourceBudget::new(4, 1, 4));
        let general: Vec<_> = (0..3)
            .map(|_| budget.try_command(CommandClass::General).expect("general permit"))
            .collect();
        assert!(matches!(
            budget.try_command(CommandClass::General),
            Err(RedisProviderError::ResourceLimit { resource: "commands" })
        ));
        let settlement = budget
            .try_command(CommandClass::Settlement)
            .expect("reserved settlement permit");
        assert!(matches!(
            budget.try_command(CommandClass::Settlement),
            Err(RedisProviderError::ResourceLimit { resource: "commands" })
        ));
        drop(settlement);
        drop(general);
        assert_eq!(budget.commands.load(Ordering::Acquire), 0);
        assert_eq!(budget.reserved_settlements.load(Ordering::Acquire), 0);
    }

    /// Keeps every contender alive until all have attempted general admission.
    #[test]
    fn test_general_lane_remains_bounded_under_cross_thread_contention() {
        let budget = Arc::new(ResourceBudget::new(4, 1, 8));
        let start = Arc::new(Barrier::new(17));
        let attempted = Arc::new(Barrier::new(17));
        let workers: Vec<_> = (0..16)
            .map(|_| {
                let budget = Arc::clone(&budget);
                let start = Arc::clone(&start);
                let attempted = Arc::clone(&attempted);
                thread::spawn(move || {
                    start.wait();
                    let permit = budget.try_command(CommandClass::General).ok();
                    attempted.wait();
                    permit.is_some()
                })
            })
            .collect();
        start.wait();
        attempted.wait();
        let admitted = workers
            .into_iter()
            .map(|worker| worker.join().expect("worker"))
            .filter(|ok| *ok)
            .count();
        assert_eq!(admitted, 3, "only the unreserved general lane admits work");
        assert_eq!(budget.commands.load(Ordering::Acquire), 0);
    }

    /// Panic unwinding drops the permit and restores its exact lane.
    #[test]
    fn test_reserved_settlement_permit_released_by_panic_unwind() {
        let budget = Arc::new(ResourceBudget::new(2, 1, 1));
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _permit = budget.try_command(CommandClass::Settlement).expect("reserved permit");
            panic!("intentional unwind after admission");
        }));
        assert!(result.is_err());
        assert_eq!(budget.reserved_settlements.load(Ordering::Acquire), 0);
        assert!(budget.try_command(CommandClass::Settlement).is_ok());
    }
}
