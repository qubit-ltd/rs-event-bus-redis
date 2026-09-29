// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared command ordering for synchronous and asynchronous Redis receives.

use std::time::Duration;
use std::time::Instant;

use super::recovery_scan_budget::RecoveryScanBudget;
use super::recovery_scan_stage::RecoveryScanStage;
use crate::error::RedisProviderError;

/// Redis operation selected by the common receive state machine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReceiveAction {
    /// Recover work owned by another consumer.
    Claim,
    /// Continue reading this consumer's pending entries.
    Pending,
    /// Read new group entries, optionally blocking for this many milliseconds.
    ReadNew {
        /// Bounded Redis block duration, or `None` for a zero-timeout read.
        block_ms: Option<usize>,
    },
    /// The receive deadline expired or its zero-timeout reads completed.
    TimedOut,
}

/// Meaning of a completed Redis receive command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReceiveReply {
    /// Claim cursor reached the end of its scan.
    ClaimAtEnd,
    /// Claim returned another page to scan.
    ClaimHasMore,
    /// The pending scan returned one entry.
    PendingEntry,
    /// The pending scan has no more entries.
    PendingEmpty,
    /// A read for new entries returned one entry.
    NewEntry,
    /// A read for new entries returned no entries.
    NewEmpty,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Stage {
    Claim,
    Pending,
    ReadNew,
    TimedOut,
}

/// Shared recovery cursor decision state and command budget for one receive.
pub(crate) struct ReceiveDriver {
    timeout: Duration,
    budget: RecoveryScanBudget,
    stage: Stage,
}

impl ReceiveDriver {
    /// Creates a driver and validates the receive deadline.
    pub(crate) fn new(
        timeout: Duration,
        started: Instant,
        recovery_interval: Duration,
    ) -> Result<Self, RedisProviderError> {
        Ok(Self {
            timeout,
            budget: RecoveryScanBudget::new(timeout, started, recovery_interval)?,
            stage: Stage::Claim,
        })
    }

    /// Selects the next Redis operation using shared recovery and deadline
    /// rules.
    pub(crate) fn next_action(&mut self, now: Instant) -> ReceiveAction {
        loop {
            match self.stage {
                Stage::Claim => {
                    if self.budget.take_recovery_command(RecoveryScanStage::Claim, now) {
                        return ReceiveAction::Claim;
                    }
                    self.stage = Stage::Pending;
                }
                Stage::Pending => {
                    if self.budget.take_recovery_command(RecoveryScanStage::Pending, now) {
                        return ReceiveAction::Pending;
                    }
                    self.stage = Stage::ReadNew;
                }
                Stage::ReadNew => {
                    if self.timeout.is_zero() {
                        if self.budget.can_read_new(now) {
                            return ReceiveAction::ReadNew { block_ms: None };
                        }
                        self.stage = Stage::TimedOut;
                        continue;
                    }
                    if self.budget.recovery_due(now) {
                        self.budget.start_recovery_round(now);
                        self.stage = Stage::Claim;
                        continue;
                    }
                    let Some(interval) = self.budget.block_interval(now) else {
                        self.stage = Stage::TimedOut;
                        continue;
                    };
                    if !self.budget.can_read_new(now) {
                        self.stage = Stage::TimedOut;
                        continue;
                    }
                    let block_ms = interval.as_millis().clamp(1, 1_000) as usize;
                    return ReceiveAction::ReadNew {
                        block_ms: Some(block_ms),
                    };
                }
                Stage::TimedOut => return ReceiveAction::TimedOut,
            }
        }
    }

    /// Advances the shared stage after a Redis operation completes.
    pub(crate) fn reply(&mut self, reply: ReceiveReply) {
        match (self.stage, reply) {
            (Stage::Claim, ReceiveReply::ClaimAtEnd) => self.stage = Stage::Pending,
            (Stage::Claim, ReceiveReply::ClaimHasMore) => {}
            (Stage::Pending, ReceiveReply::PendingEntry) => {}
            (Stage::Pending, ReceiveReply::PendingEmpty) => self.stage = Stage::ReadNew,
            (Stage::ReadNew, ReceiveReply::NewEntry | ReceiveReply::NewEmpty) if self.timeout.is_zero() => {
                self.stage = Stage::TimedOut;
            }
            (Stage::ReadNew, ReceiveReply::NewEntry | ReceiveReply::NewEmpty) => {}
            _ => unreachable!("receive reply does not match the current stage"),
        }
    }

    /// Returns the mutable deadline and maintenance command budget.
    pub(crate) fn budget_mut(&mut self) -> &mut RecoveryScanBudget {
        &mut self.budget
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::time::Instant;

    use super::ReceiveAction;
    use super::ReceiveDriver;
    use super::ReceiveReply;

    #[test]
    fn zero_timeout_scans_claims_pending_and_new_once() {
        let started = Instant::now();
        let mut driver = ReceiveDriver::new(Duration::ZERO, started, Duration::from_secs(1)).unwrap();

        assert_eq!(driver.next_action(started), ReceiveAction::Claim);
        driver.reply(ReceiveReply::ClaimAtEnd);
        assert_eq!(driver.next_action(started), ReceiveAction::Pending);
        driver.reply(ReceiveReply::PendingEmpty);
        assert_eq!(driver.next_action(started), ReceiveAction::ReadNew { block_ms: None });
        driver.reply(ReceiveReply::NewEmpty);
        assert_eq!(driver.next_action(started), ReceiveAction::TimedOut);
    }

    #[test]
    fn claim_pages_continue_until_the_cursor_reaches_the_end() {
        let started = Instant::now();
        let mut driver = ReceiveDriver::new(Duration::from_secs(1), started, Duration::from_secs(1)).unwrap();

        assert_eq!(driver.next_action(started), ReceiveAction::Claim);
        driver.reply(ReceiveReply::ClaimHasMore);
        assert_eq!(driver.next_action(started), ReceiveAction::Claim);
        driver.reply(ReceiveReply::ClaimAtEnd);
        assert_eq!(driver.next_action(started), ReceiveAction::Pending);
    }

    #[test]
    fn blocking_reads_use_bounded_intervals_and_restart_recovery_when_due() {
        let started = Instant::now();
        let interval = Duration::from_millis(50);
        let mut driver = ReceiveDriver::new(Duration::MAX, started, interval).unwrap();

        assert_eq!(driver.next_action(started), ReceiveAction::Claim);
        driver.reply(ReceiveReply::ClaimAtEnd);
        assert_eq!(driver.next_action(started), ReceiveAction::Pending);
        driver.reply(ReceiveReply::PendingEmpty);
        assert_eq!(
            driver.next_action(started),
            ReceiveAction::ReadNew { block_ms: Some(50) }
        );
        driver.reply(ReceiveReply::NewEmpty);
        assert_eq!(driver.next_action(started + interval), ReceiveAction::Claim);
    }
}
