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

use super::receive_action::ReceiveAction;
use super::receive_reply::ReceiveReply;
use super::receive_stage::ReceiveStage;
use super::recovery_scan_budget::RecoveryScanBudget;
use super::recovery_scan_stage::RecoveryScanStage;
use crate::error::RedisProviderError;

/// Shared recovery cursor decision state and command budget for one receive.
pub(crate) struct ReceiveDriver {
    /// Requested wait duration; zero permits one read per stage.
    timeout: Duration,
    /// Deadline and per-stage command allowances.
    budget: RecoveryScanBudget,
    /// Phase used for the next command dispatch.
    stage: ReceiveStage,
}

impl ReceiveDriver {
    /// Creates a driver and validates the receive deadline.
    ///
    /// # Parameters
    ///
    /// - `timeout`: Requested receive wait, or `Duration::MAX` for no deadline.
    /// - `started`: Beginning of this receive call.
    /// - `recovery_interval`: Delay between bounded recovery rounds.
    ///
    /// # Returns
    ///
    /// A driver starting with the claim phase.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the finite receive deadline or
    /// `started + recovery_interval` cannot be represented.
    pub(crate) fn new(
        timeout: Duration,
        started: Instant,
        recovery_interval: Duration,
    ) -> Result<Self, RedisProviderError> {
        Ok(Self {
            timeout,
            budget: RecoveryScanBudget::new(timeout, started, recovery_interval)?,
            stage: ReceiveStage::Claim,
        })
    }

    /// Returns the mutable deadline and maintenance command budget.
    ///
    /// # Returns
    ///
    /// The budget used to reserve poison and tombstone maintenance commands.
    #[inline]
    #[must_use]
    pub(crate) fn budget_mut(&mut self) -> &mut RecoveryScanBudget {
        &mut self.budget
    }

    /// Selects the next Redis operation using shared recovery and deadline
    /// rules. Selecting an action reserves its command allowance exactly once.
    ///
    /// # Parameters
    ///
    /// - `now`: Scheduling time used for deadlines and recovery intervals.
    ///
    /// # Returns
    ///
    /// The next command to dispatch or the terminal timeout action.
    pub(crate) fn next_action(&mut self, now: Instant) -> ReceiveAction {
        loop {
            match self.stage {
                ReceiveStage::Claim => {
                    if self.budget.take_recovery_command(RecoveryScanStage::Claim, now) {
                        return ReceiveAction::Claim;
                    }
                    self.stage = ReceiveStage::Pending;
                }
                ReceiveStage::Pending => {
                    if self.budget.take_recovery_command(RecoveryScanStage::Pending, now) {
                        return ReceiveAction::Pending;
                    }
                    self.stage = ReceiveStage::ReadNew;
                }
                ReceiveStage::ReadNew => {
                    if self.timeout.is_zero() {
                        if self.budget.can_read_new(now) {
                            return ReceiveAction::ReadNew { block_ms: None };
                        }
                        self.stage = ReceiveStage::TimedOut;
                        continue;
                    }
                    if self.budget.recovery_due(now) {
                        self.budget.start_recovery_round(now);
                        self.stage = ReceiveStage::Claim;
                        continue;
                    }
                    let Some(interval) = self.budget.block_interval(now) else {
                        self.stage = ReceiveStage::TimedOut;
                        continue;
                    };
                    if !self.budget.can_read_new(now) {
                        self.stage = ReceiveStage::TimedOut;
                        continue;
                    }
                    let block_ms = interval.as_millis().clamp(1, 1_000) as usize;
                    return ReceiveAction::ReadNew {
                        block_ms: Some(block_ms),
                    };
                }
                ReceiveStage::TimedOut => return ReceiveAction::TimedOut,
            }
        }
    }

    /// Advances the shared stage after a Redis operation completes.
    ///
    /// # Parameters
    ///
    /// - `reply`: Classification of the command most recently dispatched.
    ///
    /// # Panics
    ///
    /// Panics if the reply does not belong to the current receive phase.
    pub(crate) fn reply(&mut self, reply: ReceiveReply) {
        match (self.stage, reply) {
            (ReceiveStage::Claim, ReceiveReply::ClaimAtEnd) => self.stage = ReceiveStage::Pending,
            (ReceiveStage::Claim, ReceiveReply::ClaimHasMore) => {}
            (ReceiveStage::Pending, ReceiveReply::PendingEntry) => {}
            (ReceiveStage::Pending, ReceiveReply::PendingEmpty) => self.stage = ReceiveStage::ReadNew,
            (ReceiveStage::ReadNew, ReceiveReply::NewEntry | ReceiveReply::NewEmpty) if self.timeout.is_zero() => {
                self.stage = ReceiveStage::TimedOut;
            }
            (ReceiveStage::ReadNew, ReceiveReply::NewEntry | ReceiveReply::NewEmpty) => {}
            _ => unreachable!("receive reply does not match the current stage"),
        }
    }
}
