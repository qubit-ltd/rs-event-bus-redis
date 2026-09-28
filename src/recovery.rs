// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Per-subscription pending-entry cursors and active-delivery tracking.

use std::collections::HashSet;
use std::time::Duration;
use std::time::Instant;

/// Maximum recovery commands one receive call may issue before yielding.
pub(crate) const MAX_RECOVERY_COMMANDS_PER_RECEIVE: usize = 16;

/// Which Redis recovery scan is consuming a receive-call command budget.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryScanStage {
    /// Scan pending entries eligible for claim by this consumer.
    Claim,
    /// Revisit pending entries already owned by this consumer.
    Pending,
}

/// Deadline and bounded scan accounting shared by sync and async receivers.
pub(crate) struct RecoveryScanBudget {
    deadline: Option<Instant>,
    zero_timeout: bool,
    command_count: usize,
    zero_claim_used: bool,
    zero_pending_used: bool,
}

impl RecoveryScanBudget {
    /// Starts a receive budget; `Duration::MAX` represents an unbounded wait.
    pub(crate) fn new(timeout: Duration, started: Instant) -> Self {
        let deadline = (timeout != Duration::MAX)
            .then(|| started.checked_add(timeout))
            .flatten();
        Self {
            deadline,
            zero_timeout: timeout.is_zero(),
            command_count: 0,
            zero_claim_used: false,
            zero_pending_used: false,
        }
    }

    /// Consumes one recovery command if the stage, deadline, and shared limit
    /// allow it.
    pub(crate) fn take_recovery_command(&mut self, stage: RecoveryScanStage, now: Instant) -> bool {
        if self.command_count >= MAX_RECOVERY_COMMANDS_PER_RECEIVE {
            return false;
        }
        if self.deadline.is_some_and(|deadline| now >= deadline) {
            if !self.zero_timeout {
                return false;
            }
            let used = match stage {
                RecoveryScanStage::Claim => &mut self.zero_claim_used,
                RecoveryScanStage::Pending => &mut self.zero_pending_used,
            };
            if *used {
                return false;
            }
            *used = true;
        }
        self.command_count += 1;
        true
    }

    /// Returns whether a Redis new-message read can still start within the
    /// deadline.
    pub(crate) fn can_read_new(&self, now: Instant) -> bool {
        self.zero_timeout || self.deadline.is_none_or(|deadline| now < deadline)
    }

    /// Returns the bounded block interval for the next Redis read.
    pub(crate) fn block_interval(&self, now: Instant) -> Option<Duration> {
        let remaining = self.deadline.map(|deadline| deadline.saturating_duration_since(now));
        match remaining {
            Some(remaining) if remaining.is_zero() => None,
            Some(remaining) => Some(remaining.min(Duration::from_secs(1))),
            None => Some(Duration::from_secs(1)),
        }
    }
}

/// Tracks unsettled Redis IDs and the cursors used to revisit consumer state.
pub(crate) struct RecoveryState {
    /// IDs already handed to the local facade and not yet settled.
    active: HashSet<String>,
    /// Last ID visited while scanning this consumer's pending entries.
    pending_cursor: String,
    /// Resume cursor used by `XAUTOCLAIM` to inspect reclaimable PEL entries.
    claim_cursor: String,
}

impl RecoveryState {
    /// Creates empty receiver state with both Redis scans positioned at the
    /// start.
    pub(crate) fn new() -> Self {
        Self {
            active: HashSet::new(),
            pending_cursor: "0-0".to_owned(),
            claim_cursor: "0-0".to_owned(),
        }
    }

    /// Returns whether this stream ID is not already being handled locally.
    pub(crate) fn can_deliver(&self, id: &str) -> bool {
        !self.active.contains(id)
    }

    /// Marks an ID active if it is new and the receiver still has capacity.
    pub(crate) fn mark_delivered(&mut self, id: String, limit: usize) -> bool {
        if self.active.contains(&id) || self.active.len() >= limit {
            return false;
        }
        self.active.insert(id);
        true
    }

    /// Makes a retried ID eligible for the next pending scan.
    pub(crate) fn mark_retry(&mut self, id: &str) {
        self.active.remove(id);
        self.pending_cursor = "0-0".to_owned();
    }

    /// Releases a terminally settled ID from the local delivery bound.
    pub(crate) fn mark_terminal(&mut self, id: &str) {
        self.active.remove(id);
    }

    /// Returns the number of active local deliveries.
    pub(crate) fn active_len(&self) -> usize {
        self.active.len()
    }

    /// Borrows the cursor for this consumer's pending-entry scan.
    pub(crate) fn pending_cursor(&self) -> &str {
        &self.pending_cursor
    }

    /// Advances the pending cursor after a successful Redis response.
    pub(crate) fn set_pending_cursor(&mut self, cursor: String) {
        self.pending_cursor = cursor;
    }

    /// Borrows the cursor for reclaiming entries owned by inactive consumers.
    pub(crate) fn claim_cursor(&self) -> &str {
        &self.claim_cursor
    }

    /// Advances the claim cursor after a successful Redis response.
    pub(crate) fn set_claim_cursor(&mut self, cursor: String) {
        self.claim_cursor = cursor;
    }

    /// Restarts a pending scan when a local retry makes an earlier ID eligible.
    pub(crate) fn reset_pending_scan(&mut self) {
        self.pending_cursor = "0-0".to_owned();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::time::Instant;

    use super::MAX_RECOVERY_COMMANDS_PER_RECEIVE;
    use super::RecoveryScanBudget;
    use super::RecoveryScanStage;
    use super::RecoveryState;

    #[test]
    fn test_active_delivery_bound_and_retry_release() {
        let mut state = RecoveryState::new();
        assert!(state.mark_delivered("1-0".into(), 2));
        assert!(state.mark_delivered("2-0".into(), 2));
        assert!(!state.mark_delivered("3-0".into(), 2));
        assert!(!state.can_deliver("1-0"));

        state.mark_retry("1-0");
        assert!(state.can_deliver("1-0"));
        assert_eq!(state.active_len(), 1);
        assert_eq!(state.pending_cursor(), "0-0");
    }

    #[test]
    fn test_recovery_cursors_advance_and_retry_restarts_pending_scan() {
        let mut state = RecoveryState::new();
        assert_eq!(state.pending_cursor(), "0-0");
        assert_eq!(state.claim_cursor(), "0-0");

        state.set_pending_cursor("2-0".into());
        state.set_claim_cursor("3-0".into());
        assert_eq!(state.pending_cursor(), "2-0");
        assert_eq!(state.claim_cursor(), "3-0");

        state.reset_pending_scan();
        assert_eq!(state.pending_cursor(), "0-0");
        assert_eq!(state.claim_cursor(), "3-0");
    }

    #[test]
    fn test_recovery_scan_budget_caps_commands_per_receive() {
        let started = Instant::now();
        let mut budget = RecoveryScanBudget::new(Duration::MAX, started);
        for _ in 0..MAX_RECOVERY_COMMANDS_PER_RECEIVE {
            assert!(budget.take_recovery_command(RecoveryScanStage::Claim, started));
        }
        assert!(!budget.take_recovery_command(RecoveryScanStage::Pending, started));
    }

    #[test]
    fn test_zero_timeout_allows_one_command_per_recovery_stage() {
        let started = Instant::now();
        let mut budget = RecoveryScanBudget::new(Duration::ZERO, started);
        assert!(budget.take_recovery_command(RecoveryScanStage::Claim, started));
        assert!(!budget.take_recovery_command(RecoveryScanStage::Claim, started));
        assert!(budget.take_recovery_command(RecoveryScanStage::Pending, started));
        assert!(!budget.take_recovery_command(RecoveryScanStage::Pending, started));
        assert!(budget.can_read_new(started));
    }

    #[test]
    fn test_finite_deadline_stops_recovery_and_new_reads() {
        let started = Instant::now();
        let deadline = started + Duration::from_millis(10);
        let mut budget = RecoveryScanBudget::new(Duration::from_millis(10), started);
        assert!(budget.take_recovery_command(RecoveryScanStage::Claim, started));
        assert!(!budget.take_recovery_command(RecoveryScanStage::Pending, deadline));
        assert!(!budget.can_read_new(deadline));
        assert_eq!(budget.block_interval(deadline), None);
    }

    #[test]
    fn test_max_timeout_uses_bounded_blocking_intervals_without_deadline() {
        let started = Instant::now();
        let budget = RecoveryScanBudget::new(Duration::MAX, started);
        assert!(budget.can_read_new(started));
        assert_eq!(budget.block_interval(started), Some(Duration::from_secs(1)));
    }
}
