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

use crate::error::RedisProviderError;

/// Maximum recovery commands one receive call may issue before yielding.
pub(crate) const MAX_RECOVERY_COMMANDS_PER_RECEIVE: usize = 16;
const MAX_RECOVERY_COMMANDS_PER_STAGE: usize = MAX_RECOVERY_COMMANDS_PER_RECEIVE / 2;

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
    recovery_interval: Duration,
    next_recovery: Instant,
    claim_commands: usize,
    pending_commands: usize,
}

impl RecoveryScanBudget {
    /// Starts a receive budget; `Duration::MAX` represents an unbounded wait.
    pub(crate) fn new(
        timeout: Duration,
        started: Instant,
        recovery_interval: Duration,
    ) -> Result<Self, RedisProviderError> {
        let deadline = if timeout == Duration::MAX {
            None
        } else {
            Some(
                started
                    .checked_add(timeout)
                    .ok_or(RedisProviderError::Configuration("receive timeout is out of range"))?,
            )
        };
        let next_recovery = started
            .checked_add(recovery_interval)
            .ok_or(RedisProviderError::Configuration("recovery interval is out of range"))?;
        Ok(Self {
            deadline,
            zero_timeout: timeout.is_zero(),
            command_count: 0,
            zero_claim_used: false,
            zero_pending_used: false,
            recovery_interval,
            next_recovery,
            claim_commands: 0,
            pending_commands: 0,
        })
    }

    /// Consumes one recovery command if the stage, deadline, and shared limit
    /// allow it.
    pub(crate) fn take_recovery_command(&mut self, stage: RecoveryScanStage, now: Instant) -> bool {
        if self.command_count >= MAX_RECOVERY_COMMANDS_PER_RECEIVE {
            return false;
        }
        let (stage_count, zero_used) = match stage {
            RecoveryScanStage::Claim => (&mut self.claim_commands, &mut self.zero_claim_used),
            RecoveryScanStage::Pending => (&mut self.pending_commands, &mut self.zero_pending_used),
        };
        if *stage_count >= MAX_RECOVERY_COMMANDS_PER_STAGE {
            return false;
        }
        if self.deadline.is_some_and(|deadline| now >= deadline) {
            if !self.zero_timeout {
                return false;
            }
            if *zero_used {
                return false;
            }
            *zero_used = true;
        }
        self.command_count += 1;
        *stage_count += 1;
        true
    }

    /// Returns whether another bounded recovery round is due.
    pub(crate) fn recovery_due(&self, now: Instant) -> bool {
        !self.zero_timeout && now >= self.next_recovery
    }

    /// Starts the next recovery round and resets its scan-command quotas.
    pub(crate) fn start_recovery_round(&mut self, now: Instant) {
        self.command_count = 0;
        self.claim_commands = 0;
        self.pending_commands = 0;
        self.next_recovery = now.checked_add(self.recovery_interval).unwrap_or(now);
    }

    /// Returns whether a Redis new-message read can still start within the
    /// deadline.
    pub(crate) fn can_read_new(&self, now: Instant) -> bool {
        self.zero_timeout || self.deadline.is_none_or(|deadline| now < deadline)
    }

    /// Returns the bounded block interval for the next Redis read.
    pub(crate) fn block_interval(&self, now: Instant) -> Option<Duration> {
        let remaining = self.deadline.map(|deadline| deadline.saturating_duration_since(now));
        let recovery_delay = self.next_recovery.saturating_duration_since(now);
        match remaining {
            Some(remaining) if remaining.is_zero() => None,
            Some(remaining) => Some(remaining.min(recovery_delay).min(Duration::from_secs(1))),
            None => Some(recovery_delay.min(Duration::from_secs(1))),
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
    /// One claimed entry held while a deleted-entry gap is reported.
    deferred_claim: Option<redis::streams::StreamId>,
}

impl RecoveryState {
    /// Creates empty receiver state with both Redis scans positioned at the
    /// start.
    pub(crate) fn new() -> Self {
        Self {
            active: HashSet::new(),
            pending_cursor: "0-0".to_owned(),
            claim_cursor: "0-0".to_owned(),
            deferred_claim: None,
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

    /// Holds one claimed entry until a higher-priority gap outcome is returned.
    pub(crate) fn defer_claim(&mut self, entry: redis::streams::StreamId) {
        self.deferred_claim = Some(entry);
    }

    /// Takes the single entry held behind a gap outcome.
    pub(crate) fn take_deferred_claim(&mut self) -> Option<redis::streams::StreamId> {
        self.deferred_claim.take()
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

    use super::MAX_RECOVERY_COMMANDS_PER_STAGE;
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
        let mut budget = RecoveryScanBudget::new(Duration::MAX, started, Duration::from_secs(1)).unwrap();
        for _ in 0..MAX_RECOVERY_COMMANDS_PER_STAGE {
            assert!(budget.take_recovery_command(RecoveryScanStage::Claim, started));
        }
        assert!(!budget.take_recovery_command(RecoveryScanStage::Claim, started));
        for _ in 0..MAX_RECOVERY_COMMANDS_PER_STAGE {
            assert!(budget.take_recovery_command(RecoveryScanStage::Pending, started));
        }
        assert!(!budget.take_recovery_command(RecoveryScanStage::Pending, started));
    }

    #[test]
    fn test_zero_timeout_allows_one_command_per_recovery_stage() {
        let started = Instant::now();
        let mut budget = RecoveryScanBudget::new(Duration::ZERO, started, Duration::from_secs(1)).unwrap();
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
        let mut budget = RecoveryScanBudget::new(Duration::from_millis(10), started, Duration::from_secs(1)).unwrap();
        assert!(budget.take_recovery_command(RecoveryScanStage::Claim, started));
        assert!(!budget.take_recovery_command(RecoveryScanStage::Pending, deadline));
        assert!(!budget.can_read_new(deadline));
        assert_eq!(budget.block_interval(deadline), None);
    }

    #[test]
    fn test_max_timeout_uses_bounded_blocking_intervals_without_deadline() {
        let started = Instant::now();
        let budget = RecoveryScanBudget::new(Duration::MAX, started, Duration::from_secs(1)).unwrap();
        assert!(budget.can_read_new(started));
        assert_eq!(budget.block_interval(started), Some(Duration::from_secs(1)));
    }

    #[test]
    fn test_recovery_budget_resets_per_interval_and_rejects_overflow() {
        let started = Instant::now();
        let mut budget = RecoveryScanBudget::new(Duration::MAX, started, Duration::from_millis(50)).unwrap();
        for _ in 0..super::MAX_RECOVERY_COMMANDS_PER_STAGE {
            assert!(budget.take_recovery_command(RecoveryScanStage::Claim, started));
        }
        assert!(!budget.take_recovery_command(RecoveryScanStage::Claim, started));
        let later = started + Duration::from_millis(50);
        assert!(budget.recovery_due(later));
        budget.start_recovery_round(later);
        assert!(budget.take_recovery_command(RecoveryScanStage::Claim, later));

        assert!(RecoveryScanBudget::new(Duration::from_secs(u64::MAX), started, Duration::from_secs(1)).is_err());
    }

    #[test]
    fn test_gap_preserves_one_claimed_entry_for_the_next_receive() {
        let mut state = RecoveryState::new();
        let entry = redis::streams::StreamId {
            id: "4-0".into(),
            map: Default::default(),
        };
        state.defer_claim(entry);
        assert_eq!(
            state.take_deferred_claim().map(|entry| entry.id.as_str().to_owned()),
            Some("4-0".to_owned())
        );
        assert!(state.take_deferred_claim().is_none());
    }
}
