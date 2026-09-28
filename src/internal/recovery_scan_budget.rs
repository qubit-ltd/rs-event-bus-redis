// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Receive deadlines and bounded Redis recovery command accounting.

use std::time::Duration;
use std::time::Instant;

use super::recovery_scan_stage::RecoveryScanStage;
use crate::error::RedisProviderError;

/// Maximum recovery scan commands issued during one recovery round.
pub(crate) const MAX_SCAN_COMMANDS_PER_ROUND: usize = 16;
pub(crate) const MAX_SCAN_COMMANDS_PER_STAGE: usize = MAX_SCAN_COMMANDS_PER_ROUND / 2;
pub(crate) const MAX_TOMBSTONE_RANGES_PER_ROUND: usize = 4;
const MAX_MAINTENANCE_EVALUATIONS_PER_ROUND: usize = 4;

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
    tombstone_probe_used: bool,
    tombstone_ranges: usize,
    maintenance_evaluations: usize,
    zero_quarantine_used: bool,
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
            tombstone_probe_used: false,
            tombstone_ranges: 0,
            maintenance_evaluations: 0,
            zero_quarantine_used: false,
        })
    }

    /// Consumes one recovery command if the stage, deadline, and shared limit
    /// allow it.
    pub(crate) fn take_recovery_command(&mut self, stage: RecoveryScanStage, now: Instant) -> bool {
        if self.command_count >= MAX_SCAN_COMMANDS_PER_ROUND {
            return false;
        }
        let (stage_count, zero_used) = match stage {
            RecoveryScanStage::Claim => (&mut self.claim_commands, &mut self.zero_claim_used),
            RecoveryScanStage::Pending => (&mut self.pending_commands, &mut self.zero_pending_used),
        };
        if *stage_count >= MAX_SCAN_COMMANDS_PER_STAGE {
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

    /// Reserves the single Redis 6.2 tombstone PEL probe in this round.
    pub(crate) fn take_tombstone_probe(&mut self, now: Instant) -> bool {
        if self.zero_timeout || !self.within_deadline(now) || self.tombstone_probe_used {
            return false;
        }
        self.tombstone_probe_used = true;
        true
    }

    /// Reserves one of four Redis 6.2 stream-presence checks in this round.
    pub(crate) fn take_tombstone_range(&mut self, now: Instant) -> bool {
        if self.zero_timeout
            || !self.within_deadline(now)
            || !self.tombstone_probe_used
            || self.tombstone_ranges >= MAX_TOMBSTONE_RANGES_PER_ROUND
        {
            return false;
        }
        self.tombstone_ranges += 1;
        true
    }

    /// Reserves a quarantine or tombstone Lua command against the shared EVAL
    /// limit. A zero-timeout receive may quarantine one malformed entry.
    pub(crate) fn take_maintenance_evaluation(&mut self, now: Instant) -> bool {
        if !self.within_deadline(now) {
            return false;
        }
        if self.zero_timeout {
            if self.zero_quarantine_used {
                return false;
            }
            self.zero_quarantine_used = true;
            return true;
        }
        if self.maintenance_evaluations >= MAX_MAINTENANCE_EVALUATIONS_PER_ROUND {
            return false;
        }
        self.maintenance_evaluations += 1;
        true
    }

    /// Returns whether a maintenance command starts before the receive
    /// deadline.
    fn within_deadline(&self, now: Instant) -> bool {
        self.zero_timeout || self.deadline.is_none_or(|deadline| now < deadline)
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
        self.tombstone_probe_used = false;
        self.tombstone_ranges = 0;
        self.maintenance_evaluations = 0;
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
