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
/// Each claim or own-pending phase may consume half of the shared round quota.
pub(crate) const MAX_SCAN_COMMANDS_PER_STAGE: usize = MAX_SCAN_COMMANDS_PER_ROUND / 2;
/// Maximum Redis 6.2 stream-presence probes reserved during one recovery round.
pub(crate) const MAX_TOMBSTONE_RANGES_PER_ROUND: usize = 4;
/// Maximum quarantine or tombstone Lua evaluations in one finite-wait recovery
/// round.
const MAX_MAINTENANCE_EVALUATIONS_PER_ROUND: usize = 4;

/// Deadline and bounded scan accounting shared by sync and async receivers.
pub(crate) struct RecoveryScanBudget {
    /// `Some` is the finite receive deadline; `None` represents Duration::MAX.
    deadline: Option<Instant>,
    /// Permits exactly one read command per recovery phase without BLOCK.
    zero_timeout: bool,
    /// Total claim and pending commands reserved in the current round.
    command_count: usize,
    /// Whether the zero-timeout claim allowance was already reserved.
    zero_claim_used: bool,
    /// Whether the zero-timeout pending allowance was already reserved.
    zero_pending_used: bool,
    /// Minimum scheduling interval before another bounded recovery round.
    recovery_interval: Duration,
    /// Next instant at which the driver restarts its recovery phases.
    next_recovery: Instant,
    /// Claim-phase commands reserved in this recovery round.
    claim_commands: usize,
    /// Own-pending read commands reserved in this recovery round.
    pending_commands: usize,
    /// Whether this round already reserved its detailed PEL probe.
    tombstone_probe_used: bool,
    /// Redis 6.2 stream-presence checks reserved after that PEL probe.
    tombstone_ranges: usize,
    /// Quarantine or tombstone Lua commands reserved in this recovery round.
    maintenance_evaluations: usize,
    /// Whether the zero-timeout receive already reserved its one poison
    /// evaluation.
    zero_quarantine_used: bool,
}

impl RecoveryScanBudget {
    /// Starts a per-receive budget without issuing Redis commands.
    ///
    /// # Parameters
    ///
    /// - `timeout`: Finite scheduling wait, zero for one read per phase, or
    ///   Duration::MAX.
    /// - `started`: Start instant used for deadline calculation.
    /// - `recovery_interval`: Delay before another recovery round may be
    ///   scheduled.
    ///
    /// # Returns
    ///
    /// Empty recovery and maintenance quotas with a checked deadline.
    ///
    /// # Errors
    ///
    /// Returns a configuration error if a finite deadline or next recovery
    /// instant overflows.
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
                    .ok_or(RedisProviderError::Configuration(
                        "receive timeout is out of range",
                    ))?,
            )
        };
        let next_recovery =
            started
                .checked_add(recovery_interval)
                .ok_or(RedisProviderError::Configuration(
                    "recovery interval is out of range",
                ))?;
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

    /// Checks whether another bounded recovery round may start at `now`.
    ///
    /// # Parameters
    ///
    /// - `now`: Current scheduling instant.
    ///
    /// # Returns
    ///
    /// `true` when the interval elapsed and this is not a zero-timeout receive.
    #[must_use]
    #[inline]
    pub(crate) fn recovery_due(&self, now: Instant) -> bool {
        !self.zero_timeout && now >= self.next_recovery
    }

    /// Restores a subscription's next recovery deadline for this receive.
    ///
    /// `next` is a monotonic instant saved only after a complete prior round.
    pub(crate) fn schedule_recovery_at(&mut self, next: Instant) {
        self.next_recovery = next;
    }

    /// Schedules the next scan after a complete claim and pending round.
    ///
    /// Returns the new deadline for persistence in the subscription. An
    /// overflowing instant remains due immediately rather than losing recovery.
    pub(crate) fn schedule_after_completed_round(&mut self, now: Instant) -> Instant {
        self.next_recovery = now.checked_add(self.recovery_interval).unwrap_or(now);
        self.next_recovery
    }

    /// Checks whether a new-message read may still be scheduled at `now`.
    ///
    /// # Parameters
    ///
    /// - `now`: Current scheduling instant, before sending a new Redis read.
    ///
    /// # Returns
    ///
    /// `true` for zero-timeout or unbounded waits, or before the finite
    /// deadline. The driver separately limits zero-timeout reads to one
    /// dispatch.
    #[must_use]
    #[inline]
    pub(crate) fn can_read_new(&self, now: Instant) -> bool {
        self.zero_timeout || self.deadline.is_none_or(|deadline| now < deadline)
    }

    /// Calculates the next Redis BLOCK duration without reserving a command.
    ///
    /// # Parameters
    ///
    /// - `now`: Current instant used to calculate remaining deadline and
    ///   recovery delay.
    ///
    /// # Returns
    ///
    /// `None` after a finite deadline; `Some` is at most one second and does
    /// not exceed remaining deadline or recovery delay. A zero duration can
    /// occur when recovery is already due; the driver restarts recovery
    /// before BLOCK.
    #[must_use]
    #[inline]
    pub(crate) fn block_interval(&self, now: Instant) -> Option<Duration> {
        let remaining = self
            .deadline
            .map(|deadline| deadline.saturating_duration_since(now));
        let recovery_delay = self.next_recovery.saturating_duration_since(now);
        match remaining {
            Some(remaining) if remaining.is_zero() => None,
            Some(remaining) => Some(remaining.min(recovery_delay).min(Duration::from_secs(1))),
            None => Some(recovery_delay.min(Duration::from_secs(1))),
        }
    }

    /// Reserves one recovery command against shared and phase quotas.
    ///
    /// # Parameters
    ///
    /// - `stage`: Claim or own-pending phase consuming this command.
    /// - `now`: Current instant before starting the command.
    ///
    /// # Returns
    ///
    /// `true` after exactly one reservation, or `false` when the phase,
    /// deadline, or shared limit forbids it. Zero timeout permits one
    /// command per phase.
    #[must_use]
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

    /// Reserves the single Redis 6.2 detailed PEL probe for this round.
    ///
    /// # Parameters
    ///
    /// - `now`: Instant before starting the probe.
    ///
    /// # Returns
    ///
    /// `true` after reservation; `false` for zero timeout, an expired deadline,
    /// or a previously reserved probe. This method does not issue Redis I/O.
    #[must_use]
    pub(crate) fn take_tombstone_probe(&mut self, now: Instant) -> bool {
        if self.zero_timeout || !self.within_deadline(now) || self.tombstone_probe_used {
            return false;
        }
        self.tombstone_probe_used = true;
        true
    }

    /// Reserves one bounded Redis 6.2 stream-presence check after a PEL probe.
    ///
    /// # Parameters
    ///
    /// - `now`: Instant before starting the XRANGE presence check.
    ///
    /// # Returns
    ///
    /// `true` after reservation; `false` without mutation when zero timeout,
    /// deadline, missing probe, or the four-range cap prevents a command.
    #[must_use]
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

    /// Reserves a quarantine or tombstone Lua command without issuing I/O.
    ///
    /// # Parameters
    ///
    /// - `now`: Instant before starting the maintenance command.
    ///
    /// # Returns
    ///
    /// `true` after reservation; `false` when deadline or quota prevents it.
    /// Zero timeout permits one poison evaluation and no tombstone probes.
    #[must_use]
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

    /// Resets shared, phase, and maintenance quotas for the next recovery
    /// round.
    ///
    /// # Parameters
    ///
    /// - `now`: Start of the new round; the next recovery is scheduled after
    ///   its interval.
    ///
    /// If advancing the instant overflows, recovery remains due at `now`.
    /// Zero-timeout phase allowances remain consumed; no Redis command is
    /// issued.
    pub(crate) fn start_recovery_round(&mut self, now: Instant) {
        self.command_count = 0;
        self.claim_commands = 0;
        self.pending_commands = 0;
        self.tombstone_probe_used = false;
        self.tombstone_ranges = 0;
        self.maintenance_evaluations = 0;
        self.next_recovery = now.checked_add(self.recovery_interval).unwrap_or(now);
    }

    /// Checks whether maintenance work may begin at `now` without I/O.
    ///
    /// # Parameters
    ///
    /// - `now`: Instant immediately before reserving maintenance work.
    ///
    /// # Returns
    ///
    /// `true` for zero-timeout or unbounded waits, or strictly before the
    /// finite deadline.
    #[must_use]
    #[inline]
    fn within_deadline(&self, now: Instant) -> bool {
        self.zero_timeout || self.deadline.is_none_or(|deadline| now < deadline)
    }
}
