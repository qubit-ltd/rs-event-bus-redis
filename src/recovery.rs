// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Per-subscription pending-entry cursors and active-delivery tracking.

#[path = "internal/recovery_scan_budget.rs"]
mod recovery_scan_budget;
#[path = "internal/recovery_scan_stage.rs"]
mod recovery_scan_stage;
#[path = "internal/recovery_state.rs"]
mod recovery_state;

pub(crate) use recovery_scan_budget::RecoveryScanBudget;
pub(crate) use recovery_scan_stage::RecoveryScanStage;
pub(crate) use recovery_state::RecoveryState;

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::time::Instant;

    use super::RecoveryScanBudget;
    use super::RecoveryScanStage;
    use super::RecoveryState;
    use super::recovery_scan_budget::MAX_SCAN_COMMANDS_PER_STAGE;
    use super::recovery_scan_budget::MAX_TOMBSTONE_RANGES_PER_ROUND;

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
        for _ in 0..MAX_SCAN_COMMANDS_PER_STAGE {
            assert!(budget.take_recovery_command(RecoveryScanStage::Claim, started));
        }
        assert!(!budget.take_recovery_command(RecoveryScanStage::Claim, started));
        for _ in 0..MAX_SCAN_COMMANDS_PER_STAGE {
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
        for _ in 0..super::recovery_scan_budget::MAX_SCAN_COMMANDS_PER_STAGE {
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
    fn test_tombstone_maintenance_is_bounded_per_round() {
        let started = Instant::now();
        let mut budget = RecoveryScanBudget::new(Duration::MAX, started, Duration::from_secs(1)).unwrap();
        assert!(budget.take_tombstone_probe(started));
        assert!(!budget.take_tombstone_probe(started));
        for _ in 0..MAX_TOMBSTONE_RANGES_PER_ROUND {
            assert!(budget.take_tombstone_range(started));
        }
        assert!(!budget.take_tombstone_range(started));
        for _ in 0..4 {
            assert!(budget.take_maintenance_evaluation(started));
        }
        assert!(!budget.take_maintenance_evaluation(started));
        let next = started + Duration::from_secs(1);
        budget.start_recovery_round(next);
        assert!(budget.take_tombstone_probe(next));
        assert!(budget.take_tombstone_range(next));
        assert!(budget.take_maintenance_evaluation(next));
    }

    #[test]
    fn test_zero_timeout_skips_tombstone_scans_and_allows_one_quarantine() {
        let started = Instant::now();
        let mut budget = RecoveryScanBudget::new(Duration::ZERO, started, Duration::from_secs(1)).unwrap();
        assert!(!budget.take_tombstone_probe(started));
        assert!(!budget.take_tombstone_range(started));
        assert!(budget.take_maintenance_evaluation(started));
        assert!(!budget.take_maintenance_evaluation(started));
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
