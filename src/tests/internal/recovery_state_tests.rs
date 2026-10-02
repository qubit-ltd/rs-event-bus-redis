// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Active delivery bounds and cross-call recovery cursor state.

use redis::Value;
use redis::streams::StreamId;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use crate::internal::RecoveryGuard;
use crate::internal::RecoveryState;
use crate::internal::ReceiveAction;
use crate::internal::ReceiveDriver;
use crate::internal::ReceiveReply;

/// A short receive that finishes claim but runs out of time before pending
/// must continue at pending on the next call, then enter the new-entry path.
#[test]
fn test_short_receive_continues_completed_claim_across_calls() {
    let started = Instant::now();
    let timeout = Duration::from_millis(1);
    let interval = Duration::from_secs(1);
    let mut state = RecoveryState::new();
    let (due, next, generation) = state.recovery_schedule(started);
    let mut first = ReceiveDriver::new(timeout, started, interval, due, next).unwrap();
    assert_eq!(first.next_action(started), ReceiveAction::Claim);
    first.reply(ReceiveReply::ClaimAtEnd);
    state.mark_claim_complete(generation);
    assert_eq!(
        first.next_action(started + timeout),
        ReceiveAction::TimedOut
    );

    let resumed_at = started + timeout + Duration::from_millis(1);
    let (due, next, generation) = state.recovery_schedule(resumed_at);
    assert!(due);
    let mut second = ReceiveDriver::new(timeout, resumed_at, interval, due, next).unwrap();
    if state.claim_phase_complete() {
        second.resume_pending();
    }
    assert_eq!(second.next_action(resumed_at), ReceiveAction::Pending);
    second.reply(ReceiveReply::PendingEmpty);
    let next = second
        .complete_recovery_round(resumed_at)
        .expect("complete round");
    state.complete_recovery_at(next, generation);
    let next_call_at = resumed_at + Duration::from_nanos(1);
    let (due, next, _) = state.recovery_schedule(next_call_at);
    assert!(!due);
    let mut third = ReceiveDriver::new(timeout, next_call_at, interval, due, next).unwrap();
    assert!(matches!(
        third.next_action(next_call_at),
        ReceiveAction::ReadNew { .. }
    ));
}

/// A retry or cancelled receive invalidates a saved claim-complete phase.
#[test]
fn test_retry_and_cancel_reset_saved_claim_phase() {
    let state = Arc::new(Mutex::new(RecoveryState::new()));
    {
        let mut recovery = state.lock().unwrap();
        recovery.mark_claim_complete(0);
        assert!(recovery.claim_phase_complete());
        recovery.mark_retry("1-0");
        assert!(!recovery.claim_phase_complete());
        recovery.mark_claim_complete(0);
        assert!(
            !recovery.claim_phase_complete(),
            "a stale claim response cannot undo a concurrent retry"
        );
        let generation = recovery.recovery_schedule(Instant::now()).2;
        recovery.mark_claim_complete(generation);
        assert!(recovery.claim_phase_complete());
    }
    drop(RecoveryGuard::new(Arc::clone(&state)));
    assert!(!state.lock().unwrap().claim_phase_complete());
}

#[test]
fn test_complete_recovery_persists_next_due_and_retry_forces_scan() {
    let mut state = RecoveryState::new();
    let started = Instant::now();
    let (due, _, generation) = state.recovery_schedule(started);
    assert!(due);
    state.complete_recovery_at(started + Duration::from_secs(1), generation);
    let (due, _, _) = state.recovery_schedule(started);
    assert!(!due);
    let (due, _, _) = state.recovery_schedule(started + Duration::from_secs(1));
    assert!(due);
    state.mark_retry("missing-entry");
    let (due, _, _) = state.recovery_schedule(started);
    assert!(due);
}

#[test]
fn test_retry_during_recovery_cannot_be_cleared_by_stale_completion() {
    let mut state = RecoveryState::new();
    let started = Instant::now();
    let (_, _, generation) = state.recovery_schedule(started);
    state.begin_recovery();
    state.mark_retry("concurrent-retry");
    state.complete_recovery_at(started + Duration::from_secs(1), generation);
    let (due, _, _) = state.recovery_schedule(started);
    assert!(
        due,
        "new retry obligation survives an older scan completion"
    );
}

#[test]
fn test_receive_recovery_guard_forces_only_if_armed() {
    let state = Arc::new(Mutex::new(RecoveryState::new()));
    let started = Instant::now();
    state
        .lock()
        .expect("state")
        .complete_recovery_at(started + Duration::from_secs(1), 0);
    {
        let _cancelled = RecoveryGuard::new(Arc::clone(&state));
    }
    let (due, _, generation) = state.lock().expect("state").recovery_schedule(started);
    assert!(due);
    state
        .lock()
        .expect("state")
        .complete_recovery_at(started + Duration::from_secs(1), generation);
    {
        let mut completed = RecoveryGuard::new(Arc::clone(&state));
        completed.disarm();
    }
    let (due, _, _) = state.lock().expect("state").recovery_schedule(started);
    assert!(!due);
}

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
fn test_gap_preserves_one_claimed_entry_for_the_next_receive() {
    let mut state = RecoveryState::new();
    let entry = StreamId {
        id: "4-0".into(),
        map: Default::default(),
    };
    state.defer_claim(entry);
    assert_eq!(
        state
            .take_deferred_claim()
            .map(|entry| entry.id.as_str().to_owned()),
        Some("4-0".to_owned())
    );
    assert!(state.take_deferred_claim().is_none());
}

#[test]
fn test_tombstone_scan_progress_and_reset_preserve_gap_debt_and_delivery_capacity() {
    let mut state = RecoveryState::new();
    assert!(state.mark_delivered("1-0".into(), 1));
    state.set_pending_cursor("2-0".into());
    state.set_claim_cursor("9-0".into());
    state.set_tombstone_cursor("4-0".into());
    let wire = b"claimed-wire".to_vec();
    let wire_pointer = wire.as_ptr();
    state.defer_claim(StreamId {
        id: "6-0".into(),
        map: [("wire".into(), Value::BulkString(wire))].into(),
    });

    // A bounded tombstone page retains its position while a claim waits behind a
    // Gap.
    assert_eq!(state.tombstone_cursor(), "4-0");
    assert_eq!(state.pending_cursor(), "2-0");
    assert_eq!(state.claim_cursor(), "9-0");
    assert_eq!(state.active_len(), 1);
    assert!(!state.can_deliver("1-0"));

    // Retry restarts own-pending recovery without discarding other scan progress.
    state.mark_retry("1-0");
    assert_eq!(state.pending_cursor(), "0-0");
    assert_eq!(state.tombstone_cursor(), "4-0");
    assert_eq!(state.claim_cursor(), "9-0");
    assert_eq!(state.active_len(), 0);

    // The final tombstone page resets only that scan, preserving the deferred
    // claim.
    state.set_tombstone_cursor("5-0".into());
    state.reset_tombstone_cursor();
    assert_eq!(state.tombstone_cursor(), "0-0");
    assert_eq!(state.pending_cursor(), "0-0");
    assert_eq!(state.claim_cursor(), "9-0");
    let mut deferred = state
        .take_deferred_claim()
        .expect("claim survives the Gap and scan reset");
    assert_eq!(deferred.id, "6-0");
    let Some(Value::BulkString(wire)) = deferred.map.remove("wire") else {
        panic!("deferred claim retains its original wire");
    };
    assert_eq!(wire, b"claimed-wire");
    assert_eq!(
        wire.as_ptr(),
        wire_pointer,
        "scan transitions do not copy the deferred wire"
    );
    assert!(state.can_deliver(&deferred.id));
    assert!(
        state.mark_delivered(deferred.id, 1),
        "retry released capacity for the deferred delivery"
    );
    assert_eq!(state.active_len(), 1);
    assert!(
        state.take_deferred_claim().is_none(),
        "the next delivery consumes the Gap debt once"
    );
}
