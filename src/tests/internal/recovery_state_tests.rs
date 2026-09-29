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

use crate::internal::RecoveryState;

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
        state.take_deferred_claim().map(|entry| entry.id.as_str().to_owned()),
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
