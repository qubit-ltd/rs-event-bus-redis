// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Pure receive dispatch quota and stage regressions.

use std::time::Duration;
use std::time::Instant;

use crate::internal::ReceiveAction;
use crate::internal::ReceiveDriver;
use crate::internal::ReceiveReply;

/// Zero timeout reserves one command in each read phase.
#[test]
fn test_zero_timeout_scans_claims_pending_and_new_once() {
    let started = Instant::now();
    let mut driver = ReceiveDriver::new(Duration::ZERO, started, Duration::from_secs(1), true, None).unwrap();

    assert_eq!(driver.next_action(started), ReceiveAction::Claim);
    driver.reply(ReceiveReply::ClaimAtEnd);
    assert_eq!(driver.next_action(started), ReceiveAction::Pending);
    driver.reply(ReceiveReply::PendingEmpty);
    assert_eq!(driver.next_action(started), ReceiveAction::ReadNew { block_ms: None });
    driver.reply(ReceiveReply::NewEmpty);
    assert_eq!(driver.next_action(started), ReceiveAction::TimedOut);
}

/// Claim pagination stops when Redis reports the terminal cursor.
#[test]
fn test_claim_pages_continue_until_the_cursor_reaches_the_end() {
    let started = Instant::now();
    let mut driver = ReceiveDriver::new(Duration::from_secs(1), started, Duration::from_secs(1), true, None).unwrap();

    assert_eq!(driver.next_action(started), ReceiveAction::Claim);
    driver.reply(ReceiveReply::ClaimHasMore);
    assert_eq!(driver.next_action(started), ReceiveAction::Claim);
    driver.reply(ReceiveReply::ClaimAtEnd);
    assert_eq!(driver.next_action(started), ReceiveAction::Pending);
}

/// Blocking reads remain bounded and schedule recovery at its interval.
#[test]
fn test_blocking_reads_use_bounded_intervals_and_restart_recovery_when_due() {
    let started = Instant::now();
    let interval = Duration::from_millis(50);
    let mut driver = ReceiveDriver::new(Duration::MAX, started, interval, true, None).unwrap();

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

/// Eight claim pages leave all eight pending command allowances available.
#[test]
fn test_claim_quota_preserves_all_eight_pending_dispatches() {
    let started = Instant::now();
    let mut driver = ReceiveDriver::new(Duration::from_secs(1), started, Duration::from_secs(1), true, None).unwrap();
    for _ in 0..8 {
        assert_eq!(driver.next_action(started), ReceiveAction::Claim);
        driver.reply(ReceiveReply::ClaimHasMore);
    }
    for _ in 0..8 {
        assert_eq!(driver.next_action(started), ReceiveAction::Pending);
        driver.reply(ReceiveReply::PendingEntry);
    }
    assert_eq!(
        driver.next_action(started),
        ReceiveAction::ReadNew { block_ms: Some(1_000) }
    );
}

/// Completed recovery keeps later zero and finite receives on the new-entry
/// fast path until the subscription clock expires.
#[test]
fn test_recovery_clock_is_shared_across_receive_drivers() {
    let started = Instant::now();
    let interval = Duration::from_millis(50);
    let mut first = ReceiveDriver::new(Duration::ZERO, started, interval, true, None).expect("initial driver");
    assert_eq!(first.next_action(started), ReceiveAction::Claim);
    first.reply(ReceiveReply::ClaimAtEnd);
    assert_eq!(first.next_action(started), ReceiveAction::Pending);
    first.reply(ReceiveReply::PendingEmpty);
    let next = first.complete_recovery_round(started).expect("completed recovery");

    let before_due = started + Duration::from_millis(10);
    let mut short =
        ReceiveDriver::new(Duration::ZERO, before_due, interval, false, Some(next)).expect("zero timeout driver");
    assert_eq!(short.next_action(before_due), ReceiveAction::ReadNew { block_ms: None });
    short.reply(ReceiveReply::NewEmpty);
    assert_eq!(short.next_action(before_due), ReceiveAction::TimedOut);

    let due = started + interval;
    let mut later = ReceiveDriver::new(Duration::MAX, due, interval, false, Some(next)).expect("due driver");
    assert_eq!(later.next_action(due), ReceiveAction::Claim);
}

/// Exhausting the claim page quota does not postpone an unfinished scan.
#[test]
fn test_incomplete_claim_round_keeps_recovery_due() {
    let started = Instant::now();
    let interval = Duration::from_millis(50);
    let past_due = started - Duration::from_millis(1);
    let mut driver = ReceiveDriver::new(Duration::MAX, started, interval, false, Some(past_due)).expect("driver");
    for _ in 0..8 {
        assert_eq!(driver.next_action(started), ReceiveAction::Claim);
        driver.reply(ReceiveReply::ClaimHasMore);
    }
    assert_eq!(driver.next_action(started), ReceiveAction::Pending);
    driver.reply(ReceiveReply::PendingEmpty);
    assert_eq!(driver.complete_recovery_round(started), None);
    assert_eq!(
        driver.next_action(started),
        ReceiveAction::ReadNew { block_ms: Some(50) },
        "an expired persisted deadline must not start an unbounded immediate scan loop"
    );
}

/// A forced scan before the periodic deadline still retries incomplete work
/// after one interval within the same long receive.
#[test]
fn test_forced_recovery_before_timer_due_uses_fresh_round_interval() {
    let started = Instant::now();
    let interval = Duration::from_millis(50);
    let far_future = started + Duration::from_secs(60);
    let mut driver =
        ReceiveDriver::new(Duration::MAX, started, interval, true, Some(far_future)).expect("forced driver");
    for _ in 0..8 {
        assert_eq!(driver.next_action(started), ReceiveAction::Claim);
        driver.reply(ReceiveReply::ClaimHasMore);
    }
    assert_eq!(driver.next_action(started), ReceiveAction::Pending);
    driver.reply(ReceiveReply::PendingEmpty);
    assert_eq!(driver.complete_recovery_round(started), None);
    assert_eq!(
        driver.next_action(started),
        ReceiveAction::ReadNew { block_ms: Some(50) }
    );
    driver.reply(ReceiveReply::NewEmpty);
    assert_eq!(driver.next_action(started + interval), ReceiveAction::Claim);
}
