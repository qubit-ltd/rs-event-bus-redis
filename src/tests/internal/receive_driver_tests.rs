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
    let mut driver = ReceiveDriver::new(Duration::ZERO, started, Duration::from_secs(1)).unwrap();

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
    let mut driver = ReceiveDriver::new(Duration::from_secs(1), started, Duration::from_secs(1)).unwrap();

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

/// Eight claim pages leave all eight pending command allowances available.
#[test]
fn test_claim_quota_preserves_all_eight_pending_dispatches() {
    let started = Instant::now();
    let mut driver = ReceiveDriver::new(Duration::from_secs(1), started, Duration::from_secs(1)).unwrap();
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
