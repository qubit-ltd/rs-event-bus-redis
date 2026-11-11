// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Local settlement commit acquires both locks before either mutation.
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::spawn;

use qubit_event_bus::spi::DeliveryDisposition;

use crate::internal::RecoveryState;
use crate::internal::SettlementProgress;
use crate::internal::SettlementState;

/// Constructs a locally active token without network I/O.
fn state(progress: SettlementProgress) -> SettlementState {
    let mut recovery = RecoveryState::new();
    recovery.mark_delivered("1-0".into(), 1);
    SettlementState {
        stream: "stream".into(),
        group: "group".into(),
        message_id: "1-0".into(),
        progress: Arc::new(Mutex::new(progress)),
        recovery: Arc::new(Mutex::new(recovery)),
    }
}

#[test]
fn test_commit_poisoned_recovery_retains_pending_intent_and_active_slot() {
    let state = state(SettlementProgress::AckPending(DeliveryDisposition::Accept));
    let recovery = Arc::clone(&state.recovery);
    let _ = spawn(move || {
        let _guard = recovery.lock().expect("recovery initially healthy");
        panic!("poison recovery before local commit");
    })
    .join();
    assert_eq!(
        state.commit(DeliveryDisposition::Accept),
        Err("recovery lock")
    );
    assert_eq!(
        *state.progress.lock().expect("progress remains healthy"),
        SettlementProgress::AckPending(DeliveryDisposition::Accept)
    );
    assert_eq!(
        state
            .recovery
            .lock()
            .err()
            .expect("recovery remains poisoned")
            .into_inner()
            .active_len(),
        1
    );
    state.recovery.clear_poison();
    state
        .commit(DeliveryDisposition::Accept)
        .expect("same intent can complete after lock repair");
    assert_eq!(
        state
            .recovery
            .lock()
            .expect("recovery repaired")
            .active_len(),
        0
    );
    assert_eq!(
        *state.progress.lock().expect("progress healthy"),
        SettlementProgress::Applied(DeliveryDisposition::Accept)
    );
}

#[test]
fn test_commit_poisoned_progress_never_releases_active_slot() {
    let state = state(SettlementProgress::Open);
    let progress = Arc::clone(&state.progress);
    let _ = spawn(move || {
        let _guard = progress.lock().expect("progress initially healthy");
        panic!("poison progress before local commit");
    })
    .join();
    assert_eq!(
        state.commit(DeliveryDisposition::Retry),
        Err("settlement lock")
    );
    assert_eq!(
        state
            .recovery
            .lock()
            .expect("recovery stays healthy")
            .active_len(),
        1
    );
}
