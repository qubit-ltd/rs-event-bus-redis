// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Pure decisions preserve terminal intent after an unknown acknowledgement.
use qubit_event_bus::spi::DeliveryDisposition;

use crate::internal::SettlementAction;
use crate::internal::SettlementProgress;

#[test]
fn test_settlement_progress_action_fixes_pending_intent() {
    for terminal in [DeliveryDisposition::Accept, DeliveryDisposition::Reject] {
        assert_eq!(
            SettlementProgress::Open.action(terminal),
            Ok(SettlementAction::PrepareAck)
        );
        assert_eq!(
            SettlementProgress::AckPending(terminal).action(terminal),
            Ok(SettlementAction::RepeatAck)
        );
        assert_eq!(
            SettlementProgress::Applied(terminal).action(terminal),
            Ok(SettlementAction::AlreadyApplied)
        );
        for other in [
            DeliveryDisposition::Accept,
            DeliveryDisposition::Reject,
            DeliveryDisposition::Retry,
        ] {
            if other != terminal {
                assert!(SettlementProgress::AckPending(terminal).action(other).is_err());
                assert!(SettlementProgress::Applied(terminal).action(other).is_err());
            }
        }
    }
    assert_eq!(
        SettlementProgress::Open.action(DeliveryDisposition::Retry),
        Ok(SettlementAction::ApplyRetry)
    );
    assert_eq!(
        SettlementProgress::Applied(DeliveryDisposition::Retry).action(DeliveryDisposition::Retry),
        Ok(SettlementAction::AlreadyApplied)
    );
    assert!(
        SettlementProgress::Applied(DeliveryDisposition::Retry)
            .action(DeliveryDisposition::Accept)
            .is_err()
    );
}
