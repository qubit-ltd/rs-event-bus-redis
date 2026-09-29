// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Pure disposition decisions shared by both receiver adapters.

use qubit_event_bus::spi::DeliveryDisposition;

/// Next settlement operation selected from the token's applied disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SettlementAction {
    /// The requested disposition was already committed and is idempotent.
    AlreadyApplied,
    /// Release local ownership without issuing `XACK`.
    Retry,
    /// Acknowledge the stream entry before committing local state.
    Acknowledge,
}

/// Resolves repeated or conflicting disposition requests without I/O.
pub(crate) fn settlement_action(
    applied: Option<DeliveryDisposition>,
    requested: DeliveryDisposition,
) -> Result<SettlementAction, ()> {
    match applied {
        Some(previous) if previous == requested => Ok(SettlementAction::AlreadyApplied),
        Some(_) => Err(()),
        None if requested == DeliveryDisposition::Retry => Ok(SettlementAction::Retry),
        None => Ok(SettlementAction::Acknowledge),
    }
}

#[cfg(test)]
mod tests {
    use qubit_event_bus::spi::DeliveryDisposition;

    use super::SettlementAction;
    use super::settlement_action;

    #[test]
    fn test_settlement_action_is_idempotent_and_rejects_conflicts() {
        assert!(matches!(
            settlement_action(None, DeliveryDisposition::Retry),
            Ok(SettlementAction::Retry)
        ));
        assert!(matches!(
            settlement_action(None, DeliveryDisposition::Accept),
            Ok(SettlementAction::Acknowledge)
        ));
        assert!(matches!(
            settlement_action(Some(DeliveryDisposition::Accept), DeliveryDisposition::Accept),
            Ok(SettlementAction::AlreadyApplied)
        ));
        assert!(settlement_action(Some(DeliveryDisposition::Accept), DeliveryDisposition::Reject).is_err());
    }
}
