use std::sync::Arc;

use crate::client::ResourceBudget;
use crate::diagnostics::RedisDiagnosticCounter;
use crate::diagnostics::RedisDiagnosticsState;
use crate::diagnostics::RedisProviderDiagnostics;
use crate::diagnostics::RedisProviderMode;

#[test]
fn test_snapshot_exposes_all_provider_counters() {
    let state = RedisDiagnosticsState::register(
        RedisProviderMode::Sync,
        "diagnostics_unit_test",
        Arc::new(ResourceBudget::new(4, 1, 4)),
    )
    .expect("register diagnostics state");
    for counter in [
        RedisDiagnosticCounter::CommandRejections,
        RedisDiagnosticCounter::ReceiverRejections,
        RedisDiagnosticCounter::ConnectionAttempts,
        RedisDiagnosticCounter::ConnectionFailures,
        RedisDiagnosticCounter::PublishAccepted,
        RedisDiagnosticCounter::PublishUnknown,
        RedisDiagnosticCounter::ReceiveUnknown,
        RedisDiagnosticCounter::SettlementUnknown,
        RedisDiagnosticCounter::RecoveryClaimCommands,
        RedisDiagnosticCounter::QuarantineSucceeded,
        RedisDiagnosticCounter::DeliveryGaps,
    ] {
        state.increment(counter);
    }

    let snapshot = RedisProviderDiagnostics::snapshots()
        .into_iter()
        .find(|snapshot| snapshot.namespace() == "diagnostics_unit_test")
        .expect("registered state is visible in diagnostics");

    assert!(snapshot.instance_id() > 0);
    assert_eq!(snapshot.mode(), RedisProviderMode::Sync);
    assert_eq!(snapshot.namespace(), "diagnostics_unit_test");
    assert_eq!(snapshot.general_in_flight(), 0);
    assert_eq!(snapshot.settlement_in_flight(), 0);
    assert_eq!(snapshot.active_receivers(), 0);
    assert_eq!(snapshot.command_rejections(), 1);
    assert_eq!(snapshot.receiver_rejections(), 1);
    assert_eq!(snapshot.connection_attempts(), 1);
    assert_eq!(snapshot.connection_failures(), 1);
    assert_eq!(snapshot.publish_accepted(), 1);
    assert_eq!(snapshot.publish_unknown(), 1);
    assert_eq!(snapshot.receive_unknown(), 1);
    assert_eq!(snapshot.settlement_unknown(), 1);
    assert_eq!(snapshot.recovery_claim_commands(), 1);
    assert_eq!(snapshot.quarantine_succeeded(), 1);
    assert_eq!(snapshot.delivery_gaps(), 1);
}
