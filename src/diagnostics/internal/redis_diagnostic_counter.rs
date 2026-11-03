// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal monotonic counters used by providers.

/// Counter selected by an operation outcome or admission boundary.
#[cfg(any(feature = "sync", feature = "async"))]
#[derive(Clone, Copy, Debug)]
pub(crate) enum RedisDiagnosticCounter {
    /// Rejected general or settlement command admission attempts.
    CommandRejections,
    /// Rejected receiver lease acquisition attempts.
    ReceiverRejections,
    /// Provider connection open attempts.
    ConnectionAttempts,
    /// Provider connection open failures.
    ConnectionFailures,
    /// Publishes confirmed accepted by Redis.
    PublishAccepted,
    /// Publishes whose Redis outcome is unknown.
    PublishUnknown,
    /// Receive calls whose outcome is unknown.
    ReceiveUnknown,
    /// Settlement calls whose outcome is unknown.
    SettlementUnknown,
    /// Recovery XAUTOCLAIM commands sent.
    RecoveryClaimCommands,
    /// Confirmed quarantine copies.
    QuarantineSucceeded,
    /// Delivery gaps returned to callers.
    DeliveryGaps,
}
