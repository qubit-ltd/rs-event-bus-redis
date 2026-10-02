// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private recovery and wire-decision types shared by both Redis adapters.

#[path = "internal/command.rs"]
mod command;
#[path = "internal/decode.rs"]
mod decode;
#[path = "internal/decode_failure.rs"]
mod decode_failure;
#[path = "internal/poison_outcome.rs"]
mod poison_outcome;
#[path = "internal/poison_reason.rs"]
mod poison_reason;
mod receive_action;
mod receive_driver;
mod receive_reply;
mod receive_stage;
#[path = "internal/recovery_scan_budget.rs"]
pub(crate) mod recovery_scan_budget;
#[path = "internal/recovery_scan_stage.rs"]
mod recovery_scan_stage;
#[path = "internal/recovery_state.rs"]
mod recovery_state;
#[path = "internal/settlement_action.rs"]
mod settlement_action;
#[path = "internal/settlement_state.rs"]
mod settlement_state;

pub(crate) use command::read_group_command;
pub(crate) use decode::decode_entry;
pub(crate) use decode_failure::DecodeFailure;
pub(crate) use poison_outcome::PoisonOutcome;
pub(crate) use poison_reason::PoisonReason;
pub(crate) use receive_action::ReceiveAction;
pub(crate) use receive_driver::ReceiveDriver;
pub(crate) use receive_reply::ReceiveReply;
#[cfg(test)]
pub(crate) use recovery_scan_budget::RecoveryScanBudget;
#[cfg(test)]
pub(crate) use recovery_scan_stage::RecoveryScanStage;
pub(crate) use recovery_state::RecoveryGuard;
pub(crate) use recovery_state::RecoveryState;
pub(crate) use settlement_action::SettlementAction;
pub(crate) use settlement_state::SettlementState;

/// Shared transport policy rules.
mod transport_policy;
pub(crate) use transport_policy::TransportPolicy;

/// Shared wire limits rules.
mod wire_limits;
pub(crate) use wire_limits::WireLimits;

/// Shared settlement intention and completion state.
mod settlement_progress;
pub(crate) use settlement_progress::SettlementProgress;
