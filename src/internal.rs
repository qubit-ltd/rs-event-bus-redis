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
mod receive_driver;
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
pub(crate) use receive_driver::ReceiveAction;
pub(crate) use receive_driver::ReceiveDriver;
pub(crate) use receive_driver::ReceiveReply;
#[cfg(test)]
pub(crate) use recovery_scan_budget::RecoveryScanBudget;
#[cfg(test)]
pub(crate) use recovery_scan_stage::RecoveryScanStage;
pub(crate) use recovery_state::RecoveryState;
pub(crate) use settlement_action::SettlementAction;
pub(crate) use settlement_action::settlement_action;
pub(crate) use settlement_state::SettlementState;
