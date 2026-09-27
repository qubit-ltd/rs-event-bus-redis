// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private synchronous subscription state shared with settlement tokens.

#[path = "internal/settlement_state.rs"]
mod settlement_state;

pub(super) use settlement_state::SettlementState;
