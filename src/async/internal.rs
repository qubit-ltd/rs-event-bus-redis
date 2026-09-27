// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private asynchronous subscription state shared with settlement tokens.

#[path = "internal/async_settlement_state.rs"]
mod async_settlement_state;

pub(super) use async_settlement_state::AsyncSettlementState;
