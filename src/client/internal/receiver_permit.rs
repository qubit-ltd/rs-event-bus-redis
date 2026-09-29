// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Receiver admission separate from tokens and Redis pending entries.
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::resource_budget::ResourceBudget;
/// Non-cloneable receiver slot released by setup failure, close, or drop.
pub(crate) struct ReceiverPermit {
    /// Shared receiver counter decremented exactly once when this permit is
    /// dropped.
    pub(super) budget: Arc<ResourceBudget>,
}
impl Drop for ReceiverPermit {
    /// Releases the slot without acknowledging or deleting Redis records.
    fn drop(&mut self) {
        self.budget.receivers.fetch_sub(1, Ordering::AcqRel);
    }
}
