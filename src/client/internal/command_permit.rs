// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! RAII ownership of one short application-operation admission slot.
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::resource_budget::ResourceBudget;
/// Non-cloneable permit; cancellation releases admission, not Redis execution.
pub(crate) struct CommandPermit {
    /// Shared short-command counter decremented exactly once when this permit
    /// is dropped.
    pub(super) budget: Arc<ResourceBudget>,
}
impl Drop for CommandPermit {
    /// Releases exactly the slot acquired for this application operation.
    fn drop(&mut self) {
        self.budget.commands.fetch_sub(1, Ordering::AcqRel);
    }
}
