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
/// Counter owned by a short-command permit.
#[derive(Clone, Copy, Debug)]
pub(super) enum CommandLane {
    General,
    ReservedSettlement,
}
/// Non-cloneable permit; cancellation releases admission, not Redis execution.
#[must_use]
pub(crate) struct CommandPermit {
    /// Shared short-command counter decremented exactly once when this permit
    /// is dropped.
    pub(super) budget: Arc<ResourceBudget>,
    /// Counter that was incremented when this permit was acquired.
    pub(super) lane: CommandLane,
}
impl Drop for CommandPermit {
    /// Releases exactly the slot acquired for this application operation.
    #[inline]
    fn drop(&mut self) {
        match self.lane {
            CommandLane::General => self.budget.commands.fetch_sub(1, Ordering::AcqRel),
            CommandLane::ReservedSettlement => self.budget.reserved_settlements.fetch_sub(1, Ordering::AcqRel),
        };
    }
}
