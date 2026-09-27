// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Per-subscription pending-entry cursors and active-delivery tracking.

use std::collections::HashSet;

/// Tracks unsettled Redis IDs and the cursors used to revisit consumer state.
pub(crate) struct RecoveryState {
    /// IDs already handed to the local facade and not yet settled.
    active: HashSet<String>,
    /// Last ID visited while scanning this consumer's pending entries.
    pending_cursor: String,
    /// Resume cursor used by `XAUTOCLAIM` to inspect reclaimable PEL entries.
    claim_cursor: String,
}

impl RecoveryState {
    /// Creates empty receiver state with both Redis scans positioned at the
    /// start.
    pub(crate) fn new() -> Self {
        Self {
            active: HashSet::new(),
            pending_cursor: "0-0".to_owned(),
            claim_cursor: "0-0".to_owned(),
        }
    }

    /// Returns whether this stream ID is not already being handled locally.
    pub(crate) fn can_deliver(&self, id: &str) -> bool {
        !self.active.contains(id)
    }

    /// Marks an ID active if it is new and the receiver still has capacity.
    pub(crate) fn mark_delivered(&mut self, id: String, limit: usize) -> bool {
        if self.active.contains(&id) || self.active.len() >= limit {
            return false;
        }
        self.active.insert(id);
        true
    }

    /// Makes a retried ID eligible for the next pending scan.
    pub(crate) fn mark_retry(&mut self, id: &str) {
        self.active.remove(id);
        self.pending_cursor = "0-0".to_owned();
    }

    /// Releases a terminally settled ID from the local delivery bound.
    pub(crate) fn mark_terminal(&mut self, id: &str) {
        self.active.remove(id);
    }

    /// Returns the number of active local deliveries.
    pub(crate) fn active_len(&self) -> usize {
        self.active.len()
    }

    /// Borrows the cursor for this consumer's pending-entry scan.
    pub(crate) fn pending_cursor(&self) -> &str {
        &self.pending_cursor
    }

    /// Advances the pending cursor after a successful Redis response.
    pub(crate) fn set_pending_cursor(&mut self, cursor: String) {
        self.pending_cursor = cursor;
    }

    /// Borrows the cursor for reclaiming entries owned by inactive consumers.
    pub(crate) fn claim_cursor(&self) -> &str {
        &self.claim_cursor
    }

    /// Advances the claim cursor after a successful Redis response.
    pub(crate) fn set_claim_cursor(&mut self, cursor: String) {
        self.claim_cursor = cursor;
    }

    /// Restarts a pending scan when a local retry makes an earlier ID eligible.
    pub(crate) fn reset_pending_scan(&mut self) {
        self.pending_cursor = "0-0".to_owned();
    }
}

#[cfg(test)]
mod tests {
    use super::RecoveryState;

    #[test]
    fn test_active_delivery_bound_and_retry_release() {
        let mut state = RecoveryState::new();
        assert!(state.mark_delivered("1-0".into(), 2));
        assert!(state.mark_delivered("2-0".into(), 2));
        assert!(!state.mark_delivered("3-0".into(), 2));
        assert!(!state.can_deliver("1-0"));

        state.mark_retry("1-0");
        assert!(state.can_deliver("1-0"));
        assert_eq!(state.active_len(), 1);
        assert_eq!(state.pending_cursor(), "0-0");
    }
}
