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
    /// One claimed entry held while a deleted-entry gap is reported.
    deferred_claim: Option<redis::streams::StreamId>,
    /// Exclusive XPENDING cursor for bounded Redis 6.2 tombstone scans.
    tombstone_cursor: String,
}

impl RecoveryState {
    /// Creates empty receiver state with both Redis scans positioned at the
    /// start.
    pub(crate) fn new() -> Self {
        Self {
            active: HashSet::new(),
            pending_cursor: "0-0".to_owned(),
            claim_cursor: "0-0".to_owned(),
            deferred_claim: None,
            tombstone_cursor: "0-0".to_owned(),
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

    /// Holds one claimed entry until a higher-priority gap outcome is returned.
    pub(crate) fn defer_claim(&mut self, entry: redis::streams::StreamId) {
        self.deferred_claim = Some(entry);
    }

    /// Takes the single entry held behind a gap outcome.
    pub(crate) fn take_deferred_claim(&mut self) -> Option<redis::streams::StreamId> {
        self.deferred_claim.take()
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

    /// Returns the exclusive cursor for the next tombstone PEL scan.
    pub(crate) fn tombstone_cursor(&self) -> &str {
        &self.tombstone_cursor
    }

    /// Saves progress through the bounded tombstone PEL scan.
    pub(crate) fn set_tombstone_cursor(&mut self, cursor: String) {
        self.tombstone_cursor = cursor;
    }

    /// Restarts tombstone scans after reaching the end of the PEL.
    pub(crate) fn reset_tombstone_cursor(&mut self) {
        self.tombstone_cursor = "0-0".to_owned();
    }
}
