// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Per-subscription pending-entry cursors and active-delivery tracking.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use redis::streams::StreamId;

/// Tracks unsettled Redis IDs and the cursors used to revisit consumer state.
pub(crate) struct RecoveryState {
    /// IDs already handed to the local facade and not yet settled.
    active: HashSet<String>,
    /// Last ID visited while scanning this consumer's pending entries.
    pending_cursor: String,
    /// Resume cursor used by `XAUTOCLAIM` to inspect reclaimable PEL entries.
    claim_cursor: String,
    /// One claimed entry held while a deleted-entry gap is reported.
    deferred_claim: Option<StreamId>,
    /// Exclusive XPENDING cursor for bounded Redis 6.2 tombstone scans.
    tombstone_cursor: String,
    /// Next periodic scan after the last complete recovery round.
    next_recovery_at: Option<Instant>,
    /// Failure, cancellation, retry, or partial work requires immediate scan.
    force_recovery: bool,
    /// Advances when retry, failure, or cancellation creates new recovery duty.
    force_generation: u64,
}

/// Marks recovery due if an in-progress receive is cancelled or unwinds.
pub(crate) struct RecoveryGuard {
    /// Shared receiver state; this guard never retains its mutex across I/O.
    state: Arc<Mutex<RecoveryState>>,
    /// Cleared only after a receive returns normally.
    armed: bool,
}

impl RecoveryGuard {
    /// Arms cancellation recovery after a receive future has actually been
    /// polled. The guard owns an `Arc`, not a mutex lock.
    pub(crate) fn new(state: Arc<Mutex<RecoveryState>>) -> Self {
        Self { state, armed: true }
    }

    /// Prevents normal completion from forcing an extra recovery round.
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RecoveryGuard {
    /// Conservatively forces the next scan after cancellation or unwind.
    fn drop(&mut self) {
        if self.armed
            && let Ok(mut state) = self.state.lock()
        {
            state.force_recovery();
        }
    }
}

impl RecoveryState {
    /// Creates empty active state and start-position cursors without Redis I/O.
    ///
    /// # Returns
    ///
    /// An empty registry with pending, claim, and tombstone cursors at `0-0`.
    /// Cursor strings are owned by this per-receiver state.
    pub(crate) fn new() -> Self {
        Self {
            active: HashSet::new(),
            pending_cursor: "0-0".to_owned(),
            claim_cursor: "0-0".to_owned(),
            deferred_claim: None,
            tombstone_cursor: "0-0".to_owned(),
            next_recovery_at: None,
            force_recovery: true,
            force_generation: 0,
        }
    }

    /// Returns whether recovery is due, the next scan instant, and the current
    /// external recovery generation.
    ///
    /// `now` is the start of a receive; no state is changed by inspection. The
    /// generation must be supplied when that receive finishes its scan.
    pub(crate) fn recovery_schedule(&self, now: Instant) -> (bool, Option<Instant>, u64) {
        (
            self.force_recovery || self.next_recovery_at.is_none_or(|next| now >= next),
            self.next_recovery_at,
            self.force_generation,
        )
    }

    /// Marks a scan in progress without creating a new external recovery
    /// generation. A retry during this scan will then remain distinguishable.
    pub(crate) fn begin_recovery(&mut self) {
        self.force_recovery = true;
    }

    /// Forces claim and pending scans on the next receive without discarding
    /// existing cursors or active delivery state.
    pub(crate) fn force_recovery(&mut self) {
        self.force_recovery = true;
        self.force_generation = self.force_generation.saturating_add(1);
    }

    /// Records the next scan instant only after a complete recovery round.
    ///
    /// `next` is calculated from the completion time, so normal short calls do
    /// not continuously defer recovery. `observed_generation` prevents this
    /// scan from clearing a retry or cancellation raised during its I/O.
    pub(crate) fn complete_recovery_at(&mut self, next: Instant, observed_generation: u64) {
        self.next_recovery_at = Some(next);
        if self.force_generation == observed_generation {
            self.force_recovery = false;
        }
    }

    /// Checks whether `id` is absent from the locally active delivery set.
    ///
    /// # Parameters
    ///
    /// - `id`: Redis entry identity being considered for delivery.
    ///
    /// # Returns
    ///
    /// `true` when no handler currently owns this ID locally; capacity is
    /// checked separately.
    #[must_use]
    #[inline]
    pub(crate) fn can_deliver(&self, id: &str) -> bool {
        !self.active.contains(id)
    }

    /// Returns the number of IDs retained by unsettled local handlers.
    ///
    /// # Returns
    ///
    /// The current active-set size without allocation or Redis I/O.
    #[must_use]
    #[inline]
    pub(crate) fn active_len(&self) -> usize {
        self.active.len()
    }

    /// Borrows the last visited position in this consumer's pending scan.
    ///
    /// # Returns
    ///
    /// The state-owned Redis cursor; `0-0` restarts from the beginning.
    #[must_use]
    #[inline]
    pub(crate) fn pending_cursor(&self) -> &str {
        &self.pending_cursor
    }

    /// Borrows the resume position for reclaiming inactive consumers' entries.
    ///
    /// # Returns
    ///
    /// The last successful XAUTOCLAIM cursor, or `0-0` for the start/end of a
    /// round.
    #[must_use]
    #[inline]
    pub(crate) fn claim_cursor(&self) -> &str {
        &self.claim_cursor
    }

    /// Borrows the exclusive position for bounded tombstone PEL scans.
    ///
    /// # Returns
    ///
    /// The last visited ID, or `0-0` when the next probe begins at the PEL
    /// start.
    #[must_use]
    #[inline]
    pub(crate) fn tombstone_cursor(&self) -> &str {
        &self.tombstone_cursor
    }

    /// Transfers ownership of the successfully read pending position.
    ///
    /// # Parameters
    ///
    /// - `cursor`: Redis ID to resume after; replaces the previous owned
    ///   cursor.
    ///
    /// No Redis command is issued; callers update only after a valid response.
    #[inline]
    pub(crate) fn set_pending_cursor(&mut self, cursor: String) {
        self.pending_cursor = cursor;
    }

    /// Transfers ownership of the successfully returned XAUTOCLAIM position.
    ///
    /// # Parameters
    ///
    /// - `cursor`: Next scan position returned by Redis, including terminal
    ///   `0-0`.
    ///
    /// No Redis command is issued; a failed request must not advance this
    /// cursor.
    #[inline]
    pub(crate) fn set_claim_cursor(&mut self, cursor: String) {
        self.claim_cursor = cursor;
    }

    /// Saves progress through the successfully visited tombstone PEL range.
    ///
    /// # Parameters
    ///
    /// - `cursor`: Exclusive next-probe position, replacing the previous owned
    ///   value.
    ///
    /// No Redis command is issued.
    #[inline]
    pub(crate) fn set_tombstone_cursor(&mut self, cursor: String) {
        self.tombstone_cursor = cursor;
    }

    /// Holds a claimed entry while a higher-priority deleted-entry gap is
    /// delivered.
    ///
    /// # Parameters
    ///
    /// - `entry`: Owned RESP entry whose field allocations remain retained for
    ///   the next receive.
    ///
    /// Replaces any previous deferred entry; the adapter guarantees at most one
    /// retained entry.
    #[inline]
    pub(crate) fn defer_claim(&mut self, entry: StreamId) {
        self.deferred_claim = Some(entry);
    }

    /// Transfers the single claimed entry retained behind an earlier gap.
    ///
    /// # Returns
    ///
    /// `Some` owns the retained entry and clears the slot; `None` means no
    /// entry is deferred. Field buffers move without cloning or issuing
    /// Redis commands.
    #[must_use]
    #[inline]
    pub(crate) fn take_deferred_claim(&mut self) -> Option<StreamId> {
        self.deferred_claim.take()
    }

    /// Restarts the pending cursor after retry makes an earlier ID eligible.
    ///
    /// Replaces the owned position with `0-0`; no active slot or Redis PEL
    /// entry changes.
    pub(crate) fn reset_pending_scan(&mut self) {
        self.pending_cursor = "0-0".to_owned();
    }

    /// Restarts the tombstone cursor after the current PEL scan reaches its
    /// end.
    ///
    /// Replaces the owned position with `0-0` without sending a Redis command.
    pub(crate) fn reset_tombstone_cursor(&mut self) {
        self.tombstone_cursor = "0-0".to_owned();
    }

    /// Marks a newly delivered Redis ID active if this receiver has capacity.
    ///
    /// # Parameters
    ///
    /// - `id`: Entry identity whose ownership transfers into the active
    ///   registry on success.
    /// - `limit`: Inclusive local unsettled-delivery cap.
    ///
    /// # Returns
    ///
    /// `true` after insertion; `false` for an already active ID or a full
    /// registry. Failure leaves the registry intact; no Redis command is
    /// issued.
    pub(crate) fn mark_delivered(&mut self, id: String, limit: usize) -> bool {
        if self.active.contains(&id) || self.active.len() >= limit {
            return false;
        }
        self.active.insert(id);
        true
    }

    /// Releases local ownership and restarts the pending scan for a retried ID.
    ///
    /// # Parameters
    ///
    /// - `id`: Previously delivered Redis ID, if it is still active.
    ///
    /// This does not acknowledge Redis PEL state; the next receive may revisit
    /// the entry.
    pub(crate) fn mark_retry(&mut self, id: &str) {
        self.active.remove(id);
        self.pending_cursor = "0-0".to_owned();
        self.force_recovery();
    }

    /// Releases the local active slot after terminal settlement is confirmed.
    ///
    /// # Parameters
    ///
    /// - `id`: Entry whose valid XACK result has already been observed by the
    ///   caller.
    ///
    /// Removes local bookkeeping only; no Redis command is issued here.
    pub(crate) fn mark_terminal(&mut self, id: &str) {
        self.active.remove(id);
    }
}
