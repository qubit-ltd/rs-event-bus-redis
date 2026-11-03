// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Process-wide weak-reference directory for live provider diagnostics.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::Weak;

use super::RedisDiagnosticsState;

/// Weak references to states registered by providers.
pub(in crate::diagnostics) struct RedisDiagnosticsDirectory {
    entries: Vec<Weak<RedisDiagnosticsState>>,
}

impl RedisDiagnosticsDirectory {
    /// Creates an empty directory, primarily for isolated lifecycle checks.
    fn new() -> Self {
        Self { entries: Vec::new() }
    }

    /// Drops expired entries before retaining the new state.
    pub(in crate::diagnostics) fn register(&mut self, state: &Arc<RedisDiagnosticsState>) {
        self.entries.retain(|entry| entry.strong_count() > 0);
        self.entries.push(Arc::downgrade(state));
    }

    /// Upgrades live states and removes entries that can no longer be upgraded.
    pub(in crate::diagnostics) fn live(&mut self) -> Vec<Arc<RedisDiagnosticsState>> {
        let mut live = Vec::new();
        self.entries.retain(|entry| {
            if let Some(state) = entry.upgrade() {
                live.push(state);
                true
            } else {
                false
            }
        });
        live
    }
}

/// Returns the lazily initialized process-wide directory.
pub(in crate::diagnostics) fn directory() -> &'static Mutex<RedisDiagnosticsDirectory> {
    static DIRECTORY: OnceLock<Mutex<RedisDiagnosticsDirectory>> = OnceLock::new();
    DIRECTORY.get_or_init(|| Mutex::new(RedisDiagnosticsDirectory::new()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use super::RedisDiagnosticsDirectory;
    use crate::client::ResourceBudget;
    use crate::diagnostics::RedisDiagnosticsState;
    use crate::diagnostics::RedisProviderMode;

    fn state(namespace: &str) -> Arc<RedisDiagnosticsState> {
        RedisDiagnosticsState::register(
            RedisProviderMode::Sync,
            namespace,
            Arc::new(ResourceBudget::new(1, 1, 1)),
        )
        .expect("register diagnostics state")
    }

    #[test]
    fn registration_bounds_released_weak_entries_without_sampling() {
        let mut directory = RedisDiagnosticsDirectory::new();

        for _ in 0..1_000 {
            let state = state("directory_released");
            directory.register(&state);
            assert_eq!(directory.entries.len(), 1);
            drop(state);
            assert_eq!(directory.entries.len(), 1);
        }

        let final_state = state("directory_final");
        directory.register(&final_state);
        assert_eq!(directory.entries.len(), 1);
    }

    #[test]
    fn registration_and_live_keep_only_states_that_remain_alive() {
        let mut directory = RedisDiagnosticsDirectory::new();
        let first = state("directory_first");
        let second = state("directory_second");
        directory.register(&first);
        directory.register(&second);

        for _ in 0..8 {
            let expired = state("directory_expired");
            directory.register(&expired);
            drop(expired);
        }

        assert_eq!(directory.entries.len(), 3);
        let actual: BTreeSet<_> = directory.live().iter().map(|state| state.instance_id).collect();
        let expected = BTreeSet::from([first.instance_id, second.instance_id]);
        assert_eq!(actual, expected);
        assert_eq!(directory.entries.len(), 2);
    }
}
