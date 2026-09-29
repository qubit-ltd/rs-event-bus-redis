// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Completed worker run with retained receivers for active-client observation.

use std::time::Duration;

use super::measurement::Sample;
use super::receiver::Receiver;

/// Owns samples and receiver leases until the caller records its final
/// snapshot.
pub struct WorkloadResult {
    pub elapsed: Duration,
    pub groups: Vec<Vec<Sample>>,
    pub receivers: Vec<Receiver>,
}
