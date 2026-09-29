// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! One measured public SPI operation attempt.

/// One complete operation attempt, with successful and failed samples retained.
pub struct Sample {
    pub nanos: u128,
    pub outcome: String,
}
