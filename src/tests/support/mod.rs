// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared fixtures for crate-private Redis contract tests.

/// Existing isolated Redis fixtures shared with external contract tests.
#[path = "../../../tests/support/mod.rs"]
pub(crate) mod redis_support;
