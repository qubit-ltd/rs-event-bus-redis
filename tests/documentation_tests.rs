// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Executes the actual Markdown examples in independent consumer projects.

#![cfg(all(feature = "sync", feature = "async", feature = "discovery"))]

mod support;

mod documentation {
    mod markdown_examples_tests;
}
