// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
//! Private unit regression tests.

#[cfg(any(feature = "sync", feature = "async"))]
mod bounded_wire_decoder_tests;
mod client;
mod consumer_identity_tests;
mod error_tests;
mod internal;
mod redis_event_bus_config_tests;
mod redis_provider_error_tests;
mod stream_protocol_tests;
/// Shared fixtures for crate-private Redis contract tests.
pub(crate) mod support;
