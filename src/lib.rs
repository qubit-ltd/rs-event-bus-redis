// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Redis Streams providers for the Qubit Event Bus.
//!
//! The crate offers synchronous and runtime-neutral asynchronous SPI providers.
//! Redis connection and provider APIs are added by the implementation modules.

#![forbid(unsafe_code)]

/// Standalone and Sentinel connection providers.
#[cfg(any(feature = "sync", feature = "async"))]
mod client;
/// Redis Streams backend configuration.
pub mod config;
/// Unique Redis consumer identity generation.
#[cfg(any(feature = "sync", feature = "async"))]
mod consumer_identity;
/// Shared provider discovery submissions.
#[cfg(feature = "discovery")]
mod discovery;
/// Secret-safe provider failures.
pub mod error;
/// Redis key naming helpers.
pub mod naming;
/// Malformed Redis stream entry quarantine protocol.
#[cfg(any(feature = "sync", feature = "async"))]
mod poison;
/// Per-subscription pending-entry recovery state.
#[cfg(any(feature = "sync", feature = "async"))]
mod recovery;
/// Redis backend configuration.
mod redis_event_bus_config;
/// Redis provider failures.
mod redis_provider_error;
/// Redis Streams response normalization shared by both receiver modes.
#[cfg(any(feature = "sync", feature = "async"))]
mod stream_protocol;
/// Message wire format.
pub mod wire;
/// Versioned Redis wire fields.
mod wire_fields;

/// Synchronous SPI implementation.
#[cfg(feature = "sync")]
pub mod sync;

/// Asynchronous SPI implementation.
#[cfg(feature = "async")]
pub mod r#async;
