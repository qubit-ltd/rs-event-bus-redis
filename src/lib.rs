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
/// Shared provider discovery submissions.
#[cfg(feature = "discovery")]
mod discovery;
/// Secret-safe provider failures.
pub mod error;
/// Redis key naming helpers.
pub mod naming;
/// Redis backend configuration.
mod redis_event_bus_config;
/// Redis provider failures.
mod redis_provider_error;
/// Message wire format.
pub mod wire;
/// Versioned Redis wire fields.
mod wire_fields;

#[cfg(feature = "sync")]
/// Synchronous SPI implementation.
pub mod sync;

#[cfg(feature = "async")]
/// Asynchronous SPI implementation.
pub mod r#async;
