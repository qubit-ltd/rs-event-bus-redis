// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Redis Streams providers for the Qubit Event Bus.
//!
//! This crate implements durable event-bus publication and consumption over
//! Redis Streams. It provides blocking and runtime-neutral asynchronous
//! adapters, optional provider discovery, explicit subscription profiles,
//! versioned wire fields, and process-local diagnostics.
//!
//! # Features
//!
//! - `sync` enables the synchronous provider module.
//! - `async` enables the asynchronous provider module, which does not start a
//!   runtime.
//! - `discovery` enables provider inventory registration.
//!
//! # Getting started
//!
//! Construct and validate backend settings before supplying them to a provider.
//! This example performs no Redis I/O:
//!
//! ```
//! use qubit_event_bus::model::StartPosition;
//! use qubit_event_bus_redis::config::RedisEventBusConfig;
//! use qubit_event_bus_redis::RedisSubscriptionProfile;
//!
//! let config = RedisEventBusConfig::new("redis://127.0.0.1/", "orders")?;
//! let profile = RedisSubscriptionProfile::new(StartPosition::New);
//! let options = profile.options::<Vec<u8>>().build();
//! assert_eq!(config.namespace(), "orders");
//! assert_eq!(options.start_position(), &StartPosition::New);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! Provider creation validates configuration and builds an SPI object; it does
//! not prove that Redis is reachable. Network errors are returned when an SPI
//! operation needs a connection.
//!
//! With the `sync` feature, a provider can be constructed without opening a
//! network connection:
//!
//! ```
//! fn main() {
//!     #[cfg(feature = "sync")]
//!     {
//!         use qubit_event_bus::EventBusConfig;
//!         use qubit_event_bus_redis::sync::RedisEventBusProvider;
//!         use qubit_spi::ServiceProvider;
//!
//!         let bus = RedisEventBusProvider
//!             .create_configured(&EventBusConfig::default())
//!             .expect("valid default settings");
//!     }
//! }
//! ```
//!
//! With the `async` feature, the same lazy construction is runtime-neutral:
//!
//! ```
//! fn main() {
//!     #[cfg(feature = "async")]
//!     futures_lite::future::block_on(async {
//!         use qubit_event_bus::EventBusConfig;
//!         use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
//!         use qubit_spi::AsyncServiceProvider;
//!
//!         let bus = AsyncRedisEventBusProvider
//!             .create_configured(&EventBusConfig::default())
//!             .await
//!             .expect("valid default settings");
//!     });
//! }
//! ```
//!
//! # Delivery boundaries
//!
//! Existing consumer groups resume their saved cursor when configured with
//! [`StartPosition::New`](qubit_event_bus::model::StartPosition::New) or the
//! explicit existing-group resume policy. `Earliest` chooses the start of a
//! newly created group; it does not rewind an existing group. A publish with
//! an unknown result must not be blindly replayed because Redis may already
//! have appended it. `Accepted` means Redis replied to `XADD`; it does not mean
//! the record was fsynced, processed successfully, or delivered exactly once.
//!
//! # API map
//!
//! - [`config::RedisEventBusConfig`] contains validated Redis settings.
//! - [`RedisSubscriptionProfile`] builds durable subscription options.
//! - [`diagnostics::RedisProviderDiagnostics`] and
//!   [`diagnostics::RedisProviderSnapshot`] expose process-local counters.
//! - [`wire::WireFields`] describes the version 1 transport fields.
//!
//! [`RedisEventBusConfig`]: config::RedisEventBusConfig
//!
//! # Further reading
//!
//! See [`doc/user_guide.md`](https://github.com/qubit-ltd/rs-event-bus-redis/blob/main/doc/user_guide.md)
//! for the English guide and [`doc/user_guide.zh_CN.md`](https://github.com/qubit-ltd/rs-event-bus-redis/blob/main/doc/user_guide.zh_CN.md)
//! for the Simplified Chinese guide.
#![cfg_attr(
    feature = "sync",
    doc = "\nThe synchronous implementation is [`sync::RedisEventBusProvider`]."
)]
#![cfg_attr(
    feature = "async",
    doc = "\nThe asynchronous implementation is [`async::AsyncRedisEventBusProvider`]."
)]
#![forbid(unsafe_code)]

#[cfg(any(feature = "sync", feature = "async"))]
#[path = "internal/bounded_wire_decoder.rs"]
mod bounded_wire_decoder;
/// Standalone and Sentinel connection providers.
#[cfg(any(feature = "sync", feature = "async"))]
mod client;
/// Redis Streams backend configuration.
pub mod config;
/// Unique Redis consumer identity generation.
#[cfg(any(feature = "sync", feature = "async"))]
mod consumer_identity;
/// Process-local diagnostics for live Redis provider instances.
pub mod diagnostics;
/// Shared provider discovery submissions.
#[cfg(feature = "discovery")]
mod discovery;
/// Secret-safe provider failures.
pub mod error;
/// Shared existing-group start-position policy.
#[cfg(any(feature = "sync", feature = "async"))]
#[path = "existing_group_start_policy.rs"]
mod existing_group_start;
/// Shared private receiver, recovery, and wire-decision state.
#[cfg(any(feature = "sync", feature = "async"))]
mod internal;
/// Redis key naming helpers.
pub mod naming;
/// Malformed Redis stream entry quarantine protocol.
#[cfg(any(feature = "sync", feature = "async"))]
mod poison;
/// Redis backend configuration.
mod redis_event_bus_config;
/// Redis provider failures.
mod redis_provider_error;
/// Explicit durable subscription settings for Redis Streams.
mod redis_subscription_profile;
/// Redis Streams response normalization shared by both receiver modes.
#[cfg(any(feature = "sync", feature = "async"))]
mod stream_protocol;
/// Message wire format.
pub mod wire;
/// Versioned Redis wire fields.
mod wire_fields;
#[cfg(any(feature = "sync", feature = "async"))]
mod wire_limits;

pub use redis_subscription_profile::RedisSubscriptionProfile;

/// Synchronous SPI implementation.
#[cfg(feature = "sync")]
pub mod sync;

/// Asynchronous SPI implementation.
#[cfg(feature = "async")]
pub mod r#async;

/// Crate-internal contract tests.
#[cfg(test)]
#[cfg(any(feature = "sync", feature = "async"))]
mod tests;
