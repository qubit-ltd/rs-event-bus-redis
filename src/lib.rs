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

/// Redis Streams backend configuration.
pub mod config;
