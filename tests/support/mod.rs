// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Test support for isolated Redis services.

#[cfg(all(feature = "sync", feature = "async"))]
#[allow(dead_code)]
pub mod bench_support;
#[allow(dead_code)]
pub mod controlled_redis;
#[allow(dead_code)]
pub mod fixture_consumer;
#[allow(dead_code)]
pub mod fixture_process;
#[allow(dead_code)]
pub mod redis_server;
#[allow(dead_code)]
pub mod scripted_redis;
#[allow(dead_code)]
pub mod sentinel;
#[cfg(all(feature = "sync", feature = "async"))]
#[allow(dead_code)]
#[path = "../../tests/sentinel_cost/mod.rs"]
pub mod sentinel_cost;
#[cfg(any(feature = "sync", feature = "async"))]
#[allow(dead_code)]
pub mod settlement_fault;
#[allow(dead_code)]
pub mod tls_redis_server;
#[allow(dead_code)]
pub mod tls_sentinel;
