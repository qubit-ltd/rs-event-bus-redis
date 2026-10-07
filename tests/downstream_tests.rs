// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Verifies task notification consumers against the current Redis provider.

#![cfg(all(feature = "sync", feature = "discovery"))]

mod support;

use std::error::Error;

use support::fixture_consumer::run as run_fixture_consumer;
use support::redis_server::RedisServer;

#[test]
fn test_task_notifications_preserve_monotonic_projection_and_business_state() -> Result<(), Box<dyn Error>> {
    let redis = RedisServer::start()?;
    run_fixture_consumer("task_notifications", redis.url(), false)
}
