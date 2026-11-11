// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! The benchmark-owned master, replica and Sentinel fixture.

#[path = "../../../../benches/support/redis_fixture/redis_node.rs"]
mod redis_node;
#[path = "../../../../benches/support/redis_fixture/sentinel_fixture.rs"]
mod sentinel_fixture;

pub use sentinel_fixture::SentinelFixture;
