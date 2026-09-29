// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Independent disposable Redis and Sentinel fixture ownership.

mod redis_node;
mod sentinel_fixture;

pub use self::redis_node::RedisNode;
pub use self::sentinel_fixture::SentinelFixture;
