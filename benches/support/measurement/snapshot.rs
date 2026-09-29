// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Redis INFO counters at a single observation point.

use std::error::Error;

use redis::Connection;
use redis::cmd;

/// Redis counters at one observation point, including observer connections.
#[derive(Clone, Copy, Default)]
pub struct Snapshot {
    pub created: u64,
    pub active: u64,
    pub claim: u64,
    pub read: u64,
}

impl Snapshot {
    /// Reads cumulative server counters without resetting them.
    pub fn read(connection: &mut Connection) -> Result<Self, Box<dyn Error>> {
        let info: String = cmd("INFO").arg("all").query(connection)?;
        let value = |key: &str| {
            info.lines()
                .find_map(|line| line.strip_prefix(key))
                .and_then(|tail| tail.trim().parse::<u64>().ok())
                .unwrap_or(0)
        };
        let calls = |key: &str| {
            info.lines()
                .find_map(|line| line.strip_prefix(key))
                .and_then(|tail| tail.split(',').next())
                .and_then(|tail| tail.strip_prefix("calls="))
                .and_then(|number| number.parse().ok())
                .unwrap_or(0)
        };
        Ok(Self {
            created: value("total_connections_received:"),
            active: value("connected_clients:"),
            claim: calls("cmdstat_xautoclaim:"),
            read: calls("cmdstat_xreadgroup:"),
        })
    }
}
