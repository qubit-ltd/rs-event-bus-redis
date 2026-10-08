// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Fresh master/replica and Sentinel quorum for one measured failover.

use std::error::Error;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use redis::Client;
use redis::Connection;
use redis::cmd;

use super::redis_node::RedisNode;

/// Owns a fresh master/replica and a three-node Sentinel quorum per round.
pub struct SentinelFixture {
    pub master: RedisNode,
    pub replica: RedisNode,
    sentinels: Vec<RedisNode>,
}

impl SentinelFixture {
    /// Starts the same topology for before and after measurements.
    pub fn start() -> Result<Self, Box<dyn Error>> {
        let master = RedisNode::start(None, None)?;
        let replica = RedisNode::start(Some(master.port), None)?;
        let mut sentinels = Vec::with_capacity(3);
        for _ in 0..3 {
            sentinels.push(RedisNode::start(None, Some(master.port))?);
        }
        let fixture = Self {
            master,
            replica,
            sentinels,
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            let mut connection = Client::open(fixture.replica.url())?.get_connection()?;
            let info: String = cmd("INFO").arg("replication").query(&mut connection)?;
            let mut sentinel = Client::open(fixture.sentinels[0].url())?.get_connection()?;
            let quorum = cmd("SENTINEL")
                .arg("CKQUORUM")
                .arg("benchmaster")
                .query::<String>(&mut sentinel);
            if info.contains("master_link_status:up")
                && quorum.is_ok_and(|reply| reply.starts_with("OK"))
            {
                return Ok(fixture);
            }
            thread::sleep(Duration::from_millis(100));
        }
        Err("benchmark Sentinel replication/quorum not ready".into())
    }

    /// Observes replication catch-up as fixture setup, without issuing WAIT.
    pub fn wait_replicated(&self, replica: &mut Connection) -> Result<(u64, u64), Box<dyn Error>> {
        let mut master = Client::open(self.master.url())?.get_connection()?;
        let info: String = cmd("INFO").arg("replication").query(&mut master)?;
        let offset = info
            .lines()
            .find_map(|line| line.strip_prefix("master_repl_offset:"))
            .ok_or("master replication offset missing")?
            .trim()
            .parse::<u64>()?;
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            let info: String = cmd("INFO").arg("replication").query(replica)?;
            let observed = info
                .lines()
                .find_map(|line| line.strip_prefix("slave_repl_offset:"))
                .and_then(|number| number.trim().parse::<u64>().ok())
                .unwrap_or(0);
            if observed >= offset {
                return Ok((offset, observed));
            }
            thread::sleep(Duration::from_millis(10));
        }
        Err("benchmark replica did not catch up before planned failure".into())
    }

    /// Returns non-secret provider endpoint options.
    pub fn endpoints(&self) -> String {
        self.sentinels
            .iter()
            .map(|node| format!("127.0.0.1:{}", node.port))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Observes promoted-master convergence without issuing any write fence.
    pub fn wait_promoted(&self, replica: &mut Connection) -> Result<(), Box<dyn Error>> {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            let votes = self
                .sentinels
                .iter()
                .filter(|node| {
                    Client::open(node.url())
                        .and_then(|client| client.get_connection())
                        .and_then(|mut connection| {
                            cmd("SENTINEL")
                                .arg("GET-MASTER-ADDR-BY-NAME")
                                .arg("benchmaster")
                                .query::<(String, u16)>(&mut connection)
                        })
                        .is_ok_and(|(_, port)| port == self.replica.port)
                })
                .count();
            if votes >= 2 {
                let role: String = match cmd("INFO").arg("replication").query(replica) {
                    Ok(role) => role,
                    Err(error) if error.is_io_error() => {
                        // Sentinel promotion deliberately closes normal clients.
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        *replica = Client::open(self.replica.url())?
                            .get_connection_with_timeout(remaining.min(Duration::from_secs(2)))?;
                        println!(
                            "fixture_observer_reconnect,observer=replica,after_sentinel_votes={votes}"
                        );
                        cmd("INFO").arg("replication").query(replica)?
                    }
                    Err(error) => return Err(error.into()),
                };
                if role.contains("role:master") {
                    println!(
                        "sentinel_promoted,votes={votes},replica_port={},replication_role=master",
                        self.replica.port
                    );
                    return Ok(());
                }
            }
            thread::sleep(Duration::from_millis(100));
        }
        Err("benchmark Sentinel promotion timed out".into())
    }
}
