// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Fresh master/replica and Sentinel quorum for one measured failover.

use std::error::Error;
use std::io::copy;
use std::net::Shutdown;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::thread;
use std::thread::JoinHandle;
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
    sentinel_proxies: Vec<SentinelConnectionCounter>,
}

/// Transparent TCP relay that counts provider sockets without counting
/// Sentinel peer/monitor traffic or observer connections.
struct SentinelConnectionCounter {
    address: std::net::SocketAddr,
    accepted: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl SentinelConnectionCounter {
    fn start(target_port: u16) -> Result<Self, Box<dyn Error>> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let accepted = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let thread_accepted = Arc::clone(&accepted);
        let thread_stop = Arc::clone(&stop);
        let worker = thread::spawn(move || {
            let mut connections = Vec::new();
            while !thread_stop.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((client, _)) => {
                        thread_accepted.fetch_add(1, Ordering::SeqCst);
                        connections.push(thread::spawn(move || {
                            if let Ok(server) = TcpStream::connect(("127.0.0.1", target_port)) {
                                relay(client, server);
                            }
                        }));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(_) => break,
                }
            }
            for connection in connections {
                let _ = connection.join();
            }
        });
        Ok(Self {
            address,
            accepted,
            stop,
            worker: Some(worker),
        })
    }

    fn endpoint(&self) -> String {
        self.address.to_string()
    }

    fn accepted(&self) -> u64 {
        self.accepted.load(Ordering::SeqCst)
    }
}

impl Drop for SentinelConnectionCounter {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.address);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn relay(client: TcpStream, server: TcpStream) {
    let Ok(mut client_reader) = client.try_clone() else {
        return;
    };
    let Ok(mut server_reader) = server.try_clone() else {
        return;
    };
    let mut client_writer = client;
    let mut server_writer = server;
    let reverse = thread::spawn(move || {
        let _ = copy(&mut server_reader, &mut client_writer);
        let _ = client_writer.shutdown(Shutdown::Write);
    });
    let _ = copy(&mut client_reader, &mut server_writer);
    let _ = server_writer.shutdown(Shutdown::Write);
    let _ = reverse.join();
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
            sentinel_proxies: Vec::new(),
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
            if info.contains("master_link_status:up") && quorum.is_ok_and(|reply| reply.starts_with("OK")) {
                let mut fixture = fixture;
                fixture.sentinel_proxies = fixture
                    .sentinels
                    .iter()
                    .map(|sentinel| SentinelConnectionCounter::start(sentinel.port))
                    .collect::<Result<_, _>>()?;
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
        self.sentinel_proxies
            .iter()
            .map(SentinelConnectionCounter::endpoint)
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Reads each Sentinel node independently, excluding observer traffic
    /// from the measured provider interval.
    pub fn sentinel_connection_counts(&self) -> Result<Vec<u64>, Box<dyn Error>> {
        Ok(self
            .sentinel_proxies
            .iter()
            .map(SentinelConnectionCounter::accepted)
            .collect())
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
                        println!("fixture_observer_reconnect,observer=replica,after_sentinel_votes={votes}");
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
