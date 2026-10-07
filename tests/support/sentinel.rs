// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Docker-backed isolated Redis master, replica, and three Sentinel nodes.

use std::error::Error;
use std::fs;
use std::net::TcpListener;
use std::process::Command;
use std::process::Stdio;
use std::thread::sleep;
use std::time::Duration;
use std::time::Instant;

use redis::Client;
use redis::cmd;
use redis::streams::StreamInfoGroupsReply;
use redis::streams::StreamPendingCountReply;
use tempfile::TempDir;

/// Owns the Redis and Sentinel containers used by a failover test.
pub struct SentinelServer {
    container_ids: Vec<String>,
    directories: Vec<TempDir>,
    sentinel_ports: Vec<u16>,
    master_port: u16,
    replica_port: u16,
}

impl SentinelServer {
    /// Starts owned master, replica, and three Sentinel containers on host
    /// networking.
    ///
    /// Returns the ready isolated fixture. Performs blocking
    /// Docker/Redis/filesystem IO; returns port, temporary-directory,
    /// configuration-write, container-start, output-decoding, or readiness
    /// errors. Owned partial state is cleaned on failure.
    pub fn start() -> Result<Self, Box<dyn Error>> {
        let mut server = Self {
            container_ids: Vec::new(),
            directories: Vec::new(),
            sentinel_ports: Vec::new(),
            master_port: free_port()?,
            replica_port: free_port()?,
        };
        let replica_port = server.replica_port;
        let data_dir = TempDir::new()?;
        let master_id = start_container([
            "run",
            "--rm",
            "-d",
            "--network",
            "host",
            "-v",
            &format!("{}:/data", data_dir.path().display()),
            "redis:7-alpine",
            "redis-server",
            "--port",
            &server.master_port.to_string(),
            "--bind",
            "127.0.0.1",
            "--dir",
            "/data",
            "--appendonly",
            "yes",
        ])?;
        server.container_ids.push(master_id);
        server.directories.push(data_dir);

        let replica_dir = TempDir::new()?;
        let replica_id = start_container([
            "run",
            "--rm",
            "-d",
            "--network",
            "host",
            "-v",
            &format!("{}:/data", replica_dir.path().display()),
            "redis:7-alpine",
            "redis-server",
            "--port",
            &replica_port.to_string(),
            "--bind",
            "127.0.0.1",
            "--dir",
            "/data",
            "--replicaof",
            "127.0.0.1",
            &server.master_port.to_string(),
        ])?;
        server.container_ids.push(replica_id);
        server.directories.push(replica_dir);

        for _ in 0..3 {
            let port = free_port()?;
            let directory = TempDir::new()?;
            let config = format!(
                "port {port}\nbind 127.0.0.1\ndir /data\nsentinel monitor qeventbus 127.0.0.1 {} 2\nsentinel down-after-milliseconds qeventbus 2000\nsentinel failover-timeout qeventbus 10000\nsentinel parallel-syncs qeventbus 1\n",
                server.master_port,
            );
            fs::write(directory.path().join("sentinel.conf"), config)?;
            let mount = format!("{}:/data", directory.path().display());
            let id = start_container([
                "run",
                "--rm",
                "-d",
                "--network",
                "host",
                "-v",
                mount.as_str(),
                "redis:7-alpine",
                "redis-server",
                "/data/sentinel.conf",
                "--sentinel",
            ])?;
            server.container_ids.push(id);
            server.directories.push(directory);
            server.sentinel_ports.push(port);
        }
        server.wait_for_quorum()?;
        Ok(server)
    }

    /// Returns allocated comma-separated local Sentinel endpoints for provider
    /// options.
    #[must_use]
    pub fn endpoints(&self) -> String {
        self.sentinel_ports
            .iter()
            .map(|port| format!("127.0.0.1:{port}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Queries the first Sentinel and returns its reported master TCP port.
    ///
    /// Performs blocking Redis IO and returns client, connection, or
    /// reply-decoding errors.
    pub fn master_port(&self) -> Result<u16, Box<dyn Error>> {
        master_port_from(self.sentinel_ports[0])
    }

    /// Returns the original replica's local TCP port without performing IO.
    #[must_use]
    #[inline]
    pub fn replica_port(&self) -> u16 {
        self.replica_port
    }

    /// Reads the group cursor and sole pending ID/owner on node `port`.
    ///
    /// `stream` and `group` are actual Redis names. Returns the stream ID and
    /// owner. Performs blocking Redis IO; returns connection, command, or
    /// missing-group errors. Panics unless the sole pending ID exactly
    /// equals the group delivery cursor.
    pub fn pending_identity(
        &self,
        port: u16,
        stream: &str,
        group: &str,
    ) -> Result<(String, String), Box<dyn Error>> {
        let (cursor, pending) = group_state(port, stream, group)?;
        assert_eq!(
            pending.ids.len(),
            1,
            "the delivered record must be the sole pending entry"
        );
        let entry = pending
            .ids
            .into_iter()
            .next()
            .ok_or("missing pending entry")?;
        assert_eq!(
            cursor, entry.id,
            "group cursor must identify the pending delivery"
        );
        eprintln!(
            "R13 node={port} cursor={cursor} pending={} owner={}",
            entry.id, entry.consumer
        );
        Ok((entry.id, entry.consumer))
    }

    /// Polls node `port` until `stream`/`group` has the exact expected
    /// delivery.
    ///
    /// Success requires cursor/PEL ID `expected_id` and owner `expected_owner`.
    /// Performs blocking Redis reads and brief sleeps; returns an error with
    /// the last observed state when the 15-second polling deadline is
    /// reached. Direct replica reads prove provider writes that WAIT on a
    /// new observer connection cannot fence.
    pub fn wait_for_pending(
        &self,
        port: u16,
        stream: &str,
        group: &str,
        expected_id: &str,
        expected_owner: &str,
    ) -> Result<(), Box<dyn Error>> {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let state = group_state(port, stream, group);
            if let Ok((cursor, pending)) = &state
                && cursor == expected_id
                && let [entry] = pending.ids.as_slice()
                && entry.id == expected_id
                && entry.consumer == expected_owner
            {
                eprintln!(
                    "R13 replicated node={port} cursor={cursor} pending={} owner={}",
                    entry.id, entry.consumer
                );
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "node {port} did not replicate cursor={expected_id}, pending owner={expected_owner}: {state:?}"
                )
                .into());
            }
            sleep(Duration::from_millis(25));
        }
    }

    /// Confirms group `group` in `stream` has no pending IDs on node `port`.
    ///
    /// Returns success after blocking Redis reads, or
    /// connection/command/missing-group errors. Panics if ACK has left any
    /// entry in the observed PEL.
    pub fn assert_pending_empty(
        &self,
        port: u16,
        stream: &str,
        group: &str,
    ) -> Result<(), Box<dyn Error>> {
        let (_, pending) = group_state(port, stream, group)?;
        assert!(
            pending.ids.is_empty(),
            "ACK must empty the PEL: {pending:?}"
        );
        eprintln!("R13 ACK node={port} PEL=[]");
        Ok(())
    }

    /// Queries Sentinel `sentinel_port` and returns its reported master TCP
    /// port.
    ///
    /// Performs blocking Redis IO and returns client, connection, or
    /// reply-decoding errors.
    fn master_port_at(&self, sentinel_port: u16) -> Result<u16, Box<dyn Error>> {
        master_port_from(sentinel_port)
    }

    /// Returns Some(promoted port) once at least two Sentinels agree on a new
    /// master.
    ///
    /// Returns None without that majority, including when available replies are
    /// insufficient. Performs blocking Sentinel IO and ignores individual query
    /// errors.
    fn promoted_master_port(&self) -> Option<u16> {
        let mut reports = self
            .sentinel_ports
            .iter()
            .filter_map(|port| self.master_port_at(*port).ok());
        let first = reports.next()?;
        let votes = 1 + reports.filter(|port| *port == first).count();
        (first != self.master_port && votes >= 2).then_some(first)
    }

    /// Stops the original master and waits for a majority to report promotion.
    ///
    /// This is the fixture's one-time failover transition. Returns success
    /// after blocking Docker/Redis IO, or process/stop/promotion-deadline
    /// failure errors.
    pub fn stop_original_master(&mut self) -> Result<(), Box<dyn Error>> {
        let master = self.container_ids.remove(0);
        let status = Command::new("docker")
            .args(["stop", &master])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;
        if !status.success() {
            return Err("could not stop the isolated Redis master".into());
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if self.promoted_master_port().is_some() {
                return Ok(());
            }
            sleep(Duration::from_millis(200));
        }
        let logs = self
            .container_ids
            .iter()
            .filter_map(|id| {
                Command::new("docker")
                    .args(["logs", "--tail", "20", id])
                    .output()
                    .ok()
                    .map(|output| String::from_utf8_lossy(&output.stdout).into_owned())
            })
            .collect::<Vec<_>>()
            .join("\n");
        Err(format!("Sentinel did not promote the isolated replica; logs:\n{logs}").into())
    }

    /// Waits for all Sentinels to report the original master and a connected
    /// replica.
    ///
    /// Performs blocking Redis reads and brief sleeps. Returns success on
    /// readiness or a readiness error at the polling deadline; individual
    /// query errors are retried.
    fn wait_for_quorum(&self) -> Result<(), Box<dyn Error>> {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            let all_sentinels_ready = self.sentinel_ports.iter().all(|port| {
                self.master_port_at(*port)
                    .is_ok_and(|master_port| master_port == self.master_port)
                    && Client::open(format!("redis://127.0.0.1:{port}/"))
                        .and_then(|client| client.get_connection())
                        .and_then(|mut connection| {
                            cmd("SENTINEL")
                                .arg("CKQUORUM")
                                .arg("qeventbus")
                                .query::<String>(&mut connection)
                        })
                        .is_ok_and(|result| result.starts_with("OK"))
            });
            let replica_ready = Client::open(format!("redis://127.0.0.1:{}/", self.master_port))
                .and_then(|client| client.get_connection())
                .and_then(|mut connection| {
                    cmd("INFO")
                        .arg("replication")
                        .query::<String>(&mut connection)
                })
                .is_ok_and(|info| info.contains("connected_slaves:1"));
            if all_sentinels_ready && replica_ready {
                return Ok(());
            }
            sleep(Duration::from_millis(100));
        }
        Err("Redis Sentinel quorum did not become ready".into())
    }
}

/// Queries Sentinel `sentinel_port` for the qeventbus master TCP port.
///
/// Returns that port after blocking Redis IO, or
/// client/connection/reply-decoding errors.
fn master_port_from(sentinel_port: u16) -> Result<u16, Box<dyn Error>> {
    let client = Client::open(format!("redis://127.0.0.1:{sentinel_port}/"))?;
    let mut connection = client.get_connection()?;
    let (_, port): (String, u16) = cmd("SENTINEL")
        .arg("GET-MASTER-ADDR-BY-NAME")
        .arg("qeventbus")
        .query(&mut connection)?;
    Ok(port)
}

impl Drop for SentinelServer {
    /// Removes only containers and temporary state owned by this fixture.
    ///
    /// Blocks on Docker process IO; container cleanup errors are deliberately
    /// ignored.
    fn drop(&mut self) {
        for id in &self.container_ids {
            let _ = Command::new("docker")
                .args(["rm", "-f", id])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        let _ = &self.directories;
    }
}

/// Starts Docker with the `N` supplied process arguments `args`.
///
/// Returns the trimmed container ID. Performs blocking process IO; returns
/// spawn, unsuccessful-exit, or invalid stdout UTF-8 errors, including Docker
/// stderr context.
fn start_container<const N: usize>(args: [&str; N]) -> Result<String, Box<dyn Error>> {
    let output = Command::new("docker").args(args).output()?;
    if !output.status.success() {
        return Err(format!(
            "could not start Redis test container: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

/// Returns a currently available loopback TCP port using a temporary listener.
///
/// Performs socket IO and releases the listener before returning. Returns bind
/// or local-address lookup errors; it does not retain a reservation for the
/// caller.
fn free_port() -> Result<u16, Box<dyn Error>> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?.port())
}

/// Reads group `group` from Redis `stream` on local node `port`.
///
/// Returns its delivery cursor and extended pending reply with up to ten IDs.
/// Performs blocking Redis IO with one-second connection/read/write timeouts.
/// Returns client, connection, timeout-setting, command, or missing-group
/// errors.
fn group_state(
    port: u16,
    stream: &str,
    group: &str,
) -> Result<(String, StreamPendingCountReply), Box<dyn Error>> {
    let client = Client::open(format!("redis://127.0.0.1:{port}/"))?;
    let mut connection = client.get_connection_with_timeout(Duration::from_secs(1))?;
    connection.set_read_timeout(Some(Duration::from_secs(1)))?;
    connection.set_write_timeout(Some(Duration::from_secs(1)))?;
    let groups: StreamInfoGroupsReply = cmd("XINFO")
        .arg("GROUPS")
        .arg(stream)
        .query(&mut connection)?;
    let cursor = groups
        .groups
        .into_iter()
        .find(|entry| entry.name == group)
        .ok_or("expected consumer group is absent")?
        .last_delivered_id;
    let pending = cmd("XPENDING")
        .arg(stream)
        .arg(group)
        .arg("-")
        .arg("+")
        .arg(10)
        .query(&mut connection)?;
    Ok((cursor, pending))
}
