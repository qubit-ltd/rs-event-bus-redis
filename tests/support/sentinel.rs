// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Docker-backed isolated Redis master, replica, and three Sentinel nodes.

use std::fs;
use std::net::TcpListener;
use std::process::Command;
use std::thread;
use std::time::Duration;

use tempfile::TempDir;

/// Owns the Redis and Sentinel containers used by a failover test.
pub struct SentinelServer {
    container_ids: Vec<String>,
    directories: Vec<TempDir>,
    sentinel_ports: Vec<u16>,
    master_port: u16,
}

impl SentinelServer {
    /// Starts a master, replica, and three Sentinel containers on host
    /// networking.
    pub fn start() -> Result<Self, Box<dyn std::error::Error>> {
        let mut server = Self {
            container_ids: Vec::new(),
            directories: Vec::new(),
            sentinel_ports: Vec::new(),
            master_port: free_port()?,
        };
        let replica_port = free_port()?;
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

    /// Returns the comma-separated Sentinel endpoints for provider options.
    pub fn endpoints(&self) -> String {
        self.sentinel_ports
            .iter()
            .map(|port| format!("127.0.0.1:{port}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Returns the current master port after querying the first Sentinel.
    pub fn master_port(&self) -> Result<u16, Box<dyn std::error::Error>> {
        master_port_from(self.sentinel_ports[0])
    }

    /// Returns the master port reported by one Sentinel endpoint.
    fn master_port_at(&self, sentinel_port: u16) -> Result<u16, Box<dyn std::error::Error>> {
        master_port_from(sentinel_port)
    }

    /// Waits for a majority of Sentinel nodes to report the promoted master.
    fn promoted_master_port(&self) -> Option<u16> {
        let mut reports = self
            .sentinel_ports
            .iter()
            .filter_map(|port| self.master_port_at(*port).ok());
        let first = reports.next()?;
        let votes = 1 + reports.filter(|port| *port == first).count();
        (first != self.master_port && votes >= 2).then_some(first)
    }

    /// Stops the original master to make Sentinel promote the replica.
    pub fn stop_original_master(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let master = self.container_ids.remove(0);
        let status = Command::new("docker")
            .args(["stop", &master])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()?;
        if !status.success() {
            return Err("could not stop the isolated Redis master".into());
        }
        for _ in 0..150 {
            if self.promoted_master_port().is_some() {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(200));
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

    /// Waits until all Sentinel nodes report the master service.
    fn wait_for_quorum(&self) -> Result<(), Box<dyn std::error::Error>> {
        for _ in 0..100 {
            let all_sentinels_ready = self.sentinel_ports.iter().all(|port| {
                self.master_port_at(*port)
                    .is_ok_and(|master_port| master_port == self.master_port)
                    && redis::Client::open(format!("redis://127.0.0.1:{port}/"))
                        .and_then(|client| client.get_connection())
                        .and_then(|mut connection| {
                            redis::cmd("SENTINEL")
                                .arg("CKQUORUM")
                                .arg("qeventbus")
                                .query::<String>(&mut connection)
                        })
                        .is_ok_and(|result| result.starts_with("OK"))
            });
            let replica_ready = redis::Client::open(format!("redis://127.0.0.1:{}/", self.master_port))
                .and_then(|client| client.get_connection())
                .and_then(|mut connection| redis::cmd("INFO").arg("replication").query::<String>(&mut connection))
                .is_ok_and(|info| info.contains("connected_slaves:1"));
            if all_sentinels_ready && replica_ready {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(100));
        }
        Err("Redis Sentinel quorum did not become ready".into())
    }
}

/// Queries one Sentinel for the configured service's current master port.
fn master_port_from(sentinel_port: u16) -> Result<u16, Box<dyn std::error::Error>> {
    let client = redis::Client::open(format!("redis://127.0.0.1:{sentinel_port}/"))?;
    let mut connection = client.get_connection()?;
    let (_, port): (String, u16) = redis::cmd("SENTINEL")
        .arg("GET-MASTER-ADDR-BY-NAME")
        .arg("qeventbus")
        .query(&mut connection)?;
    Ok(port)
}

impl Drop for SentinelServer {
    /// Removes only containers and temporary data owned by this fixture.
    fn drop(&mut self) {
        for id in &self.container_ids {
            let _ = Command::new("docker")
                .args(["rm", "-f", id])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
        }
        let _ = &self.directories;
    }
}

/// Starts one disposable Redis container and returns its container ID.
fn start_container<const N: usize>(args: [&str; N]) -> Result<String, Box<dyn std::error::Error>> {
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

/// Reserves an available loopback TCP port for a test service.
fn free_port() -> Result<u16, Box<dyn std::error::Error>> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?.port())
}
