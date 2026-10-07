// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Disposable benchmark-owned Redis and Sentinel processes.

use std::error::Error;
use std::fs;
use std::net::TcpListener;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use redis::Client;
use redis::cmd;
use tempfile::TempDir;

/// Starts a local binary when available, otherwise a Redis Docker container.
pub struct RedisNode {
    child: Option<Child>,
    container: Option<String>,
    directory: TempDir,
    pub port: u16,
}

impl RedisNode {
    /// Starts a Redis node with benchmark-owned temporary persistence.
    pub fn start(replica: Option<u16>, sentinel_master: Option<u16>) -> Result<Self, Box<dyn Error>> {
        let directory = TempDir::new()?;
        let port = TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port();
        let has_local = Command::new("redis-server").arg("--version").output().is_ok();
        let data_path = if has_local {
            directory.path().display().to_string()
        } else {
            "/data".into()
        };
        let mut config = format!("port {port}\nbind 127.0.0.1\nprotected-mode no\ndir {data_path}\n");
        if let Some(master) = sentinel_master {
            config.push_str(&format!("sentinel monitor benchmaster 127.0.0.1 {master} 2\nsentinel down-after-milliseconds benchmaster 1000\nsentinel failover-timeout benchmaster 10000\nsentinel parallel-syncs benchmaster 1\n"));
        } else {
            config.push_str("appendonly yes\nappendfsync everysec\nsave \"\"\n");
            if let Some(master) = replica {
                config.push_str(&format!("replicaof 127.0.0.1 {master}\n"));
            }
        }
        fs::write(directory.path().join("redis.conf"), config)?;
        let (child, container) = if has_local {
            let mut command = Command::new("redis-server");
            command.arg(directory.path().join("redis.conf"));
            if sentinel_master.is_some() {
                command.arg("--sentinel");
            }
            (Some(command.stdout(Stdio::null()).stderr(Stdio::null()).spawn()?), None)
        } else {
            let mount = format!("{}:/data", directory.path().display());
            let mut command = Command::new("docker");
            command.args(["run", "--rm", "-d", "--network", "host"]);
            // Keep disposable persistence owned by the host user for cleanup.
            #[cfg(unix)]
            {
                let metadata = directory.path().metadata()?;
                command.args(["--user", &format!("{}:{}", metadata.uid(), metadata.gid())]);
            }
            command.args(["-v", &mount, "redis:7-alpine", "redis-server", "/data/redis.conf"]);
            if sentinel_master.is_some() {
                command.arg("--sentinel");
            }
            let output = command.output()?;
            if !output.status.success() {
                return Err(format!("benchmark Docker startup: {}", String::from_utf8_lossy(&output.stderr)).into());
            }
            (None, Some(String::from_utf8(output.stdout)?.trim().to_owned()))
        };
        let node = Self {
            child,
            container,
            directory,
            port,
        };
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            if let Ok(mut connection) = Client::open(node.url())?.get_connection()
                && cmd("PING").query::<String>(&mut connection).is_ok()
            {
                return Ok(node);
            }
            thread::sleep(Duration::from_millis(50));
        }
        Err("benchmark Redis did not become ready".into())
    }

    /// Returns this node's loopback URL.
    pub fn url(&self) -> String {
        format!("redis://127.0.0.1:{}/", self.port)
    }

    /// Abruptly stops only the master owned by this fixture.
    pub fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(mut child) = self.child.take() {
            child.kill()?;
            child.wait()?;
        }
        if let Some(container) = self.container.take() {
            let output = Command::new("docker").args(["kill", &container]).output()?;
            if !output.status.success() {
                return Err("benchmark master stop failed".into());
            }
        }
        Ok(())
    }
}

impl Drop for RedisNode {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(container) = self.container.take() {
            let _ = Command::new("docker")
                .args(["rm", "-f", &container])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        let _ = &self.directory;
    }
}
