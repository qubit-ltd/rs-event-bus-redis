// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Docker-backed disposable Redis server for integration tests.

use std::net::TcpListener;
use std::process::Command;
use std::thread;
use std::time::Duration;

/// Owns a Redis container and removes it when dropped.
pub struct RedisServer {
    container_id: String,
    url: String,
}

impl RedisServer {
    /// Starts an isolated Redis 7 service with an available local host port.
    pub fn start() -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_version("7-alpine")
    }

    /// Starts a specific Redis image tag with an available local host port.
    pub fn start_version(image_tag: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let image = format!("redis:{image_tag}");
        let port = TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port();
        let port_mapping = format!("127.0.0.1:{port}:6379");
        let output = Command::new("docker")
            .args([
                "run",
                "-d",
                "-p",
                port_mapping.as_str(),
                &image,
                "redis-server",
                "--appendonly",
                "yes",
            ])
            .output()?;
        if !output.status.success() {
            return Err(format!("docker run failed: {}", String::from_utf8_lossy(&output.stderr)).into());
        }
        let container_id = String::from_utf8(output.stdout)?.trim().to_owned();
        let server = Self {
            container_id,
            url: format!("redis://127.0.0.1:{port}/"),
        };
        for _ in 0..50 {
            if redis::Client::open(server.url.as_str())
                .and_then(|client| client.get_connection())
                .is_ok()
            {
                return Ok(server);
            }
            thread::sleep(Duration::from_millis(100));
        }
        Err("Redis container did not become ready".into())
    }

    /// Restarts the isolated Redis container and waits for its port to accept
    /// commands again.
    pub fn restart(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let output = Command::new("docker").args(["restart", &self.container_id]).output()?;
        if !output.status.success() {
            return Err(format!(
                "could not restart the isolated Redis server: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        for _ in 0..100 {
            if redis::Client::open(self.url.as_str())
                .and_then(|client| client.get_connection())
                .is_ok()
            {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(100));
        }
        let logs = Command::new("docker")
            .args(["logs", "--tail", "30", &self.container_id])
            .output()?;
        Err(format!(
            "Redis container did not become ready after restart: {}",
            String::from_utf8_lossy(&logs.stdout)
        )
        .into())
    }

    /// Returns the connection URL of the isolated service.
    pub fn url(&self) -> &str {
        &self.url
    }
}

impl Drop for RedisServer {
    /// Stops the container owned by this test fixture.
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container_id])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}
