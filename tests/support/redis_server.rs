// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Docker-backed disposable Redis server for integration tests.

use std::process::Command;
use std::thread;
use std::time::Duration;

/// Owns a Redis container and removes it when dropped.
pub struct RedisServer {
    container_id: String,
    url: String,
}

impl RedisServer {
    /// Starts an isolated Redis 7 service with a Docker-assigned host port.
    pub fn start() -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_version("7-alpine")
    }

    /// Starts a specific Redis image tag with a Docker-assigned host port.
    pub fn start_version(image_tag: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let image = format!("redis:{image_tag}");
        let output = Command::new("docker")
            .args(["run", "--rm", "-d", "-p", "127.0.0.1::6379", &image])
            .output()?;
        if !output.status.success() {
            return Err(format!("docker run failed: {}", String::from_utf8_lossy(&output.stderr)).into());
        }
        let container_id = String::from_utf8(output.stdout)?.trim().to_owned();
        let port_output = Command::new("docker")
            .args(["port", &container_id, "6379/tcp"])
            .output()?;
        if !port_output.status.success() {
            let _ = Command::new("docker").args(["rm", "-f", &container_id]).status();
            return Err("could not find the mapped Redis port".into());
        }
        let mapped = String::from_utf8(port_output.stdout)?;
        let port = mapped.trim().rsplit(':').next().ok_or("missing mapped port")?;
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
