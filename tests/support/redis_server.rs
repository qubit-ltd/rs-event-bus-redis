// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Docker-backed disposable Redis server for integration tests.

use std::error::Error;
use std::net::TcpListener;
use std::process::Command;
use std::process::Stdio;
use std::thread::sleep;
use std::time::Duration;

use redis::Client;

/// Returns whether Redis accepts a command after opening a connection.
fn responds_to_ping(url: &str) -> bool {
    Client::open(url)
        .and_then(|client| client.get_connection())
        .and_then(|mut connection| redis::cmd("PING").query::<String>(&mut connection))
        .is_ok_and(|reply| reply == "PONG")
}

/// Owns a Redis container and removes it when dropped.
pub struct RedisServer {
    container_id: String,
    url: String,
}

impl RedisServer {
    /// Starts an owned Redis 7 container with an ephemeral local host port.
    ///
    /// Returns the ready fixture. Blocks on Docker/process and readiness IO;
    /// returns port-bind, Docker, output-decoding, or readiness failure
    /// errors.
    pub fn start() -> Result<Self, Box<dyn Error>> {
        Self::start_version("7-alpine")
    }

    /// Starts a container for Redis image tag `image_tag` on an ephemeral local
    /// port.
    ///
    /// Returns the ready owned fixture. Performs Docker/process and readiness
    /// IO; returns port-bind, Docker, output-decoding, or readiness failure
    /// errors. The partially created fixture cleans up its container if
    /// readiness fails.
    pub fn start_version(image_tag: &str) -> Result<Self, Box<dyn Error>> {
        let image = format!("redis:{image_tag}");
        let (container_id, port) =
            start_mapped_container(&["run", "-d"], &[&image, "redis-server", "--appendonly", "yes"], 6379)?;
        let server = Self {
            container_id,
            url: format!("redis://127.0.0.1:{port}/"),
        };
        for _ in 0..50 {
            if responds_to_ping(&server.url) {
                return Ok(server);
            }
            sleep(Duration::from_millis(100));
        }
        Err("Redis container did not become ready".into())
    }

    /// Restarts this fixture's owned Redis container and waits for readiness.
    ///
    /// Returns success when connections work again. Performs blocking
    /// Docker/Redis IO and returns process, unsuccessful restart, or
    /// readiness failure errors.
    pub fn restart(&mut self) -> Result<(), Box<dyn Error>> {
        let output = Command::new("docker").args(["restart", &self.container_id]).output()?;
        if !output.status.success() {
            return Err(format!(
                "could not restart the isolated Redis server: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        for _ in 0..100 {
            if responds_to_ping(&self.url) {
                return Ok(());
            }
            sleep(Duration::from_millis(100));
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

    /// Returns the connection URL borrowed from this fixture without
    /// allocating.
    #[must_use]
    #[inline]
    pub fn url(&self) -> &str {
        &self.url
    }
}

/// Starts a Docker container on a free loopback port and retries bind races.
pub(super) fn start_mapped_container(
    before_mapping: &[&str],
    after_mapping: &[&str],
    container_port: u16,
) -> Result<(String, u16), Box<dyn Error>> {
    let mut last_error = String::new();
    for _ in 0..10 {
        let port = TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port();
        let mapping = format!("127.0.0.1:{port}:{container_port}");
        let output = Command::new("docker")
            .args(before_mapping)
            .args(["-p", mapping.as_str()])
            .args(after_mapping)
            .output()?;
        if output.status.success() {
            return Ok((String::from_utf8(output.stdout)?.trim().to_owned(), port));
        }
        last_error = String::from_utf8_lossy(&output.stderr).into_owned();
        if !is_port_allocation_conflict(&last_error) {
            return Err(format!("docker run failed: {last_error}").into());
        }
    }
    Err(format!("docker could not allocate a test port after 10 attempts: {last_error}").into())
}

/// Returns whether Docker failed because another process claimed the port.
fn is_port_allocation_conflict(error: &str) -> bool {
    error.contains("port is already allocated") || error.contains("address already in use")
}

impl Drop for RedisServer {
    /// Removes the container owned by this test fixture.
    ///
    /// Blocks on Docker process IO; cleanup errors are deliberately ignored.
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container_id])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

#[cfg(test)]
mod tests {
    use super::is_port_allocation_conflict;

    #[test]
    fn test_port_allocation_conflict_is_retryable() {
        assert!(is_port_allocation_conflict(
            "Bind for 127.0.0.1:49153 failed: port is already allocated"
        ));
    }

    #[test]
    fn test_other_docker_failures_are_not_port_allocation_conflicts() {
        assert!(!is_port_allocation_conflict("Unable to find image redis:latest"));
    }
}
