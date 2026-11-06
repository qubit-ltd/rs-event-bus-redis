// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Docker-backed disposable Redis server for integration tests.

use std::error::Error;
use std::io::Read;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::net::TcpStream;
use std::process::Command;
use std::process::Stdio;
use std::thread::sleep;
use std::time::Duration;
use std::time::Instant;

const CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const IO_TIMEOUT: Duration = Duration::from_millis(250);
const RETRY_DELAY: Duration = Duration::from_millis(100);

/// Repeatedly sends a bounded RESP PING until the loopback Redis endpoint is
/// ready.
pub(crate) fn wait_until_ready(url: &str, budget: Duration) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now()
        .checked_add(budget)
        .ok_or("Redis readiness deadline overflow")?;
    let address = loopback_address(url)?;
    loop {
        if remaining_until(deadline).is_none() {
            return Err("Redis readiness deadline expired before PONG".into());
        }
        if ping_once(address, deadline) {
            return Ok(());
        }
        let Some(remaining) = remaining_until(deadline) else {
            return Err("Redis readiness deadline expired before PONG".into());
        };
        sleep(RETRY_DELAY.min(remaining));
    }
}

fn remaining_until(deadline: Instant) -> Option<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    (!remaining.is_zero()).then_some(remaining)
}

fn loopback_address(url: &str) -> Result<SocketAddr, Box<dyn Error>> {
    let authority = url
        .strip_prefix("redis://")
        .ok_or("readiness probe requires a plain redis:// URL")?
        .split('/')
        .next()
        .ok_or("Redis URL has no authority")?;
    let (host, port) = authority.rsplit_once(':').ok_or("Redis URL has no port")?;
    let ip: std::net::IpAddr = host.parse()?;
    if !ip.is_loopback() {
        return Err("readiness probe only supports loopback Redis URLs".into());
    }
    Ok(SocketAddr::new(ip, port.parse()?))
}

fn ping_once(address: SocketAddr, deadline: Instant) -> bool {
    let Some(remaining) = remaining_until(deadline) else {
        return false;
    };
    let connect_timeout = CONNECT_TIMEOUT.min(remaining);
    if connect_timeout.is_zero() {
        return false;
    }
    let Ok(mut stream) = TcpStream::connect_timeout(&address, connect_timeout) else {
        return false;
    };
    let Some(remaining) = remaining_until(deadline) else {
        return false;
    };
    let write_timeout = IO_TIMEOUT.min(remaining);
    if write_timeout.is_zero() || stream.set_write_timeout(Some(write_timeout)).is_err() {
        return false;
    }
    if stream.write_all(b"*1\r\n$4\r\nPING\r\n").is_err() {
        return false;
    }
    let Some(remaining) = remaining_until(deadline) else {
        return false;
    };
    let read_timeout = IO_TIMEOUT.min(remaining);
    if read_timeout.is_zero() || stream.set_read_timeout(Some(read_timeout)).is_err() {
        return false;
    }
    let mut reply = [0_u8; 7];
    stream.read_exact(&mut reply).is_ok() && reply == *b"+PONG\r\n"
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
        wait_until_ready(&server.url, Duration::from_secs(5))
            .map_err(|error| format!("Redis container did not become ready: {error}"))?;
        Ok(server)
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
        if let Err(error) = wait_until_ready(&self.url, Duration::from_secs(10)) {
            let logs = Command::new("docker")
                .args(["logs", "--tail", "30", &self.container_id])
                .output()?;
            return Err(format!(
                "Redis container did not become ready after restart ({error}): {}",
                String::from_utf8_lossy(&logs.stdout)
            )
            .into());
        }
        Ok(())
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
