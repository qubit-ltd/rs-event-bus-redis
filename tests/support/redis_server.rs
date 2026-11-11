// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Docker-backed disposable Redis server for integration tests.

use std::error::Error;
use std::ffi::OsString;
use std::io::Read;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::net::TcpStream;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::thread::sleep;
use std::time::Duration;
use std::time::Instant;

use redis::Client;
use redis::cmd;

const CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const IO_TIMEOUT: Duration = Duration::from_millis(250);
const RETRY_DELAY: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Copy)]
pub(crate) enum VersionRequirement {
    Supported,
    Series(u64, Option<u64>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RedisVersion {
    major: u64,
    minor: u64,
    patch: u64,
    suffix: Option<String>,
}

impl RedisVersion {
    pub(crate) fn parse_info(info: &str) -> Result<Self, Box<dyn Error>> {
        let mut found = None;
        for line in info.lines().filter(|line| line.starts_with("redis_version:")) {
            if found.is_some() {
                return Err("INFO server contains repeated redis_version fields".into());
            }
            let value = line
                .strip_prefix("redis_version:")
                .ok_or("invalid redis_version field")?;
            let (numbers, suffix) = value
                .split_once('-')
                .map_or((value, None), |(head, tail)| (head, Some(tail)));
            if suffix.is_some_and(str::is_empty) {
                return Err("Redis version suffix is empty".into());
            }
            let mut parts = numbers.split('.');
            let major = parse_version_part(parts.next())?;
            let minor = parse_version_part(parts.next())?;
            let patch = parse_version_part(parts.next())?;
            if parts.next().is_some() {
                return Err("Redis version must have three numeric components".into());
            }
            found = Some(Self {
                major,
                minor,
                patch,
                suffix: suffix.map(str::to_owned),
            });
        }
        found.ok_or_else(|| "INFO server is missing redis_version".into())
    }
}

fn parse_version_part(part: Option<&str>) -> Result<u64, Box<dyn Error>> {
    let digits = part.ok_or("Redis version must have three numeric components")?;
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("Redis version components must be decimal numbers".into());
    }
    Ok(digits.parse()?)
}

impl VersionRequirement {
    pub(crate) fn matches(self, version: &RedisVersion) -> bool {
        match self {
            Self::Supported => version.major > 6 || (version.major == 6 && version.minor >= 2),
            Self::Series(major, Some(minor)) => version.major == major && version.minor == minor,
            Self::Series(major, None) => version.major == major,
        }
    }
}

fn version_requirement_for_tag(tag: &str) -> Result<VersionRequirement, Box<dyn Error>> {
    let numeric = tag.split('-').next().ok_or("empty Redis image tag")?;
    let mut parts = numeric.split('.');
    let major = parse_version_part(parts.next())?;
    let minor = parts.next().map(|part| parse_version_part(Some(part))).transpose()?;
    if parts.next().is_some() {
        return Err(format!("unsupported Redis image version tag: {tag}").into());
    }
    Ok(VersionRequirement::Series(major, minor))
}

impl std::fmt::Display for RedisVersion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}.{}.{}", self.major, self.minor, self.patch)?;
        if let Some(suffix) = &self.suffix {
            write!(formatter, "-{suffix}")?;
        }
        Ok(())
    }
}

pub(crate) fn probe_version_at(address: SocketAddr) -> Result<RedisVersion, Box<dyn Error>> {
    let info = probe_server_info_at(address)?;
    RedisVersion::parse_info(&info)
}

fn probe_server_info_at(address: SocketAddr) -> Result<String, Box<dyn Error>> {
    let info = format!("redis://{address}/").parse::<redis::ConnectionInfo>()?;
    let redis_settings = info.redis_settings().clone().set_skip_set_lib_name();
    let client = Client::open(info.set_redis_settings(redis_settings))?;
    let mut connection = client.get_connection_with_timeout(CONNECT_TIMEOUT)?;
    connection.set_write_timeout(Some(IO_TIMEOUT))?;
    connection.set_read_timeout(Some(IO_TIMEOUT))?;
    let info: String = cmd("INFO").arg("server").query(&mut connection)?;
    Ok(info)
}

fn probe_process_id_at(address: SocketAddr) -> Result<u32, Box<dyn Error>> {
    let info = probe_server_info_at(address)?;
    let process_id = info
        .lines()
        .find_map(|line| line.strip_prefix("process_id:"))
        .ok_or("Redis INFO omitted process_id")?;
    Ok(process_id.parse()?)
}

pub(crate) fn validate_version_at(
    address: SocketAddr,
    requirement: VersionRequirement,
    backend: &str,
    requested: &str,
) -> Result<RedisVersion, Box<dyn Error>> {
    let version = probe_version_at(address)?;
    if !requirement.matches(&version) {
        return Err(format!(
            "Redis version mismatch: backend={backend}, requested={requested}, expected={requirement:?}, actual={version}"
        )
        .into());
    }
    Ok(version)
}

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

fn wait_until_local_ready(child: &mut Child, url: &str, budget: Duration) -> Result<(), Box<dyn Error>> {
    let address = loopback_address(url)?;
    let deadline = Instant::now()
        .checked_add(budget)
        .ok_or("Redis readiness deadline overflow")?;
    loop {
        if let Some(status) = child.try_wait()? {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AddrInUse,
                format!("local Redis child exited before readiness: {status}"),
            )
            .into());
        }
        if ping_once(address, deadline) {
            match probe_process_id_at(address) {
                Ok(process_id) if process_id == child.id() => return Ok(()),
                Ok(process_id) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::AddrInUse,
                        format!(
                            "local Redis port {} belongs to process {process_id}, not child {}",
                            address.port(),
                            child.id()
                        ),
                    )
                    .into());
                }
                Err(_) => {}
            }
        }
        let Some(remaining) = remaining_until(deadline) else {
            return Err("local Redis readiness deadline expired before owned PONG".into());
        };
        sleep(RETRY_DELAY.min(remaining));
    }
}

fn is_local_port_collision(error: &(dyn Error + 'static)) -> bool {
    error
        .downcast_ref::<std::io::Error>()
        .is_some_and(|error| error.kind() == std::io::ErrorKind::AddrInUse)
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

/// Owns an isolated Redis container or a local fallback process.
pub struct RedisServer {
    backend: RedisBackend,
}

enum RedisBackend {
    Docker {
        container_id: String,
        url: String,
    },
    Local {
        child: Child,
        directory: tempfile::TempDir,
        port: u16,
        url: String,
        server_binary: OsString,
    },
}

enum DockerStartError {
    Unavailable(String),
    Fixture(Box<dyn Error>),
}

impl RedisServer {
    /// Starts an owned Redis 7 container with an ephemeral local host port.
    ///
    /// Returns the ready fixture. Blocks on Docker/process and readiness IO;
    /// returns port-bind, Docker, output-decoding, or readiness failure
    /// errors.
    pub fn start() -> Result<Self, Box<dyn Error>> {
        match Self::start_docker("7-alpine", VersionRequirement::Supported) {
            Ok(server) => Ok(server),
            Err(DockerStartError::Fixture(error)) => Err(error),
            Err(DockerStartError::Unavailable(docker_error)) => Self::start_local_with_binary(
                OsString::from("redis-server"),
                VersionRequirement::Supported,
                "Supported (>=6.2)",
            )
            .map_err(|local_error| {
                format!("Docker Redis 7 unavailable ({docker_error}); local Redis fallback failed: {local_error}")
                    .into()
            }),
        }
    }

    fn start_local_with_binary(
        server_binary: OsString,
        requirement: VersionRequirement,
        requested: &str,
    ) -> Result<Self, Box<dyn Error>> {
        let mut collision = None;
        for _ in 0..10 {
            let port = TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port();
            match Self::start_local_at_port(server_binary.clone(), port, requirement, requested) {
                Ok(server) => return Ok(server),
                Err(error) if is_local_port_collision(error.as_ref()) => collision = Some(error.to_string()),
                Err(error) => return Err(error),
            }
        }
        Err(format!(
            "local Redis could not claim an ephemeral port after 10 attempts: {}",
            collision.unwrap_or_else(|| "unknown port collision".into())
        )
        .into())
    }

    fn start_local_at_port(
        server_binary: OsString,
        port: u16,
        requirement: VersionRequirement,
        requested: &str,
    ) -> Result<Self, Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let child = start_local_process(port, directory.path(), &server_binary)?;
        let process_id = child.id();
        let mut server = Self {
            backend: RedisBackend::Local {
                child,
                directory,
                port,
                url: format!("redis://127.0.0.1:{port}/"),
                server_binary,
            },
        };
        let url = server.url().to_owned();
        let RedisBackend::Local { child, .. } = &mut server.backend else {
            return Err("expected local Redis process".into());
        };
        wait_until_local_ready(child, &url, Duration::from_secs(5))?;
        if child.id() != process_id {
            return Err("local Redis child identity changed during startup".into());
        }
        server.validate_version(requirement, "local", requested)?;
        Ok(server)
    }

    /// Starts a container for Redis image tag `image_tag` on an ephemeral local
    /// port.
    ///
    /// Returns the ready owned fixture. Performs Docker/process and readiness
    /// IO; returns port-bind, Docker, output-decoding, or readiness failure
    /// errors. The partially created fixture cleans up its container if
    /// readiness fails.
    pub fn start_version(image_tag: &str) -> Result<Self, Box<dyn Error>> {
        let requirement = version_requirement_for_tag(image_tag)?;
        match Self::start_docker(image_tag, requirement) {
            Ok(server) => Ok(server),
            Err(DockerStartError::Fixture(error)) => Err(error),
            Err(DockerStartError::Unavailable(docker_error)) => {
                let server_binary = local_server_for_image(image_tag)
                    .ok_or_else(|| format!("{docker_error}; no local Redis binary configured for {image_tag}"))?;
                Self::start_local_with_binary(server_binary, requirement, image_tag)
                    .map_err(|local_error| format!("{docker_error}; local Redis fallback failed: {local_error}").into())
            }
        }
    }

    fn start_docker(image_tag: &str, requirement: VersionRequirement) -> Result<Self, DockerStartError> {
        let image = format!("redis:{image_tag}");
        let (container_id, port) =
            start_mapped_container(&["run", "-d"], &[&image, "redis-server", "--appendonly", "yes"], 6379)
                .map_err(|error| DockerStartError::Unavailable(error.to_string()))?;
        let server = Self {
            backend: RedisBackend::Docker {
                container_id,
                url: format!("redis://127.0.0.1:{port}/"),
            },
        };
        wait_until_ready(server.url(), Duration::from_secs(5)).map_err(|error| {
            DockerStartError::Fixture(format!("Redis container did not become ready: {error}").into())
        })?;
        server
            .validate_version(requirement, "docker", image_tag)
            .map_err(DockerStartError::Fixture)?;
        Ok(server)
    }

    fn validate_version(
        &self,
        requirement: VersionRequirement,
        backend: &str,
        requested: &str,
    ) -> Result<(), Box<dyn Error>> {
        let address = loopback_address(self.url())?;
        let version = validate_version_at(address, requirement, backend, requested)?;
        eprintln!("Redis fixture: backend={backend}, requested={requested}, actual_version={version}");
        Ok(())
    }

    /// Restarts this fixture's owned Redis container and waits for readiness.
    ///
    /// Returns success when connections work again. Performs blocking
    /// Docker/Redis IO and returns process, unsuccessful restart, or
    /// readiness failure errors.
    pub fn restart(&mut self) -> Result<(), Box<dyn Error>> {
        match &mut self.backend {
            RedisBackend::Docker { container_id, url } => {
                let output = Command::new("docker").args(["restart", container_id]).output()?;
                if !output.status.success() {
                    return Err(format!(
                        "could not restart the isolated Redis server: {}",
                        String::from_utf8_lossy(&output.stderr)
                    )
                    .into());
                }
                if let Err(error) = wait_until_ready(url, Duration::from_secs(10)) {
                    let logs = Command::new("docker")
                        .args(["logs", "--tail", "30", container_id])
                        .output()?;
                    return Err(format!(
                        "Redis container did not become ready after restart ({error}): {}",
                        String::from_utf8_lossy(&logs.stdout)
                    )
                    .into());
                }
            }
            RedisBackend::Local {
                child,
                directory,
                port,
                url,
                server_binary,
            } => {
                shutdown_local_process(child, url)?;
                *child = start_local_process(*port, directory.path(), server_binary)?;
                wait_until_local_ready(child, url, Duration::from_secs(10))?;
            }
        }
        Ok(())
    }

    /// Returns the connection URL borrowed from this fixture without
    /// allocating.
    #[must_use]
    #[inline]
    pub fn url(&self) -> &str {
        match &self.backend {
            RedisBackend::Docker { url, .. } | RedisBackend::Local { url, .. } => url,
        }
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
        match &mut self.backend {
            RedisBackend::Docker { container_id, .. } => {
                let _ = Command::new("docker")
                    .args(["rm", "-f", container_id])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
            RedisBackend::Local { child, .. } => {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

fn local_server_for_image(image_tag: &str) -> Option<OsString> {
    if image_tag.starts_with("6.2") {
        std::env::var_os("RER_REDIS_6_2_SERVER")
    } else if image_tag.starts_with('7') {
        std::env::var_os("RER_REDIS_7_SERVER")
    } else {
        None
    }
}

fn start_local_process(
    port: u16,
    directory: &std::path::Path,
    server_binary: &std::ffi::OsStr,
) -> Result<Child, Box<dyn Error>> {
    Ok(Command::new(server_binary)
        .args([
            "--bind",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--protected-mode",
            "no",
            "--appendonly",
            "no",
            "--dir",
            directory.to_str().ok_or("Redis temp path is not UTF-8")?,
            "--loglevel",
            "warning",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?)
}

fn shutdown_local_process(child: &mut Child, url: &str) -> Result<(), Box<dyn Error>> {
    let address = loopback_address(url)?;
    if let Ok(mut stream) = TcpStream::connect_timeout(&address, CONNECT_TIMEOUT) {
        stream.set_write_timeout(Some(IO_TIMEOUT))?;
        let _ = stream.write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$4\r\nSAVE\r\n");
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if child.try_wait()?.is_some() {
            return Ok(());
        }
        sleep(RETRY_DELAY);
    }
    child.kill()?;
    child.wait()?;
    Err("local Redis did not stop gracefully after SHUTDOWN SAVE".into())
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::process::Command;
    use std::process::Stdio;
    use std::time::Duration;

    use super::RedisBackend;
    use super::RedisServer;
    use super::VersionRequirement;
    use super::is_port_allocation_conflict;
    use super::start_local_process;
    use super::wait_until_ready;

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

    #[cfg(unix)]
    #[test]
    fn test_local_fallback_diagnostic_preserves_requested_image_tag() {
        if Command::new("redis-server").arg("--version").output().is_err() {
            return;
        }
        let error = match RedisServer::start_local_with_binary(
            std::ffi::OsString::from("redis-server"),
            VersionRequirement::Series(999, Some(1)),
            "6.2-alpine",
        ) {
            Ok(_) => panic!("fixture cannot satisfy an intentionally impossible version series"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("requested=6.2-alpine"),
            "local fallback diagnostic lost its image tag: {error}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_local_fixture_version_failure_reaps_owned_process() -> Result<(), Box<dyn std::error::Error>> {
        if Command::new("redis-server").arg("--version").output().is_err() {
            return Ok(());
        }
        let port = TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port();
        let directory = tempfile::tempdir()?;
        let binary = std::ffi::OsString::from("redis-server");
        let child = start_local_process(port, directory.path(), &binary)?;
        let process_id = child.id();
        let server = RedisServer {
            backend: RedisBackend::Local {
                child,
                directory,
                port,
                url: format!("redis://127.0.0.1:{port}/"),
                server_binary: binary,
            },
        };
        wait_until_ready(server.url(), Duration::from_secs(5))?;
        let error = server
            .validate_version(VersionRequirement::Series(6, Some(2)), "local", "6.2-alpine")
            .expect_err("local Redis 7 must fail the explicit Redis 6.2 check");
        assert!(error.to_string().contains("actual=7."));
        drop(server);
        let status = Command::new("kill")
            .args(["-0", &process_id.to_string()])
            .stderr(Stdio::null())
            .status()?;
        assert!(
            !status.success(),
            "Redis child {process_id} remained alive after fixture drop"
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn test_local_fixture_does_not_claim_another_process_on_occupied_port() -> Result<(), Box<dyn std::error::Error>> {
        if Command::new("redis-server").arg("--version").output().is_err() {
            return Ok(());
        }
        let owner = RedisServer::start_local_with_binary(
            std::ffi::OsString::from("redis-server"),
            VersionRequirement::Supported,
            "Supported (>=6.2)",
        )?;
        let RedisBackend::Local { port, .. } = &owner.backend else {
            return Err("expected local Redis owner".into());
        };
        let collision = RedisServer::start_local_at_port(
            std::ffi::OsString::from("redis-server"),
            *port,
            VersionRequirement::Supported,
            "Supported (>=6.2)",
        );
        assert!(
            collision.is_err(),
            "fixture must reject a Redis process it did not start"
        );
        drop(owner);
        Ok(())
    }
}
