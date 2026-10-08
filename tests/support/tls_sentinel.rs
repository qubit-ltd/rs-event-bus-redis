// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Docker-backed Redis 7 TLS master/replica and Sentinel discovery fixture.

use std::error::Error;
use std::fs;
use std::net::TcpListener;
use std::process::Command;
use std::process::Stdio;
use std::thread::sleep;
use std::time::Duration;
use std::time::Instant;

use redis::Client;
use redis::TlsCertificates;
use tempfile::TempDir;

/// Owns a TLS Redis pair and three Sentinel nodes using one transport mode.
pub struct TlsSentinel {
    containers: Vec<String>,
    directories: Vec<TempDir>,
    sentinel_ports: Vec<u16>,
    master_port: u16,
    ca_path: String,
    sentinel_tls: bool,
}

impl TlsSentinel {
    /// Starts a Redis 7 TLS master/replica and three TLS or plaintext
    /// Sentinels.
    pub fn start(sentinel_tls: bool) -> Result<Self, Box<dyn Error>> {
        let certs = TempDir::new()?;
        generate_certificates(certs.path())?;
        make_readable(certs.path())?;
        let master_port = free_port()?;
        let replica_port = free_port()?;
        let mut fixture = Self {
            containers: Vec::new(),
            directories: vec![certs],
            sentinel_ports: Vec::new(),
            master_port,
            ca_path: String::new(),
            sentinel_tls,
        };
        let cert_path = fixture.directories[0].path().display().to_string();
        fixture.ca_path = fixture.directories[0]
            .path()
            .join("ca.crt")
            .to_string_lossy()
            .into_owned();
        fixture.containers.push(start_container([
            "run",
            "--rm",
            "-d",
            "--network",
            "host",
            "-v",
            &format!("{cert_path}:/tls:ro"),
            "redis:7-alpine",
            "redis-server",
            "--port",
            "0",
            "--tls-port",
            &master_port.to_string(),
            "--bind",
            "127.0.0.1",
            "--tls-cert-file",
            "/tls/server.crt",
            "--tls-key-file",
            "/tls/server.key",
            "--tls-ca-cert-file",
            "/tls/ca.crt",
            "--tls-auth-clients",
            "no",
            "--tls-replication",
            "yes",
            "--appendonly",
            "yes",
        ])?);
        fixture.containers.push(start_container([
            "run",
            "--rm",
            "-d",
            "--network",
            "host",
            "-v",
            &format!("{cert_path}:/tls:ro"),
            "redis:7-alpine",
            "redis-server",
            "--port",
            "0",
            "--tls-port",
            &replica_port.to_string(),
            "--bind",
            "127.0.0.1",
            "--tls-cert-file",
            "/tls/server.crt",
            "--tls-key-file",
            "/tls/server.key",
            "--tls-ca-cert-file",
            "/tls/ca.crt",
            "--tls-auth-clients",
            "no",
            "--tls-replication",
            "yes",
            "--replicaof",
            "localhost",
            &master_port.to_string(),
        ])?);

        for _ in 0..3 {
            let port = free_port()?;
            let directory = TempDir::new()?;
            let (plain_port, tls_port) = if sentinel_tls { (0, port) } else { (port, 0) };
            let config = format!(
                "port {plain_port}\ntls-port {tls_port}\nbind 127.0.0.1\ndir /data\ntls-cert-file /tls/server.crt\ntls-key-file /tls/server.key\ntls-ca-cert-file /tls/ca.crt\ntls-auth-clients no\ntls-replication yes\nsentinel resolve-hostnames yes\nsentinel announce-hostnames yes\nsentinel monitor qeventbus localhost {master_port} 2\nsentinel down-after-milliseconds qeventbus 1500\nsentinel failover-timeout qeventbus 10000\nsentinel parallel-syncs qeventbus 1\nsentinel announce-ip localhost\n"
            );
            fs::write(directory.path().join("sentinel.conf"), config)?;
            make_sentinel_config_writable(directory.path())?;
            let config_mount = format!("{}:/data", directory.path().display());
            let id = start_container([
                "run",
                "--rm",
                "-d",
                "--network",
                "host",
                "-v",
                &config_mount,
                "-v",
                &format!("{cert_path}:/tls:ro"),
                "redis:7-alpine",
                "redis-server",
                "/data/sentinel.conf",
                "--sentinel",
            ])?;
            fixture.containers.push(id);
            fixture.directories.push(directory);
            fixture.sentinel_ports.push(port);
        }
        fixture.wait_until_discovered()?;
        Ok(fixture)
    }

    /// Returns the comma-separated Sentinel addresses accepted by provider
    /// options.
    #[must_use]
    pub fn endpoints(&self) -> String {
        self.sentinel_ports
            .iter()
            .map(|port| format!("localhost:{port}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Returns the isolated CA path used by Sentinel and master TLS clients.
    #[must_use]
    pub fn ca_path(&self) -> &str {
        &self.ca_path
    }

    /// Returns the original TLS master port used to construct `redis.url`.
    #[must_use]
    pub fn original_master_port(&self) -> u16 {
        self.master_port
    }

    /// Returns the currently reported master port, polling the Sentinel quorum.
    pub fn master_port(&self) -> Result<u16, Box<dyn Error>> {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(port) = query_master_port(self.sentinel_ports[0], self.sentinel_tls, &self.ca_path) {
                return Ok(port);
            }
            if Instant::now() >= deadline {
                return Err("Sentinel did not report a master before the deadline".into());
            }
            sleep(Duration::from_millis(100));
        }
    }

    /// Stops the original master so Sentinel can promote the TLS replica.
    pub fn stop_original_master(&mut self) -> Result<(), Box<dyn Error>> {
        let id = self.containers.first().ok_or("missing original master")?.clone();
        let output = Command::new("docker").args(["stop", &id]).output()?;
        if output.status.success() {
            self.containers.remove(0);
            Ok(())
        } else {
            Err(format!("docker stop failed: {}", String::from_utf8_lossy(&output.stderr)).into())
        }
    }

    fn wait_until_discovered(&self) -> Result<(), Box<dyn Error>> {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if query_master_port(self.sentinel_ports[0], self.sentinel_tls, &self.ca_path)
                .is_ok_and(|port| port == self.master_port)
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("Sentinel did not discover the TLS master before the deadline".into());
            }
            sleep(Duration::from_millis(100));
        }
    }
}

impl Drop for TlsSentinel {
    fn drop(&mut self) {
        for id in self.containers.iter().rev() {
            let _ = Command::new("docker")
                .args(["rm", "-f", id])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}

fn query_master_port(port: u16, tls: bool, ca_path: &str) -> Result<u16, Box<dyn Error>> {
    let client = if tls {
        let _ = rustls::crypto::ring::default_provider().install_default();
        Client::build_with_tls(
            format!("rediss://localhost:{port}/"),
            TlsCertificates {
                client_tls: None,
                root_cert: Some(fs::read(ca_path)?),
            },
        )?
    } else {
        Client::open(format!("redis://localhost:{port}/"))?
    };
    let mut connection = client.get_connection()?;
    let result: Vec<String> = redis::cmd("SENTINEL")
        .arg("get-master-addr-by-name")
        .arg("qeventbus")
        .query(&mut connection)?;
    Ok(result.get(1).ok_or("Sentinel returned no master port")?.parse()?)
}

fn make_sentinel_config_writable(directory: &std::path::Path) -> Result<(), Box<dyn Error>> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o777))?;
        fs::set_permissions(directory.join("sentinel.conf"), fs::Permissions::from_mode(0o666))?;
    }
    Ok(())
}

fn free_port() -> Result<u16, Box<dyn Error>> {
    Ok(TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port())
}

fn start_container<const N: usize>(args: [&str; N]) -> Result<String, Box<dyn Error>> {
    let output = Command::new("docker").args(args).output()?;
    if output.status.success() {
        Ok(String::from_utf8(output.stdout)?.trim().to_owned())
    } else {
        Err(format!("docker run failed: {}", String::from_utf8_lossy(&output.stderr)).into())
    }
}

fn generate_certificates(directory: &std::path::Path) -> Result<(), Box<dyn Error>> {
    openssl(
        directory,
        [
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "ca.key",
            "-out",
            "ca.crt",
            "-days",
            "1",
            "-subj",
            "/CN=Redis-Sentinel-Test-CA",
        ],
    )?;
    openssl(
        directory,
        [
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "server.key",
            "-out",
            "server.csr",
            "-subj",
            "/CN=localhost",
        ],
    )?;
    fs::write(
        directory.join("server.ext"),
        "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n",
    )?;
    openssl(
        directory,
        [
            "x509",
            "-req",
            "-in",
            "server.csr",
            "-CA",
            "ca.crt",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-out",
            "server.crt",
            "-days",
            "1",
            "-extfile",
            "server.ext",
        ],
    )?;
    Ok(())
}

fn openssl<const N: usize>(directory: &std::path::Path, args: [&str; N]) -> Result<(), Box<dyn Error>> {
    let output = Command::new("openssl").current_dir(directory).args(args).output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!("openssl failed: {}", String::from_utf8_lossy(&output.stderr)).into())
    }
}

fn make_readable(directory: &std::path::Path) -> Result<(), Box<dyn Error>> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o755))?;
        for name in ["ca.crt", "ca.key", "server.crt", "server.key"] {
            fs::set_permissions(directory.join(name), fs::Permissions::from_mode(0o644))?;
        }
    }
    Ok(())
}
