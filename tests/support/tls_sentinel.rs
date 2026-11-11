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
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::thread::sleep;
use std::time::Duration;
use std::time::Instant;

use redis::Client;
use redis::TlsCertificates;
use redis::cmd;
use redis::streams::StreamInfoGroupsReply;
use redis::streams::StreamPendingCountReply;
use tempfile::TempDir;

/// Owns a TLS Redis pair and three Sentinel nodes using one transport mode.
pub struct TlsSentinel {
    containers: Vec<String>,
    children: Vec<Child>,
    directories: Vec<TempDir>,
    sentinel_ports: Vec<u16>,
    master_port: u16,
    replica_port: u16,
    ca_path: String,
    sentinel_tls: bool,
}

impl TlsSentinel {
    /// Starts a Redis 7 TLS master/replica and three TLS or plaintext
    /// Sentinels.
    pub fn start(sentinel_tls: bool) -> Result<Self, Box<dyn Error>> {
        Self::start_docker(sentinel_tls).or_else(|_| Self::start_local(sentinel_tls))
    }

    fn start_docker(sentinel_tls: bool) -> Result<Self, Box<dyn Error>> {
        let certs = context(TempDir::new(), "create TLS certificate directory")?;
        context(
            generate_certificates(certs.path()),
            "generate TLS certificates",
        )?;
        context(
            make_readable(certs.path()),
            "make TLS certificates readable",
        )?;
        let master_port = context(free_port(), "allocate TLS master port")?;
        let replica_port = context(free_port(), "allocate TLS replica port")?;
        let mut fixture = Self {
            containers: Vec::new(),
            children: Vec::new(),
            directories: vec![certs],
            sentinel_ports: Vec::new(),
            master_port,
            replica_port,
            ca_path: String::new(),
            sentinel_tls,
        };
        let cert_path = fixture.directories[0].path().display().to_string();
        fixture.ca_path = fixture.directories[0]
            .path()
            .join("ca.crt")
            .to_string_lossy()
            .into_owned();
        fixture.containers.push(context(
            start_container([
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
            ]),
            "start TLS master container",
        )?);
        fixture.containers.push(context(
            start_container([
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
            ]),
            "start TLS replica container",
        )?);

        for _ in 0..3 {
            let port = context(free_port(), "allocate Sentinel port")?;
            let directory = context(TempDir::new(), "create Sentinel configuration directory")?;
            let (plain_port, tls_port) = if sentinel_tls { (0, port) } else { (port, 0) };
            let config = format!(
                "port {plain_port}\ntls-port {tls_port}\nbind 127.0.0.1\ndir /data\ntls-cert-file /tls/server.crt\ntls-key-file /tls/server.key\ntls-ca-cert-file /tls/ca.crt\ntls-auth-clients no\ntls-replication yes\nsentinel resolve-hostnames yes\nsentinel announce-hostnames yes\nsentinel monitor qeventbus localhost {master_port} 2\nsentinel down-after-milliseconds qeventbus 1500\nsentinel failover-timeout qeventbus 10000\nsentinel parallel-syncs qeventbus 1\nsentinel announce-ip localhost\n"
            );
            context(
                fs::write(directory.path().join("sentinel.conf"), config),
                "write Sentinel configuration",
            )?;
            context(
                make_sentinel_config_writable(directory.path()),
                "make Sentinel configuration writable",
            )?;
            let config_mount = format!("{}:/data", directory.path().display());
            let id = context(
                start_container([
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
                ]),
                "start Sentinel container",
            )?;
            fixture.containers.push(id);
            fixture.directories.push(directory);
            fixture.sentinel_ports.push(port);
        }
        context(
            fixture.wait_until_discovered(),
            "wait for Sentinel to discover TLS master",
        )?;
        Ok(fixture)
    }

    fn start_local(sentinel_tls: bool) -> Result<Self, Box<dyn Error>> {
        let certs = context(TempDir::new(), "create TLS certificate directory")?;
        context(
            generate_certificates(certs.path()),
            "generate TLS certificates",
        )?;
        context(
            make_readable(certs.path()),
            "make TLS certificates readable",
        )?;
        let master_port = context(free_port(), "allocate TLS master port")?;
        let replica_port = context(free_port(), "allocate TLS replica port")?;
        let cert_path = certs.path().to_string_lossy().into_owned();
        let mut fixture = Self {
            containers: Vec::new(),
            children: Vec::new(),
            directories: vec![certs],
            sentinel_ports: Vec::new(),
            master_port,
            replica_port,
            ca_path: String::new(),
            sentinel_tls,
        };
        fixture.ca_path = fixture.directories[0]
            .path()
            .join("ca.crt")
            .to_string_lossy()
            .into_owned();
        let tls_args = [
            "--port",
            "0",
            "--bind",
            "127.0.0.1",
            "--tls-cert-file",
            "",
            "--tls-key-file",
            "",
            "--tls-ca-cert-file",
            "",
            "--tls-auth-clients",
            "no",
            "--tls-replication",
            "yes",
        ];
        let mut master_args = tls_args.map(str::to_owned).to_vec();
        master_args[5] = format!("{cert_path}/server.crt");
        master_args[7] = format!("{cert_path}/server.key");
        master_args[9] = format!("{cert_path}/ca.crt");
        master_args.extend([
            "--tls-port".into(),
            master_port.to_string(),
            "--appendonly".into(),
            "yes".into(),
        ]);
        let master_data = context(TempDir::new(), "create local TLS master data directory")?;
        master_args.extend([
            "--dir".into(),
            master_data.path().to_string_lossy().into_owned(),
        ]);
        fixture.children.push(spawn_redis(&master_args)?);
        fixture.directories.push(master_data);

        let mut replica_args = tls_args.map(str::to_owned).to_vec();
        replica_args[5] = format!("{cert_path}/server.crt");
        replica_args[7] = format!("{cert_path}/server.key");
        replica_args[9] = format!("{cert_path}/ca.crt");
        replica_args.extend([
            "--tls-port".into(),
            replica_port.to_string(),
            "--replicaof".into(),
            "localhost".into(),
            master_port.to_string(),
        ]);
        let replica_data = context(TempDir::new(), "create local TLS replica data directory")?;
        replica_args.extend([
            "--dir".into(),
            replica_data.path().to_string_lossy().into_owned(),
        ]);
        fixture.children.push(spawn_redis(&replica_args)?);
        fixture.directories.push(replica_data);

        for _ in 0..3 {
            let port = context(free_port(), "allocate Sentinel port")?;
            let directory = context(TempDir::new(), "create Sentinel configuration directory")?;
            let (plain_port, tls_port) = if sentinel_tls { (0, port) } else { (port, 0) };
            let config = format!(
                "port {plain_port}\ntls-port {tls_port}\nbind 127.0.0.1\ndir {}\ntls-cert-file {cert_path}/server.crt\ntls-key-file {cert_path}/server.key\ntls-ca-cert-file {cert_path}/ca.crt\ntls-auth-clients no\ntls-replication yes\nsentinel resolve-hostnames yes\nsentinel announce-hostnames yes\nsentinel monitor qeventbus localhost {master_port} 2\nsentinel down-after-milliseconds qeventbus 1500\nsentinel failover-timeout qeventbus 10000\nsentinel parallel-syncs qeventbus 1\nsentinel announce-ip localhost\n",
                directory.path().display(),
            );
            context(
                fs::write(directory.path().join("sentinel.conf"), config),
                "write Sentinel configuration",
            )?;
            context(
                make_sentinel_config_writable(directory.path()),
                "make Sentinel configuration writable",
            )?;
            fixture.children.push(spawn_redis(&[
                directory
                    .path()
                    .join("sentinel.conf")
                    .to_string_lossy()
                    .into_owned(),
                "--sentinel".into(),
            ])?);
            fixture.directories.push(directory);
            fixture.sentinel_ports.push(port);
        }
        context(
            fixture.wait_until_discovered(),
            "wait for Sentinel to discover TLS master",
        )?;
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

    /// Returns the TLS replica port allocated for this fixture.
    #[must_use]
    pub fn replica_port(&self) -> u16 {
        self.replica_port
    }

    /// Reads the sole pending stream ID and its owner from a verified TLS node.
    pub fn pending_identity(
        &self,
        port: u16,
        stream: &str,
        group: &str,
    ) -> Result<(String, String), Box<dyn Error>> {
        let (cursor, pending) = self.group_state(port, stream, group)?;
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
        Ok((entry.id, entry.consumer))
    }

    /// Writes a barrier and issues WAIT on the same verified TLS connection to
    /// the master, then confirms the expected pending delivery on the replica.
    pub fn wait_for_pending_replication(
        &self,
        stream: &str,
        group: &str,
        expected_id: &str,
        expected_owner: &str,
    ) -> Result<(), Box<dyn Error>> {
        let mut connection = context(
            self.tls_connection(self.master_port),
            "open verified TLS master connection for replication barrier",
        )?;
        let _: String = context(
            cmd("SET")
                .arg(format!(
                    "tls-sentinel:barrier:{}",
                    expected_id.replace('-', "_")
                ))
                .arg("ready")
                .query(&mut connection),
            "write replication barrier on TLS master connection",
        )?;
        let replication_deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let replicas: usize = context(
                cmd("WAIT").arg(1).arg(1000).query(&mut connection),
                "WAIT for TLS master replication barrier",
            )?;
            if replicas >= 1 {
                break;
            }
            if Instant::now() >= replication_deadline {
                return Err(format!("master WAIT barrier reached only {replicas} replicas").into());
            }
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok((cursor, pending)) = self.group_state(self.replica_port, stream, group)
                && cursor == expected_id
                && let [entry] = pending.ids.as_slice()
                && entry.id == expected_id
                && entry.consumer == expected_owner
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "replica {} did not expose cursor/PEL {expected_id} owned by {expected_owner}",
                    self.replica_port
                )
                .into());
            }
            sleep(Duration::from_millis(25));
        }
    }

    /// Confirms the stream group has no pending entries on the verified TLS
    /// node.
    pub fn assert_pending_empty(
        &self,
        port: u16,
        stream: &str,
        group: &str,
    ) -> Result<(), Box<dyn Error>> {
        let (_, pending) = self.group_state(port, stream, group)?;
        assert!(
            pending.ids.is_empty(),
            "ACK must empty the PEL: {pending:?}"
        );
        Ok(())
    }

    fn tls_connection(&self, port: u16) -> Result<redis::Connection, Box<dyn Error>> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = context(
            Client::build_with_tls(
                format!("rediss://localhost:{port}/"),
                TlsCertificates::new().root_cert(fs::read(&self.ca_path)?),
            ),
            "build verified TLS Redis client",
        )?;
        let connection = context(
            client.get_connection_with_timeout(Duration::from_secs(2)),
            "connect to TLS Redis node",
        )?;
        // The same connection issues WAIT with a five-second bound. Keep its
        // read timeout longer so a valid WAIT response is not cut off early.
        context(
            connection.set_read_timeout(Some(Duration::from_secs(7))),
            "set TLS Redis read timeout",
        )?;
        context(
            connection.set_write_timeout(Some(Duration::from_secs(2))),
            "set TLS Redis write timeout",
        )?;
        Ok(connection)
    }

    fn group_state(
        &self,
        port: u16,
        stream: &str,
        group: &str,
    ) -> Result<(String, StreamPendingCountReply), Box<dyn Error>> {
        let mut connection = context(
            self.tls_connection(port),
            "open TLS connection for group inspection",
        )?;
        let groups: StreamInfoGroupsReply = context(
            cmd("XINFO")
                .arg("GROUPS")
                .arg(stream)
                .query(&mut connection),
            "query XINFO GROUPS over TLS",
        )?;
        let cursor = groups
            .groups
            .into_iter()
            .find(|entry| entry.name == group)
            .ok_or("expected consumer group is absent")?
            .last_delivered_id;
        let pending = context(
            cmd("XPENDING")
                .arg(stream)
                .arg(group)
                .arg("-")
                .arg("+")
                .arg(10)
                .query(&mut connection),
            "query XPENDING over TLS",
        )?;
        Ok((cursor, pending))
    }

    /// Returns the currently reported master port, polling the Sentinel quorum.
    pub fn master_port(&self) -> Result<u16, Box<dyn Error>> {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(port) =
                query_master_port(self.sentinel_ports[0], self.sentinel_tls, &self.ca_path)
            {
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
        if !self.children.is_empty() {
            let mut child = self.children.remove(0);
            let kill_result = child.kill();
            let wait_result = child.wait();
            kill_result?;
            wait_result?;
            return Ok(());
        }
        let id = self
            .containers
            .first()
            .ok_or("missing original master")?
            .clone();
        let output = Command::new("docker").args(["stop", &id]).output()?;
        if output.status.success() {
            self.containers.remove(0);
            Ok(())
        } else {
            Err(format!(
                "docker stop failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into())
        }
    }

    fn wait_until_discovered(&self) -> Result<(), Box<dyn Error>> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let all_sentinels_ready = self.sentinel_ports.iter().all(|port| {
                query_master_port(*port, self.sentinel_tls, &self.ca_path)
                    .is_ok_and(|master_port| master_port == self.master_port)
            });
            let healthy_sentinel_count = self
                .sentinel_ports
                .iter()
                .filter(|port| {
                    sentinel_has_healthy_replica(**port, self.sentinel_tls, &self.ca_path)
                })
                .count();
            let replica_ready = tls_replica_online(self.master_port, &self.ca_path);
            if all_sentinels_ready
                && healthy_sentinel_count > self.sentinel_ports.len() / 2
                && replica_ready
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
        for child in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn spawn_redis(args: &[String]) -> Result<Child, Box<dyn Error>> {
    let server =
        std::env::var_os("RER_TLS_TEST_REDIS_SERVER").unwrap_or_else(|| "redis-server".into());
    Ok(Command::new(server)
        .args(args)
        .arg("--daemonize")
        .arg("no")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?)
}

fn query_master_port(port: u16, tls: bool, ca_path: &str) -> Result<u16, Box<dyn Error>> {
    let client = sentinel_client(port, tls, ca_path)?;
    let mut connection = client.get_connection()?;
    let result: Vec<String> = redis::cmd("SENTINEL")
        .arg("get-master-addr-by-name")
        .arg("qeventbus")
        .query(&mut connection)?;
    Ok(result
        .get(1)
        .ok_or("Sentinel returned no master port")?
        .parse()?)
}

fn tls_client(port: u16, ca_path: &str) -> Result<Client, Box<dyn Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    Ok(Client::build_with_tls(
        format!("rediss://localhost:{port}/"),
        TlsCertificates::new().root_cert(fs::read(ca_path)?),
    )?)
}

fn sentinel_client(port: u16, tls: bool, ca_path: &str) -> Result<Client, Box<dyn Error>> {
    if tls {
        tls_client(port, ca_path)
    } else {
        Ok(Client::open(format!("redis://localhost:{port}/"))?)
    }
}

fn sentinel_has_healthy_replica(port: u16, tls: bool, ca_path: &str) -> bool {
    let Ok(client) = sentinel_client(port, tls, ca_path) else {
        return false;
    };
    let Ok(mut connection) = client.get_connection() else {
        return false;
    };
    let Ok(replicas) = redis::cmd("SENTINEL")
        .arg("REPLICAS")
        .arg("qeventbus")
        .query::<Vec<Vec<String>>>(&mut connection)
    else {
        return false;
    };
    replicas.iter().any(|replica| {
        let fields: std::collections::HashMap<String, String> = replica
            .chunks_exact(2)
            .filter_map(|pair| Some((pair.first()?.clone(), pair.get(1)?.clone())))
            .collect();
        fields
            .get("flags")
            .is_some_and(|flags| !flags.contains("s_down") && !flags.contains("o_down"))
            && fields
                .get("master-link-status")
                .is_some_and(|status| status == "ok")
    })
}

fn tls_replica_online(port: u16, ca_path: &str) -> bool {
    let Ok(client) = tls_client(port, ca_path) else {
        return false;
    };
    let Ok(mut connection) = client.get_connection() else {
        return false;
    };
    redis::cmd("INFO")
        .arg("replication")
        .query::<String>(&mut connection)
        .is_ok_and(|info| info.contains("connected_slaves:1") && info.contains("state=online"))
}

fn context<T, E: std::fmt::Display>(
    result: Result<T, E>,
    stage: &str,
) -> Result<T, Box<dyn Error>> {
    result.map_err(|error| format!("{stage}: {error}").into())
}

fn make_sentinel_config_writable(directory: &std::path::Path) -> Result<(), Box<dyn Error>> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o777))?;
        fs::set_permissions(
            directory.join("sentinel.conf"),
            fs::Permissions::from_mode(0o666),
        )?;
    }
    Ok(())
}

fn free_port() -> Result<u16, Box<dyn Error>> {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .map_err(|error| format!("bind temporary loopback port: {error}"))?;
    Ok(listener
        .local_addr()
        .map_err(|error| format!("read temporary loopback port: {error}"))?
        .port())
}

fn start_container<const N: usize>(args: [&str; N]) -> Result<String, Box<dyn Error>> {
    let output = Command::new("docker")
        .args(args)
        .output()
        .map_err(|error| format!("spawn docker command: {error}"))?;
    if output.status.success() {
        Ok(String::from_utf8(output.stdout)?.trim().to_owned())
    } else {
        Err(format!(
            "docker run failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into())
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

fn openssl<const N: usize>(
    directory: &std::path::Path,
    args: [&str; N],
) -> Result<(), Box<dyn Error>> {
    let output = Command::new("openssl")
        .current_dir(directory)
        .args(args)
        .output()
        .map_err(|error| format!("spawn openssl command: {error}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "openssl failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into())
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
