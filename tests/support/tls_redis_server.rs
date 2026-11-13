// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Docker-backed Redis TLS fixture with short-lived test-only credentials.

use std::error::Error;
use std::fs;
use std::net::TcpListener;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::thread::sleep;
use std::time::Duration;

use rcgen::BasicConstraints;
use rcgen::CertificateParams;
use rcgen::DnType;
use rcgen::ExtendedKeyUsagePurpose;
use rcgen::IsCa;
use rcgen::KeyPair;
use redis::Client;
use tempfile::TempDir;

/// Owns a Redis TLS container and its generated test CA and server identity.
pub struct TlsRedisServer {
    container_id: Option<String>,
    child: Option<Child>,
    _directory: TempDir,
    url: String,
    ca_path: String,
    client_cert_path: String,
    client_key_path: String,
    wrong_ca_path: String,
}

impl TlsRedisServer {
    /// Starts a Redis 7 TLS container bound to an ephemeral loopback port.
    ///
    /// Performs blocking certificate, Docker, and readiness IO. Returns
    /// Certificate, Docker, port-binding, or readiness errors; a failed startup
    /// removes any partially created container.
    pub fn start() -> Result<Self, Box<dyn Error>> {
        Self::start_with_client_auth(false)
    }

    /// Starts a Redis 7 TLS container that requires a client certificate.
    ///
    /// Performs the same blocking setup as [`start`](Self::start), with Redis
    /// configured to require a certificate signed by the generated test CA.
    ///
    /// # Errors
    ///
    /// Returns certificate, Docker, port-binding, or readiness errors.
    pub fn start_mtls() -> Result<Self, Box<dyn Error>> {
        Self::start_with_client_auth(true)
    }

    /// Starts the fixture with the requested client authentication policy.
    fn start_with_client_auth(require_client_auth: bool) -> Result<Self, Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        generate_certificates(directory.path(), false)?;
        make_fixture_files_readable(directory.path())?;
        let wrong_ca_path = directory.path().join("wrong-ca.crt");
        write_self_signed_ca(&wrong_ca_path, &directory.path().join("wrong-ca.key"), "Wrong-Test-CA")?;

        let mount = format!("{}:/tls:ro", directory.path().display());
        let client_auth = if require_client_auth { "yes" } else { "no" };
        let docker = super::redis_server::start_mapped_container(
            &["run", "-d", "--rm"],
            &[
                "-v",
                &mount,
                "redis:7-alpine",
                "redis-server",
                "--port",
                "0",
                "--tls-port",
                "6379",
                "--tls-cert-file",
                "/tls/server.crt",
                "--tls-key-file",
                "/tls/server.key",
                "--tls-ca-cert-file",
                "/tls/ca.crt",
                "--tls-auth-clients",
                client_auth,
                "--appendonly",
                "yes",
            ],
            6379,
        );
        let (container_id, child, port) = match docker {
            Ok((container_id, port)) => (Some(container_id), None, port),
            Err(_) => {
                let port = TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port();
                let child = start_local_tls(directory.path(), port, require_client_auth)?;
                (None, Some(child), port)
            }
        };
        let ca_path = directory.path().join("ca.crt").to_string_lossy().into_owned();
        let client_cert_path = directory.path().join("client.crt").to_string_lossy().into_owned();
        let client_key_path = directory.path().join("client.key").to_string_lossy().into_owned();
        let server = Self {
            container_id,
            child,
            _directory: directory,
            url: format!("rediss://localhost:{port}/"),
            ca_path,
            client_cert_path,
            client_key_path,
            wrong_ca_path: wrong_ca_path.to_string_lossy().into_owned(),
        };
        for _ in 0..50 {
            if responds_to_ping(
                &server.url,
                &server.ca_path,
                &server.client_cert_path,
                &server.client_key_path,
            ) {
                return Ok(server);
            }
            sleep(Duration::from_millis(100));
        }
        let logs = server
            .container_id
            .as_deref()
            .and_then(|container_id| {
                Command::new("docker")
                    .args(["logs", "--tail", "30", container_id])
                    .output()
                    .ok()
            })
            .map_or_else(String::new, |output| {
                String::from_utf8_lossy(&output.stderr).into_owned()
            });
        Err(format!("TLS Redis container did not become ready: {}", logs).into())
    }

    /// Returns the authenticated loopback TLS URL.
    #[must_use]
    #[inline]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the test CA certificate path.
    #[must_use]
    #[inline]
    pub fn ca_path(&self) -> &str {
        &self.ca_path
    }

    /// Returns an unrelated CA path for negative verification cases.
    #[must_use]
    #[inline]
    pub fn wrong_ca_path(&self) -> &str {
        &self.wrong_ca_path
    }

    /// Returns a client certificate path signed by the test CA.
    #[must_use]
    #[inline]
    pub fn client_cert_path(&self) -> &str {
        &self.client_cert_path
    }

    /// Returns a client key path paired with the test client certificate.
    #[must_use]
    #[inline]
    pub fn client_key_path(&self) -> &str {
        &self.client_key_path
    }
}

impl Drop for TlsRedisServer {
    /// Removes the owned Redis container; cleanup process errors are ignored.
    fn drop(&mut self) {
        if let Some(container_id) = &self.container_id {
            let _ = Command::new("docker")
                .args(["rm", "-f", container_id])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn start_local_tls(directory: &std::path::Path, port: u16, require_client_auth: bool) -> Result<Child, Box<dyn Error>> {
    let server = std::env::var_os("RER_TLS_TEST_REDIS_SERVER").unwrap_or_else(|| "redis-server".into());
    let client_auth = if require_client_auth { "yes" } else { "no" };
    Ok(Command::new(server)
        .arg("--bind")
        .arg("127.0.0.1")
        .arg("--port")
        .arg("0")
        .arg("--tls-port")
        .arg(port.to_string())
        .arg("--tls-cert-file")
        .arg(directory.join("server.crt"))
        .arg("--tls-key-file")
        .arg(directory.join("server.key"))
        .arg("--tls-ca-cert-file")
        .arg(directory.join("ca.crt"))
        .arg("--tls-auth-clients")
        .arg(client_auth)
        .arg("--appendonly")
        .arg("yes")
        .arg("--dir")
        .arg(directory)
        .arg("--daemonize")
        .arg("no")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?)
}

/// Generates test-only CA, server, and client PEM files in `directory`.
///
/// # Parameters
///
/// - `directory`: Isolated temporary directory that owns all generated files.
///
/// # Returns
///
/// Success after generating a server identity for `localhost` and a signed
/// client identity.
///
/// # Errors
///
/// Returns certificate-generation or filesystem failures.
pub(super) fn generate_certificates(
    directory: &std::path::Path,
    include_loopback_ip: bool,
) -> Result<(), Box<dyn Error>> {
    let ca_key = KeyPair::generate()?;
    let mut ca_params = CertificateParams::default();
    ca_params.distinguished_name.push(DnType::CommonName, "Redis-Test-CA");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_certificate = ca_params.self_signed(&ca_key)?;
    fs::write(directory.join("ca.crt"), ca_certificate.pem())?;
    fs::write(directory.join("ca.key"), ca_key.serialize_pem())?;

    let server_key = KeyPair::generate()?;
    let mut names = vec!["localhost".to_owned()];
    if include_loopback_ip {
        names.push("127.0.0.1".to_owned());
    }
    let mut server_params = CertificateParams::new(names)?;
    server_params.distinguished_name.push(DnType::CommonName, "localhost");
    server_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ServerAuth);
    let server_certificate = server_params.signed_by(&server_key, &ca_certificate, &ca_key)?;
    fs::write(directory.join("server.crt"), server_certificate.pem())?;
    fs::write(directory.join("server.key"), server_key.serialize_pem())?;

    let client_key = KeyPair::generate()?;
    let mut client_params = CertificateParams::default();
    client_params
        .distinguished_name
        .push(DnType::CommonName, "Redis-Test-Client");
    client_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ClientAuth);
    let client_certificate = client_params.signed_by(&client_key, &ca_certificate, &ca_key)?;
    fs::write(directory.join("client.crt"), client_certificate.pem())?;
    fs::write(directory.join("client.key"), client_key.serialize_pem())?;
    Ok(())
}

fn write_self_signed_ca(
    certificate_path: &std::path::Path,
    key_path: &std::path::Path,
    common_name: &str,
) -> Result<(), Box<dyn Error>> {
    let key = KeyPair::generate()?;
    let mut params = CertificateParams::default();
    params.distinguished_name.push(DnType::CommonName, common_name);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let certificate = params.self_signed(&key)?;
    fs::write(certificate_path, certificate.pem())?;
    fs::write(key_path, key.serialize_pem())?;
    Ok(())
}

/// Makes the isolated PEM fixture readable by the unprivileged Redis image.
///
/// # Parameters
///
/// - `directory`: Temporary certificate directory mounted read-only in Docker.
///
/// # Returns
///
/// Success after applying directory traversal and PEM read permissions.
///
/// # Errors
///
/// Returns a filesystem permission update error.
fn make_fixture_files_readable(directory: &std::path::Path) -> Result<(), Box<dyn Error>> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(directory, fs::Permissions::from_mode(0o755))?;
        for name in [
            "ca.crt",
            "ca.key",
            "server.crt",
            "server.key",
            "client.crt",
            "client.key",
            "wrong-ca.crt",
            "wrong-ca.key",
        ] {
            let path = directory.join(name);
            if path.exists() {
                fs::set_permissions(path, fs::Permissions::from_mode(0o644))?;
            }
        }
    }
    Ok(())
}

/// Checks Redis readiness using the fixture CA without exposing errors.
///
/// # Parameters
///
/// - `url`: TLS URL of the disposable Redis service.
/// - `ca_path`: Test CA path.
///
/// # Returns
///
/// `true` when a verified TLS connection receives `PONG`.
fn responds_to_ping(url: &str, ca_path: &str, client_cert_path: &str, client_key_path: &str) -> bool {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let Ok(root_cert) = fs::read(ca_path) else {
        return false;
    };
    let Ok(client_cert) = fs::read(client_cert_path) else {
        return false;
    };
    let Ok(client_key) = fs::read(client_key_path) else {
        return false;
    };
    let Ok(client) = Client::build_with_tls(
        url,
        redis::TlsCertificates::new()
            .client_tls_config(redis::ClientTlsConfig::new(client_cert, client_key))
            .root_cert(root_cert),
    ) else {
        return false;
    };
    client
        .get_connection()
        .and_then(|mut connection| redis::cmd("PING").query::<String>(&mut connection))
        .is_ok_and(|reply| reply == "PONG")
}
