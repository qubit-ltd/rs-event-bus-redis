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
use std::process::Command;
use std::process::Stdio;
use std::thread::sleep;
use std::time::Duration;

use redis::Client;
use tempfile::TempDir;

/// Builds owned OpenSSL arguments so temporary fixture paths remain valid.
macro_rules! openssl_args {
    ($($argument:expr),* $(,)?) => {
        vec![$(std::ffi::OsString::from($argument)),*]
    };
}

/// Owns a Redis TLS container and its generated test CA and server identity.
pub struct TlsRedisServer {
    container_id: String,
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
    /// OpenSSL, Docker, port-binding, or readiness errors; a failed startup
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
    /// Returns OpenSSL, Docker, port-binding, or readiness errors.
    pub fn start_mtls() -> Result<Self, Box<dyn Error>> {
        Self::start_with_client_auth(true)
    }

    /// Starts the fixture with the requested client authentication policy.
    fn start_with_client_auth(require_client_auth: bool) -> Result<Self, Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        generate_certificates(directory.path())?;
        make_fixture_files_readable(directory.path())?;
        let wrong_ca_path = directory.path().join("wrong-ca.crt");
        run_openssl(openssl_args![
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            path_arg(&directory.path().join("wrong-ca.key")),
            "-out",
            path_arg(&wrong_ca_path),
            "-days",
            "1",
            "-subj",
            "/CN=Wrong-Test-CA",
        ])?;

        let port = TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port();
        let port_mapping = format!("127.0.0.1:{port}:6379");
        let mount = format!("{}:/tls:ro", directory.path().display());
        let client_auth = if require_client_auth { "yes" } else { "no" };
        let output = Command::new("docker")
            .args([
                "run",
                "-d",
                "--rm",
                "-p",
                &port_mapping,
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
            ])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "docker run for TLS Redis failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        let container_id = String::from_utf8(output.stdout)?.trim().to_owned();
        let ca_path = directory.path().join("ca.crt").to_string_lossy().into_owned();
        let client_cert_path = directory.path().join("client.crt").to_string_lossy().into_owned();
        let client_key_path = directory.path().join("client.key").to_string_lossy().into_owned();
        let server = Self {
            container_id,
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
        let logs = Command::new("docker")
            .args(["logs", "--tail", "30", &server.container_id])
            .output()?;
        Err(format!(
            "TLS Redis container did not become ready: {}",
            String::from_utf8_lossy(&logs.stderr)
        )
        .into())
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
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container_id])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
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
/// Returns OpenSSL process failures, filesystem failures, or invalid output.
fn generate_certificates(directory: &std::path::Path) -> Result<(), Box<dyn Error>> {
    run_openssl(openssl_args![
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        path_arg(&directory.join("ca.key")),
        "-out",
        path_arg(&directory.join("ca.crt")),
        "-days",
        "1",
        "-subj",
        "/CN=Redis-Test-CA",
    ])?;
    run_openssl(openssl_args![
        "req",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        path_arg(&directory.join("server.key")),
        "-out",
        path_arg(&directory.join("server.csr")),
        "-subj",
        "/CN=localhost",
    ])?;
    fs::write(
        directory.join("server.ext"),
        "subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n",
    )?;
    run_openssl(openssl_args![
        "x509",
        "-req",
        "-in",
        path_arg(&directory.join("server.csr")),
        "-CA",
        path_arg(&directory.join("ca.crt")),
        "-CAkey",
        path_arg(&directory.join("ca.key")),
        "-CAcreateserial",
        "-out",
        path_arg(&directory.join("server.crt")),
        "-days",
        "1",
        "-extfile",
        path_arg(&directory.join("server.ext")),
    ])?;
    run_openssl(openssl_args![
        "req",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        path_arg(&directory.join("client.key")),
        "-out",
        path_arg(&directory.join("client.csr")),
        "-subj",
        "/CN=Redis-Test-Client",
    ])?;
    fs::write(directory.join("client.ext"), "extendedKeyUsage=clientAuth\n")?;
    run_openssl(openssl_args![
        "x509",
        "-req",
        "-in",
        path_arg(&directory.join("client.csr")),
        "-CA",
        path_arg(&directory.join("ca.crt")),
        "-CAkey",
        path_arg(&directory.join("ca.key")),
        "-CAcreateserial",
        "-out",
        path_arg(&directory.join("client.crt")),
        "-days",
        "1",
        "-extfile",
        path_arg(&directory.join("client.ext")),
    ])
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

/// Executes one OpenSSL operation and reports its sanitized process failure.
///
/// # Parameters
///
/// - `arguments`: OpenSSL arguments containing only temporary fixture paths.
///
/// # Returns
///
/// Success when OpenSSL exits successfully.
///
/// # Errors
///
/// Returns an error when OpenSSL cannot start or exits unsuccessfully.
fn run_openssl(arguments: Vec<std::ffi::OsString>) -> Result<(), Box<dyn Error>> {
    let output = Command::new("openssl").args(arguments).output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "openssl fixture generation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into())
    }
}

/// Owns a UTF-8 path argument for OpenSSL.
///
/// # Parameters
///
/// - `path`: Temporary fixture path expected to be valid UTF-8.
///
/// # Returns
///
/// An owned UTF-8 path, with an empty fallback that causes OpenSSL to fail
/// clearly.
fn path_arg(path: &std::path::Path) -> String {
    path.to_string_lossy().into_owned()
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
        redis::TlsCertificates {
            client_tls: Some(redis::ClientTlsConfig {
                client_cert,
                client_key,
            }),
            root_cert: Some(root_cert),
        },
    ) else {
        return false;
    };
    client
        .get_connection()
        .and_then(|mut connection| redis::cmd("PING").query::<String>(&mut connection))
        .is_ok_and(|reply| reply == "PONG")
}
