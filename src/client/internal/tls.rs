// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded, secret-safe loading of Redis TLS identity files.

use std::fs::File;
use std::fs::metadata;
use std::io::Read;

use redis::Client;
use redis::ClientTlsConfig;
use redis::ConnectionAddr;
use redis::ConnectionInfo;
use redis::ErrorKind;
use redis::RedisError;
use redis::TlsCertificates;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use rustls::pki_types::pem::PemObject;

const MAX_TLS_FILE_BYTES: u64 = 1_048_576;

/// Loads configured CA and client identity files and validates their PEM data.
///
/// # Parameters
///
/// - `ca_path`: Optional CA certificate path.
/// - `client_cert_path`: Optional client certificate path.
/// - `client_key_path`: Optional client private key path.
///
/// # Returns
///
/// `Some` contains the bounded PEM bytes for redis-rs; `None` selects system
/// roots and no client identity. No file content or path is retained in errors.
///
/// # Errors
///
/// Returns a fixed invalid-client-configuration error when a file cannot be
/// read, is empty, exceeds 1 MiB, or contains invalid PEM.
pub(in crate::client) fn load_certificates(
    ca_path: Option<&str>,
    client_cert_path: Option<&str>,
    client_key_path: Option<&str>,
) -> Result<Option<TlsCertificates>, RedisError> {
    if ca_path.is_none() && client_cert_path.is_none() && client_key_path.is_none() {
        return Ok(None);
    }

    let root_cert = ca_path.map(read_pem).transpose()?;
    if let Some(root_cert) = &root_cert {
        validate_certificate_pem(root_cert)?;
    }
    let client_tls = match (client_cert_path, client_key_path) {
        (Some(cert_path), Some(key_path)) => {
            let client_cert = read_pem(cert_path)?;
            let client_key = read_pem(key_path)?;
            validate_certificate_pem(&client_cert)?;
            validate_private_key_pem(&client_key)?;
            Some(ClientTlsConfig {
                client_cert,
                client_key,
            })
        }
        (None, None) => None,
        _ => return Err(tls_configuration_error()),
    };
    let certificates = TlsCertificates { client_tls, root_cert };

    // redis-rs performs PEM and key parsing while constructing the client.
    // Use a harmless TLS endpoint to validate without opening a socket.
    Client::build_with_tls("rediss://localhost/", certificates.clone()).map_err(|_| tls_configuration_error())?;

    Ok(Some(certificates))
}

/// Constructs a Redis client using custom certificates when configured.
///
/// # Parameters
///
/// - `connection_info`: Parsed endpoint with credentials already applied.
/// - `certificates`: Optional validated TLS certificate material.
///
/// # Returns
///
/// A client that preserves secure hostname verification for TLS endpoints.
///
/// # Errors
///
/// Returns a fixed TLS configuration error if redis-rs rejects the supplied
/// certificate material; otherwise returns endpoint parsing errors.
pub(in crate::client) fn build_client(
    connection_info: ConnectionInfo,
    certificates: Option<&TlsCertificates>,
) -> Result<Client, RedisError> {
    if matches!(&connection_info.addr, ConnectionAddr::TcpTls { .. }) {
        // redis-rs deliberately leaves rustls' crypto provider to the host.
        // Select ring only when no process provider is already installed.
        let _ = rustls::crypto::ring::default_provider().install_default();
    }
    match certificates {
        Some(certificates) => {
            Client::build_with_tls(connection_info, certificates.clone()).map_err(|_| tls_configuration_error())
        }
        None => Client::open(connection_info),
    }
}

/// Reads one nonempty PEM file without allowing more than 1 MiB into memory.
///
/// # Parameters
///
/// - `path`: Local path configured by the operator; never returned in errors.
///
/// # Returns
///
/// The complete file bytes when its length is in the range 1..=1 MiB.
///
/// # Errors
///
/// Returns a fixed TLS configuration error for metadata, open, read, empty,
/// or over-limit failures.
fn read_pem(path: &str) -> Result<Vec<u8>, RedisError> {
    if metadata(path).map_err(|_| tls_configuration_error())?.len() > MAX_TLS_FILE_BYTES {
        return Err(tls_configuration_error());
    }

    let file = File::open(path).map_err(|_| tls_configuration_error())?;
    let mut bytes = Vec::new();
    file.take(MAX_TLS_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| tls_configuration_error())?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_TLS_FILE_BYTES {
        return Err(tls_configuration_error());
    }
    Ok(bytes)
}

/// Rejects PEM data that does not contain at least one parseable certificate.
///
/// # Parameters
///
/// - `pem`: Bounded bytes read from an operator-configured certificate file.
///
/// # Returns
///
/// Success when the input contains one or more valid PEM certificates.
///
/// # Errors
///
/// Returns the stable TLS configuration error for malformed or empty PEM.
fn validate_certificate_pem(pem: &[u8]) -> Result<(), RedisError> {
    let certificates = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| tls_configuration_error())?;
    if certificates.is_empty() {
        return Err(tls_configuration_error());
    }
    Ok(())
}

/// Rejects PEM data that does not contain a parseable private key.
///
/// # Parameters
///
/// - `pem`: Bounded bytes read from an operator-configured private-key file.
///
/// # Returns
///
/// Success when the input contains a supported PEM private key.
///
/// # Errors
///
/// Returns the stable TLS configuration error for malformed or absent keys.
fn validate_private_key_pem(pem: &[u8]) -> Result<(), RedisError> {
    PrivateKeyDer::from_pem_slice(pem)
        .map(|_| ())
        .map_err(|_| tls_configuration_error())
}

/// Returns a stable TLS error that contains no path, PEM, or library details.
///
/// # Returns
///
/// An invalid-client-configuration error with fixed public text.
fn tls_configuration_error() -> RedisError {
    RedisError::from((ErrorKind::InvalidClientConfig, "invalid Redis TLS configuration"))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::MAX_TLS_FILE_BYTES;
    use super::build_client;
    use super::load_certificates;
    use super::read_pem;

    /// Rejects an invalid client key after parsing its certificate without exposing file content.
    #[test]
    fn test_tls_invalid_client_key_is_rejected_without_details() {
        let directory = tempdir().expect("temporary directory");
        let cert_path = directory.path().join("client-cert.pem");
        let key_path = directory.path().join("client-key.pem");
        fs::write(
            &cert_path,
            b"-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n",
        )
        .expect("write parseable certificate PEM");
        fs::write(&key_path, b"PRIVATE KEY CONTENT").expect("write invalid private key PEM");

        let result = load_certificates(
            None,
            Some(cert_path.to_str().expect("UTF-8 temporary path")),
            Some(key_path.to_str().expect("UTF-8 temporary path")),
        );
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("invalid private key is rejected"),
        };

        assert_eq!(
            error.to_string(),
            "invalid Redis TLS configuration- InvalidClientConfig"
        );
        assert!(!error.to_string().contains("PRIVATE KEY CONTENT"));
        assert!(!error.to_string().contains(key_path.to_str().expect("key path")));
    }

    /// Sanitizes redis-rs certificate-construction failures before returning them.
    #[test]
    fn test_tls_client_build_failure_is_sanitized() {
        let connection_info = redis::Client::open("redis://localhost/")
            .expect("parse plaintext connection info")
            .get_connection_info()
            .clone();
        let certificates = redis::TlsCertificates {
            client_tls: None,
            root_cert: None,
        };

        let error = match build_client(connection_info, Some(&certificates)) {
            Err(error) => error,
            Ok(_) => panic!("invalid root certificate is rejected"),
        };

        assert_eq!(
            error.to_string(),
            "invalid Redis TLS configuration- InvalidClientConfig"
        );
    }

    /// Enforces the exact inclusive per-file size limit before PEM parsing.
    #[test]
    fn test_tls_pem_file_size_limit_is_inclusive() {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("bounded.pem");
        fs::write(&path, vec![b'x'; MAX_TLS_FILE_BYTES as usize]).expect("write exact limit");
        assert_eq!(
            read_pem(path.to_str().expect("UTF-8 temporary path"))
                .expect("exact limit is accepted")
                .len(),
            MAX_TLS_FILE_BYTES as usize
        );

        fs::write(&path, vec![b'x'; MAX_TLS_FILE_BYTES as usize + 1]).expect("write over limit");
        let error = read_pem(path.to_str().expect("UTF-8 temporary path")).expect_err("over-limit file is rejected");
        assert!(!error.to_string().contains(path.to_str().expect("path")));
        assert_eq!(
            error.to_string(),
            "invalid Redis TLS configuration- InvalidClientConfig"
        );
    }

    /// Hides missing-file paths and invalid PEM details from Redis errors.
    #[test]
    fn test_tls_certificate_errors_hide_path_and_pem_details() {
        let directory = tempdir().expect("temporary directory");
        let missing_path = directory.path().join("private-ca.pem");
        let error = load_certificates(Some(missing_path.to_str().expect("UTF-8 temporary path")), None, None)
            .err()
            .expect("missing CA is rejected");
        assert!(!error.to_string().contains(missing_path.to_str().expect("path")));
        assert_eq!(
            error.to_string(),
            "invalid Redis TLS configuration- InvalidClientConfig"
        );

        let invalid_path = directory.path().join("invalid-ca.pem");
        fs::write(&invalid_path, b"PRIVATE PEM CONTENT").expect("write invalid PEM");
        let error = load_certificates(Some(invalid_path.to_str().expect("UTF-8 temporary path")), None, None)
            .err()
            .expect("invalid CA PEM is rejected");
        assert!(!error.to_string().contains(invalid_path.to_str().expect("path")));
        assert!(!error.to_string().contains("PRIVATE PEM CONTENT"));
        assert_eq!(
            error.to_string(),
            "invalid Redis TLS configuration- InvalidClientConfig"
        );
    }

    /// Leaves system roots selected when no custom TLS material is configured.
    #[test]
    fn test_tls_certificates_are_optional() {
        assert!(
            load_certificates(None, None, None)
                .expect("no custom certificate load is valid")
                .is_none()
        );
    }
}
