// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Checks the Redis fixture's version contract and bounded INFO probe.

mod support;

use std::error::Error;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

use support::redis_server::RedisVersion;
use support::redis_server::VersionRequirement;

#[test]
fn test_versions_match_requested_series_and_supported_floor() {
    let cases = [
        ("6.2.18", VersionRequirement::Series(6, Some(2)), true),
        ("7.4.2", VersionRequirement::Series(6, Some(2)), false),
        ("7.4.2", VersionRequirement::Series(7, None), true),
        ("6.2.18", VersionRequirement::Series(7, None), false),
        ("6.2.18", VersionRequirement::Supported, true),
        ("7.4.2", VersionRequirement::Supported, true),
        ("6.0.16", VersionRequirement::Supported, false),
    ];
    for (version, requirement, expected) in cases {
        let parsed =
            RedisVersion::parse_info(&format!("# Server\r\nredis_version:{version}\r\n")).expect("valid Redis version");
        assert_eq!(requirement.matches(&parsed), expected, "{version} vs {requirement:?}");
    }
}

#[test]
fn test_info_parser_rejects_missing_duplicate_malformed_and_overflow() {
    for info in [
        "# Server\r\n",
        "redis_version:6.2.1\r\nredis_version:7.0.0\r\n",
        "redis_version:6.x.1\r\n",
        "redis_version:18446744073709551616.2.1\r\n",
    ] {
        assert!(RedisVersion::parse_info(info).is_err(), "accepted {info:?}");
    }
    assert!(RedisVersion::parse_info("redis_version:7.2.1-rc1\r\n").is_ok());
}

#[test]
fn test_scripted_info_endpoint_returns_version_and_reports_missing_field() -> Result<(), Box<dyn Error>> {
    assert_eq!(scripted_probe("redis_version:7.2.1\r\n")?, "7.2.1");
    assert!(scripted_probe("# Server\r\n").is_err());
    Ok(())
}

#[test]
fn test_scripted_info_endpoint_version_mismatch_reports_backend_request_and_actual() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let addr = listener.local_addr()?;
    let worker = thread::spawn(move || -> std::io::Result<()> {
        let (mut stream, _) = listener.accept()?;
        read_until_suffix(&mut stream, b"*2\r\n$4\r\nINFO\r\n$6\r\nserver\r\n")?;
        let info = "redis_version:7.2.1\r\n";
        write!(stream, "${}\r\n{}\r\n", info.len(), info)?;
        Ok(())
    });
    let error = support::redis_server::validate_version_at(
        addr,
        VersionRequirement::Series(6, Some(2)),
        "docker",
        "6.2-alpine",
    )
    .expect_err("7.x must not satisfy the explicit 6.2 request");
    let message = error.to_string();
    assert!(message.contains("backend=docker"), "unexpected diagnostic: {message}");
    assert!(
        message.contains("requested=6.2-alpine"),
        "unexpected diagnostic: {message}"
    );
    assert!(message.contains("actual=7.2.1"), "unexpected diagnostic: {message}");
    worker.join().expect("scripted endpoint thread")?;
    Ok(())
}

#[test]
fn test_scripted_info_endpoint_timeout_is_bounded() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let addr = listener.local_addr()?;
    let worker = thread::spawn(move || {
        if let Ok((_, _)) = listener.accept() {
            thread::sleep(Duration::from_millis(600));
        }
    });
    let started = std::time::Instant::now();
    let result = support::redis_server::probe_version_at(addr);
    assert!(result.is_err());
    assert!(started.elapsed() < Duration::from_secs(2));
    worker.join().expect("scripted endpoint thread");
    Ok(())
}

fn scripted_probe(reply: &str) -> Result<String, Box<dyn Error>> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let addr = listener.local_addr()?;
    let payload = reply.to_owned();
    let worker = thread::spawn(move || -> std::io::Result<()> {
        let (mut stream, _) = listener.accept()?;
        read_until_suffix(&mut stream, b"*2\r\n$4\r\nINFO\r\n$6\r\nserver\r\n")?;
        write!(stream, "${}\r\n{}\r\n", payload.len(), payload)?;
        Ok(())
    });
    let result = support::redis_server::probe_version_at(addr).map(|version| version.to_string());
    worker.join().expect("scripted endpoint thread")?;
    result
}

fn read_until_suffix(stream: &mut TcpStream, suffix: &[u8]) -> std::io::Result<()> {
    let mut request = Vec::new();
    let mut byte = [0_u8; 1];
    while !request.ends_with(suffix) {
        stream.read_exact(&mut byte)?;
        request.push(byte[0]);
    }
    Ok(())
}
