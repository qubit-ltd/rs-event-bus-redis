// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Restricted-ACL subprocess helpers for Redis integration tests.

use std::error::Error;
use std::fs;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::thread::sleep;
use std::time::Duration;
use std::time::Instant;

use redis::Connection;
use redis::cmd;
use tempfile::TempDir;

use crate::support::redis_server::RedisServer;

const USER: &str = "qubit_acl_test";
const PASSWORD: &str = "ephemeral-acl-test-password";
const TIMEOUT: Duration = Duration::from_secs(30);

/// Runs an ignored helper in an isolated child process and validates the
/// administrator-observed quarantine result.
pub fn run_workflow(image: &str, case: &str, helper: &str) -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start_version(image)?;
    let mut admin = redis::Client::open(server.url())?.get_connection()?;
    let namespace = format!("acl-{}-{}", case.replace('_', "-"), image);
    let source = qubit_event_bus_redis::naming::stream_key(&namespace, "events");
    let group = qubit_event_bus_redis::naming::group_name(&namespace, "events", "worker", Some("workers"));
    let quarantine = qubit_event_bus_redis::naming::poison_key(&namespace, "events", &group);
    let allow_quarantine = case != "missing_quarantine_key";
    let allow_eval = case != "missing_eval";
    create_user(&mut admin, &source, &quarantine, allow_eval, allow_quarantine)?;

    let paths = TempDir::new()?;
    let ready = paths.path().join("ready");
    let proceed = paths.path().join("proceed");
    let mut child = Command::new(std::env::current_exe()?)
        .args(["--ignored", "--exact", helper, "--nocapture"])
        .env("RER_ACL_URL", server.url())
        .env("RER_ACL_NAMESPACE", &namespace)
        .env("RER_ACL_CASE", case)
        .env("RER_ACL_USER", USER)
        .env("RER_ACL_PASSWORD", PASSWORD)
        .env("RER_ACL_READY", &ready)
        .env("RER_ACL_PROCEED", &proceed)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    wait_for_marker(&mut child, &ready)?;
    let source_id: String = cmd("XADD")
        .arg(&source)
        .arg("*")
        .arg("wire")
        .arg(b"not-json".as_slice())
        .query(&mut admin)?;
    fs::write(&proceed, b"continue")?;
    let output = wait_child(child, TIMEOUT)?;
    if !output.status.success() {
        return Err(format!(
            "ACL helper {helper} failed ({}): {}{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    assert_source_state(&mut admin, &source, &group, case, &source_id)?;
    if case == "workflow" {
        assert_quarantine(&mut admin, &quarantine, &source_id)?;
    }
    Ok(())
}

fn create_user(
    admin: &mut Connection,
    source: &str,
    quarantine: &str,
    allow_eval: bool,
    allow_quarantine: bool,
) -> Result<(), Box<dyn Error>> {
    let mut command = cmd("ACL");
    command.arg("SETUSER").arg(USER).arg("reset").arg("on");
    command.arg(format!(">{PASSWORD}"));
    command.arg(format!("~{source}"));
    if allow_quarantine {
        command.arg(format!("~{quarantine}"));
    }
    for permission in [
        "+ping",
        "+client|setinfo",
        "+xadd",
        "+xgroup|create",
        "+xreadgroup",
        "+xautoclaim",
        "+xpending",
        "+xrange",
        "+xack",
        "+type",
        "+role",
    ] {
        command.arg(permission);
    }
    if allow_eval {
        command.arg("+eval");
    }
    command.query::<()>(&mut *admin)?;
    Ok(())
}

fn wait_for_marker(child: &mut Child, marker: &std::path::Path) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline {
        if marker.exists() {
            return Ok(());
        }
        if let Some(status) = child.try_wait()? {
            return Err(format!("ACL child exited before readiness marker: {status}").into());
        }
        sleep(Duration::from_millis(20));
    }
    child.kill()?;
    let _ = child.wait();
    Err("ACL helper did not reach its readiness marker within 30 seconds".into())
}

fn wait_child(mut child: Child, timeout: Duration) -> Result<std::process::Output, Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        if child.try_wait()?.is_some() {
            return child.wait_with_output().map_err(Into::into);
        }
        if Instant::now() >= deadline {
            child.kill()?;
            let output = child.wait_with_output()?;
            return Err(format!(
                "ACL helper exceeded {timeout:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        sleep(Duration::from_millis(20));
    }
}

fn assert_source_state(
    admin: &mut Connection,
    source: &str,
    group: &str,
    case: &str,
    source_id: &str,
) -> Result<(), Box<dyn Error>> {
    let pending: Vec<redis::Value> = cmd("XPENDING")
        .arg(source)
        .arg(group)
        .arg("-")
        .arg("+")
        .arg(100)
        .query(admin)?;
    if case == "workflow" {
        assert!(pending.is_empty(), "successful quarantine must ACK the source record");
    } else {
        assert_eq!(
            pending.len(),
            1,
            "denied EVAL must not be reported as successful quarantine"
        );
        let row: Vec<redis::Value> = redis::from_redis_value(pending.first().ok_or("missing PEL record")?.clone())?;
        let id: String = redis::from_redis_value(row.first().ok_or("missing PEL ID")?.clone())?;
        assert_eq!(id, source_id);
    }
    Ok(())
}

fn assert_quarantine(admin: &mut Connection, quarantine: &str, source_id: &str) -> Result<(), Box<dyn Error>> {
    let rows: redis::streams::StreamRangeReply = cmd("XRANGE").arg(quarantine).arg("-").arg("+").query(admin)?;
    assert_eq!(rows.ids.len(), 1);
    let values = &rows.ids[0].map;
    assert_eq!(
        values.get("source_id"),
        Some(&redis::Value::BulkString(source_id.as_bytes().to_vec()))
    );
    assert_eq!(
        values.get("reason"),
        Some(&redis::Value::BulkString(b"invalid_json".to_vec()))
    );
    assert!(values.contains_key("group"));
    Ok(())
}

/// Verifies restricted Sentinel lookup and master ROLE permissions using the
/// real three-node Sentinel fixture.
pub fn run_sentinel_checks(
    sentinel: &crate::support::sentinel::SentinelServer,
    helper: &str,
) -> Result<(), Box<dyn Error>> {
    let namespace = "acl-sentinel-orders";
    let source = qubit_event_bus_redis::naming::stream_key(namespace, "events");
    let mut master =
        redis::Client::open(format!("redis://127.0.0.1:{}/", sentinel.master_port()?))?.get_connection()?;
    set_master_user(&mut master, &source, true)?;
    let endpoints = sentinel.endpoints();
    configure_sentinel_users(&endpoints, true)?;
    run_sentinel_child(helper, &endpoints, namespace, "success")?;
    let length: usize = cmd("XLEN").arg(&source).query(&mut master)?;
    assert_eq!(length, 1, "restricted Sentinel discovery and ROLE must allow XADD");

    set_master_user(&mut master, &source, false)?;
    run_sentinel_child(helper, &endpoints, namespace, "role_denied")?;
    set_master_user(&mut master, &source, true)?;
    configure_sentinel_users(&endpoints, false)?;
    run_sentinel_child(helper, &endpoints, namespace, "sentinel_denied")?;
    Ok(())
}

fn set_master_user(master: &mut Connection, source: &str, allow_role: bool) -> Result<(), Box<dyn Error>> {
    let mut command = cmd("ACL");
    command.arg("SETUSER").arg("qubit_master_test").arg("reset").arg("on");
    command.arg(format!(">{PASSWORD}")).arg(format!("~{source}"));
    command.arg("+client|setinfo").arg("+xadd");
    if allow_role {
        command.arg("+role");
    }
    command.query::<()>(&mut *master)?;
    Ok(())
}

fn configure_sentinel_users(endpoints: &str, allow_lookup: bool) -> Result<(), Box<dyn Error>> {
    for endpoint in endpoints.split(',') {
        let mut connection = redis::Client::open(format!("redis://{endpoint}/"))?.get_connection()?;
        let mut command = cmd("ACL");
        command.arg("SETUSER").arg("qubit_sentinel_test").arg("reset").arg("on");
        command.arg(format!(">{PASSWORD}")).arg("+ping").arg("+client|setinfo");
        if allow_lookup {
            command.arg("+sentinel|get-master-addr-by-name");
        }
        command.query::<()>(&mut connection)?;
    }
    Ok(())
}

fn run_sentinel_child(helper: &str, endpoints: &str, namespace: &str, case: &str) -> Result<(), Box<dyn Error>> {
    let child = Command::new(std::env::current_exe()?)
        .args(["--ignored", "--exact", helper, "--nocapture"])
        .env("RER_ACL_NAMESPACE", namespace)
        .env("RER_ACL_CASE", "workflow")
        .env("RER_ACL_USER", "qubit_master_test")
        .env("RER_ACL_PASSWORD", PASSWORD)
        .env("RER_SENTINEL_USER", "qubit_sentinel_test")
        .env("RER_SENTINEL_PASSWORD", PASSWORD)
        .env("RER_SENTINEL_NODES", endpoints)
        .env("RER_SENTINEL_CASE", case)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let output = wait_child(child, TIMEOUT)?;
    if !output.status.success() {
        return Err(format!(
            "Sentinel ACL helper {helper} ({case}) failed ({}): {}{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(())
}
