// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runs the published facade examples against isolated Redis services.

mod support;

use std::error::Error;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::process::Command;
use std::process::Stdio;

use support::redis_server::RedisServer;
use support::sentinel::SentinelServer;

#[test]
fn standalone_examples_publish_consume_and_close() -> Result<(), Box<dyn Error>> {
    let redis = RedisServer::start()?;
    let target = std::env::current_exe()?
        .parent()
        .and_then(std::path::Path::parent)
        .ok_or("integration test executable has no target directory")?
        .join("examples");

    let sync = Command::new(target.join(format!("sync_orders{}", std::env::consts::EXE_SUFFIX)))
        .arg(redis.url())
        .arg("documentation-sync")
        .output()?;
    assert!(
        sync.status.success(),
        "sync example failed: {}",
        String::from_utf8_lossy(&sync.stderr)
    );
    assert!(String::from_utf8_lossy(&sync.stdout).contains("consumed order event: order-42"));

    let mut asynchronous = Command::new(target.join(format!("async_orders{}", std::env::consts::EXE_SUFFIX)))
        .arg(redis.url())
        .arg("documentation-async")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let stdout = asynchronous
        .stdout
        .take()
        .ok_or("async example stdout is unavailable")?;
    let lines = BufReader::new(stdout).lines();
    let mut saw_delivery = false;
    for line in lines {
        let line = line?;
        if line.contains("consumed order event: order-43") {
            saw_delivery = true;
            break;
        }
    }
    assert!(saw_delivery, "async example exited without consuming its event");
    asynchronous
        .stdin
        .take()
        .ok_or("async example stdin is unavailable")?
        .write_all(b"\n")?;
    let result = asynchronous.wait_with_output()?;
    assert!(
        result.status.success(),
        "async example failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );

    Ok(())
}

#[test]
fn sentinel_example_resolves_and_uses_the_master() -> Result<(), Box<dyn Error>> {
    let sentinel = SentinelServer::start()?;
    let target = std::env::current_exe()?
        .parent()
        .and_then(std::path::Path::parent)
        .ok_or("integration test executable has no target directory")?
        .join("examples")
        .join(format!("sentinel_orders{}", std::env::consts::EXE_SUFFIX));
    let output = Command::new(target)
        .arg("documentation-sentinel")
        .env("REDIS_SENTINEL_NODES", sentinel.endpoints())
        .env("REDIS_SENTINEL_SERVICE_NAME", "qeventbus")
        .output()?;
    assert!(
        output.status.success(),
        "Sentinel example failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("consumed Sentinel order event: order-44"));
    Ok(())
}
