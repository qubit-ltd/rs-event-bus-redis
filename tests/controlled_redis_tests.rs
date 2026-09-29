// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Verifies that the proxy gates replies only after Redis applies commands.

mod support;

use std::error::Error;
use std::thread::spawn;
use std::time::Duration;

use futures_lite::future::block_on;
use redis::Client;
use redis::RedisError;
use redis::Value;
use redis::cmd;
use support::controlled_redis::proxy::ControlledRedis;
use support::redis_server::RedisServer;

#[test]
fn test_xadd_reply_gate_observes_applied_stream_entry() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let proxy = ControlledRedis::start(server.url())?;
    let gate = proxy.pause_after_reply("XADD");
    let proxy_url = proxy.url();
    let worker = spawn(move || -> Result<String, RedisError> {
        let mut connection = Client::open(proxy_url)?.get_connection()?;
        cmd("XADD")
            .arg("controlled-test")
            .arg("*")
            .arg("payload")
            .arg("value")
            .query(&mut connection)
    });

    block_on(gate.wait_applied());
    let mut observer = Client::open(server.url())?.get_connection()?;
    let length: usize = cmd("XLEN").arg("controlled-test").query(&mut observer)?;
    assert_eq!(length, 1, "Redis applied XADD before the proxy held its reply");
    gate.release();
    let id = worker.join().expect("proxy command worker should finish")?;
    assert!(!id.is_empty());
    Ok(())
}

#[test]
fn test_xack_reply_gate_observes_applied_ack() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let mut setup = Client::open(server.url())?.get_connection()?;
    let id: String = cmd("XADD")
        .arg("controlled-test")
        .arg("*")
        .arg("payload")
        .arg("value")
        .query(&mut setup)?;
    cmd("XGROUP")
        .arg("CREATE")
        .arg("controlled-test")
        .arg("workers")
        .arg("0")
        .query::<()>(&mut setup)?;
    cmd("XREADGROUP")
        .arg("GROUP")
        .arg("workers")
        .arg("worker-1")
        .arg("STREAMS")
        .arg("controlled-test")
        .arg(">")
        .query::<Value>(&mut setup)?;

    let proxy = ControlledRedis::start(server.url())?;
    let gate = proxy.pause_after_reply("XACK");
    let proxy_url = proxy.url();
    let worker_id = id.clone();
    let worker = spawn(move || -> Result<usize, RedisError> {
        let mut connection = Client::open(proxy_url)?.get_connection()?;
        cmd("XACK")
            .arg("controlled-test")
            .arg("workers")
            .arg(worker_id)
            .query(&mut connection)
    });

    assert!(gate.wait_until_reached(Duration::from_secs(3)));
    let pending: Vec<Value> = cmd("XPENDING")
        .arg("controlled-test")
        .arg("workers")
        .arg("-")
        .arg("+")
        .arg(10)
        .query(&mut setup)?;
    assert_eq!(pending.len(), 0, "Redis applied XACK before the proxy held its reply");
    gate.release();
    assert_eq!(worker.join().expect("proxy command worker should finish")?, 1);
    Ok(())
}

#[test]
fn test_xack_reply_can_be_lost_after_redis_applies_ack() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let mut setup = Client::open(server.url())?.get_connection()?;
    let id: String = cmd("XADD")
        .arg("controlled-test-lost-reply")
        .arg("*")
        .arg("payload")
        .arg("value")
        .query(&mut setup)?;
    cmd("XGROUP")
        .arg("CREATE")
        .arg("controlled-test-lost-reply")
        .arg("workers")
        .arg("0")
        .query::<()>(&mut setup)?;
    cmd("XREADGROUP")
        .arg("GROUP")
        .arg("workers")
        .arg("worker-1")
        .arg("STREAMS")
        .arg("controlled-test-lost-reply")
        .arg(">")
        .query::<Value>(&mut setup)?;

    let proxy = ControlledRedis::start(server.url())?;
    let gate = proxy.pause_after_reply("XACK");
    let proxy_url = proxy.url();
    let worker_id = id.clone();
    let worker = spawn(move || -> Result<usize, RedisError> {
        let mut connection = Client::open(proxy_url)?.get_connection()?;
        cmd("XACK")
            .arg("controlled-test-lost-reply")
            .arg("workers")
            .arg(worker_id)
            .query(&mut connection)
    });

    assert!(gate.wait_until_reached(Duration::from_secs(3)));
    let pending: Vec<Value> = cmd("XPENDING")
        .arg("controlled-test-lost-reply")
        .arg("workers")
        .arg("-")
        .arg("+")
        .arg(10)
        .query(&mut setup)?;
    assert!(pending.is_empty());
    gate.release_without_reply();
    assert!(worker.join().expect("proxy command worker should finish").is_err());
    let repeated_ack: usize = cmd("XACK")
        .arg("controlled-test-lost-reply")
        .arg("workers")
        .arg(id)
        .query(&mut setup)?;
    assert_eq!(repeated_ack, 0, "the original XACK was already applied");
    Ok(())
}
