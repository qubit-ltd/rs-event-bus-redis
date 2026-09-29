// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Verifies that the proxy gates replies only after Redis applies commands.

mod support;

use support::controlled_redis::proxy::ControlledRedis;
use support::redis_server::RedisServer;

#[test]
fn test_xadd_reply_gate_observes_applied_stream_entry() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let proxy = ControlledRedis::start(server.url())?;
    let gate = proxy.pause_after_reply("XADD");
    let proxy_url = proxy.url();
    let worker = std::thread::spawn(move || -> Result<String, redis::RedisError> {
        let mut connection = redis::Client::open(proxy_url)?.get_connection()?;
        redis::cmd("XADD")
            .arg("controlled-test")
            .arg("*")
            .arg("payload")
            .arg("value")
            .query(&mut connection)
    });

    futures_lite::future::block_on(gate.wait_applied());
    let mut observer = redis::Client::open(server.url())?.get_connection()?;
    let length: usize = redis::cmd("XLEN").arg("controlled-test").query(&mut observer)?;
    assert_eq!(length, 1, "Redis applied XADD before the proxy held its reply");
    gate.release();
    let id = worker.join().expect("proxy command worker should finish")?;
    assert!(!id.is_empty());
    Ok(())
}

#[test]
fn test_xack_reply_gate_observes_applied_ack() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let mut setup = redis::Client::open(server.url())?.get_connection()?;
    let id: String = redis::cmd("XADD")
        .arg("controlled-test")
        .arg("*")
        .arg("payload")
        .arg("value")
        .query(&mut setup)?;
    redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("controlled-test")
        .arg("workers")
        .arg("0")
        .query::<()>(&mut setup)?;
    redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("workers")
        .arg("worker-1")
        .arg("STREAMS")
        .arg("controlled-test")
        .arg(">")
        .query::<redis::Value>(&mut setup)?;

    let proxy = ControlledRedis::start(server.url())?;
    let gate = proxy.pause_after_reply("XACK");
    let proxy_url = proxy.url();
    let worker_id = id.clone();
    let worker = std::thread::spawn(move || -> Result<usize, redis::RedisError> {
        let mut connection = redis::Client::open(proxy_url)?.get_connection()?;
        redis::cmd("XACK")
            .arg("controlled-test")
            .arg("workers")
            .arg(worker_id)
            .query(&mut connection)
    });

    assert!(gate.wait_until_reached(std::time::Duration::from_secs(3)));
    let pending: Vec<redis::Value> = redis::cmd("XPENDING")
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
fn test_xack_reply_can_be_lost_after_redis_applies_ack() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let mut setup = redis::Client::open(server.url())?.get_connection()?;
    let id: String = redis::cmd("XADD")
        .arg("controlled-test-lost-reply")
        .arg("*")
        .arg("payload")
        .arg("value")
        .query(&mut setup)?;
    redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("controlled-test-lost-reply")
        .arg("workers")
        .arg("0")
        .query::<()>(&mut setup)?;
    redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("workers")
        .arg("worker-1")
        .arg("STREAMS")
        .arg("controlled-test-lost-reply")
        .arg(">")
        .query::<redis::Value>(&mut setup)?;

    let proxy = ControlledRedis::start(server.url())?;
    let gate = proxy.pause_after_reply("XACK");
    let proxy_url = proxy.url();
    let worker_id = id.clone();
    let worker = std::thread::spawn(move || -> Result<usize, redis::RedisError> {
        let mut connection = redis::Client::open(proxy_url)?.get_connection()?;
        redis::cmd("XACK")
            .arg("controlled-test-lost-reply")
            .arg("workers")
            .arg(worker_id)
            .query(&mut connection)
    });

    assert!(gate.wait_until_reached(std::time::Duration::from_secs(3)));
    let pending: Vec<redis::Value> = redis::cmd("XPENDING")
        .arg("controlled-test-lost-reply")
        .arg("workers")
        .arg("-")
        .arg("+")
        .arg(10)
        .query(&mut setup)?;
    assert!(pending.is_empty());
    gate.release_without_reply();
    assert!(worker.join().expect("proxy command worker should finish").is_err());
    let repeated_ack: usize = redis::cmd("XACK")
        .arg("controlled-test-lost-reply")
        .arg("workers")
        .arg(id)
        .query(&mut setup)?;
    assert_eq!(repeated_ack, 0, "the original XACK was already applied");
    Ok(())
}
