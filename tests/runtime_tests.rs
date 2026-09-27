// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Confirms Redis asynchronous connections work under supported host executors.

#![cfg(feature = "async")]

mod support;

use futures_lite::future::block_on;
use redis::streams::StreamAutoClaimReply;
use redis::streams::StreamReadReply;
use support::redis_server::RedisServer;

async fn ping(url: &str) -> Result<String, redis::RedisError> {
    let client = redis::Client::open(url)?;
    let mut connection = client.get_multiplexed_async_connection().await?;
    redis::cmd("PING").query_async(&mut connection).await
}

#[test]
fn smol_host_can_ping() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let response = block_on(ping(server.url()))?;
    assert_eq!(response, "PONG");
    Ok(())
}

#[tokio::test]
async fn tokio_host_can_ping() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let response = ping(server.url()).await?;
    assert_eq!(response, "PONG");
    Ok(())
}

#[test]
fn unified_redis_features_do_not_require_global_preference() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let response = block_on(ping(server.url()))?;
    assert_eq!(response, "PONG");
    Ok(())
}

#[test]
fn redis_6_2_supports_stream_ping() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start_version("6.2-alpine")?;
    let response = block_on(ping(server.url()))?;
    assert_eq!(response, "PONG");
    Ok(())
}

#[test]
fn redis_6_2_supports_pending_entry_recovery() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start_version("6.2-alpine")?;
    let client = redis::Client::open(server.url())?;
    let mut connection = client.get_connection()?;
    let _: String = redis::cmd("XADD")
        .arg("test:stream")
        .arg("*")
        .arg("wire")
        .arg("value")
        .query(&mut connection)?;
    let _: () = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("test:stream")
        .arg("test-group")
        .arg("0-0")
        .query(&mut connection)?;
    let read: Option<StreamReadReply> = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("test-group")
        .arg("first-consumer")
        .arg("STREAMS")
        .arg("test:stream")
        .arg(">")
        .query(&mut connection)?;
    assert_eq!(
        read.map(|reply| reply.keys.into_iter().map(|stream| stream.ids.len()).sum::<usize>()),
        Some(1)
    );
    let claimed: StreamAutoClaimReply = redis::cmd("XAUTOCLAIM")
        .arg("test:stream")
        .arg("test-group")
        .arg("second-consumer")
        .arg(0)
        .arg("0-0")
        .arg("COUNT")
        .arg(1)
        .query(&mut connection)?;
    assert_eq!(claimed.claimed.len(), 1);
    Ok(())
}
