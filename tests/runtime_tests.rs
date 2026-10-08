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

use std::error::Error;

use futures_lite::future::block_on;
use redis::Client;
use redis::RedisError;
use redis::cmd;
use redis::streams::StreamAutoClaimReply;
use redis::streams::StreamReadReply;
use support::fixture_consumer::run as run_fixture_consumer;
use support::redis_server::RedisServer;

#[test]
fn test_smol_consumer_uses_isolated_production_features() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    run_fixture_consumer("async_smol_consumer", server.url(), true)
}

#[test]
fn test_tokio_consumer_uses_isolated_production_features() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    run_fixture_consumer("async_tokio_consumer", server.url(), true)
}

/// Opens `url` and awaits PING on the host executor; returns its string reply
/// or the Redis setup/command/transport error.
async fn ping(url: &str) -> Result<String, RedisError> {
    let client = Client::open(url)?;
    let mut connection = client.get_multiplexed_async_connection().await?;
    cmd("PING").query_async(&mut connection).await
}

#[test]
fn test_smol_host_can_ping() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let response = block_on(ping(server.url()))?;
    assert_eq!(response, "PONG");
    Ok(())
}

#[tokio::test]
async fn test_tokio_host_can_ping() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let response = ping(server.url()).await?;
    assert_eq!(response, "PONG");
    Ok(())
}

#[test]
fn test_unified_redis_features_do_not_require_global_preference() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let response = block_on(ping(server.url()))?;
    assert_eq!(response, "PONG");
    Ok(())
}

#[test]
fn test_redis_6_2_supports_stream_ping() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start_version("6.2-alpine")?;
    let response = block_on(ping(server.url()))?;
    assert_eq!(response, "PONG");
    Ok(())
}

#[test]
fn test_redis_6_2_supports_pending_entry_recovery() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start_version("6.2-alpine")?;
    let client = Client::open(server.url())?;
    let mut connection = client.get_connection()?;
    let _: String = cmd("XADD")
        .arg("test:stream")
        .arg("*")
        .arg("wire")
        .arg("value")
        .query(&mut connection)?;
    let _: () = cmd("XGROUP")
        .arg("CREATE")
        .arg("test:stream")
        .arg("test-group")
        .arg("0-0")
        .query(&mut connection)?;
    let read: Option<StreamReadReply> = cmd("XREADGROUP")
        .arg("GROUP")
        .arg("test-group")
        .arg("first-consumer")
        .arg("STREAMS")
        .arg("test:stream")
        .arg(">")
        .query(&mut connection)?;
    assert_eq!(
        read.map(|reply| reply
            .keys
            .into_iter()
            .map(|stream| stream.ids.len())
            .sum::<usize>()),
        Some(1)
    );
    let claimed: StreamAutoClaimReply = cmd("XAUTOCLAIM")
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
