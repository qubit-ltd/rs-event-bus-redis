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
fn test_redis_fixture_starts_with_testcontainers() -> Result<(), Box<dyn Error>> {
    use crate::support::testcontainers_support::{
        start_redis_container, start_redis_container_with_host_port,
    };

    let container = start_redis_container(
        "7-alpine",
        ["redis-server", "--appendonly", "no"],
        &[],
        None,
    )?;
    assert_redis_port_is_loopback_only(&container)?;
    let port = container.get_host_port_ipv4(6379)?;
    let client = Client::open(format!("redis://127.0.0.1:{port}/"))?;
    let mut connection = client.get_connection()?;
    let response: String = cmd("PING").query(&mut connection)?;
    assert_eq!(response, "PONG");

    let occupied = std::net::TcpListener::bind(("127.0.0.1", 0))?;
    let fixed_port = occupied.local_addr()?.port();
    drop(occupied);
    let fixed_container = start_redis_container_with_host_port(
        "7-alpine",
        ["redis-server", "--appendonly", "no"],
        &[],
        None,
        fixed_port,
    )?;
    assert_redis_port_is_loopback_only(&fixed_container)?;
    Ok(())
}

fn assert_redis_port_is_loopback_only(
    container: &crate::support::testcontainers_support::RedisContainer,
) -> Result<(), Box<dyn Error>> {
    use testcontainers::core::client::docker_client_instance;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let docker = runtime.block_on(docker_client_instance())?;
    let inspection = runtime.block_on(docker.inspect_container(container.id(), None))?;
    let bindings = inspection
        .network_settings
        .and_then(|settings| settings.ports)
        .and_then(|ports| ports.get("6379/tcp").cloned().flatten())
        .ok_or("Redis container has no published port binding")?;
    assert!(
        !bindings.is_empty()
            && bindings
                .iter()
                .all(|binding| binding.host_ip.as_deref() == Some("127.0.0.1")),
        "expected Redis host bindings to use loopback only, got {bindings:?}"
    );
    Ok(())
}

#[test]
fn test_failed_container_start_cleans_owned_attempt() -> Result<(), Box<dyn Error>> {
    use support::testcontainers_support::ContainerStartError;
    use support::testcontainers_support::start_redis_container_with_host_port;
    use testcontainers::TestcontainersError;
    use testcontainers::bollard::errors::Error as DockerError;
    use testcontainers::core::client::docker_client_instance;
    use testcontainers::core::error::ClientError;

    let occupied = std::net::TcpListener::bind(("0.0.0.0", 0))?;
    let port = occupied.local_addr()?.port();
    let error = match start_redis_container_with_host_port(
        "7-alpine",
        ["redis-server", "--appendonly", "no"],
        &[],
        None,
        port,
    ) {
        Ok(_) => return Err("container unexpectedly claimed the occupied host port".into()),
        Err(error) => error,
    };
    let ContainerStartError::Fixture {
        source: TestcontainersError::Client(ClientError::StartContainer(_)),
        container_name: Some(name),
        cleanup_error,
        ..
    } = &error
    else {
        return Err(format!("expected a post-creation port conflict, got: {error}").into());
    };
    assert!(cleanup_error.is_none(), "{error}");
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let inspection = runtime.block_on(async {
        let docker = docker_client_instance().await?;
        docker.inspect_container(name, None).await.map_err(ClientError::InspectContainer)
    });
    assert!(
        matches!(
            inspection,
            Err(ClientError::InspectContainer(DockerError::DockerResponseServerError {
                status_code: 404,
                ..
            }))
        ),
        "failed start leaked its owned container {name}: {inspection:?}"
    );
    Ok(())
}

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
        read.map(|reply| reply.keys.into_iter().map(|stream| stream.ids.len()).sum::<usize>()),
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
