// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Real Redis regression tests for a lost XGROUP CREATE reply.

#![cfg(any(feature = "sync", feature = "async"))]

mod support;

use std::any::TypeId;
use std::error::Error;
use std::net::TcpListener;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::sync::mpsc::channel;
use std::thread::sleep;
use std::thread::spawn;
use std::time::Duration;
use std::time::Instant;

#[cfg(feature = "async")]
use futures_lite::future::block_on;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::SpiError;
use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
#[cfg(feature = "async")]
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
#[cfg(feature = "sync")]
use qubit_event_bus_redis::sync::RedisEventBusProvider;
use qubit_id::Id;
#[cfg(feature = "async")]
use qubit_spi::AsyncServiceProvider;
#[cfg(feature = "sync")]
use qubit_spi::ServiceProvider;
use redis::Client;
use redis::cmd;
use redis::streams::StreamInfoGroupsReply;
use redis::streams::StreamPendingCountReply;
use support::controlled_redis::proxy::ControlledRedis;
use support::redis_server::RedisServer;

const WAIT: Duration = Duration::from_secs(5);

/// Starts a disposable local Redis server and removes its process on drop.
enum RedisBackend {
    Docker(RedisServer),
    Local {
        child: Child,
        _directory: tempfile::TempDir,
        url: String,
    },
}

struct RedisHarness {
    backend: RedisBackend,
}

impl RedisHarness {
    fn start() -> Result<Self, Box<dyn Error>> {
        if let Ok(server) = RedisServer::start() {
            return Ok(Self {
                backend: RedisBackend::Docker(server),
            });
        }
        let port = TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port();
        let directory = tempfile::tempdir()?;
        let child = Command::new("redis-server")
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--protected-mode",
                "no",
                "--save",
                "",
                "--appendonly",
                "no",
                "--dir",
                directory.path().to_str().ok_or("Redis temp path is not UTF-8")?,
                "--loglevel",
                "warning",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let mut backend = RedisBackend::Local {
            child,
            _directory: directory,
            url: format!("redis://127.0.0.1:{port}/"),
        };
        let deadline = Instant::now() + WAIT;
        while Instant::now() < deadline {
            if Client::open(backend.url())
                .and_then(|client| client.get_connection())
                .and_then(|mut connection| cmd("PING").query::<String>(&mut connection))
                .is_ok_and(|reply| reply == "PONG")
            {
                return Ok(Self { backend });
            }
            if backend.child_mut().try_wait()?.is_some() {
                return Err("redis-server exited before becoming ready".into());
            }
            sleep(Duration::from_millis(10));
        }
        Err("redis-server did not become ready within the test deadline".into())
    }

    fn url(&self) -> &str {
        self.backend.url()
    }
}

impl Drop for RedisHarness {
    fn drop(&mut self) {
        if let RedisBackend::Local { child, .. } = &mut self.backend {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl RedisBackend {
    fn url(&self) -> &str {
        match self {
            Self::Docker(server) => server.url(),
            Self::Local { url, .. } => url,
        }
    }

    fn child_mut(&mut self) -> &mut Child {
        let Self::Local { child, .. } = self else {
            unreachable!("local server readiness only checks a local process")
        };
        child
    }
}

fn request(start: StartPosition, group: &str, options: ProviderOptions) -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(1),
        TopicAddress::new("events").expect("valid topic"),
        SubscriberId::new("worker").expect("valid subscriber"),
        Some(ConsumerGroup::new(group).expect("valid group")),
        SubscriptionDurability::Durable,
        start,
        options,
        TypeId::of::<Vec<u8>>(),
    )
}

fn options(url: &str, namespace: &str) -> ProviderOptions {
    [
        ("redis.url".into(), url.into()),
        ("redis.namespace".into(), namespace.into()),
    ]
    .into()
}

fn assert_unknown(error: SpiError, group: &str) {
    let SpiError::Operation {
        provider_id,
        operation,
        resource,
        kind,
        retryable,
        source,
    } = error
    else {
        panic!("expected SPI operation error");
    };
    assert_eq!(provider_id.as_ref(), "redis-streams");
    assert_eq!(operation, "subscribe");
    assert_eq!(resource.as_deref(), Some("events"));
    assert_eq!(kind, "outcome_unknown");
    assert_eq!(retryable, Some(false));
    assert_eq!(source.to_string(), "Redis operation outcome unknown (subscribe)");
    assert!(!source.to_string().contains(group));
    assert!(source.source().is_none());
}

fn group_cursor(url: &str, namespace: &str, group: &str) -> Result<String, Box<dyn Error>> {
    let client = Client::open(url)?;
    let mut connection = client.get_connection()?;
    let key = qubit_event_bus_redis::naming::stream_key(namespace, "events");
    let groups: StreamInfoGroupsReply = cmd("XINFO").arg("GROUPS").arg(key).query(&mut connection)?;
    let expected_group = qubit_event_bus_redis::naming::group_name(namespace, "events", "worker", Some(group));
    let found = groups
        .groups
        .iter()
        .find(|item| item.name == expected_group)
        .ok_or("Redis group missing")?;
    Ok(found.last_delivered_id.clone())
}

fn pending_ids(url: &str, namespace: &str, group: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let client = Client::open(url)?;
    let mut connection = client.get_connection()?;
    let stream = qubit_event_bus_redis::naming::stream_key(namespace, "events");
    let group = qubit_event_bus_redis::naming::group_name(namespace, "events", "worker", Some(group));
    let pending: StreamPendingCountReply = cmd("XPENDING")
        .arg(stream)
        .arg(group)
        .arg("-")
        .arg("+")
        .arg(10)
        .query(&mut connection)?;
    Ok(pending.ids.into_iter().map(|entry| entry.id).collect())
}

fn seed_pending(
    url: &str,
    namespace: &str,
    group: &str,
    existing_cursor: &str,
) -> Result<(String, Vec<String>), Box<dyn Error>> {
    let client = Client::open(url)?;
    let mut connection = client.get_connection()?;
    let stream = qubit_event_bus_redis::naming::stream_key(namespace, "events");
    let entry_id = if existing_cursor == "1-0" { "2-0" } else { "1-0" };
    let _: String = cmd("XADD")
        .arg(&stream)
        .arg(entry_id)
        .arg("field")
        .arg("pending")
        .query(&mut connection)?;
    let redis_group = qubit_event_bus_redis::naming::group_name(namespace, "events", "worker", Some(group));
    // The value is intentionally ignored; XPENDING below proves Redis applied
    // the read without coupling this test to the fork's typed reply parser.
    let _: redis::Value = cmd("XREADGROUP")
        .arg("GROUP")
        .arg(&redis_group)
        .arg("probe")
        .arg("COUNT")
        .arg(1)
        .arg("STREAMS")
        .arg(&stream)
        .arg(">")
        .query(&mut connection)?;
    let cursor = group_cursor(url, namespace, group)?;
    let pending = pending_ids(url, namespace, group)?;
    assert_eq!(pending.len(), 1);
    Ok((cursor, pending))
}

#[cfg(feature = "sync")]
fn run_sync_lost_reply(
    start: StartPosition,
    expected_cursor: &str,
    suffix: &str,
    resume: bool,
    expect_unknown: bool,
) -> Result<(), Box<dyn Error>> {
    let server = RedisHarness::start()?;
    let proxy = ControlledRedis::start(server.url())?;
    let namespace = format!("subscribe-unknown-sync-{suffix}");
    let group = format!("group-{suffix}");
    let stream = qubit_event_bus_redis::naming::stream_key(&namespace, "events");
    let mut observer = Client::open(server.url())?.get_connection()?;
    if expected_cursor == "1-0" {
        let _: String = cmd("XADD")
            .arg(&stream)
            .arg("1-0")
            .arg("field")
            .arg("value")
            .query(&mut observer)?;
    }
    let bus = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(options(proxy.url().as_str(), &namespace)))
        .map_err(|failure| failure.into_error())?;
    let gate = proxy.pause_after_reply("XGROUP");
    let subscribe_bus = bus.clone();
    let mut request_options = ProviderOptions::new();
    if resume {
        request_options.insert("redis.existing_group_start".into(), "resume".into());
    }
    let subscribe_request = request(start, &group, request_options);
    let (sender, receiver) = channel();
    let worker = spawn(move || {
        let result = subscribe_bus.subscribe(subscribe_request).and_then(|mut subscription| {
            subscription.close()?;
            Ok(())
        });
        let _ = sender.send(result);
    });
    if !gate.wait_until_reached(WAIT) {
        gate.release_without_reply();
        let _ = worker.join();
        return Err("XGROUP reply gate was not reached".into());
    }
    assert_eq!(group_cursor(server.url(), &namespace, &group)?, expected_cursor);
    // Leave a real entry pending while the original XGROUP reply is lost.
    let (cursor_after_probe, pending_before_retry) = seed_pending(server.url(), &namespace, &group, expected_cursor)?;
    gate.release_without_reply();
    let result = receiver
        .recv_timeout(WAIT)
        .map_err(|_| "subscribe did not finish before deadline")?;
    worker.join().map_err(|_| "subscribe worker panicked")?;
    if expect_unknown {
        assert_unknown(
            result.expect_err("lost reply followed by BUSYGROUP must be uncertain"),
            &group,
        );
    } else {
        result.expect("New and explicit resume accept the existing group cursor");
    }
    assert_eq!(group_cursor(server.url(), &namespace, &group)?, cursor_after_probe);
    assert_eq!(pending_ids(server.url(), &namespace, &group)?, pending_before_retry);
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_real_redis_create_reply_loss_is_unknown_and_keeps_cursor() -> Result<(), Box<dyn Error>> {
    run_sync_lost_reply(StartPosition::Earliest, "0-0", "earliest", false, true)?;
    run_sync_lost_reply(StartPosition::At("1-0".into()), "1-0", "at", false, true)?;
    run_sync_lost_reply(StartPosition::New, "0-0", "new", false, false)?;
    run_sync_lost_reply(StartPosition::Earliest, "0-0", "resume", true, false)
}

#[cfg(feature = "async")]
fn run_async_lost_reply(
    start: StartPosition,
    expected_cursor: &str,
    suffix: &str,
    resume: bool,
    expect_unknown: bool,
) -> Result<(), Box<dyn Error>> {
    let server = RedisHarness::start()?;
    let proxy = ControlledRedis::start(server.url())?;
    let namespace = format!("subscribe-unknown-async-{suffix}");
    let group = format!("group-{suffix}");
    let stream = qubit_event_bus_redis::naming::stream_key(&namespace, "events");
    let mut observer = Client::open(server.url())?.get_connection()?;
    if expected_cursor == "1-0" {
        let _: String = cmd("XADD")
            .arg(&stream)
            .arg("1-0")
            .arg("field")
            .arg("value")
            .query(&mut observer)?;
    }
    let bus = block_on(AsyncRedisEventBusProvider.create_configured(
        &EventBusConfig::default().with_provider_options(options(proxy.url().as_str(), &namespace)),
    ))
    .map_err(|failure| failure.into_error())?;
    let gate = proxy.pause_after_reply("XGROUP");
    let subscribe_bus = bus.clone();
    let mut request_options = ProviderOptions::new();
    if resume {
        request_options.insert("redis.existing_group_start".into(), "resume".into());
    }
    let subscribe_request = request(start, &group, request_options);
    let (sender, receiver) = channel();
    let worker = spawn(move || {
        let result = block_on(async {
            let mut subscription = subscribe_bus.subscribe(subscribe_request).await?;
            subscription.close().await?;
            Ok::<(), SpiError>(())
        });
        let _ = sender.send(result);
    });
    if !gate.wait_until_reached(WAIT) {
        gate.release_without_reply();
        let _ = worker.join();
        return Err("XGROUP reply gate was not reached".into());
    }
    assert_eq!(group_cursor(server.url(), &namespace, &group)?, expected_cursor);
    let (cursor_after_probe, pending_before_retry) = seed_pending(server.url(), &namespace, &group, expected_cursor)?;
    gate.release_without_reply();
    let result = receiver
        .recv_timeout(WAIT)
        .map_err(|_| "subscribe did not finish before deadline")?;
    worker.join().map_err(|_| "subscribe worker panicked")?;
    if expect_unknown {
        assert_unknown(
            result.expect_err("lost reply followed by BUSYGROUP must be uncertain"),
            &group,
        );
    } else {
        result.expect("New and explicit resume accept the existing group cursor");
    }
    assert_eq!(group_cursor(server.url(), &namespace, &group)?, cursor_after_probe);
    assert_eq!(pending_ids(server.url(), &namespace, &group)?, pending_before_retry);
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_real_redis_create_reply_loss_is_unknown_and_keeps_cursor() -> Result<(), Box<dyn Error>> {
    run_async_lost_reply(StartPosition::Earliest, "0-0", "earliest", false, true)?;
    run_async_lost_reply(StartPosition::At("1-0".into()), "1-0", "at", false, true)?;
    run_async_lost_reply(StartPosition::New, "0-0", "new", false, false)?;
    run_async_lost_reply(StartPosition::Earliest, "0-0", "resume", true, false)
}
