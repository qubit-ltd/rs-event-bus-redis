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
#[cfg(feature = "async")]
use qubit_event_bus::model::ContentType;
#[cfg(feature = "async")]
use qubit_event_bus::model::EventId;
#[cfg(feature = "async")]
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
#[cfg(feature = "async")]
use qubit_event_bus::spi::EncodedPayload;
#[cfg(feature = "async")]
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
#[cfg(feature = "async")]
use qubit_event_bus::spi::TransportPayload;
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
use support::scripted_redis::ScriptedRedis;
use support::scripted_redis::Step;

const INVALID_CREATE_REPLIES: [&[u8]; 8] = [
    b"$-1\r\n",
    b":1\r\n",
    b"+PONG\r\n",
    b"$2\r\nOK\r\n",
    b"*0\r\n",
    b"*1\r\n-ERR nested\r\n",
    b"%0\r\n",
    b"?malformed\r\n",
];

const RETRY_REJECTION_REPLIES: [&[u8]; 3] = [
    b"-NOPERM no permission\r\n",
    b"-READONLY replica\r\n",
    b"-ERR later rejection\r\n",
];

fn assert_spi_kind(error: SpiError, expected: &str) {
    let SpiError::Operation { kind, .. } = error else {
        panic!("expected SPI operation error");
    };
    assert_eq!(kind, expected);
}

#[cfg(feature = "sync")]
#[test]
fn test_subscribe_create_sync_keeps_unknown_after_retry_rejection_or_bad_reply()
-> Result<(), Box<dyn Error>> {
    for (index, reply) in RETRY_REJECTION_REPLIES
        .iter()
        .chain(INVALID_CREATE_REPLIES.iter())
        .enumerate()
    {
        let server = ScriptedRedis::start(vec![
            Step::disconnect("XGROUP", false),
            Step::reply("XGROUP", reply),
        ])?;
        let namespace = format!("unknown-retry-sync-{index}");
        let bus = RedisEventBusProvider
            .create_configured(
                &EventBusConfig::default().with_provider_options(options(server.url(), &namespace)),
            )
            .map_err(|failure| failure.into_error())?;
        let result = bus.subscribe(request(StartPosition::New, "group", ProviderOptions::new()));
        let error = match result {
            Err(error) => error,
            Ok(_) => {
                panic!("retry rejection or malformed reply cannot erase the first unknown result")
            }
        };
        assert_unknown(error, "group");
        let commands = server.finish();
        assert_eq!(
            commands
                .iter()
                .filter(|command| command[0] == "XGROUP")
                .count(),
            2
        );
    }
    let server = ScriptedRedis::start(vec![Step::disconnect("XGROUP", true)])?;
    let bus = RedisEventBusProvider
        .create_configured(
            &EventBusConfig::default()
                .with_provider_options(options(server.url(), "unknown-retry-sync-connect")),
        )
        .map_err(|failure| failure.into_error())?;
    let result = bus.subscribe(request(StartPosition::New, "group", ProviderOptions::new()));
    assert_unknown(
        result
            .err()
            .expect("retry connection failure remains uncertain"),
        "group",
    );
    assert_eq!(
        server
            .finish()
            .iter()
            .filter(|command| command[0] == "XGROUP")
            .count(),
        1
    );
    let server = ScriptedRedis::start(vec![Step::reply("XGROUP", b"-NOPERM denied\r\n")])?;
    let bus = RedisEventBusProvider
        .create_configured(
            &EventBusConfig::default()
                .with_provider_options(options(server.url(), "subscribe-first-reject")),
        )
        .map_err(|failure| failure.into_error())?;
    assert_spi_kind(
        bus.subscribe(request(StartPosition::New, "group", ProviderOptions::new()))
            .err()
            .expect("first rejection is definite"),
        "authentication",
    );
    assert_eq!(
        server
            .finish()
            .iter()
            .filter(|command| command[0] == "XGROUP")
            .count(),
        1
    );

    let server = ScriptedRedis::start(vec![
        Step::disconnect("XGROUP", false),
        Step::reply("XGROUP", b"+OK\r\n"),
    ])?;
    let bus = RedisEventBusProvider
        .create_configured(
            &EventBusConfig::default()
                .with_provider_options(options(server.url(), "subscribe-retry-ok")),
        )
        .map_err(|failure| failure.into_error())?;
    assert!(
        bus.subscribe(request(StartPosition::New, "group", ProviderOptions::new()))
            .is_ok()
    );
    assert_eq!(
        server
            .finish()
            .iter()
            .filter(|command| command[0] == "XGROUP")
            .count(),
        2
    );
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_subscribe_create_async_keeps_unknown_after_retry_rejection_or_bad_reply()
-> Result<(), Box<dyn Error>> {
    for (index, reply) in RETRY_REJECTION_REPLIES
        .iter()
        .chain(INVALID_CREATE_REPLIES.iter())
        .enumerate()
    {
        let server = ScriptedRedis::start(vec![
            Step::disconnect("XGROUP", false),
            Step::reply("XGROUP", reply),
        ])?;
        let namespace = format!("unknown-retry-async-{index}");
        let bus = block_on(AsyncRedisEventBusProvider.create_configured(
            &EventBusConfig::default().with_provider_options(options(server.url(), &namespace)),
        ))
        .map_err(|failure| failure.into_error())?;
        let result =
            block_on(bus.subscribe(request(StartPosition::New, "group", ProviderOptions::new())));
        let error = match result {
            Err(error) => error,
            Ok(_) => {
                panic!("retry rejection or malformed reply cannot erase the first unknown result")
            }
        };
        assert_unknown(error, "group");
        let commands = server.finish();
        assert_eq!(
            commands
                .iter()
                .filter(|command| command[0] == "XGROUP")
                .count(),
            2
        );
    }
    let server = ScriptedRedis::start(vec![Step::disconnect("XGROUP", true)])?;
    let bus = block_on(
        AsyncRedisEventBusProvider.create_configured(
            &EventBusConfig::default()
                .with_provider_options(options(server.url(), "unknown-retry-async-connect")),
        ),
    )
    .map_err(|failure| failure.into_error())?;
    let result =
        block_on(bus.subscribe(request(StartPosition::New, "group", ProviderOptions::new())));
    assert_unknown(
        result
            .err()
            .expect("retry connection failure remains uncertain"),
        "group",
    );
    assert_eq!(
        server
            .finish()
            .iter()
            .filter(|command| command[0] == "XGROUP")
            .count(),
        1
    );
    let server = ScriptedRedis::start(vec![Step::reply("XGROUP", b"-NOPERM denied\r\n")])?;
    let bus = block_on(
        AsyncRedisEventBusProvider.create_configured(
            &EventBusConfig::default()
                .with_provider_options(options(server.url(), "subscribe-first-reject-async")),
        ),
    )
    .map_err(|failure| failure.into_error())?;
    assert_spi_kind(
        block_on(bus.subscribe(request(StartPosition::New, "group", ProviderOptions::new())))
            .err()
            .expect("first rejection is definite"),
        "authentication",
    );
    assert_eq!(
        server
            .finish()
            .iter()
            .filter(|command| command[0] == "XGROUP")
            .count(),
        1
    );

    let server = ScriptedRedis::start(vec![
        Step::disconnect("XGROUP", false),
        Step::reply("XGROUP", b"+OK\r\n"),
    ])?;
    let bus = block_on(
        AsyncRedisEventBusProvider.create_configured(
            &EventBusConfig::default()
                .with_provider_options(options(server.url(), "subscribe-retry-ok-async")),
        ),
    )
    .map_err(|failure| failure.into_error())?;
    assert!(
        block_on(bus.subscribe(request(StartPosition::New, "group", ProviderOptions::new())))
            .is_ok()
    );
    assert_eq!(
        server
            .finish()
            .iter()
            .filter(|command| command[0] == "XGROUP")
            .count(),
        2
    );
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_subscribe_create_sync_accepts_only_ok_on_first_reply() -> Result<(), Box<dyn Error>> {
    for (index, reply) in INVALID_CREATE_REPLIES.iter().enumerate() {
        let server = ScriptedRedis::start(vec![Step::reply("XGROUP", reply)])?;
        let namespace = format!("strict-create-sync-{index}");
        let bus = RedisEventBusProvider
            .create_configured(
                &EventBusConfig::default().with_provider_options(options(server.url(), &namespace)),
            )
            .map_err(|failure| failure.into_error())?;
        let result = bus.subscribe(request(StartPosition::New, "group", ProviderOptions::new()));
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("non-OK CREATE reply must be uncertain"),
        };
        assert_unknown(error, "group");
        let commands = server.finish();
        assert_eq!(
            commands
                .iter()
                .filter(|command| command[0] == "XGROUP")
                .count(),
            1
        );
    }
    let server = ScriptedRedis::start(vec![Step::reply("XGROUP", b"+OK\r\n")])?;
    let bus = RedisEventBusProvider
        .create_configured(
            &EventBusConfig::default()
                .with_provider_options(options(server.url(), "strict-create-sync-ok")),
        )
        .map_err(|failure| failure.into_error())?;
    assert!(
        bus.subscribe(request(StartPosition::New, "group", ProviderOptions::new()))
            .is_ok()
    );
    assert_eq!(
        server
            .finish()
            .iter()
            .filter(|command| command[0] == "XGROUP")
            .count(),
        1
    );
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_subscribe_create_async_accepts_only_ok_on_first_reply() -> Result<(), Box<dyn Error>> {
    for (index, reply) in INVALID_CREATE_REPLIES.iter().enumerate() {
        let server = ScriptedRedis::start(vec![Step::reply("XGROUP", reply)])?;
        let namespace = format!("strict-create-async-{index}");
        let bus = block_on(AsyncRedisEventBusProvider.create_configured(
            &EventBusConfig::default().with_provider_options(options(server.url(), &namespace)),
        ))
        .map_err(|failure| failure.into_error())?;
        let result =
            block_on(bus.subscribe(request(StartPosition::New, "group", ProviderOptions::new())));
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("non-OK CREATE reply must be uncertain"),
        };
        assert_unknown(error, "group");
        let commands = server.finish();
        assert_eq!(
            commands
                .iter()
                .filter(|command| command[0] == "XGROUP")
                .count(),
            1
        );
    }
    let server = ScriptedRedis::start(vec![Step::reply("XGROUP", b"+OK\r\n")])?;
    let bus = block_on(
        AsyncRedisEventBusProvider.create_configured(
            &EventBusConfig::default()
                .with_provider_options(options(server.url(), "strict-create-async-ok")),
        ),
    )
    .map_err(|failure| failure.into_error())?;
    assert!(
        block_on(bus.subscribe(request(StartPosition::New, "group", ProviderOptions::new())))
            .is_ok()
    );
    assert_eq!(
        server
            .finish()
            .iter()
            .filter(|command| command[0] == "XGROUP")
            .count(),
        1
    );
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_subscribe_create_sync_connect_failure_before_send_keeps_transport_error()
-> Result<(), Box<dyn Error>> {
    let bus =
        RedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(options(
                "redis://127.0.0.1:0/",
                "subscribe-connect-failure",
            )))
            .map_err(|failure| failure.into_error())?;
    let error = match bus.subscribe(request(StartPosition::New, "group", ProviderOptions::new())) {
        Err(error) => error,
        Ok(_) => panic!("port zero cannot accept a Redis connection"),
    };
    assert_spi_kind(error, "transport");
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_subscribe_create_async_connect_failure_before_send_keeps_transport_error()
-> Result<(), Box<dyn Error>> {
    let bus = block_on(AsyncRedisEventBusProvider.create_configured(
        &EventBusConfig::default().with_provider_options(options(
            "redis://127.0.0.1:0/",
            "subscribe-connect-failure-async",
        )),
    ))
    .map_err(|failure| failure.into_error())?;
    let error =
        match block_on(bus.subscribe(request(StartPosition::New, "group", ProviderOptions::new())))
        {
            Err(error) => error,
            Ok(_) => panic!("port zero cannot accept a Redis connection"),
        };
    assert_spi_kind(error, "transport");
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_subscribe_create_sync_malformed_reply_discards_connection_and_receiver()
-> Result<(), Box<dyn Error>> {
    let server = ScriptedRedis::start(vec![
        Step::reply("XGROUP", b":1\r\n"),
        Step::reply("XGROUP", b"+OK\r\n"),
    ])?;
    let bus = RedisEventBusProvider
        .create_configured(
            &EventBusConfig::default()
                .with_provider_options(options(server.url(), "malformed-sync-recovery")),
        )
        .map_err(|failure| failure.into_error())?;
    let error = match bus.subscribe(request(StartPosition::New, "group", ProviderOptions::new())) {
        Err(error) => error,
        Ok(_) => panic!("integer reply does not confirm group creation"),
    };
    assert_unknown(error, "group");
    let connections_after_unknown = server.connection_count();
    assert!(
        bus.subscribe(request(StartPosition::New, "group", ProviderOptions::new()))
            .is_ok(),
        "a later healthy subscribe can acquire the released receiver permit"
    );
    let commands = server.finish();
    assert_eq!(
        commands
            .iter()
            .filter(|command| command[0] == "XGROUP")
            .count(),
        2
    );
    assert!(server.connection_count() > connections_after_unknown);
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_subscribe_create_async_malformed_reply_invalidates_connection_and_receiver()
-> Result<(), Box<dyn Error>> {
    let server = ScriptedRedis::start(vec![
        Step::reply("XGROUP", b":1\r\n"),
        Step::reply("XGROUP", b"+OK\r\n"),
    ])?;
    let bus = block_on(
        AsyncRedisEventBusProvider.create_configured(
            &EventBusConfig::default()
                .with_provider_options(options(server.url(), "malformed-async-recovery")),
        ),
    )
    .map_err(|failure| failure.into_error())?;
    let error =
        match block_on(bus.subscribe(request(StartPosition::New, "group", ProviderOptions::new())))
        {
            Err(error) => error,
            Ok(_) => panic!("integer reply does not confirm group creation"),
        };
    assert_unknown(error, "group");
    let connections_after_unknown = server.connection_count();
    assert!(
        block_on(bus.subscribe(request(StartPosition::New, "group", ProviderOptions::new())))
            .is_ok(),
        "a later healthy subscribe can acquire the released receiver permit"
    );
    let commands = server.finish();
    assert_eq!(
        commands
            .iter()
            .filter(|command| command[0] == "XGROUP")
            .count(),
        2
    );
    assert!(server.connection_count() > connections_after_unknown);
    Ok(())
}

#[cfg(feature = "async")]
fn async_publish_message() -> Result<OutboundMessage, Box<dyn Error>> {
    Ok(OutboundMessage::new(
        TopicAddress::new("events")?,
        EventId::new("recovery-publish")?,
        std::time::SystemTime::now(),
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            std::sync::Arc::from(b"payload".to_vec()),
            ContentType::new("application/octet-stream")?,
            None,
        )),
    ))
}

#[cfg(feature = "async")]
#[test]
fn test_async_second_xgroup_disconnect_recovers_next_publish() -> Result<(), Box<dyn Error>> {
    let server = ScriptedRedis::start(vec![
        Step::disconnect("XGROUP", false),
        Step::disconnect("XGROUP", false),
        Step::reply("XADD", b"$3\r\n1-0\r\n"),
    ])?;
    let namespace = "second-xgroup-disconnect-recovery";
    let bus = block_on(AsyncRedisEventBusProvider.create_configured(
        &EventBusConfig::default().with_provider_options(options(server.url(), namespace)),
    ))
    .map_err(|failure| failure.into_error())?;
    let subscribe_result = block_on(bus.subscribe(request(
        StartPosition::Earliest,
        "group-second-disconnect",
        ProviderOptions::new(),
    )));
    assert!(
        subscribe_result.is_err(),
        "both scripted XGROUP requests disconnect"
    );
    // The two scripted steps are consumed only after the endpoint has parsed
    // both XGROUP requests. The next SPI operation must issue one XADD.
    assert_eq!(server.finish_allow_remaining().len(), 2);
    let connections_after_subscribe = server.connection_count();
    assert!(
        connections_after_subscribe >= 3,
        "receiver and command generations are distinct"
    );

    let publish_result = block_on(bus.publish(async_publish_message()?));
    assert!(
        publish_result.is_ok(),
        "next publish should use a live command connection: {publish_result:?}"
    );
    let commands = server.finish();
    assert_eq!(
        commands
            .iter()
            .filter(|command| command[0] == "XGROUP")
            .count(),
        2
    );
    assert_eq!(
        commands
            .iter()
            .filter(|command| command[0] == "XADD")
            .count(),
        1
    );
    assert!(
        server.connection_count() > connections_after_subscribe,
        "publish reconnects after the dead retry generation"
    );
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_second_xgroup_timeout_does_not_reuse_timed_out_connection()
-> Result<(), Box<dyn Error>> {
    let server = ScriptedRedis::start(vec![
        Step::disconnect("XGROUP", false),
        Step::delayed_reply("XGROUP", b"+OK\r\n", Duration::from_millis(300)),
        Step::reply("XADD", b"$3\r\n1-0\r\n"),
    ])?;
    let mut provider_options = options(server.url(), "second-xgroup-timeout-recovery");
    provider_options.insert("redis.command_timeout_ms".into(), "100".into());
    let bus = block_on(
        AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(provider_options)),
    )
    .map_err(|failure| failure.into_error())?;
    let subscribe_result = block_on(bus.subscribe(request(
        StartPosition::Earliest,
        "group-second-timeout",
        ProviderOptions::new(),
    )));
    assert!(
        subscribe_result.is_err(),
        "the delayed retry reply exceeds the command timeout"
    );
    let xgroups = server.finish_allow_remaining();
    assert_eq!(xgroups.len(), 2);
    let connections_after_subscribe = server.connection_count();

    let publish_result = block_on(bus.publish(async_publish_message()?));
    assert!(
        publish_result.is_ok(),
        "timeout invalidation permits a fresh publish connection: {publish_result:?}"
    );
    let commands = server.finish();
    assert_eq!(
        commands
            .iter()
            .filter(|command| command[0] == "XGROUP")
            .count(),
        2
    );
    assert_eq!(
        commands
            .iter()
            .filter(|command| command[0] == "XADD")
            .count(),
        1
    );
    assert!(server.connection_count() > connections_after_subscribe);
    Ok(())
}

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
                directory
                    .path()
                    .to_str()
                    .ok_or("Redis temp path is not UTF-8")?,
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
    assert_eq!(
        source.to_string(),
        "Redis operation outcome unknown (subscribe)"
    );
    assert!(!source.to_string().contains(group));
    assert!(source.source().is_none());
}

fn group_cursor(url: &str, namespace: &str, group: &str) -> Result<String, Box<dyn Error>> {
    let client = Client::open(url)?;
    let mut connection = client.get_connection()?;
    let key = qubit_event_bus_redis::naming::stream_key(namespace, "events");
    let groups: StreamInfoGroupsReply =
        cmd("XINFO").arg("GROUPS").arg(key).query(&mut connection)?;
    let expected_group =
        qubit_event_bus_redis::naming::group_name(namespace, "events", "worker", Some(group));
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
    let group =
        qubit_event_bus_redis::naming::group_name(namespace, "events", "worker", Some(group));
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
    let entry_id = if existing_cursor == "1-0" {
        "2-0"
    } else {
        "1-0"
    };
    let _: String = cmd("XADD")
        .arg(&stream)
        .arg(entry_id)
        .arg("field")
        .arg("pending")
        .query(&mut connection)?;
    let redis_group =
        qubit_event_bus_redis::naming::group_name(namespace, "events", "worker", Some(group));
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
        .create_configured(
            &EventBusConfig::default()
                .with_provider_options(options(proxy.url().as_str(), &namespace)),
        )
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
        let result = subscribe_bus
            .subscribe(subscribe_request)
            .and_then(|mut subscription| {
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
    assert_eq!(
        group_cursor(server.url(), &namespace, &group)?,
        expected_cursor
    );
    // Leave a real entry pending while the original XGROUP reply is lost.
    let (cursor_after_probe, pending_before_retry) =
        seed_pending(server.url(), &namespace, &group, expected_cursor)?;
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
    assert_eq!(
        group_cursor(server.url(), &namespace, &group)?,
        cursor_after_probe
    );
    assert_eq!(
        pending_ids(server.url(), &namespace, &group)?,
        pending_before_retry
    );
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_real_redis_create_reply_loss_is_unknown_and_keeps_cursor() -> Result<(), Box<dyn Error>>
{
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
    assert_eq!(
        group_cursor(server.url(), &namespace, &group)?,
        expected_cursor
    );
    let (cursor_after_probe, pending_before_retry) =
        seed_pending(server.url(), &namespace, &group, expected_cursor)?;
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
    assert_eq!(
        group_cursor(server.url(), &namespace, &group)?,
        cursor_after_probe
    );
    assert_eq!(
        pending_ids(server.url(), &namespace, &group)?,
        pending_before_retry
    );
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_real_redis_create_reply_loss_is_unknown_and_keeps_cursor()
-> Result<(), Box<dyn Error>> {
    run_async_lost_reply(StartPosition::Earliest, "0-0", "earliest", false, true)?;
    run_async_lost_reply(StartPosition::At("1-0".into()), "1-0", "at", false, true)?;
    run_async_lost_reply(StartPosition::New, "0-0", "new", false, false)?;
    run_async_lost_reply(StartPosition::Earliest, "0-0", "resume", true, false)
}
