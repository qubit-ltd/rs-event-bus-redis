// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Resource permits observed through public SPI operations and Redis traffic.

use std::any::TypeId;
#[cfg(feature = "sync")]
use std::sync::Arc;
#[cfg(feature = "sync")]
use std::sync::mpsc::channel;
#[cfg(feature = "sync")]
use std::thread::spawn;
use std::time::Duration;

#[cfg(feature = "async")]
use futures_lite::future::block_on;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::SpiError;
use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::ReceiveOutcome;
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
#[cfg(feature = "sync")]
use redis::Client;
#[cfg(feature = "sync")]
use redis::cmd;

use super::assert_error;
use super::message;
use super::options;
#[cfg(feature = "sync")]
use crate::support::controlled_redis::proxy::ControlledRedis;
use crate::support::redis_server::RedisServer;
use crate::support::scripted_redis::ScriptedRedis;
use crate::support::scripted_redis::Step;

/// Uses distinct groups so each successful subscribe must send XGROUP CREATE.
fn request(id: u64) -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(id),
        TopicAddress::new("events").expect("topic"),
        SubscriberId::new("permit-worker").expect("subscriber"),
        Some(ConsumerGroup::new(&format!("permit-group-{id}")).expect("group")),
        SubscriptionDurability::Durable,
        StartPosition::Earliest,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    )
}

/// Rejects admission failures with the stable public retry contract.
fn assert_receiver_limit<T>(result: Result<T, SpiError>) {
    match result {
        Err(error) => assert_error(error, "resource_limit", true),
        Ok(_) => panic!("receiver admission must fail before XGROUP"),
    }
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_command_cap_releases_after_pending_publish_completes() {
    let server = RedisServer::start().expect("isolated Redis");
    let proxy = ControlledRedis::start(server.url()).expect("proxy");
    let mut settings = options(&proxy.url(), 1);
    settings.insert("redis.command_timeout_ms".into(), "2000".into());
    let bus = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(settings))
        .expect("provider");
    let gate = proxy.pause_after_reply("XADD");
    let first_bus = Arc::clone(&bus);
    let first = spawn(move || first_bus.publish(message()));
    let reached = gate.wait_until_reached(Duration::from_secs(1));
    if !reached {
        gate.release();
    }
    assert!(reached, "first XADD must be pending at reply gate");
    let (sender, receiver) = channel();
    let second_bus = Arc::clone(&bus);
    let second = spawn(move || sender.send(second_bus.publish(message())).expect("watchdog"));
    let rejected = receiver.recv_timeout(Duration::from_millis(250));
    let mut connection = Client::open(server.url())
        .expect("Redis client")
        .get_connection()
        .expect("inspection connection");
    let keys: Vec<String> = cmd("KEYS").arg("*").query(&mut connection).expect("stream keys");
    assert_eq!(keys.len(), 1);
    let entries: usize = cmd("XLEN").arg(&keys[0]).query(&mut connection).expect("stream length");
    gate.release();
    first.join().expect("first worker").expect("first publish");
    second.join().expect("second worker");
    assert_error(
        rejected.expect("second publish fails fast").expect_err("command cap"),
        "resource_limit",
        true,
    );
    assert_eq!(entries, 1, "rejected publish must not send XADD");
    bus.publish(message())
        .expect("completed publish releases command permit");
    let entries: usize = cmd("XLEN").arg(&keys[0]).query(&mut connection).expect("stream length");
    assert_eq!(entries, 2);
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_receiver_cap_close_and_drop_release_exactly_once() {
    let server = ScriptedRedis::start(vec![
        Step::reply("XGROUP", b"+OK\r\n"),
        Step::reply("XGROUP", b"+OK\r\n"),
        Step::reply("XGROUP", b"+OK\r\n"),
    ])
    .expect("scripted Redis");
    let bus = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(options(server.url(), 8)))
        .expect("provider");
    let mut first = bus.subscribe(request(1)).expect("first receiver");
    assert_receiver_limit(bus.subscribe(request(2)));
    first.close().expect("close releases receiver permit");
    first.close().expect("close is idempotent");
    let second = bus.subscribe(request(2)).expect("close restores admission");
    drop(first);
    assert_receiver_limit(bus.subscribe(request(3)));
    drop(second);
    let third = bus.subscribe(request(3)).expect("drop restores admission");
    drop(third);
    let commands = server.finish();
    assert_eq!(commands.len(), 3, "only admitted subscriptions send XGROUP");
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_failed_subscribe_releases_receiver_permit() {
    let server = ScriptedRedis::start(vec![
        Step::reply("XGROUP", b"-WRONGTYPE explicit setup failure\r\n"),
        Step::reply("XGROUP", b"+OK\r\n"),
    ])
    .expect("scripted Redis");
    let bus = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(options(server.url(), 8)))
        .expect("provider");
    match bus.subscribe(request(1)) {
        Err(error) => assert_error(error, "wrong_type", false),
        Ok(_) => panic!("explicit XGROUP error must fail subscribe"),
    }
    let receiver = bus
        .subscribe(request(2))
        .expect("failed setup releases receiver permit");
    drop(receiver);
    assert_eq!(server.finish().len(), 2);
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_retained_message_does_not_hold_receiver_permit_after_close() {
    let server = RedisServer::start().expect("isolated Redis");
    let bus = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(options(server.url(), 8)))
        .expect("provider");
    bus.publish(message()).expect("publish");
    let mut first = bus.subscribe(request(1)).expect("first receiver");
    let ReceiveOutcome::Message(received) = first.receive(Duration::from_secs(1)).expect("receive") else {
        panic!("published message must be received");
    };
    first.close().expect("close");
    let second = bus
        .subscribe(request(2))
        .expect("retained message cannot reserve receiver slot");
    assert!(
        received.settlement().is_some(),
        "message and token remain alive across close"
    );
    drop(second);
    drop(received);
}

#[cfg(feature = "async")]
#[test]
fn test_async_receiver_cap_close_and_drop_release_exactly_once() {
    block_on(async {
        let server = ScriptedRedis::start(vec![
            Step::reply("XGROUP", b"+OK\r\n"),
            Step::reply("XGROUP", b"+OK\r\n"),
            Step::reply("XGROUP", b"+OK\r\n"),
        ])
        .expect("scripted Redis");
        let bus = AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(options(server.url(), 8)))
            .await
            .expect("provider");
        let mut first = bus.subscribe(request(1)).await.expect("first receiver");
        assert_receiver_limit(bus.subscribe(request(2)).await);
        first.close().await.expect("close releases receiver permit");
        first.close().await.expect("close is idempotent");
        let second = bus.subscribe(request(2)).await.expect("close restores admission");
        drop(first);
        assert_receiver_limit(bus.subscribe(request(3)).await);
        drop(second);
        let third = bus.subscribe(request(3)).await.expect("drop restores admission");
        drop(third);
        let commands = server.finish();
        assert_eq!(commands.len(), 3, "only admitted subscriptions send XGROUP");
    });
}

#[cfg(feature = "async")]
#[test]
fn test_async_failed_subscribe_releases_receiver_permit() {
    block_on(async {
        let server = ScriptedRedis::start(vec![
            Step::reply("XGROUP", b"-WRONGTYPE explicit setup failure\r\n"),
            Step::reply("XGROUP", b"+OK\r\n"),
        ])
        .expect("scripted Redis");
        let bus = AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(options(server.url(), 8)))
            .await
            .expect("provider");
        match bus.subscribe(request(1)).await {
            Err(error) => assert_error(error, "wrong_type", false),
            Ok(_) => panic!("explicit XGROUP error must fail subscribe"),
        }
        let receiver = bus
            .subscribe(request(2))
            .await
            .expect("failed setup releases receiver permit");
        drop(receiver);
        assert_eq!(server.finish().len(), 2);
    });
}

#[cfg(feature = "async")]
#[test]
fn test_async_retained_message_does_not_hold_receiver_permit_after_close() {
    block_on(async {
        let server = RedisServer::start().expect("isolated Redis");
        let bus = AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(options(server.url(), 8)))
            .await
            .expect("provider");
        bus.publish(message()).await.expect("publish");
        let mut first = bus.subscribe(request(1)).await.expect("first receiver");
        let ReceiveOutcome::Message(received) = first.receive(Duration::from_secs(1)).await.expect("receive") else {
            panic!("published message must be received");
        };
        first.close().await.expect("close");
        let second = bus
            .subscribe(request(2))
            .await
            .expect("retained message cannot reserve receiver slot");
        assert!(
            received.settlement().is_some(),
            "message and token remain alive across close"
        );
        drop(second);
        drop(received);
    });
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_short_connection_setup_failure_releases_subscribe_resources() {
    let server = ScriptedRedis::start(vec![
        Step::reply("SELECT", b"+OK\r\n"),
        Step::reply("SELECT", b"-NOPERM short setup rejected\r\n"),
        Step::reply("SELECT", b"+OK\r\n"),
        Step::reply("SELECT", b"+OK\r\n"),
        Step::reply("XGROUP", b"+OK\r\n"),
    ])
    .expect("scripted Redis");
    let settings = options(&format!("{}3", server.url()), 1);
    let bus = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(settings))
        .expect("provider");
    match bus.subscribe(request(1)) {
        // redis 0.32.7 SELECT setup keeps rejection details but loses its
        // server error code; classification cannot infer NOPERM from text.
        Err(SpiError::Operation {
            operation,
            kind,
            retryable,
            source,
            ..
        }) => {
            assert_eq!(operation, "subscribe");
            assert_eq!(kind, "redis_error");
            assert_eq!(retryable, None);
            assert!(!source.to_string().contains("short setup rejected"));
        }
        Err(error) => panic!("unexpected setup error: {error}"),
        Ok(_) => panic!("short connection setup must fail after dedicated setup succeeds"),
    }
    let mut receiver = bus
        .subscribe(request(2))
        .expect("failed setup releases receiver and command permits");
    receiver.close().expect("close recovered receiver");
    let observed = server.finish();
    assert_eq!(
        observed.iter().map(|command| command[0].as_str()).collect::<Vec<_>>(),
        ["SELECT", "SELECT", "SELECT", "SELECT", "XGROUP"],
        "failed short setup must not send XGROUP or publish a reusable connection"
    );
    assert!(
        observed[..4]
            .iter()
            .all(|command| command == &["SELECT".to_owned(), "3".to_owned()])
    );
}

#[cfg(feature = "async")]
#[test]
fn test_async_short_connection_setup_failure_releases_subscribe_resources() {
    let server = ScriptedRedis::start(vec![
        Step::reply("SELECT", b"+OK\r\n"),
        Step::reply("SELECT", b"-NOPERM short setup rejected\r\n"),
        Step::reply("SELECT", b"+OK\r\n"),
        Step::reply("SELECT", b"+OK\r\n"),
        Step::reply("XGROUP", b"+OK\r\n"),
    ])
    .expect("scripted Redis");
    let settings = options(&format!("{}3", server.url()), 1);
    block_on(async {
        let bus = AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(settings))
            .await
            .expect("provider");
        match bus.subscribe(request(1)).await {
            // redis 0.32.7 SELECT setup keeps rejection details but loses its
            // server error code; classification cannot infer NOPERM from text.
            Err(SpiError::Operation {
                operation,
                kind,
                retryable,
                source,
                ..
            }) => {
                assert_eq!(operation, "subscribe");
                assert_eq!(kind, "redis_error");
                assert_eq!(retryable, None);
                assert!(!source.to_string().contains("short setup rejected"));
            }
            Err(error) => panic!("unexpected setup error: {error}"),
            Ok(_) => panic!("short connection setup must fail after dedicated setup succeeds"),
        }
        let mut receiver = bus
            .subscribe(request(2))
            .await
            .expect("failed initialization leaves cache empty and releases receiver/command permits");
        receiver.close().await.expect("close recovered receiver");
    });
    let observed = server.finish();
    assert_eq!(
        observed.iter().map(|command| command[0].as_str()).collect::<Vec<_>>(),
        ["SELECT", "SELECT", "SELECT", "SELECT", "XGROUP"],
        "failed short setup must not send XGROUP or publish a reusable connection"
    );
    assert!(
        observed[..4]
            .iter()
            .all(|command| command == &["SELECT".to_owned(), "3".to_owned()])
    );
}
