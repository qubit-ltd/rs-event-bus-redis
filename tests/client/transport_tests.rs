// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! TCP blackholes and concurrent cold initialization.
use std::any::TypeId;
use std::env::var;
#[cfg(feature = "async")]
use std::sync::Arc;
#[cfg(feature = "async")]
use std::sync::Barrier;
use std::sync::mpsc::channel;
#[cfg(feature = "async")]
use std::thread::sleep;
use std::thread::spawn;
use std::time::Duration;
#[cfg(feature = "async")]
use std::time::Instant;

#[cfg(feature = "async")]
use futures_lite::future::block_on;
#[cfg(feature = "async")]
use futures_lite::future::poll_once;
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

use super::assert_error;
use super::message;
use super::options;
use super::support::blackhole_redis::BlackholeRedis;

#[cfg(feature = "sync")]
#[test]
fn test_sync_setup_blackhole_returns_within_watchdog() {
    let server = BlackholeRedis::start(false, None);
    let bus = RedisEventBusProvider
        .create_configured(
            &EventBusConfig::default().with_provider_options(options(server.url(), 64)),
        )
        .expect("provider");
    let (sender, receiver) = channel();
    spawn(move || {
        sender
            .send(bus.publish(message()))
            .expect("watchdog receiver");
    });
    let result = receiver.recv_timeout(Duration::from_secs(2));
    drop(server);
    assert!(
        result
            .expect("200ms setup timeout must beat 2s watchdog")
            .is_err()
    );
}
#[cfg(feature = "async")]
#[test]
fn test_async_setup_blackhole_returns_within_watchdog() {
    let server = BlackholeRedis::start(false, None);
    let bus = block_on(AsyncRedisEventBusProvider.create_configured(
        &EventBusConfig::default().with_provider_options(options(server.url(), 64)),
    ))
    .expect("provider");
    let (sender, receiver) = channel();
    spawn(move || {
        sender
            .send(block_on(bus.publish(message())))
            .expect("watchdog receiver");
    });
    let result = receiver.recv_timeout(Duration::from_secs(2));
    drop(server);
    assert!(
        result
            .expect("200ms setup timeout must beat 2s watchdog")
            .is_err()
    );
}
#[cfg(feature = "sync")]
#[test]
fn test_sync_publish_blackhole_is_unknown_and_no_replay() {
    let server = BlackholeRedis::start(true, None);
    let bus = RedisEventBusProvider
        .create_configured(
            &EventBusConfig::default().with_provider_options(options(server.url(), 64)),
        )
        .expect("provider");
    let (sender, receiver) = channel();
    spawn(move || {
        sender
            .send(bus.publish(message()))
            .expect("watchdog receiver");
    });
    let result = receiver.recv_timeout(Duration::from_secs(2));
    let commands = server.commands();
    drop(server);
    assert_error(
        result
            .expect("200ms command timeout must beat watchdog")
            .expect_err("reply missing"),
        "outcome_unknown",
        false,
    );
    assert_eq!(commands, 1, "XADD must never replay transparently");
}
#[cfg(feature = "async")]
#[test]
fn test_async_cold_publish_singleflight_opens_one_connection() {
    let server = BlackholeRedis::start(true, Some(b"$3\r\n1-0\r\n"));
    let bus = block_on(AsyncRedisEventBusProvider.create_configured(
        &EventBusConfig::default().with_provider_options(options(server.url(), 64)),
    ))
    .expect("provider");
    let barrier = Arc::new(Barrier::new(16));
    let workers = (0..16)
        .map(|_| {
            let bus = Arc::clone(&bus);
            let barrier = Arc::clone(&barrier);
            spawn(move || {
                barrier.wait();
                let _ = block_on(bus.publish(message())).expect("publish succeeds");
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().expect("publish worker");
    }
    assert_eq!(server.commands(), 16);
    assert_eq!(
        server.connections(),
        1,
        "cold concurrent calls must share initialization"
    );
}
#[cfg(feature = "async")]
#[test]
fn test_async_invalid_publish_reply_is_unknown() {
    let server = BlackholeRedis::start(true, Some(b"*1\r\n-WRONGTYPE nested\r\n"));
    let bus = block_on(AsyncRedisEventBusProvider.create_configured(
        &EventBusConfig::default().with_provider_options(options(server.url(), 64)),
    ))
    .expect("provider");
    let error = block_on(bus.publish(message())).expect_err("invalid ID");
    assert_error(error, "outcome_unknown", false);
    assert_eq!(server.commands(), 1);
}

#[cfg(feature = "async")]
#[test]
fn test_async_cancelled_cold_setup_leaves_cache_empty() {
    let server = BlackholeRedis::start(false, Some(b"$3\r\n1-0\r\n"));
    let mut settings = options(server.url(), 2);
    settings.insert("redis.connect_timeout_ms".into(), "2000".into());
    let bus = block_on(
        AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(settings)),
    )
    .expect("provider");
    block_on(async {
        let mut first = bus.publish(message());
        let deadline = Instant::now() + Duration::from_secs(1);
        while server.connections() == 0 && Instant::now() < deadline {
            assert!(poll_once(&mut first).await.is_none());
            sleep(Duration::from_millis(1));
        }
        assert_eq!(server.connections(), 1);
        drop(first);
        server.enable_setup_replies();
        let _ = bus
            .publish(message())
            .await
            .expect("next call must initialize after cancelled setup");
        assert_eq!(
            server.connections(),
            2,
            "cancelled initialization must not publish a cached handle"
        );
        assert_eq!(server.commands(), 1);
    });
}

#[cfg(feature = "async")]
#[test]
fn test_async_cold_setup_failure_is_shared_and_retryable() {
    let server = BlackholeRedis::start(false, Some(b"$3\r\n1-0\r\n"));
    let bus = block_on(AsyncRedisEventBusProvider.create_configured(
        &EventBusConfig::default().with_provider_options(options(server.url(), 16)),
    ))
    .expect("provider");
    let mut first = bus.publish(message());
    let mut second = bus.publish(message());
    let deadline = Instant::now() + Duration::from_secs(1);
    while server.connections() == 0 && Instant::now() < deadline {
        assert!(block_on(poll_once(&mut first)).is_none());
        sleep(Duration::from_millis(1));
    }
    assert_eq!(server.connections(), 1, "first call opened the cold socket");
    let second_ready = block_on(poll_once(&mut second));
    assert!(
        second_ready.is_none(),
        "second call completed before setup failure: {second_ready:?}"
    );
    let (first_error, second_error) = block_on(futures_lite::future::zip(first, second));
    assert_error(
        first_error.expect_err("setup timeout is shared"),
        "transport",
        true,
    );
    assert_error(
        second_error.expect_err("setup timeout is shared"),
        "transport",
        true,
    );
    assert_eq!(
        server.connections(),
        1,
        "one connection attempt serves the failed round"
    );
    assert_eq!(server.commands(), 0, "failed setup never sends XADD");

    server.enable_setup_replies();
    let _ =
        block_on(bus.publish(message())).expect("a new round recovers after setup replies resume");
    assert_eq!(
        server.connections(),
        2,
        "a later call retries initialization"
    );
    assert_eq!(server.commands(), 1, "only the recovered call publishes");
}

#[cfg(feature = "async")]
#[test]
fn test_async_cold_success_is_shared_with_same_round() {
    let server = BlackholeRedis::start(false, Some(b"$3\r\n1-0\r\n"));
    let mut settings = options(server.url(), 16);
    settings.insert("redis.connect_timeout_ms".into(), "10000".into());
    let bus = block_on(
        AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(settings)),
    )
    .expect("provider");
    let (first, second) = block_on(async {
        let mut first = bus.publish(message());
        let mut second = bus.publish(message());
        let deadline = Instant::now() + Duration::from_secs(1);
        while server.setup_commands() == 0 && Instant::now() < deadline {
            assert!(poll_once(&mut first).await.is_none());
            sleep(Duration::from_millis(1));
        }
        assert_eq!(server.connections(), 1, "first call opened the cold socket");
        assert!(
            server.setup_commands() > 0,
            "fixture received setup command"
        );
        assert!(
            poll_once(&mut second).await.is_none(),
            "second call joined pending setup"
        );
        server.enable_setup_replies();
        futures_lite::future::zip(first, second).await
    });
    let _ = first.expect("first call shares successful setup");
    let _ = second.expect("second call shares successful setup");
    assert_eq!(
        server.connections(),
        1,
        "one successful initializer serves the round"
    );
    assert_eq!(server.commands(), 2, "both admitted calls publish once");
}

#[cfg(feature = "async")]
#[test]
fn test_async_old_failure_waiter_keeps_result_after_new_round_succeeds() {
    let server = BlackholeRedis::start(false, Some(b"$3\r\n1-0\r\n"));
    let bus = block_on(AsyncRedisEventBusProvider.create_configured(
        &EventBusConfig::default().with_provider_options(options(server.url(), 16)),
    ))
    .expect("provider");
    let mut old_leader = bus.publish(message());
    let mut old_waiter = bus.publish(message());
    let old_leader_result = block_on(async {
        let deadline = Instant::now() + Duration::from_secs(1);
        while server.connections() == 0 && Instant::now() < deadline {
            assert!(poll_once(&mut old_leader).await.is_none());
            sleep(Duration::from_millis(1));
        }
        assert_eq!(server.connections(), 1, "old round opened its socket");
        assert!(
            poll_once(&mut old_waiter).await.is_none(),
            "old waiter joined the pending round"
        );
        loop {
            if let Some(result) = poll_once(&mut old_leader).await {
                break result;
            }
            sleep(Duration::from_millis(1));
        }
    });
    assert_error(
        old_leader_result.expect_err("old round times out"),
        "transport",
        true,
    );

    server.enable_setup_replies();
    let _ = block_on(bus.publish(message())).expect("new round succeeds");
    let old_waiter_error = block_on(old_waiter).expect_err("old waiter keeps old failure");
    assert_error(old_waiter_error, "transport", true);
    assert_eq!(server.connections(), 2, "new round retries and succeeds");
    assert_eq!(
        server.commands(),
        1,
        "only the new successful call publishes"
    );
}

/// Converts a local fixture URL into the configured Sentinel endpoint.
fn sentinel_options(url: &str) -> ProviderOptions {
    let mut options = options("redis://127.0.0.1:1/", 64);
    options.insert(
        "redis.sentinel.nodes".into(),
        url.strip_prefix("redis://")
            .expect("fixture URL")
            .trim_end_matches('/')
            .into(),
    );
    options.insert("redis.sentinel.service_name".into(), "master".into());
    options
}

/// Builds RESP2 bulk strings with their exact encoded byte length.
fn sentinel_budget_bulk(bytes: &[u8]) -> Vec<u8> {
    let mut reply = format!("${}\r\n", bytes.len()).into_bytes();
    reply.extend_from_slice(bytes);
    reply.extend_from_slice(b"\r\n");
    reply
}

/// Returns a complete Sentinel address reply whose encoded RESP frame exceeds
/// the fixed discovery budget while remaining structurally valid.
fn oversized_sentinel_address_reply() -> Vec<u8> {
    let host = vec![b'a'; 1024 * 1024];
    let mut reply = b"*2\r\n".to_vec();
    reply.extend(sentinel_budget_bulk(&host));
    reply.extend_from_slice(b"$4\r\n6379\r\n");
    reply
}

/// Returns a valid master ROLE reply with an oversized replica-list element.
fn oversized_master_role_reply() -> Vec<u8> {
    let replica = vec![b'r'; 1024 * 1024];
    let mut reply = b"*3\r\n$6\r\nmaster\r\n:0\r\n*1\r\n".to_vec();
    reply.extend(sentinel_budget_bulk(&replica));
    reply
}

/// Configures one or more scripted Sentinel endpoint URLs.
fn sentinel_budget_options(urls: &[&str]) -> ProviderOptions {
    let mut settings = options("redis://127.0.0.1:1/", 64);
    let endpoints = urls
        .iter()
        .map(|url| {
            url.strip_prefix("redis://")
                .expect("scripted Redis URL")
                .trim_end_matches('/')
        })
        .collect::<Vec<_>>()
        .join(",");
    settings.insert("redis.sentinel.nodes".into(), endpoints);
    settings.insert("redis.sentinel.service_name".into(), "master".into());
    settings
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_sentinel_response_budget_rejects_oversized_sentinel_reply() {
    use crate::support::scripted_redis::ScriptedRedis;
    use crate::support::scripted_redis::Step;

    let sentinel = ScriptedRedis::start(vec![Step::reply(
        "SENTINEL",
        &oversized_sentinel_address_reply(),
    )])
    .expect("scripted Sentinel");
    let bus = RedisEventBusProvider
        .create_configured(
            &EventBusConfig::default()
                .with_provider_options(sentinel_budget_options(&[sentinel.url()])),
        )
        .expect("provider");

    assert!(
        bus.publish(message()).is_err(),
        "oversized discovery reply must fail"
    );
    assert_eq!(
        sentinel.finish().len(),
        1,
        "no target connection may be opened"
    );
}

#[cfg(feature = "async")]
#[test]
fn test_async_sentinel_response_budget_rejects_oversized_sentinel_reply() {
    use crate::support::scripted_redis::ScriptedRedis;
    use crate::support::scripted_redis::Step;

    let sentinel = ScriptedRedis::start(vec![Step::reply(
        "SENTINEL",
        &oversized_sentinel_address_reply(),
    )])
    .expect("scripted Sentinel");
    let bus = block_on(
        AsyncRedisEventBusProvider.create_configured(
            &EventBusConfig::default()
                .with_provider_options(sentinel_budget_options(&[sentinel.url()])),
        ),
    )
    .expect("provider");

    assert!(
        block_on(bus.publish(message())).is_err(),
        "oversized discovery reply must fail"
    );
    assert_eq!(
        sentinel.finish().len(),
        1,
        "no target connection may be opened"
    );
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_sentinel_response_budget_rejects_oversized_role_without_xadd() {
    use crate::support::scripted_redis::ScriptedRedis;
    use crate::support::scripted_redis::Step;

    let target = ScriptedRedis::start(vec![Step::reply("ROLE", &oversized_master_role_reply())])
        .expect("scripted target");
    let sentinel = ScriptedRedis::start(vec![Step::reply(
        "SENTINEL",
        format!(
            "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            "127.0.0.1".len(),
            "127.0.0.1",
            target
                .url()
                .rsplit(':')
                .next()
                .expect("port")
                .trim_end_matches('/')
                .len(),
            target
                .url()
                .rsplit(':')
                .next()
                .expect("port")
                .trim_end_matches('/')
        )
        .as_bytes(),
    )])
    .expect("scripted Sentinel");
    let bus = RedisEventBusProvider
        .create_configured(
            &EventBusConfig::default()
                .with_provider_options(sentinel_budget_options(&[sentinel.url()])),
        )
        .expect("provider");

    assert!(
        bus.publish(message()).is_err(),
        "oversized ROLE must fail discovery"
    );
    let commands = target.finish();
    assert_eq!(commands.len(), 1);
    assert_eq!(
        commands[0][0], "ROLE",
        "XADD must wait for master validation"
    );
    assert_eq!(sentinel.finish().len(), 1);
}

#[cfg(feature = "async")]
#[test]
fn test_async_sentinel_response_budget_rejects_oversized_role_without_xadd() {
    use crate::support::scripted_redis::ScriptedRedis;
    use crate::support::scripted_redis::Step;

    let target = ScriptedRedis::start(vec![Step::reply("ROLE", &oversized_master_role_reply())])
        .expect("scripted target");
    let port = target
        .url()
        .rsplit(':')
        .next()
        .expect("port")
        .trim_end_matches('/');
    let sentinel_reply = format!("*2\r\n$9\r\n127.0.0.1\r\n${}\r\n{port}\r\n", port.len());
    let sentinel = ScriptedRedis::start(vec![Step::reply("SENTINEL", sentinel_reply.as_bytes())])
        .expect("scripted Sentinel");
    let bus = block_on(
        AsyncRedisEventBusProvider.create_configured(
            &EventBusConfig::default()
                .with_provider_options(sentinel_budget_options(&[sentinel.url()])),
        ),
    )
    .expect("provider");

    assert!(
        block_on(bus.publish(message())).is_err(),
        "oversized ROLE must fail discovery"
    );
    let commands = target.finish();
    assert_eq!(commands.len(), 1);
    assert_eq!(
        commands[0][0], "ROLE",
        "XADD must wait for master validation"
    );
    assert_eq!(sentinel.finish().len(), 1);
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_sentinel_response_budget_retries_next_node_after_oversized_reply() {
    use crate::support::scripted_redis::ScriptedRedis;
    use crate::support::scripted_redis::Step;

    let first = ScriptedRedis::start(vec![Step::reply(
        "SENTINEL",
        &oversized_sentinel_address_reply(),
    )])
    .expect("first Sentinel");
    let target = ScriptedRedis::start(vec![
        Step::reply("ROLE", b"*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n"),
        Step::reply("XADD", b"$3\r\n1-0\r\n"),
    ])
    .expect("healthy target");
    let second = ScriptedRedis::start(vec![Step::reply(
        "SENTINEL",
        format!(
            "*2\r\n$9\r\n127.0.0.1\r\n${}\r\n{}\r\n",
            target
                .url()
                .rsplit(':')
                .next()
                .expect("port")
                .trim_end_matches('/')
                .len(),
            target
                .url()
                .rsplit(':')
                .next()
                .expect("port")
                .trim_end_matches('/')
        )
        .as_bytes(),
    )])
    .expect("second Sentinel");
    let bus = RedisEventBusProvider
        .create_configured(
            &EventBusConfig::default()
                .with_provider_options(sentinel_budget_options(&[first.url(), second.url()])),
        )
        .expect("provider");

    assert!(
        bus.publish(message()).is_ok(),
        "second node discovery succeeds"
    );
    assert_eq!(first.finish().len(), 1);
    assert_eq!(second.finish().len(), 1);
    let commands = target.finish();
    assert_eq!(
        commands
            .iter()
            .map(|command| command[0].as_str())
            .collect::<Vec<_>>(),
        ["ROLE", "XADD"]
    );
}

#[cfg(feature = "async")]
#[test]
fn test_async_sentinel_response_budget_retries_next_node_after_oversized_reply() {
    use crate::support::scripted_redis::ScriptedRedis;
    use crate::support::scripted_redis::Step;

    let first = ScriptedRedis::start(vec![Step::reply(
        "SENTINEL",
        &oversized_sentinel_address_reply(),
    )])
    .expect("first Sentinel");
    let target = ScriptedRedis::start(vec![
        Step::reply("ROLE", b"*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n"),
        Step::reply("XADD", b"$3\r\n1-0\r\n"),
    ])
    .expect("healthy target");
    let port = target
        .url()
        .rsplit(':')
        .next()
        .expect("port")
        .trim_end_matches('/');
    let reply = format!("*2\r\n$9\r\n127.0.0.1\r\n${}\r\n{port}\r\n", port.len());
    let second = ScriptedRedis::start(vec![Step::reply("SENTINEL", reply.as_bytes())])
        .expect("second Sentinel");
    let bus = block_on(
        AsyncRedisEventBusProvider.create_configured(
            &EventBusConfig::default()
                .with_provider_options(sentinel_budget_options(&[first.url(), second.url()])),
        ),
    )
    .expect("provider");

    assert!(
        block_on(bus.publish(message())).is_ok(),
        "second node discovery succeeds"
    );
    assert_eq!(first.finish().len(), 1);
    assert_eq!(second.finish().len(), 1);
    let commands = target.finish();
    assert_eq!(
        commands
            .iter()
            .map(|command| command[0].as_str())
            .collect::<Vec<_>>(),
        ["ROLE", "XADD"]
    );
}
#[cfg(feature = "sync")]
#[test]
fn test_sync_sentinel_setup_blackhole_is_bounded() {
    let server = BlackholeRedis::start(false, None);
    let bus = RedisEventBusProvider
        .create_configured(
            &EventBusConfig::default().with_provider_options(sentinel_options(server.url())),
        )
        .expect("provider");
    let (sender, receiver) = channel();
    spawn(move || {
        let _ = sender.send(bus.publish(message()));
    });
    let result = receiver.recv_timeout(Duration::from_secs(2));
    drop(server);
    assert!(result.expect("bounded Sentinel setup").is_err());
}
#[cfg(feature = "async")]
#[test]
fn test_async_sentinel_setup_blackhole_is_bounded() {
    let server = BlackholeRedis::start(false, None);
    let bus = block_on(AsyncRedisEventBusProvider.create_configured(
        &EventBusConfig::default().with_provider_options(sentinel_options(server.url())),
    ))
    .expect("provider");
    let (sender, receiver) = channel();
    spawn(move || {
        let _ = sender.send(block_on(bus.publish(message())));
    });
    let result = receiver.recv_timeout(Duration::from_secs(2));
    drop(server);
    assert!(result.expect("bounded Sentinel setup").is_err());
}
/// Scripts Sentinel master discovery while the real target socket withholds
/// ROLE.
fn sentinel_for_target(target_url: &str) -> crate::support::scripted_redis::ScriptedRedis {
    use crate::support::scripted_redis::ScriptedRedis;
    use crate::support::scripted_redis::Step;
    let address = target_url
        .strip_prefix("redis://")
        .expect("target URL")
        .trim_end_matches('/');
    let (host, port) = address.rsplit_once(':').expect("TCP address");
    let reply = format!(
        "*2\r\n${}\r\n{host}\r\n${}\r\n{port}\r\n",
        host.len(),
        port.len()
    );
    ScriptedRedis::start(vec![Step::reply("SENTINEL", reply.as_bytes())])
        .expect("Sentinel endpoint")
}
#[cfg(feature = "sync")]
#[test]
fn test_sync_sentinel_target_role_blackhole_is_bounded_without_xadd() {
    let target = BlackholeRedis::start(true, None);
    let sentinel = sentinel_for_target(target.url());
    let bus = RedisEventBusProvider
        .create_configured(
            &EventBusConfig::default().with_provider_options(sentinel_options(sentinel.url())),
        )
        .expect("provider");
    let (sender, receiver) = channel();
    spawn(move || {
        let _ = sender.send(bus.publish(message()));
    });
    let result = receiver.recv_timeout(Duration::from_secs(2));
    assert_eq!(
        sentinel.finish().len(),
        1,
        "each endpoint is probed only once"
    );
    let commands = target.commands();
    drop(target);
    let error = result
        .expect("bounded ROLE")
        .expect_err("ROLE reply missing");
    assert_error(error, "transport", true);
    assert_eq!(
        commands, 1,
        "only ROLE may be sent; XADD must wait for master validation"
    );
}
#[cfg(feature = "async")]
#[test]
fn test_async_sentinel_target_role_blackhole_is_bounded_without_xadd() {
    let target = BlackholeRedis::start(true, None);
    let sentinel = sentinel_for_target(target.url());
    let bus = block_on(AsyncRedisEventBusProvider.create_configured(
        &EventBusConfig::default().with_provider_options(sentinel_options(sentinel.url())),
    ))
    .expect("provider");
    let (sender, receiver) = channel();
    spawn(move || {
        let _ = sender.send(block_on(bus.publish(message())));
    });
    let result = receiver.recv_timeout(Duration::from_secs(2));
    assert_eq!(sentinel.finish().len(), 1);
    let commands = target.commands();
    drop(target);
    assert_error(
        result
            .expect("bounded ROLE")
            .expect_err("ROLE reply missing"),
        "transport",
        true,
    );
    assert_eq!(commands, 1);
}
/// Constructs a durable public request for timeout policy regressions.
fn read_request() -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(300),
        TopicAddress::new("events").expect("topic"),
        SubscriberId::new("reader").expect("subscriber"),
        Some(ConsumerGroup::new("workers").expect("group")),
        SubscriptionDurability::Durable,
        StartPosition::Earliest,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    )
}
#[cfg(feature = "sync")]
#[test]
fn test_sync_block_adds_command_margin_and_next_poll_restores_short_wait() {
    use crate::support::controlled_redis::proxy::ControlledRedis;
    let server = crate::support::redis_server::RedisServer::start().expect("Redis");
    let proxy = ControlledRedis::start(server.url()).expect("proxy");
    let mut settings = options(&proxy.url(), 64);
    settings.insert("redis.command_timeout_ms".into(), "1000".into());
    let bus = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(settings))
        .expect("provider");
    let mut subscription = bus.subscribe(read_request()).expect("subscribe");
    assert!(matches!(
        subscription
            .receive(Duration::from_secs(1))
            .expect("BLOCK plus command margin must allow server wait"),
        ReceiveOutcome::TimedOut
    ));
    let gate = proxy.pause_after_reply("XAUTOCLAIM");
    let (sender, receiver) = channel();
    let worker = spawn(move || {
        let _ = sender.send(subscription.receive(Duration::ZERO));
    });
    let reached = gate.wait_until_reached(Duration::from_secs(3));
    let result = receiver.recv_timeout(Duration::from_millis(1800));
    gate.release();
    worker.join().expect("receive worker");
    assert!(reached, "zero poll must send the short claim query");
    assert_error(
        result
            .expect("1s short timeout must beat 1.8s watchdog; stale BLOCK budget exceeds 2s")
            .err()
            .expect("withheld claim reply"),
        "outcome_unknown",
        true,
    );
}
#[cfg(feature = "async")]
#[test]
fn test_async_block_adds_command_margin_and_next_poll_restores_short_wait() {
    use crate::support::controlled_redis::proxy::ControlledRedis;
    let server = crate::support::redis_server::RedisServer::start().expect("Redis");
    let proxy = ControlledRedis::start(server.url()).expect("proxy");
    let mut settings = options(&proxy.url(), 64);
    settings.insert("redis.command_timeout_ms".into(), "1000".into());
    let mut subscription = block_on(async {
        let bus = AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(settings))
            .await
            .expect("provider");
        let mut receiver = bus.subscribe(read_request()).await.expect("subscribe");
        assert!(matches!(
            receiver
                .receive(Duration::from_secs(1))
                .await
                .expect("BLOCK plus margin"),
            ReceiveOutcome::TimedOut
        ));
        receiver
    });
    let gate = proxy.pause_after_reply("XAUTOCLAIM");
    let (sender, receiver) = channel();
    let worker = spawn(move || {
        let _ = sender.send(block_on(subscription.receive(Duration::ZERO)));
    });
    let reached = gate.wait_until_reached(Duration::from_secs(2));
    let result = receiver.recv_timeout(Duration::from_millis(1800));
    gate.release();
    worker.join().expect("receive worker");
    assert!(reached, "zero poll must send the short claim query");
    assert_error(
        result
            .expect("1s short timeout must beat 1.8s watchdog; stale BLOCK budget exceeds 2s")
            .err()
            .expect("withheld claim reply"),
        "outcome_unknown",
        true,
    );
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_receive_disconnect_is_unknown_and_recoverable() {
    use crate::support::scripted_redis::ScriptedRedis;
    use crate::support::scripted_redis::Step;
    let server = ScriptedRedis::start(vec![
        Step::reply("XGROUP", b"+OK\r\n"),
        Step::disconnect("XAUTOCLAIM", false),
    ])
    .expect("endpoint");
    let bus = RedisEventBusProvider
        .create_configured(
            &EventBusConfig::default().with_provider_options(options(server.url(), 64)),
        )
        .expect("provider");
    let mut receiver = bus.subscribe(read_request()).expect("subscribe");
    assert_error(
        receiver
            .receive(Duration::ZERO)
            .err()
            .expect("receive reply lost"),
        "outcome_unknown",
        true,
    );
    assert_eq!(server.finish().len(), 2);
}
#[cfg(feature = "async")]
#[test]
fn test_async_receive_disconnect_is_unknown_and_recoverable() {
    use crate::support::scripted_redis::ScriptedRedis;
    use crate::support::scripted_redis::Step;
    let server = ScriptedRedis::start(vec![
        Step::reply("XGROUP", b"+OK\r\n"),
        Step::disconnect("XAUTOCLAIM", false),
    ])
    .expect("endpoint");
    block_on(async {
        let bus = AsyncRedisEventBusProvider
            .create_configured(
                &EventBusConfig::default().with_provider_options(options(server.url(), 64)),
            )
            .await
            .expect("provider");
        let mut receiver = bus.subscribe(read_request()).await.expect("subscribe");
        assert_error(
            receiver
                .receive(Duration::ZERO)
                .await
                .err()
                .expect("receive reply lost"),
            "outcome_unknown",
            true,
        );
        assert_eq!(server.finish().len(), 2);
    });
}
#[cfg(feature = "sync")]
#[test]
fn test_sync_quarantine_reply_loss_is_unknown_without_safe_replay() {
    use crate::support::scripted_redis::ScriptedRedis;
    use crate::support::scripted_redis::Step;
    let server = ScriptedRedis::start(vec![
        Step::reply("XGROUP", b"+OK\r\n"),
        Step::reply(
            "XAUTOCLAIM",
            b"*3\r\n$3\r\n0-0\r\n*1\r\n*2\r\n$3\r\n1-0\r\n*2\r\n$4\r\nwire\r\n$3\r\nbad\r\n*0\r\n",
        ),
        Step::disconnect("EVAL", false),
    ])
    .expect("endpoint");
    let bus = RedisEventBusProvider
        .create_configured(
            &EventBusConfig::default().with_provider_options(options(server.url(), 64)),
        )
        .expect("provider");
    let mut receiver = bus.subscribe(read_request()).expect("subscribe");
    assert_error(
        receiver
            .receive(Duration::ZERO)
            .err()
            .expect("quarantine reply lost"),
        "outcome_unknown",
        false,
    );
    assert_eq!(server.finish().len(), 3);
}
#[cfg(feature = "async")]
#[test]
fn test_async_quarantine_reply_loss_is_unknown_without_safe_replay() {
    use crate::support::scripted_redis::ScriptedRedis;
    use crate::support::scripted_redis::Step;
    let server = ScriptedRedis::start(vec![
        Step::reply("XGROUP", b"+OK\r\n"),
        Step::reply(
            "XAUTOCLAIM",
            b"*3\r\n$3\r\n0-0\r\n*1\r\n*2\r\n$3\r\n1-0\r\n*2\r\n$4\r\nwire\r\n$3\r\nbad\r\n*0\r\n",
        ),
        Step::disconnect("EVAL", false),
    ])
    .expect("endpoint");
    block_on(async {
        let bus = AsyncRedisEventBusProvider
            .create_configured(
                &EventBusConfig::default().with_provider_options(options(server.url(), 64)),
            )
            .await
            .expect("provider");
        let mut receiver = bus.subscribe(read_request()).await.expect("subscribe");
        assert_error(
            receiver
                .receive(Duration::ZERO)
                .await
                .err()
                .expect("quarantine reply lost"),
            "outcome_unknown",
            false,
        );
        assert_eq!(server.finish().len(), 3);
    });
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_publish_rejects_zero_or_overflow_stream_id_as_unknown() {
    for reply in [
        b"$3\r\n0-0\r\n".as_slice(),
        b"$22\r\n18446744073709551616-0\r\n".as_slice(),
    ] {
        let server = BlackholeRedis::start(true, Some(reply));
        let bus = RedisEventBusProvider
            .create_configured(
                &EventBusConfig::default().with_provider_options(options(server.url(), 64)),
            )
            .expect("provider");
        assert_error(
            bus.publish(message()).expect_err("invalid publish ID"),
            "outcome_unknown",
            false,
        );
    }
}
#[cfg(feature = "async")]
#[test]
fn test_async_publish_rejects_zero_or_overflow_stream_id_as_unknown() {
    for reply in [
        b"$3\r\n0-0\r\n".as_slice(),
        b"$22\r\n18446744073709551616-0\r\n".as_slice(),
    ] {
        let server = BlackholeRedis::start(true, Some(reply));
        let bus = block_on(AsyncRedisEventBusProvider.create_configured(
            &EventBusConfig::default().with_provider_options(options(server.url(), 64)),
        ))
        .expect("provider");
        assert_error(
            block_on(bus.publish(message())).expect_err("invalid publish ID"),
            "outcome_unknown",
            false,
        );
    }
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_sentinel_query_blackhole_is_bounded_and_attempted_once() {
    let sentinel = BlackholeRedis::start(true, None);
    let bus = RedisEventBusProvider
        .create_configured(
            &EventBusConfig::default().with_provider_options(sentinel_options(sentinel.url())),
        )
        .expect("provider");
    let (sender, receiver) = channel();
    spawn(move || {
        let _ = sender.send(bus.publish(message()));
    });
    let result = receiver.recv_timeout(Duration::from_secs(2));
    let commands = sentinel.commands();
    drop(sentinel);
    assert_error(
        result
            .expect("bounded Sentinel query")
            .expect_err("query reply missing"),
        "transport",
        true,
    );
    assert_eq!(commands, 1);
}
#[cfg(feature = "async")]
#[test]
fn test_async_sentinel_query_blackhole_is_bounded_and_attempted_once() {
    let sentinel = BlackholeRedis::start(true, None);
    let bus = block_on(AsyncRedisEventBusProvider.create_configured(
        &EventBusConfig::default().with_provider_options(sentinel_options(sentinel.url())),
    ))
    .expect("provider");
    let (sender, receiver) = channel();
    spawn(move || {
        let _ = sender.send(block_on(bus.publish(message())));
    });
    let result = receiver.recv_timeout(Duration::from_secs(2));
    let commands = sentinel.commands();
    drop(sentinel);
    assert_error(
        result
            .expect("bounded Sentinel query")
            .expect_err("query reply missing"),
        "transport",
        true,
    );
    assert_eq!(commands, 1);
}

/// Uses existing environment variables as non-secret stand-ins for separate
/// ACLs.
fn authenticated_sentinel_fixture() -> (
    crate::support::scripted_redis::ScriptedRedis,
    crate::support::scripted_redis::ScriptedRedis,
    ProviderOptions,
) {
    use crate::support::scripted_redis::ScriptedRedis;
    use crate::support::scripted_redis::Step;
    let target = ScriptedRedis::start(vec![
        Step::reply("AUTH", b"+OK\r\n"),
        Step::reply("SELECT", b"+OK\r\n"),
        Step::reply("ROLE", b"*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n"),
        Step::reply("XADD", b"$3\r\n1-0\r\n"),
    ])
    .expect("target endpoint");
    let address = target
        .url()
        .strip_prefix("redis://")
        .expect("URL")
        .trim_end_matches('/');
    let (host, port) = address.rsplit_once(':').expect("address");
    let response = format!(
        "*2\r\n${}\r\n{host}\r\n${}\r\n{port}\r\n",
        host.len(),
        port.len()
    );
    let sentinel = ScriptedRedis::start(vec![
        Step::reply("AUTH", b"+OK\r\n"),
        Step::reply("SENTINEL", response.as_bytes()),
    ])
    .expect("sentinel endpoint");
    let mut settings = sentinel_options(sentinel.url());
    settings.insert("redis.url".into(), "redis://127.0.0.1:1/3".into());
    settings.insert("redis.username_env".into(), "PATH".into());
    settings.insert("redis.password_env".into(), "HOME".into());
    settings.insert("redis.sentinel.username_env".into(), "HOME".into());
    settings.insert("redis.sentinel.password_env".into(), "PATH".into());
    (sentinel, target, settings)
}
/// Checks Sentinel and master receive their own ACLs and only the master
/// selects DB.
fn assert_separate_acl_and_database(
    sentinel: &crate::support::scripted_redis::ScriptedRedis,
    target: &crate::support::scripted_redis::ScriptedRedis,
) {
    let sentinel_commands = sentinel.finish();
    let target_commands = target.finish();
    assert_eq!(sentinel_commands[0][1], var("HOME").expect("HOME"));
    assert_eq!(sentinel_commands[0][2], var("PATH").expect("PATH"));
    assert_eq!(target_commands[0][1], var("PATH").expect("PATH"));
    assert_eq!(target_commands[0][2], var("HOME").expect("HOME"));
    assert_eq!(target_commands[1], ["SELECT", "3"]);
    assert_eq!(target_commands[2], ["ROLE"]);
}
#[cfg(feature = "sync")]
#[test]
fn test_sync_sentinel_preserves_separate_acl_and_target_database() {
    let (sentinel, target, settings) = authenticated_sentinel_fixture();
    let bus = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(settings))
        .expect("provider");
    let _ = bus.publish(message()).expect("authenticated publish");
    assert_separate_acl_and_database(&sentinel, &target);
}
#[cfg(feature = "async")]
#[test]
fn test_async_sentinel_preserves_separate_acl_and_target_database() {
    let (sentinel, target, settings) = authenticated_sentinel_fixture();
    let bus = block_on(
        AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(settings)),
    )
    .expect("provider");
    let _ = block_on(bus.publish(message())).expect("authenticated publish");
    assert_separate_acl_and_database(&sentinel, &target);
}

/// Scripts an explicit permission rejection at Sentinel query or target ROLE.
fn sentinel_rejection_fixture(
    query_error: bool,
) -> (
    crate::support::scripted_redis::ScriptedRedis,
    crate::support::scripted_redis::ScriptedRedis,
) {
    use crate::support::scripted_redis::ScriptedRedis;
    use crate::support::scripted_redis::Step;
    let target = ScriptedRedis::start(vec![Step::reply("ROLE", b"-NOPERM role rejected\r\n")])
        .expect("target endpoint");
    let address = target
        .url()
        .strip_prefix("redis://")
        .expect("URL")
        .trim_end_matches('/');
    let (host, port) = address.rsplit_once(':').expect("address");
    let reply = if query_error {
        b"-NOPERM sentinel rejected\r\n".to_vec()
    } else {
        format!(
            "*2\r\n${}\r\n{host}\r\n${}\r\n{port}\r\n",
            host.len(),
            port.len()
        )
        .into_bytes()
    };
    let sentinel =
        ScriptedRedis::start(vec![Step::reply("SENTINEL", &reply)]).expect("sentinel endpoint");
    (sentinel, target)
}
#[cfg(feature = "sync")]
#[test]
fn test_sync_sentinel_explicit_permission_rejection_keeps_authentication_category() {
    let mut classifications = Vec::new();
    for query_error in [true, false] {
        let (sentinel, target) = sentinel_rejection_fixture(query_error);
        let bus = RedisEventBusProvider
            .create_configured(
                &EventBusConfig::default().with_provider_options(sentinel_options(sentinel.url())),
            )
            .expect("provider");
        match bus.publish(message()).expect_err("explicit rejection") {
            SpiError::Operation {
                kind, retryable, ..
            }
            | SpiError::Publish {
                kind, retryable, ..
            } => classifications.push((kind, retryable)),
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(sentinel.finish().len(), 1);
        if query_error {
            assert!(target.finish_allow_remaining().is_empty());
        } else {
            assert_eq!(target.finish().len(), 1);
        }
    }
    assert_eq!(
        classifications,
        [("authentication", Some(false)); 2],
        "both Sentinel query and ROLE must preserve explicit rejection categories"
    );
}
#[cfg(feature = "async")]
#[test]
fn test_async_sentinel_explicit_permission_rejection_keeps_authentication_category() {
    let mut classifications = Vec::new();
    for query_error in [true, false] {
        let (sentinel, target) = sentinel_rejection_fixture(query_error);
        let bus = block_on(AsyncRedisEventBusProvider.create_configured(
            &EventBusConfig::default().with_provider_options(sentinel_options(sentinel.url())),
        ))
        .expect("provider");
        match block_on(bus.publish(message())).expect_err("explicit rejection") {
            SpiError::Operation {
                kind, retryable, ..
            }
            | SpiError::Publish {
                kind, retryable, ..
            } => classifications.push((kind, retryable)),
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(sentinel.finish().len(), 1);
        if query_error {
            assert!(target.finish_allow_remaining().is_empty());
        } else {
            assert_eq!(target.finish().len(), 1);
        }
    }
    assert_eq!(
        classifications,
        [("authentication", Some(false)); 2],
        "both Sentinel query and ROLE must preserve explicit rejection categories"
    );
}
