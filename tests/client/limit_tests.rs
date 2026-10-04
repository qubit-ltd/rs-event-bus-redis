// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Fail-fast command admission observed while another XADD is pending.
use std::any::TypeId;
use std::sync::Arc;
use std::sync::mpsc::channel;
use std::thread::sleep;
use std::thread::spawn;
use std::time::Duration;
use std::time::Instant;

use futures_lite::future::block_on;
use futures_lite::future::poll_once;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
use qubit_event_bus_redis::diagnostics::RedisProviderDiagnostics;
use qubit_event_bus_redis::diagnostics::RedisProviderSnapshot;
#[cfg(feature = "sync")]
use qubit_event_bus_redis::sync::RedisEventBusProvider;
use qubit_id::Id;
use qubit_spi::AsyncServiceProvider;
#[cfg(feature = "sync")]
use qubit_spi::ServiceProvider;

use super::assert_error;
use super::message;
use super::options;
use super::support::blackhole_redis::BlackholeRedis;
use crate::support::redis_server::RedisServer;

/// Records the current highest instance ID before a provider is created.
fn latest_instance_id() -> u64 {
    RedisProviderDiagnostics::snapshots()
        .last()
        .map_or(0, RedisProviderSnapshot::instance_id)
}

/// Finds the provider created for this test even when other tests run
/// concurrently.
fn new_instance_id(previous_id: u64, namespace: &str) -> u64 {
    RedisProviderDiagnostics::snapshots()
        .into_iter()
        .find(|snapshot| snapshot.instance_id() > previous_id && snapshot.namespace() == namespace)
        .expect("this test's provider snapshot")
        .instance_id()
}

/// Reads a still-live provider by its immutable instance ID.
fn snapshot(instance_id: u64) -> RedisProviderSnapshot {
    RedisProviderDiagnostics::snapshots()
        .into_iter()
        .find(|snapshot| snapshot.instance_id() == instance_id)
        .expect("provider remains live")
}

/// Builds one valid durable subscription request for receiver admission.
#[cfg(feature = "sync")]
fn request() -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(1),
        TopicAddress::new("events").expect("topic"),
        SubscriberId::new("diagnostic-receiver").expect("subscriber"),
        None,
        SubscriptionDurability::Durable,
        StartPosition::Earliest,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    )
}

#[cfg(feature = "async")]
#[test]
fn test_async_command_cap_fails_before_second_xadd_and_cancel_releases() {
    let server = BlackholeRedis::start(true, None);
    let mut settings = options(server.url(), 2);
    let namespace = "diagnostics-async-command-cap";
    settings.insert("redis.namespace".into(), namespace.into());
    settings.insert("redis.command_timeout_ms".into(), "2000".into());
    let previous_id = latest_instance_id();
    let bus = block_on(
        AsyncRedisEventBusProvider.create_configured(&EventBusConfig::default().with_provider_options(settings)),
    )
    .expect("provider");
    let instance_id = new_instance_id(previous_id, namespace);
    block_on(async {
        let mut first = bus.publish(message());
        assert!(poll_once(&mut first).await.is_none());
        let deadline = Instant::now() + Duration::from_secs(1);
        while server.commands() == 0 && Instant::now() < deadline {
            assert!(poll_once(&mut first).await.is_none());
            sleep(Duration::from_millis(1));
        }
        assert_eq!(server.commands(), 1);
        assert_eq!(snapshot(instance_id).general_in_flight(), 1);
        assert_eq!(snapshot(instance_id).settlement_in_flight(), 0);
        assert_eq!(snapshot(instance_id).connection_attempts(), 1);
        let (sender, receiver) = channel();
        let concurrent = Arc::clone(&bus);
        spawn(move || {
            sender.send(block_on(concurrent.publish(message()))).expect("watchdog");
        });
        let result = receiver
            .recv_timeout(Duration::from_millis(250))
            .expect("cap must fail fast");
        assert_error(result.expect_err("second command rejected"), "resource_limit", true);
        assert_eq!(server.commands(), 1);
        assert_eq!(snapshot(instance_id).command_rejections(), 1);
        assert_eq!(snapshot(instance_id).general_in_flight(), 1);
        drop(first);
        assert_eq!(snapshot(instance_id).general_in_flight(), 0);
        let mut next = bus.publish(message());
        assert!(
            poll_once(&mut next).await.is_none(),
            "cancelled operation releases application permit"
        );
        assert_eq!(snapshot(instance_id).general_in_flight(), 1);
        assert_eq!(snapshot(instance_id).connection_attempts(), 1);
        drop(next);
        assert_eq!(snapshot(instance_id).general_in_flight(), 0);
        assert_eq!(snapshot(instance_id).command_rejections(), 1);
    });
}

#[cfg(feature = "sync")]
#[test]
fn test_receiver_limit_counts_one_rejection_and_drop_releases_gauge() {
    let server = RedisServer::start().expect("isolated Redis");
    let namespace = "diagnostics-receiver-limit";
    let mut settings = options(server.url(), 2);
    settings.insert("redis.namespace".into(), namespace.into());
    let previous_id = latest_instance_id();
    let bus = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(settings))
        .expect("provider");
    let instance_id = new_instance_id(previous_id, namespace);

    let receiver = bus.subscribe(request()).expect("first receiver");
    assert_eq!(snapshot(instance_id).active_receivers(), 1);
    assert_eq!(snapshot(instance_id).general_in_flight(), 0);
    let rejection = match bus.subscribe(request()) {
        Ok(_) => panic!("receiver quota is full"),
        Err(error) => error,
    };
    assert_error(rejection, "resource_limit", true);
    assert_eq!(snapshot(instance_id).receiver_rejections(), 1);
    assert_eq!(snapshot(instance_id).active_receivers(), 1);
    drop(receiver);
    assert_eq!(snapshot(instance_id).active_receivers(), 0);
    assert_eq!(snapshot(instance_id).receiver_rejections(), 1);
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_pool_hit_does_not_count_another_connection_attempt() {
    let server = RedisServer::start().expect("isolated Redis");
    let namespace = "diagnostics-sync-pool-hit";
    let mut settings = options(server.url(), 2);
    settings.insert("redis.namespace".into(), namespace.into());
    let previous_id = latest_instance_id();
    let bus = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(settings))
        .expect("provider");
    let instance_id = new_instance_id(previous_id, namespace);
    assert_eq!(snapshot(instance_id).connection_attempts(), 0);

    let _ = bus.publish(message()).expect("first publish opens socket");
    assert_eq!(snapshot(instance_id).connection_attempts(), 1);
    assert_eq!(snapshot(instance_id).connection_failures(), 0);
    let _ = bus.publish(message()).expect("second publish reuses idle socket");
    assert_eq!(snapshot(instance_id).connection_attempts(), 1);
    assert_eq!(snapshot(instance_id).general_in_flight(), 0);
}

#[cfg(feature = "async")]
#[test]
fn test_async_cache_hit_does_not_count_another_connection_attempt() {
    let server = RedisServer::start().expect("isolated Redis");
    let namespace = "diagnostics-async-cache-hit";
    let mut settings = options(server.url(), 2);
    settings.insert("redis.namespace".into(), namespace.into());
    let previous_id = latest_instance_id();
    let bus = block_on(
        AsyncRedisEventBusProvider.create_configured(&EventBusConfig::default().with_provider_options(settings)),
    )
    .expect("provider");
    let instance_id = new_instance_id(previous_id, namespace);
    assert_eq!(snapshot(instance_id).connection_attempts(), 0);

    let _ = block_on(bus.publish(message())).expect("first publish opens socket");
    assert_eq!(snapshot(instance_id).connection_attempts(), 1);
    assert_eq!(snapshot(instance_id).connection_failures(), 0);
    let _ = block_on(bus.publish(message())).expect("second publish reuses cached socket");
    assert_eq!(snapshot(instance_id).connection_attempts(), 1);
    assert_eq!(snapshot(instance_id).general_in_flight(), 0);
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_setup_timeout_counts_one_failed_connection_attempt() {
    let server = BlackholeRedis::start(false, None);
    let namespace = "diagnostics-sync-setup-timeout";
    let mut settings = options(server.url(), 2);
    settings.insert("redis.namespace".into(), namespace.into());
    let previous_id = latest_instance_id();
    let bus = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(settings))
        .expect("provider");
    let instance_id = new_instance_id(previous_id, namespace);

    assert_error(bus.publish(message()).expect_err("setup times out"), "transport", true);
    assert_eq!(snapshot(instance_id).connection_attempts(), 1);
    assert_eq!(snapshot(instance_id).connection_failures(), 1);
    assert_eq!(snapshot(instance_id).general_in_flight(), 0);
}

#[cfg(feature = "async")]
#[test]
fn test_async_setup_timeout_counts_one_failed_connection_attempt() {
    let server = BlackholeRedis::start(false, None);
    let namespace = "diagnostics-async-setup-timeout";
    let mut settings = options(server.url(), 2);
    settings.insert("redis.namespace".into(), namespace.into());
    let previous_id = latest_instance_id();
    let bus = block_on(
        AsyncRedisEventBusProvider.create_configured(&EventBusConfig::default().with_provider_options(settings)),
    )
    .expect("provider");
    let instance_id = new_instance_id(previous_id, namespace);

    assert_error(
        block_on(bus.publish(message())).expect_err("setup times out"),
        "transport",
        true,
    );
    assert_eq!(snapshot(instance_id).connection_attempts(), 1);
    assert_eq!(snapshot(instance_id).connection_failures(), 1);
    assert_eq!(snapshot(instance_id).general_in_flight(), 0);
}

/// Configures two independent Sentinel nodes whose discovery replies are
/// withheld.
fn two_sentinel_nodes(first: &BlackholeRedis, second: &BlackholeRedis, namespace: &str) -> ProviderOptions {
    let mut settings = options("redis://127.0.0.1:1/", 2);
    settings.insert("redis.namespace".into(), namespace.into());
    let endpoint = |server: &BlackholeRedis| {
        server
            .url()
            .strip_prefix("redis://")
            .expect("fixture URL")
            .trim_end_matches('/')
            .to_owned()
    };
    settings.insert(
        "redis.sentinel.nodes".into(),
        format!("{},{}", endpoint(first), endpoint(second)),
    );
    settings.insert("redis.sentinel.service_name".into(), "master".into());
    settings
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_sentinel_multiple_probes_count_one_provider_open() {
    let first = BlackholeRedis::start(true, None);
    let second = BlackholeRedis::start(true, None);
    let namespace = "diagnostics-sync-sentinel-probes";
    let previous_id = latest_instance_id();
    let bus = RedisEventBusProvider
        .create_configured(
            &EventBusConfig::default().with_provider_options(two_sentinel_nodes(&first, &second, namespace)),
        )
        .expect("provider");
    let instance_id = new_instance_id(previous_id, namespace);

    assert_error(
        bus.publish(message()).expect_err("discovery times out"),
        "transport",
        true,
    );
    assert_eq!(first.commands(), 1, "first Sentinel node was probed");
    assert_eq!(second.commands(), 1, "second Sentinel node was probed");
    assert_eq!(snapshot(instance_id).connection_attempts(), 1);
    assert_eq!(snapshot(instance_id).connection_failures(), 1);
}

#[cfg(feature = "async")]
#[test]
fn test_async_sentinel_multiple_probes_count_one_provider_open() {
    let first = BlackholeRedis::start(true, None);
    let second = BlackholeRedis::start(true, None);
    let namespace = "diagnostics-async-sentinel-probes";
    let previous_id = latest_instance_id();
    let bus = block_on(AsyncRedisEventBusProvider.create_configured(
        &EventBusConfig::default().with_provider_options(two_sentinel_nodes(&first, &second, namespace)),
    ))
    .expect("provider");
    let instance_id = new_instance_id(previous_id, namespace);

    assert_error(
        block_on(bus.publish(message())).expect_err("discovery times out"),
        "transport",
        true,
    );
    assert_eq!(first.commands(), 1, "first Sentinel node was probed");
    assert_eq!(second.commands(), 1, "second Sentinel node was probed");
    assert_eq!(snapshot(instance_id).connection_attempts(), 1);
    assert_eq!(snapshot(instance_id).connection_failures(), 1);
}
