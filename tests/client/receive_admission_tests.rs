// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Quarantine admission after a BLOCK read through the public SPI.

use std::any::TypeId;
use std::sync::Arc;
use std::thread::sleep;
use std::thread::spawn;
use std::time::Duration;
use std::time::Instant;

#[cfg(feature = "async")]
use futures_lite::future::block_on;
use qubit_event_bus::EventBusConfig;
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
use redis::Client;
use redis::Connection;
use redis::Value;
use redis::cmd;
use redis::from_redis_value;

use super::assert_error;
use super::message;
use super::options;
use crate::support::controlled_redis::proxy::ControlledRedis;
use crate::support::redis_server::RedisServer;

/// Builds a durable earliest-position request for malformed-entry admission
/// tests.
///
/// # Returns
///
/// A request for the fixed fixture topic and subscriber without Redis I/O.
///
/// # Panics
///
/// Panics if the fixed topic or subscriber identifiers are invalid.
fn request() -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(1),
        TopicAddress::new("events").expect("topic"),
        SubscriberId::new("admission-worker").expect("subscriber"),
        None,
        SubscriptionDurability::Durable,
        StartPosition::Earliest,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    )
}

/// Waits for a real blocked XREADGROUP before the fixture injects malformed
/// wire.
///
/// # Parameters
///
/// - `connection`: Observer connection used for blocking CLIENT LIST queries.
///
/// # Panics
///
/// Panics on a Redis query/conversion error or if no blocked reader is observed
/// within two seconds; sleeps briefly between observations.
fn wait_for_blocked_reader(connection: &mut Connection) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let clients: String = cmd("CLIENT").arg("LIST").query(connection).expect("client list");
        if clients
            .lines()
            .any(|line| line.contains("flags=b") && line.contains("cmd=xreadgroup"))
        {
            return;
        }
        assert!(Instant::now() < deadline, "receiver must reach BLOCK before injection");
        sleep(Duration::from_millis(1));
    }
}

/// Reads the actual EVAL call count through blocking Redis I/O.
///
/// # Parameters
///
/// - `connection`: Observer connection used for INFO commandstats.
///
/// # Returns
///
/// The reported count, or zero when Redis has no EVAL statistic.
///
/// # Panics
///
/// Panics on query/conversion errors or an invalid reported count.
fn eval_calls(connection: &mut Connection) -> u64 {
    let info: String = cmd("INFO")
        .arg("commandstats")
        .query(connection)
        .expect("command statistics");
    info.lines()
        .find_map(|line| {
            line.strip_prefix("cmdstat_eval:calls=")
                .and_then(|value| value.split(',').next())
                .map(|value| value.parse().expect("EVAL call count"))
        })
        .unwrap_or(0)
}

/// Reads the real pending-entry count without production-private naming
/// helpers.
///
/// # Parameters
///
/// - `connection`: Observer connection used for blocking XINFO/XPENDING I/O.
/// - `key`: Stream key whose sole fixture group is inspected.
///
/// # Returns
///
/// The group's current pending-entry count.
///
/// # Panics
///
/// Panics on query/conversion errors, unexpected reply shapes, or a group count
/// other than one.
fn pending_count(connection: &mut Connection, key: &str) -> usize {
    let groups: Vec<Value> = cmd("XINFO")
        .arg("GROUPS")
        .arg(key)
        .query(connection)
        .expect("group details");
    assert_eq!(groups.len(), 1);
    let fields: Vec<Value> = from_redis_value(&groups[0]).expect("group fields");
    let group: String = from_redis_value(&fields[1]).expect("group name");
    let summary: Vec<Value> = cmd("XPENDING")
        .arg(key)
        .arg(group)
        .query(connection)
        .expect("pending summary");
    from_redis_value(&summary[0]).expect("pending count")
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_malformed_block_read_rejects_quarantine_when_command_cap_is_held() {
    let server = RedisServer::start().expect("isolated Redis");
    let publish_proxy = ControlledRedis::start(server.url()).expect("publish gate proxy");
    let receive_proxy = ControlledRedis::start(&publish_proxy.url()).expect("read gate proxy");
    let mut settings = options(&receive_proxy.url(), 1);
    // Two forwarding hops need a setup budget independent of admission checks.
    settings.insert("redis.connect_timeout_ms".into(), "3000".into());
    settings.insert("redis.command_timeout_ms".into(), "3000".into());
    settings.insert("redis.claim_min_idle_ms".into(), "0".into());
    let bus = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(settings))
        .expect("provider");
    let mut receiver = bus.subscribe(request()).expect("receiver");
    let mut inspection = Client::open(server.url())
        .expect("inspection client")
        .get_connection()
        .expect("inspection connection");
    let keys: Vec<String> = cmd("KEYS").arg("*").query(&mut inspection).expect("stream key");
    assert_eq!(keys.len(), 1);
    let key = &keys[0];
    let before = eval_calls(&mut inspection);
    let read_gate = receive_proxy.gate();
    read_gate.arm();
    let receiving = spawn(move || {
        let result = receiver.receive(Duration::from_secs(2));
        (receiver, result)
    });
    wait_for_blocked_reader(&mut inspection);
    let _: String = cmd("XADD")
        .arg(key)
        .arg("*")
        .arg("wire")
        .arg("malformed-json")
        .query(&mut inspection)
        .expect("inject malformed record");
    assert!(
        read_gate.wait_until_reached(Duration::from_secs(2)),
        "BLOCK reply must be held"
    );
    let publish_gate = publish_proxy.pause_after_reply("XADD");
    let concurrent = Arc::clone(&bus);
    let publishing = spawn(move || concurrent.publish(message()));
    let publish_reached = publish_gate.wait_until_reached(Duration::from_secs(2));
    if !publish_reached {
        read_gate.release();
        publish_gate.release();
    }
    assert!(publish_reached, "public publish must hold sole command permit");
    read_gate.release();
    let (mut receiver, result) = receiving.join().expect("receive worker");
    let observed_eval = eval_calls(&mut inspection);
    let observed_pending = pending_count(&mut inspection, key);
    publish_gate.release();
    let _ = publishing
        .join()
        .expect("publish worker")
        .expect("held publish completes");
    assert_error(
        result.err().expect("quarantine admission rejected"),
        "resource_limit",
        true,
    );
    assert_eq!(observed_eval, before, "rejected quarantine must not send EVAL");
    assert_eq!(observed_pending, 1, "poison entry remains pending until recovery");
    assert!(matches!(
        receiver.receive(Duration::from_secs(1)).expect("quarantine recovery"),
        ReceiveOutcome::Gap(_)
    ));
    assert!(matches!(
        receiver
            .receive(Duration::from_secs(1))
            .expect("healthy receive after permit release"),
        ReceiveOutcome::Message(_)
    ));
    assert!(
        eval_calls(&mut inspection) > before,
        "recovery actually executes quarantine"
    );
    receiver.close().expect("close");
}

#[cfg(feature = "async")]
#[test]
fn test_async_malformed_block_read_rejects_quarantine_when_command_cap_is_held() {
    let server = RedisServer::start().expect("isolated Redis");
    let publish_proxy = ControlledRedis::start(server.url()).expect("publish gate proxy");
    let receive_proxy = ControlledRedis::start(&publish_proxy.url()).expect("read gate proxy");
    let mut settings = options(&receive_proxy.url(), 1);
    // Two forwarding hops need a setup budget independent of admission checks.
    settings.insert("redis.connect_timeout_ms".into(), "3000".into());
    settings.insert("redis.command_timeout_ms".into(), "3000".into());
    settings.insert("redis.claim_min_idle_ms".into(), "0".into());
    let bus = block_on(
        AsyncRedisEventBusProvider.create_configured(&EventBusConfig::default().with_provider_options(settings)),
    )
    .expect("provider");
    let mut receiver = block_on(bus.subscribe(request())).expect("receiver");
    let mut inspection = Client::open(server.url())
        .expect("inspection client")
        .get_connection()
        .expect("inspection connection");
    let keys: Vec<String> = cmd("KEYS").arg("*").query(&mut inspection).expect("stream key");
    assert_eq!(keys.len(), 1);
    let key = &keys[0];
    let before = eval_calls(&mut inspection);
    let read_gate = receive_proxy.gate();
    read_gate.arm();
    let receiving = spawn(move || {
        let result = block_on(receiver.receive(Duration::from_secs(2)));
        (receiver, result)
    });
    wait_for_blocked_reader(&mut inspection);
    let _: String = cmd("XADD")
        .arg(key)
        .arg("*")
        .arg("wire")
        .arg("malformed-json")
        .query(&mut inspection)
        .expect("inject malformed record");
    assert!(
        read_gate.wait_until_reached(Duration::from_secs(2)),
        "BLOCK reply must be held"
    );
    let publish_gate = publish_proxy.pause_after_reply("XADD");
    let concurrent = Arc::clone(&bus);
    let publishing = spawn(move || block_on(concurrent.publish(message())));
    let publish_reached = publish_gate.wait_until_reached(Duration::from_secs(2));
    if !publish_reached {
        read_gate.release();
        publish_gate.release();
    }
    assert!(publish_reached, "public publish must hold sole command permit");
    read_gate.release();
    let (mut receiver, result) = receiving.join().expect("receive worker");
    let observed_eval = eval_calls(&mut inspection);
    let observed_pending = pending_count(&mut inspection, key);
    publish_gate.release();
    let _ = publishing
        .join()
        .expect("publish worker")
        .expect("held publish completes");
    assert_error(
        result.err().expect("quarantine admission rejected"),
        "resource_limit",
        true,
    );
    assert_eq!(observed_eval, before, "rejected quarantine must not send EVAL");
    assert_eq!(observed_pending, 1, "poison entry remains pending until recovery");
    assert!(matches!(
        block_on(receiver.receive(Duration::from_secs(1))).expect("quarantine recovery"),
        ReceiveOutcome::Gap(_)
    ));
    assert!(matches!(
        block_on(receiver.receive(Duration::from_secs(1))).expect("healthy receive after permit release"),
        ReceiveOutcome::Message(_)
    ));
    assert!(
        eval_calls(&mut inspection) > before,
        "recovery actually executes quarantine"
    );
    block_on(receiver.close()).expect("close");
}
