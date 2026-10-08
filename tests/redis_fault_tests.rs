// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public SPI fault tests using real Redis client connections and scripted
//! RESP.

#![cfg(any(feature = "sync", feature = "async"))]

mod support;

use std::any::TypeId;
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
#[cfg(feature = "async")]
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::DeliveryDisposition;
#[cfg(feature = "sync")]
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
#[cfg(feature = "async")]
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
#[cfg(feature = "sync")]
use qubit_event_bus_redis::sync::RedisEventBusProvider;
use qubit_event_bus_redis::wire::WireFields;
use qubit_id::Id;
#[cfg(feature = "async")]
use qubit_spi::AsyncServiceProvider;
#[cfg(feature = "sync")]
use qubit_spi::ServiceProvider;
use serde_json::to_string;
use support::scripted_redis::ScriptedRedis;
use support::scripted_redis::Step;

const EMPTY_CLAIM: &[u8] = b"*2\r\n$3\r\n0-0\r\n*0\r\n";
const TOMBSTONE_CLAIM: &[u8] = b"*2\r\n$3\r\n0-0\r\n*1\r\n*1\r\n$-1\r\n";
const PENDING_ROW: &[u8] = b"*1\r\n*4\r\n$3\r\n1-0\r\n$5\r\nowner\r\n:100\r\n:1\r\n";
const FAULT: &[u8] = b"-ERR password=fault-secret\r\n";

struct ReceiveFault {
    name: &'static str,
    timeout: Duration,
    expected_kind: &'static str,
    expected_retryable: Option<bool>,
    steps: Vec<Step>,
}

/// Builds independent scripts that fail at each distinct recovery/read stage.
fn receive_faults() -> Vec<ReceiveFault> {
    vec![
        ReceiveFault {
            name: "malformed XAUTOCLAIM",
            timeout: Duration::MAX,
            expected_kind: "outcome_unknown",
            expected_retryable: Some(true),
            steps: vec![Step::reply("XAUTOCLAIM", b":1\r\n")],
        },
        ReceiveFault {
            name: "XPENDING command failure",
            timeout: Duration::MAX,
            expected_kind: "redis_error",
            expected_retryable: None,
            steps: vec![
                Step::reply("XAUTOCLAIM", TOMBSTONE_CLAIM),
                Step::reply("XPENDING", FAULT),
            ],
        },
        ReceiveFault {
            name: "malformed XPENDING",
            timeout: Duration::MAX,
            expected_kind: "outcome_unknown",
            expected_retryable: Some(true),
            steps: vec![
                Step::reply("XAUTOCLAIM", TOMBSTONE_CLAIM),
                Step::reply("XPENDING", b":1\r\n"),
            ],
        },
        ReceiveFault {
            name: "XRANGE command failure",
            timeout: Duration::MAX,
            expected_kind: "redis_error",
            expected_retryable: None,
            steps: vec![
                Step::reply("XAUTOCLAIM", TOMBSTONE_CLAIM),
                Step::reply("XPENDING", PENDING_ROW),
                Step::reply("XRANGE", FAULT),
            ],
        },
        ReceiveFault {
            name: "tombstone acknowledgement failure",
            timeout: Duration::MAX,
            expected_kind: "outcome_unknown",
            expected_retryable: Some(false),
            steps: vec![
                Step::reply("XAUTOCLAIM", TOMBSTONE_CLAIM),
                Step::reply("XPENDING", PENDING_ROW),
                Step::reply("XRANGE", b"*0\r\n"),
                Step::reply("EVAL", FAULT),
            ],
        },
        ReceiveFault {
            name: "pending XREADGROUP failure",
            timeout: Duration::ZERO,
            expected_kind: "redis_error",
            expected_retryable: None,
            steps: vec![Step::reply("XAUTOCLAIM", EMPTY_CLAIM), Step::reply("XREADGROUP", FAULT)],
        },
        ReceiveFault {
            name: "nonblocking XREADGROUP failure",
            timeout: Duration::ZERO,
            expected_kind: "redis_error",
            expected_retryable: None,
            steps: vec![
                Step::reply("XAUTOCLAIM", EMPTY_CLAIM),
                Step::reply("XREADGROUP", b"*0\r\n"),
                Step::reply("XREADGROUP", FAULT),
            ],
        },
        ReceiveFault {
            name: "blocking XREADGROUP failure",
            timeout: Duration::MAX,
            expected_kind: "redis_error",
            expected_retryable: None,
            steps: vec![
                Step::reply("XAUTOCLAIM", EMPTY_CLAIM),
                Step::reply("XREADGROUP", b"*0\r\n"),
                Step::reply("XREADGROUP", FAULT),
            ],
        },
    ]
}

/// Wraps a receive fault with subscription creation and a healthy retry scan.
fn receive_script(steps: Vec<Step>) -> Vec<Step> {
    let mut script = vec![Step::reply("XGROUP", b"+OK\r\n")];
    script.extend(steps);
    script.extend([
        Step::reply("XAUTOCLAIM", EMPTY_CLAIM),
        Step::reply("XREADGROUP", b"*0\r\n"),
        Step::reply("XREADGROUP", b"*0\r\n"),
    ]);
    script
}

/// Supplies the scripted endpoint and immediate reclaim settings to providers.
fn config(server: &ScriptedRedis) -> EventBusConfig {
    let options: ProviderOptions = [
        ("redis.url".into(), server.url().into()),
        ("redis.namespace".into(), "fault-tests".into()),
        ("redis.claim_min_idle_ms".into(), "0".into()),
        ("redis.recovery_interval_ms".into(), "60000".into()),
    ]
    .into();
    EventBusConfig::default().with_provider_options(options)
}

/// Builds the durable subscription whose topic is checked in error assertions.
fn request() -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(1),
        TopicAddress::new("events").expect("valid topic"),
        SubscriberId::new("worker").expect("valid subscriber"),
        Some(ConsumerGroup::new("workers").expect("valid group")),
        SubscriptionDurability::Durable,
        StartPosition::Earliest,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    )
}

/// Checks retry policy, exact provider category, and removal of raw
/// diagnostics.
fn assert_failure(error: SpiError, expected_operation: &str, expected_kind: &str, expected_retryable: Option<bool>) {
    let SpiError::Operation {
        provider_id,
        operation,
        resource,
        kind,
        retryable,
        source,
    } = error
    else {
        panic!("expected an operation error, got {error:?}");
    };
    assert_eq!(provider_id.as_ref(), "redis-streams");
    assert_eq!(operation, expected_operation);
    assert_eq!(resource.as_deref(), Some("events"));
    assert_eq!(kind, expected_kind);
    assert_eq!(retryable, expected_retryable);
    assert!(!source.to_string().contains("fault-secret"));
    assert!(
        source.source().is_none(),
        "raw Redis diagnostics must not survive as a nested source"
    );
}

/// Creates a sync bus without making a connection; invalid setup fails the
/// test.
#[cfg(feature = "sync")]
fn sync_bus(server: &ScriptedRedis) -> Arc<dyn EventBusSpi> {
    RedisEventBusProvider
        .create_configured(&config(server))
        .expect("valid scripted Redis configuration")
}

/// Creates an async bus without making a connection; invalid setup fails the
/// test.
#[cfg(feature = "async")]
async fn async_bus(server: &ScriptedRedis) -> Arc<dyn AsyncEventBusSpi> {
    AsyncRedisEventBusProvider
        .create_configured(&config(server))
        .await
        .expect("valid scripted Redis configuration")
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_receive_protocol_faults_are_sanitized_and_retryable() {
    for fault in receive_faults() {
        let server = ScriptedRedis::start(receive_script(fault.steps)).expect("start RESP endpoint");
        let bus = sync_bus(&server);
        let mut subscription = bus.subscribe(request()).expect("scripted group creation succeeds");
        let error = match subscription.receive(fault.timeout) {
            Err(error) => error,
            Ok(_) => panic!("{} should fail", fault.name),
        };
        assert_failure(error, "receive", fault.expected_kind, fault.expected_retryable);
        assert!(
            matches!(subscription.receive(Duration::ZERO), Ok(ReceiveOutcome::TimedOut)),
            "{} must permit retry",
            fault.name
        );
        verify_receive_commands(&server.finish(), fault.name);
    }
}

#[cfg(feature = "async")]
#[test]
fn test_async_receive_protocol_faults_are_sanitized_and_retryable() {
    block_on(async {
        for fault in receive_faults() {
            let server = ScriptedRedis::start(receive_script(fault.steps)).expect("start RESP endpoint");
            let bus = async_bus(&server).await;
            let mut subscription = bus
                .subscribe(request())
                .await
                .expect("scripted group creation succeeds");
            let error = match subscription.receive(fault.timeout).await {
                Err(error) => error,
                Ok(_) => panic!("{} should fail", fault.name),
            };
            assert_failure(error, "receive", fault.expected_kind, fault.expected_retryable);
            assert!(
                matches!(subscription.receive(Duration::ZERO).await, Ok(ReceiveOutcome::TimedOut)),
                "{} must permit retry",
                fault.name
            );
            verify_receive_commands(&server.finish(), fault.name);
        }
    });
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_zero_timeout_skips_tombstone_maintenance() {
    let server = ScriptedRedis::start(vec![
        Step::reply("XGROUP", b"+OK\r\n"),
        Step::reply("XAUTOCLAIM", TOMBSTONE_CLAIM),
        Step::reply("XREADGROUP", b"*0\r\n"),
        Step::reply("XREADGROUP", b"*0\r\n"),
    ])
    .expect("start RESP endpoint");
    let bus = sync_bus(&server);
    let mut subscription = bus.subscribe(request()).expect("create scripted group");

    assert!(matches!(
        subscription.receive(Duration::ZERO),
        Ok(ReceiveOutcome::TimedOut)
    ));
    let commands = server.finish();
    assert_eq!(commands.iter().filter(|command| command[0] == "XAUTOCLAIM").count(), 1);
    assert_eq!(commands.iter().filter(|command| command[0] == "XREADGROUP").count(), 2);
    assert!(!commands.iter().any(|command| command[0] == "XPENDING"));
    assert!(!commands.iter().any(|command| command[0] == "XRANGE"));
    assert!(!commands.iter().any(|command| command[0] == "EVAL"));
}

#[cfg(feature = "async")]
#[test]
fn test_async_zero_timeout_skips_tombstone_maintenance() {
    block_on(async {
        let server = ScriptedRedis::start(vec![
            Step::reply("XGROUP", b"+OK\r\n"),
            Step::reply("XAUTOCLAIM", TOMBSTONE_CLAIM),
            Step::reply("XREADGROUP", b"*0\r\n"),
            Step::reply("XREADGROUP", b"*0\r\n"),
        ])
        .expect("start RESP endpoint");
        let bus = async_bus(&server).await;
        let mut subscription = bus.subscribe(request()).await.expect("create scripted group");

        assert!(matches!(
            subscription.receive(Duration::ZERO).await,
            Ok(ReceiveOutcome::TimedOut)
        ));
        let commands = server.finish();
        assert_eq!(commands.iter().filter(|command| command[0] == "XAUTOCLAIM").count(), 1);
        assert_eq!(commands.iter().filter(|command| command[0] == "XREADGROUP").count(), 2);
        assert!(!commands.iter().any(|command| command[0] == "XPENDING"));
        assert!(!commands.iter().any(|command| command[0] == "XRANGE"));
        assert!(!commands.iter().any(|command| command[0] == "EVAL"));
    });
}

/// Verifies the failed read stage, independently of the common error category.
fn verify_receive_commands(commands: &[Vec<String>], scenario: &str) {
    let read_commands: Vec<_> = commands.iter().filter(|command| command[0] == "XREADGROUP").collect();
    if scenario == "pending XREADGROUP failure" {
        assert_eq!(read_commands[0].last().map(String::as_str), Some("0-0"));
    } else if scenario == "nonblocking XREADGROUP failure" || scenario == "blocking XREADGROUP failure" {
        let failed_read = read_commands[1];
        assert_eq!(failed_read.last().map(String::as_str), Some(">"));
        assert_eq!(
            failed_read.iter().any(|argument| argument == "BLOCK"),
            scenario == "blocking XREADGROUP failure"
        );
    }
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_subscribe_retries_transport_loss_and_reports_reconnection_failure() {
    for refuse in [false, true] {
        let mut script = vec![Step::disconnect("XGROUP", refuse)];
        if !refuse {
            script.push(Step::reply("XGROUP", b"+OK\r\n"));
        }
        let server = ScriptedRedis::start(script).expect("start RESP endpoint");
        let bus = sync_bus(&server);
        match bus.subscribe(request()) {
            Ok(mut subscription) if !refuse => subscription.close().expect("close succeeds"),
            Err(error) if refuse => assert_failure(error, "subscribe", "transport", Some(true)),
            _ => panic!("unexpected subscribe result for refuse={refuse}"),
        }
        let commands = server.finish();
        assert_eq!(commands.len(), if refuse { 1 } else { 2 });
        if !refuse {
            assert_eq!(
                commands[0], commands[1],
                "retry must preserve the group and starting cursor"
            );
        }
    }
}

#[cfg(feature = "async")]
#[test]
fn test_async_subscribe_retries_transport_loss_and_reports_reconnection_failure() {
    block_on(async {
        for refuse in [false, true] {
            let mut script = vec![Step::disconnect("XGROUP", refuse)];
            if !refuse {
                script.push(Step::reply("XGROUP", b"+OK\r\n"));
            }
            let server = ScriptedRedis::start(script).expect("start RESP endpoint");
            let bus = async_bus(&server).await;
            match bus.subscribe(request()).await {
                Ok(mut subscription) if !refuse => subscription.close().await.expect("close succeeds"),
                Err(error) if refuse => assert_failure(error, "subscribe", "transport", Some(true)),
                _ => panic!("unexpected subscribe result for refuse={refuse}"),
            }
            let commands = server.finish();
            assert_eq!(commands.len(), if refuse { 1 } else { 2 });
            if !refuse {
                assert_eq!(
                    commands[0], commands[1],
                    "retry must preserve the group and starting cursor"
                );
            }
        }
    });
}

/// Returns a valid claimed record with optional Redis 7 deleted-entry IDs.
fn claimed_record(with_deleted_entry: bool) -> Vec<u8> {
    let wire = to_string(&WireFields {
        version: 1,
        event_id: "claimed-event".into(),
        timestamp_ms: 0,
        headers_json: "{}".into(),
        ordering_key: None,
        content_type: "application/octet-stream".into(),
        schema_id: None,
        payload: vec![1, 2, 3],
    })
    .expect("valid wire serializes");
    let entry = resp_array(&[
        resp_bulk(b"1-0"),
        resp_array(&[resp_bulk(b"wire"), resp_bulk(wire.as_bytes())]),
    ]);
    let mut parts = vec![resp_bulk(b"0-0"), resp_array(&[entry])];
    if with_deleted_entry {
        parts.push(resp_array(&[resp_bulk(b"2-0")]));
    }
    resp_array(&parts)
}

/// Encodes a RESP2 array of preencoded values for the scripted endpoint.
fn resp_array(parts: &[Vec<u8>]) -> Vec<u8> {
    let mut bytes = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        bytes.extend_from_slice(part);
    }
    bytes
}

/// Encodes a RESP2 bulk string without modifying its payload bytes.
fn resp_bulk(value: &[u8]) -> Vec<u8> {
    let mut bytes = format!("${}\r\n", value.len()).into_bytes();
    bytes.extend_from_slice(value);
    bytes.extend_from_slice(b"\r\n");
    bytes
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_gap_preserves_a_claimed_record_without_another_redis_read() {
    let (completed, received) = channel();
    let worker = spawn(move || {
        let server = ScriptedRedis::start(vec![
            Step::reply("XGROUP", b"+OK\r\n"),
            Step::reply("XAUTOCLAIM", &claimed_record(true)),
        ])
        .expect("start RESP endpoint");
        let bus = sync_bus(&server);
        let mut subscription = bus.subscribe(request()).expect("group creation succeeds");
        assert!(matches!(
            subscription.receive(Duration::ZERO),
            Ok(ReceiveOutcome::Gap(_))
        ));
        let Ok(ReceiveOutcome::Message(message)) = subscription.receive(Duration::ZERO) else {
            panic!("claimed record must survive the preceding gap");
        };
        assert_eq!(message.id().as_str(), "claimed-event");
        assert!(message.settlement().is_some());
        assert_eq!(server.finish().len(), 2, "deferred delivery needs no Redis command");
        completed.send(()).expect("test still waits for deferred delivery");
    });
    received
        .recv_timeout(Duration::from_secs(3))
        .expect("deferred sync delivery blocked: recovery lock may be held twice");
    worker.join().expect("deferred delivery worker succeeds");
}

#[cfg(feature = "async")]
#[test]
fn test_async_gap_preserves_a_claimed_record_without_another_redis_read() {
    block_on(async {
        let server = ScriptedRedis::start(vec![
            Step::reply("XGROUP", b"+OK\r\n"),
            Step::reply("XAUTOCLAIM", &claimed_record(true)),
        ])
        .expect("start RESP endpoint");
        let bus = async_bus(&server).await;
        let mut subscription = bus.subscribe(request()).await.expect("group creation succeeds");
        assert!(matches!(
            subscription.receive(Duration::ZERO).await,
            Ok(ReceiveOutcome::Gap(_))
        ));
        let Ok(ReceiveOutcome::Message(message)) = subscription.receive(Duration::ZERO).await else {
            panic!("claimed record must survive the preceding gap");
        };
        assert_eq!(message.id().as_str(), "claimed-event");
        assert!(message.settlement().is_some());
        assert_eq!(server.finish().len(), 2, "deferred delivery needs no Redis command");
    });
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_settlement_connection_failure_keeps_the_token_unapplied() {
    let server = ScriptedRedis::start(vec![
        Step::reply("XGROUP", b"+OK\r\n"),
        Step::reply("XAUTOCLAIM", &claimed_record(false)),
        Step::disconnect("XACK", true),
    ])
    .expect("start RESP endpoint");
    let bus = sync_bus(&server);
    let mut subscription = bus.subscribe(request()).expect("group creation succeeds");
    let Ok(ReceiveOutcome::Message(message)) = subscription.receive(Duration::ZERO) else {
        panic!("claimed record must be delivered");
    };
    let token = message.settlement().expect("settlement token");
    assert_failure(
        subscription
            .settle(token, DeliveryDisposition::Accept)
            .expect_err("first XACK loses transport"),
        "settle",
        "outcome_unknown",
        Some(true),
    );
    assert_failure(
        subscription
            .settle(token, DeliveryDisposition::Accept)
            .expect_err("second settle cannot reconnect"),
        "settle",
        "transport",
        Some(true),
    );
    assert!(
        matches!(
            subscription.settle(token, DeliveryDisposition::Retry),
            Err(SpiError::InvalidSettlementToken {
                operation: "settle",
                retryable: Some(false),
                ..
            })
        ),
        "unknown acknowledgement retains the original terminal intent"
    );
    server.finish();
}

#[cfg(feature = "async")]
#[test]
fn test_async_settlement_connection_failure_keeps_the_token_unapplied() {
    block_on(async {
        let server = ScriptedRedis::start(vec![
            Step::reply("XGROUP", b"+OK\r\n"),
            Step::reply("XAUTOCLAIM", &claimed_record(false)),
            Step::disconnect("XACK", true),
        ])
        .expect("start RESP endpoint");
        let bus = async_bus(&server).await;
        let mut subscription = bus.subscribe(request()).await.expect("group creation succeeds");
        let Ok(ReceiveOutcome::Message(message)) = subscription.receive(Duration::ZERO).await else {
            panic!("claimed record must be delivered");
        };
        let token = message.settlement().expect("settlement token");
        assert_failure(
            subscription
                .settle(token, DeliveryDisposition::Accept)
                .await
                .expect_err("first XACK loses transport"),
            "settle",
            "outcome_unknown",
            Some(true),
        );
        assert_failure(
            subscription
                .settle(token, DeliveryDisposition::Accept)
                .await
                .expect_err("second settle cannot reconnect"),
            "settle",
            "transport",
            Some(true),
        );
        assert!(
            matches!(
                subscription.settle(token, DeliveryDisposition::Retry).await,
                Err(SpiError::InvalidSettlementToken {
                    operation: "settle",
                    retryable: Some(false),
                    ..
                })
            ),
            "unknown acknowledgement retains the original terminal intent"
        );
        server.finish();
    });
}

/// Checks an invalid receive timeout before any recovery command can be issued.
fn assert_timeout_overflow(error: SpiError) {
    let SpiError::Operation { operation, source, .. } = error else {
        panic!("expected an operation error");
    };
    assert_eq!(operation, "receive");
    assert_eq!(
        source.to_string(),
        "invalid Redis provider configuration: receive timeout is out of range"
    );
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_receive_rejects_timeout_overflow_before_recovery() {
    let server = ScriptedRedis::start(vec![Step::reply("XGROUP", b"+OK\r\n")]).expect("start RESP endpoint");
    let bus = sync_bus(&server);
    let mut subscription = bus.subscribe(request()).expect("group creation succeeds");
    match subscription.receive(Duration::from_secs(u64::MAX - 1)) {
        Err(error) => assert_timeout_overflow(error),
        Ok(_) => panic!("overflowing finite timeout must fail"),
    }
    assert_eq!(server.finish().len(), 1, "overflow must fail before recovery commands");
}

#[cfg(feature = "async")]
#[test]
fn test_async_receive_rejects_timeout_overflow_before_recovery() {
    block_on(async {
        let server = ScriptedRedis::start(vec![Step::reply("XGROUP", b"+OK\r\n")]).expect("start RESP endpoint");
        let bus = async_bus(&server).await;
        let mut subscription = bus.subscribe(request()).await.expect("group creation succeeds");
        match subscription.receive(Duration::from_secs(u64::MAX - 1)).await {
            Err(error) => assert_timeout_overflow(error),
            Ok(_) => panic!("overflowing finite timeout must fail"),
        }
        assert_eq!(server.finish().len(), 1, "overflow must fail before recovery commands");
    });
}
