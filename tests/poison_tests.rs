// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Poison size policies and script outcomes checked against the real PEL.

#![cfg(any(feature = "sync", feature = "async"))]

mod support;

use std::any::TypeId;
use std::error::Error;
use std::thread::spawn;
use std::time::Duration;

#[cfg(feature = "async")]
use futures_lite::future::block_on;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
#[cfg(feature = "async")]
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
use qubit_event_bus_redis::naming::group_name;
use qubit_event_bus_redis::naming::poison_key;
use qubit_event_bus_redis::naming::stream_key;
#[cfg(feature = "sync")]
use qubit_event_bus_redis::sync::RedisEventBusProvider;
use qubit_event_bus_redis::wire::WireFields;
use qubit_id::Id;
#[cfg(feature = "async")]
use qubit_spi::AsyncServiceProvider;
#[cfg(feature = "sync")]
use qubit_spi::ServiceProvider;
use redis::Client;
use redis::Connection;
use redis::RedisError;
use redis::Value;
use redis::cmd;
use redis::from_redis_value;
use redis::streams::StreamRangeReply;
#[cfg(feature = "sync")]
use serde_json::to_string;
use serde_json::to_vec;
use support::controlled_redis::proxy::ControlledRedis;
use support::redis_server::RedisServer;

type TestResult = Result<(), Box<dyn Error>>;

/// Supplies finite receiver limits without limiting observer commands.
fn config(url: &str, namespace: &str) -> EventBusConfig {
    config_limits(url, namespace, 1, 1024)
}

/// Sets exact byte budgets for boundary fixtures sharing the receiver policy.
fn config_limits(url: &str, namespace: &str, payload: usize, wire: usize) -> EventBusConfig {
    let options: ProviderOptions = [
        ("redis.url".into(), url.into()),
        ("redis.namespace".into(), namespace.into()),
        ("redis.max_payload_bytes".into(), payload.to_string()),
        ("redis.max_wire_bytes".into(), wire.to_string()),
        ("redis.claim_min_idle_ms".into(), "0".into()),
    ]
    .into();
    EventBusConfig::default().with_provider_options(options)
}

/// Creates the same stable durable group for observation.
fn request() -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(7001),
        TopicAddress::new("events").expect("topic"),
        SubscriberId::new("worker").expect("subscriber"),
        Some(ConsumerGroup::new("group").expect("group")),
        SubscriptionDurability::Durable,
        StartPosition::Earliest,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    )
}

/// Writes a historical wire directly, bypassing publisher limits.
fn inject(observer: &mut Connection, namespace: &str, bytes: &[u8]) -> Result<String, RedisError> {
    cmd("XADD")
        .arg(stream_key(namespace, "events"))
        .arg("*")
        .arg("wire")
        .arg(bytes)
        .query(observer)
}

/// Reads the actual pending IDs for the stable group.
fn pending(observer: &mut Connection, namespace: &str) -> Result<Vec<Value>, RedisError> {
    cmd("XPENDING")
        .arg(stream_key(namespace, "events"))
        .arg(group_name(namespace, "events", "worker", Some("group")))
        .arg("-")
        .arg("+")
        .arg(100)
        .query(observer)
}

/// Verifies bytes, reason, source association, and acknowledgement together.
fn assert_quarantined(
    observer: &mut Connection,
    namespace: &str,
    id: &str,
    wire: &[u8],
    reason: &str,
) -> TestResult {
    assert!(
        pending(observer, namespace)?.is_empty(),
        "successful quarantine must clear source PEL"
    );
    let group = group_name(namespace, "events", "worker", Some("group"));
    let rows: StreamRangeReply = cmd("XRANGE")
        .arg(poison_key(namespace, "events", &group))
        .arg("-")
        .arg("+")
        .query(observer)?;
    assert_eq!(
        rows.ids.len(),
        1,
        "exactly one script invocation produced a quarantine copy"
    );
    let fields = &rows.ids[0].map;
    assert_eq!(
        fields.get("reason"),
        Some(&Value::BulkString(reason.as_bytes().to_vec()))
    );
    assert_eq!(
        fields.get("source_id"),
        Some(&Value::BulkString(id.as_bytes().to_vec()))
    );
    assert_eq!(fields.get("wire"), Some(&Value::BulkString(wire.to_vec())));
    assert_eq!(
        fields.get("wire_missing"),
        Some(&Value::BulkString(b"0".to_vec()))
    );
    Ok(())
}

/// Verifies a limit failure leaves the source pending and creates no poison
/// copy.
fn assert_retained(observer: &mut Connection, namespace: &str, id: &str) -> TestResult {
    let entries = pending(observer, namespace)?;
    assert_eq!(entries.len(), 1, "limit failure must retain source in PEL");
    let row: Vec<Value> = from_redis_value(&entries[0])?;
    let pending_id: String = from_redis_value(&row[0])?;
    assert_eq!(pending_id, id);
    let group = group_name(namespace, "events", "worker", Some("group"));
    let rows: StreamRangeReply = cmd("XRANGE")
        .arg(poison_key(namespace, "events", &group))
        .arg("-")
        .arg("+")
        .query(observer)?;
    assert!(rows.ids.is_empty(), "limit failure must not quarantine");
    Ok(())
}

/// Deterministic malformed and oversized representations share one fixture.
fn malformed_cases() -> Vec<(Vec<u8>, &'static str)> {
    let payload = WireFields {
        version: 1,
        event_id: "historical-event".into(),
        timestamp_ms: 0,
        headers_json: "{}".into(),
        ordering_key: None,
        content_type: "application/octet-stream".into(),
        schema_id: None,
        payload: vec![0, 1],
    };
    vec![
        (vec![b'x'; 1025], "oversized_wire"),
        (to_vec(&payload).expect("wire"), "oversized_payload"),
        (vec![255], "invalid_wire_field"),
        (
            format!(
                "{{\"version\":1,\"payload\":{}0{}}}",
                "[".repeat(129),
                "]".repeat(129)
            )
            .into_bytes(),
            "invalid_json",
        ),
    ]
}

/// Builds legal version 1 JSON with a payload exactly at the one-byte budget.
fn boundary_wire() -> Vec<u8> {
    to_vec(&WireFields {
        version: 1,
        event_id: "exact-history-event".into(),
        timestamp_ms: 0,
        headers_json: "{}".into(),
        ordering_key: None,
        content_type: "application/octet-stream".into(),
        schema_id: None,
        payload: vec![255],
    })
    .expect("legal wire")
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_historical_valid_wire_is_inclusive_at_exact_byte_limit() -> TestResult {
    let server = RedisServer::start()?;
    let mut observer = Client::open(server.url())?.get_connection()?;
    let wire = boundary_wire();
    let exact = wire.len();
    for over in [false, true] {
        let namespace = format!("history-exact-sync-{over}");
        let bytes = if over {
            let mut bytes = wire.clone();
            bytes.push(b' ');
            bytes
        } else {
            wire.clone()
        };
        let id = inject(&mut observer, &namespace, &bytes)?;
        let bus = RedisEventBusProvider
            .create_configured(&config_limits(server.url(), &namespace, 1, exact))
            .map_err(|failure| failure.into_error())?;
        let mut receiver = bus.subscribe(request())?;
        let outcome = receiver.receive(Duration::ZERO);
        if over {
            assert!(matches!(
                outcome,
                Err(SpiError::Operation {
                    kind: "receive_limit_exceeded",
                    ..
                })
            ));
            assert_retained(&mut observer, &namespace, &id)?;
        } else {
            let ReceiveOutcome::Message(message) = outcome? else {
                panic!("exact wire and payload limits must be inclusive");
            };
            let TransportPayload::Encoded(payload) = message.payload() else {
                panic!("encoded payload");
            };
            assert_eq!(payload.bytes(), [255]);
            receiver.settle(
                message.settlement().expect("token"),
                DeliveryDisposition::Accept,
            )?;
            assert!(pending(&mut observer, &namespace)?.is_empty());
        }
    }
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_historical_valid_wire_is_inclusive_at_exact_byte_limit() -> TestResult {
    block_on(async {
        let server = RedisServer::start()?;
        let mut observer = Client::open(server.url())?.get_connection()?;
        let wire = boundary_wire();
        let exact = wire.len();
        for over in [false, true] {
            let namespace = format!("history-exact-async-{over}");
            let bytes = if over {
                let mut bytes = wire.clone();
                bytes.push(b' ');
                bytes
            } else {
                wire.clone()
            };
            let id = inject(&mut observer, &namespace, &bytes)?;
            let bus = AsyncRedisEventBusProvider
                .create_configured(&config_limits(server.url(), &namespace, 1, exact))
                .await
                .map_err(|failure| failure.into_error())?;
            let mut receiver = bus.subscribe(request()).await?;
            let outcome = receiver.receive(Duration::ZERO).await;
            if over {
                assert!(matches!(
                    outcome,
                    Err(SpiError::Operation {
                        kind: "receive_limit_exceeded",
                        ..
                    })
                ));
                assert_retained(&mut observer, &namespace, &id)?;
            } else {
                let ReceiveOutcome::Message(message) = outcome? else {
                    panic!("exact wire and payload limits must be inclusive");
                };
                let TransportPayload::Encoded(payload) = message.payload() else {
                    panic!("encoded payload");
                };
                assert_eq!(payload.bytes(), [255]);
                receiver
                    .settle(
                        message.settlement().expect("token"),
                        DeliveryDisposition::Accept,
                    )
                    .await?;
                assert!(pending(&mut observer, &namespace)?.is_empty());
            }
        }
        Ok(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_historical_oversize_is_retained_and_malformed_wire_is_quarantined() -> TestResult {
    let server = RedisServer::start()?;
    let mut observer = Client::open(server.url())?.get_connection()?;
    for (index, (wire, reason)) in malformed_cases().into_iter().enumerate() {
        let namespace = format!("poison-size-sync-{index}");
        let id = inject(&mut observer, &namespace, &wire)?;
        let bus = RedisEventBusProvider
            .create_configured(&config(server.url(), &namespace))
            .map_err(|failure| failure.into_error())?;
        let mut receiver = bus.subscribe(request())?;
        let outcome = receiver.receive(Duration::from_secs(2));
        if index < 2 {
            assert!(
                matches!(
                    outcome,
                    Err(SpiError::Operation {
                        kind: "receive_limit_exceeded",
                        ..
                    })
                ),
                "{reason} must retain the pending record"
            );
            assert_retained(&mut observer, &namespace, &id)?;
        } else {
            assert!(
                matches!(outcome?, ReceiveOutcome::Gap(_)),
                "{reason} must return Gap"
            );
            assert_quarantined(&mut observer, &namespace, &id, &wire, reason)?;
            assert!(matches!(
                receiver.receive(Duration::ZERO)?,
                ReceiveOutcome::TimedOut
            ));
        }
    }
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_historical_oversize_is_retained_and_malformed_wire_is_quarantined() -> TestResult {
    block_on(async {
        let server = RedisServer::start()?;
        let mut observer = Client::open(server.url())?.get_connection()?;
        for (index, (wire, reason)) in malformed_cases().into_iter().enumerate() {
            let namespace = format!("poison-size-async-{index}");
            let id = inject(&mut observer, &namespace, &wire)?;
            let bus = AsyncRedisEventBusProvider
                .create_configured(&config(server.url(), &namespace))
                .await
                .map_err(|failure| failure.into_error())?;
            let mut receiver = bus.subscribe(request()).await?;
            let outcome = receiver.receive(Duration::from_secs(2)).await;
            if index < 2 {
                assert!(
                    matches!(
                        outcome,
                        Err(SpiError::Operation {
                            kind: "receive_limit_exceeded",
                            ..
                        })
                    ),
                    "{reason} must retain the pending record"
                );
                assert_retained(&mut observer, &namespace, &id)?;
            } else {
                assert!(
                    matches!(outcome?, ReceiveOutcome::Gap(_)),
                    "{reason} must return Gap"
                );
                assert_quarantined(&mut observer, &namespace, &id, &wire, reason)?;
                assert!(matches!(
                    receiver.receive(Duration::ZERO).await?,
                    ReceiveOutcome::TimedOut
                ));
            }
        }
        Ok(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_historical_payload_over_limit_remains_pending() -> TestResult {
    let server = RedisServer::start()?;
    let namespace = "history-payload-sync";
    let (wire, _) = malformed_cases().remove(1);
    let mut observer = Client::open(server.url())?.get_connection()?;
    let id = inject(&mut observer, namespace, &wire)?;
    let bus = RedisEventBusProvider
        .create_configured(&config(server.url(), namespace))
        .map_err(|failure| failure.into_error())?;
    let mut receiver = bus.subscribe(request())?;
    assert!(
        matches!(
            receiver.receive(Duration::ZERO),
            Err(SpiError::Operation {
                kind: "receive_limit_exceeded",
                ..
            })
        ),
        "historical max + 1 payload must remain pending"
    );
    assert_retained(&mut observer, namespace, &id)?;
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_historical_payload_over_limit_remains_pending() -> TestResult {
    block_on(async {
        let server = RedisServer::start()?;
        let namespace = "history-payload-async";
        let (wire, _) = malformed_cases().remove(1);
        let mut observer = Client::open(server.url())?.get_connection()?;
        let id = inject(&mut observer, namespace, &wire)?;
        let bus = AsyncRedisEventBusProvider
            .create_configured(&config(server.url(), namespace))
            .await
            .map_err(|failure| failure.into_error())?;
        let mut receiver = bus.subscribe(request()).await?;
        assert!(
            matches!(
                receiver.receive(Duration::ZERO).await,
                Err(SpiError::Operation {
                    kind: "receive_limit_exceeded",
                    ..
                })
            ),
            "historical max + 1 payload must remain pending"
        );
        assert_retained(&mut observer, namespace, &id)?;
        Ok(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn test_unknown_numeric_wire_versions_remain_pending_without_quarantine() -> TestResult {
    let server = RedisServer::start()?;
    let mut observer = Client::open(server.url())?.get_connection()?;
    for version in [2, 999, u64::MAX] {
        let namespace = format!("unknown-wire-{version}");
        inject(
            &mut observer,
            &namespace,
            format!("{{\"version\":{version}}}").as_bytes(),
        )?;
        let bus = RedisEventBusProvider
            .create_configured(&config(server.url(), &namespace))
            .map_err(|failure| failure.into_error())?;
        let mut receiver = bus.subscribe(request())?;
        assert!(matches!(
            receiver.receive(Duration::ZERO),
            Err(SpiError::Operation {
                kind: "unsupported_wire_version",
                retryable: Some(false),
                ..
            })
        ));
        assert_eq!(
            pending(&mut observer, &namespace)?.len(),
            1,
            "unknown version must preserve PEL"
        );
        let group = group_name(&namespace, "events", "worker", Some("group"));
        let copies: usize = cmd("XLEN")
            .arg(poison_key(&namespace, "events", &group))
            .query(&mut observer)?;
        assert_eq!(copies, 0);
    }
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_quarantine_reply_loss_reports_unknown_after_real_copy_and_ack() -> TestResult {
    let server = RedisServer::start()?;
    let proxy = ControlledRedis::start(server.url())?;
    let namespace = "quarantine-reply-loss-sync";
    let mut observer = Client::open(server.url())?.get_connection()?;
    let wire = b"invalid JSON";
    let id = inject(&mut observer, namespace, wire)?;
    let bus = RedisEventBusProvider
        .create_configured(&config(&proxy.url(), namespace))
        .map_err(|failure| failure.into_error())?;
    let mut receiver = bus.subscribe(request())?;
    let gate = proxy.pause_after_reply("EVAL");
    let worker = spawn(move || {
        let result = receiver.receive(Duration::from_secs(2));
        (result, receiver)
    });
    let reached = gate.wait_until_reached(Duration::from_secs(3));
    if !reached {
        gate.release_without_reply();
    }
    assert!(reached, "quarantine EVAL must reach applied reply gate");
    assert_quarantined(&mut observer, namespace, &id, wire, "invalid_json")?;
    gate.release_without_reply();
    let (result, mut receiver) = worker.join().expect("receive worker");
    assert!(
        matches!(
            result,
            Err(SpiError::Operation {
                kind: "outcome_unknown",
                retryable: Some(false),
                ..
            })
        ),
        "lost EVAL reply may conceal a successful copy and ACK"
    );
    assert!(matches!(
        receiver.receive(Duration::ZERO)?,
        ReceiveOutcome::TimedOut
    ));
    assert_quarantined(&mut observer, namespace, &id, wire, "invalid_json")?;
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_quarantine_reply_loss_reports_unknown_after_real_copy_and_ack() -> TestResult {
    let server = RedisServer::start()?;
    let proxy = ControlledRedis::start(server.url())?;
    let namespace = "quarantine-reply-loss-async";
    let mut observer = Client::open(server.url())?.get_connection()?;
    let wire = b"invalid JSON";
    let id = inject(&mut observer, namespace, wire)?;
    let bus =
        block_on(AsyncRedisEventBusProvider.create_configured(&config(&proxy.url(), namespace)))
            .map_err(|failure| failure.into_error())?;
    let mut receiver = block_on(bus.subscribe(request()))?;
    let gate = proxy.pause_after_reply("EVAL");
    let worker = spawn(move || {
        let result = block_on(receiver.receive(Duration::from_secs(2)));
        (result, receiver)
    });
    let reached = gate.wait_until_reached(Duration::from_secs(3));
    if !reached {
        gate.release_without_reply();
    }
    assert!(reached, "quarantine EVAL must reach applied reply gate");
    assert_quarantined(&mut observer, namespace, &id, wire, "invalid_json")?;
    gate.release_without_reply();
    let (result, mut receiver) = worker.join().expect("receive worker");
    assert!(matches!(
        result,
        Err(SpiError::Operation {
            kind: "outcome_unknown",
            retryable: Some(false),
            ..
        })
    ));
    assert!(matches!(
        block_on(receiver.receive(Duration::ZERO))?,
        ReceiveOutcome::TimedOut
    ));
    assert_quarantined(&mut observer, namespace, &id, wire, "invalid_json")?;
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_source_removed_before_quarantine_reports_gap_without_fabricated_copy() -> TestResult {
    let server = RedisServer::start()?;
    let proxy = ControlledRedis::start(server.url())?;
    let namespace = "quarantine-source-gone";
    let mut observer = Client::open(server.url())?.get_connection()?;
    inject(&mut observer, namespace, b"invalid JSON")?;
    let bus = RedisEventBusProvider
        .create_configured(&config(&proxy.url(), namespace))
        .map_err(|failure| failure.into_error())?;
    let mut receiver = bus.subscribe(request())?;
    let gate = proxy.gate();
    gate.arm();
    let worker = spawn(move || receiver.receive(Duration::from_secs(2)));
    let reached = gate.wait_until_reached(Duration::from_secs(3));
    if !reached {
        gate.release_without_reply();
    }
    assert!(reached, "read must be captured before source removal");
    cmd("DEL")
        .arg(stream_key(namespace, "events"))
        .query::<usize>(&mut observer)?;
    gate.release();
    assert!(
        matches!(
            worker.join().expect("receive worker")?,
            ReceiveOutcome::Gap(_)
        ),
        "removed source should produce SourceGone gap"
    );
    let group = group_name(namespace, "events", "worker", Some("group"));
    let copies: usize = cmd("XLEN")
        .arg(poison_key(namespace, "events", &group))
        .query(&mut observer)?;
    assert_eq!(
        copies, 0,
        "Lua must not fabricate wire when source disappeared"
    );
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_quarantine_wrong_type_preserves_source_pending_and_existing_value() -> TestResult {
    let server = RedisServer::start()?;
    let namespace = "quarantine-wrong-type";
    let mut observer = Client::open(server.url())?.get_connection()?;
    let id = inject(&mut observer, namespace, b"invalid JSON")?;
    let group = group_name(namespace, "events", "worker", Some("group"));
    let quarantine = poison_key(namespace, "events", &group);
    cmd("SET")
        .arg(&quarantine)
        .arg("preserve-existing")
        .query::<()>(&mut observer)?;
    let bus = RedisEventBusProvider
        .create_configured(&config(server.url(), namespace))
        .map_err(|failure| failure.into_error())?;
    let mut receiver = bus.subscribe(request())?;
    assert!(matches!(
        receiver.receive(Duration::ZERO),
        Err(SpiError::Operation {
            retryable: Some(false),
            ..
        })
    ));
    let entries = pending(&mut observer, namespace)?;
    assert_eq!(entries.len(), 1);
    let Value::Array(fields) = &entries[0] else {
        panic!("pending row");
    };
    assert_eq!(
        fields[0],
        Value::BulkString(id.into_bytes()),
        "type rejection must preserve the exact pending source"
    );
    let original: String = cmd("GET").arg(quarantine).query(&mut observer)?;
    assert_eq!(
        original, "preserve-existing",
        "preflight must not mutate wrong-type quarantine"
    );
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_duplicate_wire_quarantines_the_last_value_used_for_decoding() -> TestResult {
    let server = RedisServer::start()?;
    let namespace = "duplicate-wire-sync";
    let mut observer = Client::open(server.url())?.get_connection()?;
    let valid = boundary_wire();
    let invalid = b"last wire is malformed JSON";
    let id: String = cmd("XADD")
        .arg(stream_key(namespace, "events"))
        .arg("*")
        .arg("wire")
        .arg(&valid)
        .arg("note")
        .arg("first")
        .arg("wire")
        .arg(invalid)
        .arg("note")
        .arg("last")
        .query(&mut observer)?;
    let bus = RedisEventBusProvider
        .create_configured(&config(server.url(), namespace))
        .map_err(|failure| failure.into_error())?;
    let mut receiver = bus.subscribe(request())?;
    assert!(
        matches!(receiver.receive(Duration::ZERO)?, ReceiveOutcome::Gap(_)),
        "last malformed wire decides decoding"
    );
    assert_quarantined(&mut observer, namespace, &id, invalid, "invalid_json")?;
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_duplicate_wire_quarantines_the_last_value_used_for_decoding() -> TestResult {
    block_on(async {
        let server = RedisServer::start()?;
        let namespace = "duplicate-wire-async";
        let mut observer = Client::open(server.url())?.get_connection()?;
        let valid = boundary_wire();
        let invalid = b"last wire is malformed JSON";
        let id: String = cmd("XADD")
            .arg(stream_key(namespace, "events"))
            .arg("*")
            .arg("wire")
            .arg(&valid)
            .arg("note")
            .arg("first")
            .arg("wire")
            .arg(invalid)
            .arg("note")
            .arg("last")
            .query(&mut observer)?;
        let bus = AsyncRedisEventBusProvider
            .create_configured(&config(server.url(), namespace))
            .await
            .map_err(|failure| failure.into_error())?;
        let mut receiver = bus.subscribe(request()).await?;
        assert!(
            matches!(
                receiver.receive(Duration::ZERO).await?,
                ReceiveOutcome::Gap(_)
            ),
            "last malformed wire decides decoding"
        );
        assert_quarantined(&mut observer, namespace, &id, invalid, "invalid_json")?;
        Ok(())
    })
}

/// Adds an unknown JSON field without changing any known version 1 metadata.
fn wire_with_nested_extra(depth: usize) -> Vec<u8> {
    let wire = String::from_utf8(boundary_wire()).expect("UTF-8 JSON");
    format!(
        "{},\"extra\":{}0{}}}",
        wire.strip_suffix('}').expect("object"),
        "[".repeat(depth),
        "]".repeat(depth)
    )
    .into_bytes()
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_json_depth_limit_applies_to_unknown_v1_fields() -> TestResult {
    let server = RedisServer::start()?;
    let mut observer = Client::open(server.url())?.get_connection()?;
    for depth in [126, 127, 129] {
        let namespace = format!("unknown-depth-sync-{depth}");
        let wire = wire_with_nested_extra(depth);
        let id = inject(&mut observer, &namespace, &wire)?;
        let bus = RedisEventBusProvider
            .create_configured(&config(server.url(), &namespace))
            .map_err(|failure| failure.into_error())?;
        let mut receiver = bus.subscribe(request())?;
        let outcome = receiver.receive(Duration::ZERO)?;
        if depth == 126 {
            let ReceiveOutcome::Message(message) = outcome else {
                panic!("127 total containers remain within default depth budget");
            };
            receiver.settle(
                message.settlement().expect("token"),
                DeliveryDisposition::Accept,
            )?;
        } else {
            assert!(
                matches!(outcome, ReceiveOutcome::Gap(_)),
                "unknown fields at total depth {} must be rejected",
                depth + 1
            );
            assert_quarantined(&mut observer, &namespace, &id, &wire, "invalid_json")?;
        }
    }
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_json_depth_limit_applies_to_unknown_v1_fields() -> TestResult {
    block_on(async {
        let server = RedisServer::start()?;
        let mut observer = Client::open(server.url())?.get_connection()?;
        for depth in [126, 127, 129] {
            let namespace = format!("unknown-depth-async-{depth}");
            let wire = wire_with_nested_extra(depth);
            let id = inject(&mut observer, &namespace, &wire)?;
            let bus = AsyncRedisEventBusProvider
                .create_configured(&config(server.url(), &namespace))
                .await
                .map_err(|failure| failure.into_error())?;
            let mut receiver = bus.subscribe(request()).await?;
            let outcome = receiver.receive(Duration::ZERO).await?;
            if depth == 126 {
                let ReceiveOutcome::Message(message) = outcome else {
                    panic!("127 total containers remain within default depth budget");
                };
                receiver
                    .settle(
                        message.settlement().expect("token"),
                        DeliveryDisposition::Accept,
                    )
                    .await?;
            } else {
                assert!(
                    matches!(outcome, ReceiveOutcome::Gap(_)),
                    "unknown fields at total depth {} must be rejected",
                    depth + 1
                );
                assert_quarantined(&mut observer, &namespace, &id, &wire, "invalid_json")?;
            }
        }
        Ok(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn test_json_depth_scan_ignores_structural_characters_inside_escaped_strings() -> TestResult {
    let server = RedisServer::start()?;
    let namespace = "quoted-json-depth";
    let wire = String::from_utf8(boundary_wire()).expect("wire JSON");
    let quoted = to_string(&"[{\\\"".repeat(128))?;
    let wire = format!(
        "{},\"extra\":{quoted}}}",
        wire.strip_suffix('}').expect("object")
    );
    let mut observer = Client::open(server.url())?.get_connection()?;
    inject(&mut observer, namespace, wire.as_bytes())?;
    let bus = RedisEventBusProvider
        .create_configured(&config_limits(server.url(), namespace, 1, wire.len()))
        .map_err(|failure| failure.into_error())?;
    let mut receiver = bus.subscribe(request())?;
    let ReceiveOutcome::Message(message) = receiver.receive(Duration::ZERO)? else {
        panic!("quoted delimiters must not count as containers");
    };
    receiver.settle(
        message.settlement().expect("token"),
        DeliveryDisposition::Accept,
    )?;
    assert!(pending(&mut observer, namespace)?.is_empty());
    Ok(())
}
