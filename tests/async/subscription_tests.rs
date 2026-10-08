// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Unknown-XACK intent remains fixed until identical settlement succeeds.
use std::any::TypeId;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use futures_lite::future::block_on;
use futures_lite::future::race;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::SpiError;
use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
use qubit_event_bus_redis::naming::group_name;
use qubit_event_bus_redis::naming::stream_key;
use qubit_id::Id;
use qubit_spi::AsyncServiceProvider;
use redis::Client;
use redis::Value;
use redis::cmd;

use crate::support::controlled_redis::proxy::ControlledRedis;
use crate::support::redis_server::RedisServer;
use crate::support::scripted_redis::ScriptedRedis;
use crate::support::scripted_redis::Step;

type TestResult = Result<(), Box<dyn Error>>;

/// Creates a lazy single-slot bus for `url`; returns configuration/provider
/// validation errors without connecting to the controlled endpoint.
fn create_bus(url: &str) -> Result<Arc<dyn AsyncEventBusSpi>, Box<dyn Error>> {
    create_bus_with_claim_min_idle(url, 0)
}

/// Creates a lazy bus with the requested claim delay; returns configuration
/// errors before connecting to Redis.
fn create_bus_with_claim_min_idle(
    url: &str,
    claim_min_idle_ms: usize,
) -> Result<Arc<dyn AsyncEventBusSpi>, Box<dyn Error>> {
    let options: ProviderOptions = [
        ("redis.url".into(), url.into()),
        ("redis.namespace".into(), "settlement-tests".into()),
        ("redis.claim_min_idle_ms".into(), claim_min_idle_ms.to_string()),
        ("redis.max_unsettled_per_subscription".into(), "1".into()),
    ]
    .into();
    let config = EventBusConfig::default().with_provider_options(options);
    block_on(AsyncRedisEventBusProvider.create_configured(&config))
        .map_err(|failure| failure.into_error())
        .map_err(Into::into)
}

/// A new stream delivery has one known provider attempt, while recovery paths
/// retain unknown attempt counts.
#[test]
fn test_receive_provider_attempt_distinguishes_new_pending_and_claimed() -> TestResult {
    block_on(async {
        let pending_server = RedisServer::start()?;
        let pending_bus = create_bus_with_claim_min_idle(pending_server.url(), 60_000)?;
        let _ = pending_bus.publish(message("pending-attempt")?).await?;
        let mut pending_receiver = pending_bus.subscribe(request()?).await?;
        let ReceiveOutcome::Message(new_message) = pending_receiver.receive(Duration::from_secs(2)).await? else {
            return Err("new message missing".into());
        };
        assert_eq!(new_message.provider_attempt().map(|attempt| attempt.get()), Some(1));
        pending_receiver
            .settle(
                new_message.settlement().ok_or("new message settlement token missing")?,
                DeliveryDisposition::Retry,
            )
            .await?;
        let ReceiveOutcome::Message(pending_message) = pending_receiver.receive(Duration::from_secs(2)).await? else {
            return Err("pending message missing".into());
        };
        assert_eq!(pending_message.provider_attempt(), None);

        let claim_server = RedisServer::start()?;
        let claim_bus = create_bus(claim_server.url())?;
        let _ = claim_bus.publish(message("claimed-attempt")?).await?;
        let mut first_receiver = claim_bus.subscribe(request()?).await?;
        let ReceiveOutcome::Message(first_message) = first_receiver.receive(Duration::from_secs(2)).await? else {
            return Err("first claimed message delivery missing".into());
        };
        assert_eq!(first_message.provider_attempt().map(|attempt| attempt.get()), Some(1));
        drop(first_receiver);
        let mut claiming_receiver = claim_bus.subscribe(request_at(StartPosition::New)?).await?;
        let ReceiveOutcome::Message(claimed_message) = claiming_receiver.receive(Duration::from_secs(2)).await? else {
            return Err("claimed message missing".into());
        };
        assert_eq!(claimed_message.provider_attempt(), None);
        Ok(())
    })
}
/// Builds the settlement event for `id` without I/O; returns invalid event
/// identifier or fixed metadata validation errors.
fn message(id: &str) -> Result<OutboundMessage, Box<dyn Error>> {
    Ok(OutboundMessage::new(
        TopicAddress::new("settlement")?,
        EventId::new(id)?,
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(&b"payload"[..]),
            ContentType::new("application/octet-stream")?,
            None,
        )),
    ))
}
/// Builds a durable earliest-position request without I/O; returns fixed
/// identifier validation errors before any receiver is opened.
fn request() -> Result<SpiSubscriptionRequest, Box<dyn Error>> {
    request_at(StartPosition::Earliest)
}

/// Builds a durable request with `start_position` without I/O; returns fixed
/// identifier validation errors before any receiver is opened.
fn request_at(start_position: StartPosition) -> Result<SpiSubscriptionRequest, Box<dyn Error>> {
    Ok(SpiSubscriptionRequest::new(
        Id::new(1),
        TopicAddress::new("settlement")?,
        SubscriberId::new("worker")?,
        Some(ConsumerGroup::new("group")?),
        SubscriptionDurability::Durable,
        start_position,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    ))
}
/// Checks `server` has no pending settlement entries using blocking Redis
/// I/O; returns transport errors and panics if XACK was not applied.
fn assert_ack_applied(server: &RedisServer) -> TestResult {
    let mut observer = Client::open(server.url())?.get_connection()?;
    let pending: Vec<Value> = cmd("XPENDING")
        .arg(stream_key("settlement-tests", "settlement"))
        .arg(group_name("settlement-tests", "settlement", "worker", Some("group")))
        .arg("-")
        .arg("+")
        .arg(10)
        .query(&mut observer)?;
    assert!(
        pending.is_empty(),
        "Redis must apply XACK before the reply is lost or cancelled"
    );
    Ok(())
}

#[test]
fn test_settle_cancelled_applied_xack_preserves_intent_and_same_retry_releases_slot() -> TestResult {
    let server = RedisServer::start()?;
    let proxy = ControlledRedis::start(server.url())?;
    let bus = create_bus(&proxy.url())?;
    block_on(async {
        let _ = bus.publish(message("first")?).await?;
        let _ = bus.publish(message("second")?).await?;
        let mut receiver = bus.subscribe(request()?).await?;
        let ReceiveOutcome::Message(received) = receiver.receive(Duration::from_secs(2)).await? else {
            return Err("first event missing".into());
        };
        let token = received.settlement().ok_or("token missing")?;
        let gate = proxy.pause_after_reply("XACK");
        let result = race(
            async { Some(receiver.settle(token, DeliveryDisposition::Accept).await) },
            async {
                gate.wait_applied().await;
                None
            },
        )
        .await;
        gate.release_without_reply();
        assert!(result.is_none(), "settlement is cancelled after Redis applies XACK");
        assert_ack_applied(&server)?;
        assert!(
            receiver.settle(token, DeliveryDisposition::Retry).await.is_err(),
            "unknown Accept must reject direct Retry"
        );
        assert!(
            receiver.settle(token, DeliveryDisposition::Reject).await.is_err(),
            "unknown Accept must reject direct Reject"
        );
        assert!(matches!(
            receiver.receive(Duration::ZERO).await?,
            ReceiveOutcome::TimedOut
        ));
        // The cancelled multiplexed connection may still be consuming its lost reply.
        let _ = receiver.settle(token, DeliveryDisposition::Accept).await;
        receiver.settle(token, DeliveryDisposition::Accept).await?;
        receiver.settle(token, DeliveryDisposition::Accept).await?;
        assert!(matches!(
            receiver.receive(Duration::from_secs(2)).await?,
            ReceiveOutcome::Message(_)
        ));
        receiver.close().await?;
        Ok(())
    })
}

#[test]
fn test_settle_unpolled_future_and_first_explicit_rejection_allow_retry() -> TestResult {
    let server = RedisServer::start()?;
    let bus = create_bus(server.url())?;
    block_on(async {
        let _ = bus.publish(message("rejected")?).await?;
        let mut receiver = bus.subscribe(request()?).await?;
        let ReceiveOutcome::Message(received) = receiver.receive(Duration::from_secs(2)).await? else {
            return Err("event missing".into());
        };
        let token = received.settlement().ok_or("token missing")?;
        drop(receiver.settle(token, DeliveryDisposition::Reject));
        let mut observer = Client::open(server.url())?.get_connection()?;
        cmd("SET")
            .arg(stream_key("settlement-tests", "settlement"))
            .arg("wrong type")
            .query::<()>(&mut observer)?;
        assert!(receiver.settle(token, DeliveryDisposition::Accept).await.is_err());
        receiver.settle(token, DeliveryDisposition::Retry).await?;
        Ok(())
    })
}

#[test]
fn test_settle_explicit_rejection_after_cancelled_xack_never_unlocks_intent() -> TestResult {
    let server = RedisServer::start()?;
    let proxy = ControlledRedis::start(server.url())?;
    let bus = create_bus(&proxy.url())?;
    block_on(async {
        let _ = bus.publish(message("unknown")?).await?;
        let mut receiver = bus.subscribe(request()?).await?;
        let ReceiveOutcome::Message(received) = receiver.receive(Duration::from_secs(2)).await? else {
            return Err("event missing".into());
        };
        let token = received.settlement().ok_or("token missing")?;
        let gate = proxy.pause_after_reply("XACK");
        let result = race(
            async { Some(receiver.settle(token, DeliveryDisposition::Accept).await) },
            async {
                gate.wait_applied().await;
                None
            },
        )
        .await;
        gate.release();
        assert!(result.is_none());
        assert_ack_applied(&server)?;
        let mut observer = Client::open(server.url())?.get_connection()?;
        cmd("SET")
            .arg(stream_key("settlement-tests", "settlement"))
            .arg("wrong type")
            .query::<()>(&mut observer)?;
        assert!(
            receiver.settle(token, DeliveryDisposition::Accept).await.is_err(),
            "retry now receives explicit WRONGTYPE"
        );
        assert!(
            receiver.settle(token, DeliveryDisposition::Retry).await.is_err(),
            "later rejection cannot disprove earlier applied XACK"
        );
        assert!(receiver.settle(token, DeliveryDisposition::Reject).await.is_err());
        Ok(())
    })
}

/// A nonterminal claim cursor must preserve both zero-timeout read stages.
#[test]
fn test_zero_timeout_claim_cursor_preserves_pending_and_new_reads() -> TestResult {
    block_on(async {
        let server = ScriptedRedis::start(vec![
            Step::reply("XGROUP", b"+OK\r\n"),
            Step::reply("XAUTOCLAIM", b"*2\r\n$3\r\n9-0\r\n*1\r\n*1\r\n$-1\r\n"),
            Step::reply("XREADGROUP", b"*0\r\n"),
            Step::reply("XREADGROUP", b"*0\r\n"),
        ])?;
        let bus = create_bus(server.url())?;
        let mut receiver = bus.subscribe(request()?).await?;
        assert!(matches!(
            receiver.receive(Duration::ZERO).await?,
            ReceiveOutcome::TimedOut
        ));
        let commands = server.finish_allow_remaining();
        assert_eq!(
            commands.iter().map(|command| command[0].as_str()).collect::<Vec<_>>(),
            vec!["XGROUP", "XAUTOCLAIM", "XREADGROUP", "XREADGROUP"]
        );
        let reads: Vec<_> = commands.iter().filter(|command| command[0] == "XREADGROUP").collect();
        assert_eq!(reads.len(), 2, "own pending and new reads must both dispatch");
        assert_eq!(reads[0].last().map(String::as_str), Some("0-0"));
        assert_eq!(reads[1].last().map(String::as_str), Some(">"));
        assert!(
            !commands
                .iter()
                .any(|command| matches!(command[0].as_str(), "XPENDING" | "XRANGE" | "EVAL"))
        );
        assert!(
            reads
                .iter()
                .all(|command| !command.iter().any(|argument| argument == "BLOCK"))
        );
        Ok(())
    })
}

#[cfg(feature = "async")]
#[test]
fn test_async_subscribe_rejects_invalid_stream_position_before_connecting() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let bus = AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default())
            .await
            .map_err(|failure| failure.into_error())?;
        let request = SpiSubscriptionRequest::new(
            Id::new(91_002),
            TopicAddress::new("invalid-position")?,
            SubscriberId::new("invalid-position-async-worker")?,
            None,
            SubscriptionDurability::Durable,
            StartPosition::At("not-a-stream-id".into()),
            ProviderOptions::new(),
            TypeId::of::<Vec<u8>>(),
        );
        assert!(bus.subscribe(request).await.is_err());
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn test_settle_rejects_an_unrecognized_token_state() -> TestResult {
    let server = RedisServer::start()?;
    let bus = create_bus(server.url())?;
    block_on(async {
        let mut receiver = bus.subscribe(request()?).await?;
        let token = SettlementToken::new(Id::new(1), StartPosition::New);
        assert!(receiver.settle(&token, DeliveryDisposition::Accept).await.is_err());
        Ok(())
    })
}

#[test]
fn test_close_makes_future_receives_return_closed() -> TestResult {
    let server = RedisServer::start()?;
    let bus = create_bus(server.url())?;
    block_on(async {
        let mut receiver = bus.subscribe(request()?).await?;
        receiver.close().await?;
        assert!(matches!(
            receiver.receive(Duration::ZERO).await,
            Ok(ReceiveOutcome::Closed)
        ));
        Ok(())
    })
}

#[test]
fn test_retry_settlement_is_idempotent_and_rejects_a_conflicting_disposition() -> TestResult {
    let server = RedisServer::start()?;
    let bus = create_bus(server.url())?;
    block_on(async {
        let _ = bus.publish(message("retry-contract")?).await?;
        let mut receiver = bus.subscribe(request()?).await?;
        let ReceiveOutcome::Message(received) = receiver.receive(Duration::from_secs(2)).await? else {
            return Err("event missing".into());
        };
        let token = received.settlement().ok_or("token missing")?;
        receiver.settle(token, DeliveryDisposition::Retry).await?;
        receiver.settle(token, DeliveryDisposition::Retry).await?;
        assert!(receiver.settle(token, DeliveryDisposition::Accept).await.is_err());
        Ok(())
    })
}

/// Malformed replies in each receive stage stay unknown and permit a fresh
/// scan.
#[test]
fn test_receive_malformed_range_pending_and_new_replies_recover_safely() -> TestResult {
    block_on(async {
        let empty_claim = b"*2\r\n$3\r\n0-0\r\n*0\r\n";
        let missing_claim = b"*2\r\n$3\r\n9-0\r\n*1\r\n*1\r\n$-1\r\n";
        let pending_owner = b"*1\r\n*4\r\n$3\r\n1-0\r\n$5\r\nowner\r\n:100\r\n:1\r\n";
        for stage in ["range", "pending", "new"] {
            for malformed in [
                b"$12\r\nfault-secret\r\n".as_slice(),
                b"*1\r\n-ERR fault-secret\r\n".as_slice(),
            ] {
                let mut steps = vec![Step::reply("XGROUP", b"+OK\r\n")];
                let mut expected = vec!["XGROUP", "XAUTOCLAIM"];
                if stage == "range" {
                    steps.extend([
                        Step::reply("XAUTOCLAIM", missing_claim),
                        Step::reply("XPENDING", pending_owner),
                        Step::reply("XRANGE", malformed),
                    ]);
                    expected.extend(["XPENDING", "XRANGE"]);
                } else {
                    steps.push(Step::reply("XAUTOCLAIM", empty_claim));
                    if stage == "new" {
                        steps.push(Step::reply("XREADGROUP", b"*0\r\n"));
                        expected.push("XREADGROUP");
                    }
                    steps.push(Step::reply("XREADGROUP", malformed));
                    expected.push("XREADGROUP");
                }
                let failure_command_count = expected.len();
                steps.extend([
                    Step::reply("XAUTOCLAIM", empty_claim),
                    Step::reply("XREADGROUP", b"*0\r\n"),
                    Step::reply("XREADGROUP", b"*0\r\n"),
                ]);
                expected.extend(["XAUTOCLAIM", "XREADGROUP", "XREADGROUP"]);
                let server = ScriptedRedis::start(steps)?;
                let bus = create_bus(server.url())?;
                let mut receiver = bus.subscribe(request()?).await?;
                let timeout = if stage == "range" {
                    Duration::from_secs(1)
                } else {
                    Duration::ZERO
                };
                let error = match receiver.receive(timeout).await {
                    Err(error) => error,
                    Ok(_) => return Err(format!("malformed {stage} reply was accepted").into()),
                };
                let SpiError::Operation {
                    provider_id,
                    operation,
                    resource,
                    kind,
                    retryable,
                    source,
                } = error
                else {
                    return Err(format!("malformed {stage} reply returned the wrong error variant").into());
                };
                assert_eq!(provider_id.as_ref(), "redis-streams");
                assert_eq!(operation, "receive");
                assert_eq!(resource.as_deref(), Some("settlement"));
                assert_eq!(kind, "outcome_unknown", "{stage}");
                assert_eq!(retryable, Some(true));
                assert!(!source.to_string().contains("fault-secret"));
                assert!(source.source().is_none(), "raw protocol diagnostics must not escape");
                assert!(matches!(
                    receiver.receive(Duration::ZERO).await?,
                    ReceiveOutcome::TimedOut
                ));
                let commands = server.finish();
                assert_eq!(
                    commands.iter().map(|command| command[0].as_str()).collect::<Vec<_>>(),
                    expected
                );
                assert_eq!(
                    commands[failure_command_count][5],
                    if stage == "range" { "9-0" } else { "0-0" },
                    "the retry must resume the last completed claim cursor"
                );
                assert_eq!(
                    commands[failure_command_count + 1].last().map(String::as_str),
                    Some("0-0")
                );
                assert_eq!(
                    commands[failure_command_count + 2].last().map(String::as_str),
                    Some(">")
                );
                if stage == "pending" || stage == "new" {
                    assert_eq!(
                        commands[failure_command_count - 1].last().map(String::as_str),
                        Some(if stage == "pending" { "0-0" } else { ">" })
                    );
                }
                assert!(
                    !commands.iter().any(|command| command[0] == "EVAL"),
                    "protocol failure must not acknowledge or quarantine an unknown record"
                );
            }
        }
        Ok(())
    })
}
