// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Verifies asynchronous SPI behavior under Smol and Tokio hosts.

#![cfg(feature = "async")]

mod support;

use std::any::TypeId;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc::channel;
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Waker;
use std::thread::scope;
use std::time::Duration;
use std::time::SystemTime;

use futures_lite::future::block_on;
#[cfg(feature = "conformance")]
use futures_lite::future::poll_once;
use futures_lite::future::race;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::AsyncConformanceCheck;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::AsyncConformanceHooks;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::ConformanceProfile;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::run_async_with_profile;
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
use qubit_event_bus_redis::naming::group_name;
use qubit_event_bus_redis::naming::stream_key;
use qubit_id::Id;
use qubit_spi::AsyncServiceProvider;
use redis::Client;
use redis::Value;
use redis::cmd;
use redis::from_redis_value;
use redis::streams::StreamPendingCountReply;
use support::controlled_redis::proxy::ControlledRedis;
use support::redis_server::RedisServer;

static SUBSCRIPTION_IDS: AtomicU64 = AtomicU64::new(100);

#[test]
fn test_async_close_makes_future_receives_return_closed() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    block_on(async {
        let mut subscription = bus
            .subscribe(request("async-close-events", "close-worker")?)
            .await?;
        subscription.close().await?;
        assert!(matches!(
            subscription.receive(Duration::ZERO).await?,
            ReceiveOutcome::Closed
        ));
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn test_async_unpolled_close_has_no_effect_and_polled_close_converges() -> Result<(), Box<dyn Error>>
{
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    block_on(async {
        let shutdown = bus.shutdown(ShutdownMode::Immediate);
        drop(shutdown);
        let _ = bus
            .publish(message(
                "close-cancellation",
                "after-unpolled-shutdown",
                b"open",
            )?)
            .await?;
        let mut subscription = bus
            .subscribe(request("close-cancellation", "close-cancel-worker")?)
            .await?;
        let close = subscription.close();
        drop(close);
        assert!(!matches!(
            subscription.receive(Duration::ZERO).await?,
            ReceiveOutcome::Closed
        ));
        subscription.close().await?;
        assert!(matches!(
            subscription.receive(Duration::ZERO).await?,
            ReceiveOutcome::Closed
        ));
        subscription.close().await?;
        let outcome = bus.shutdown(ShutdownMode::Immediate).await?;
        assert!(matches!(outcome, ShutdownOutcome::Complete));
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn test_async_receive_future_is_send() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    block_on(async {
        let mut subscription = bus
            .subscribe(request("send-contract", "send-worker")?)
            .await?;
        // Consumes a future only to enforce its compile-time Send contract.
        fn assert_send<T: Send>(_: T) {}
        assert_send(subscription.receive(Duration::from_millis(50)));
        subscription.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn test_async_approximate_stream_limit_trims_old_entries_when_enabled() -> Result<(), Box<dyn Error>>
{
    let server = RedisServer::start()?;
    let options: ProviderOptions = [
        ("redis.url".into(), server.url().into()),
        ("redis.namespace".into(), "async-limit-tests".into()),
        ("redis.stream_maxlen_approx".into(), "10".into()),
        ("redis.allow_lossy_retention".into(), "true".into()),
    ]
    .into();
    let bus = block_on(
        AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(options)),
    )
    .map_err(|failure| failure.into_error())?;
    block_on(async {
        for index in 0..250 {
            let _ = bus
                .publish(message(
                    "trim-events",
                    &format!("trim-{index}"),
                    b"payload",
                )?)
                .await?;
        }
        Ok::<(), Box<dyn Error>>(())
    })?;

    let mut connection = Client::open(server.url())?.get_connection()?;
    let length: usize = cmd("XLEN")
        .arg(stream_key("async-limit-tests", "trim-events"))
        .query(&mut connection)?;
    assert!(length < 250, "approximate trim left {length} entries");
    Ok(())
}

#[test]
fn test_async_stream_is_untrimmed_by_default() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let options: ProviderOptions = [
        ("redis.url".into(), server.url().into()),
        ("redis.namespace".into(), "async-default-retention".into()),
    ]
    .into();
    let bus = block_on(
        AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(options)),
    )
    .map_err(|failure| failure.into_error())?;
    block_on(async {
        for index in 0..250 {
            let _ = bus
                .publish(message(
                    "default-events",
                    &format!("default-{index}"),
                    b"payload",
                )?)
                .await?;
        }
        Ok::<(), Box<dyn Error>>(())
    })?;

    let mut connection = Client::open(server.url())?.get_connection()?;
    let length: usize = cmd("XLEN")
        .arg(stream_key("async-default-retention", "default-events"))
        .query(&mut connection)?;
    assert_eq!(length, 250);
    Ok(())
}

/// Returns a lazy public bus for `server`; configuration/provider validation
/// errors propagate without opening a Redis connection.
fn create_bus(server: &RedisServer) -> Result<Arc<dyn AsyncEventBusSpi>, Box<dyn Error>> {
    create_bus_url(server.url())
}

/// Returns a lazy single-slot bus for `url`; configuration/provider validation
/// errors propagate without opening a Redis connection.
fn create_bus_url(url: &str) -> Result<Arc<dyn AsyncEventBusSpi>, Box<dyn Error>> {
    let options: ProviderOptions = [
        ("redis.url".into(), url.into()),
        ("redis.namespace".into(), "async-tests".into()),
        ("redis.claim_min_idle_ms".into(), "0".into()),
        ("redis.max_unsettled_per_subscription".into(), "1".into()),
    ]
    .into();
    let config = EventBusConfig::default().with_provider_options(options);
    block_on(AsyncRedisEventBusProvider.create_configured(&config))
        .map_err(|failure| failure.into_error())
        .map_err(Into::into)
}

/// Builds an encoded event from `topic`, `id`, and `payload` without I/O;
/// returns metadata validation errors for rejected topic, ID, or content type.
fn message(topic: &str, id: &str, payload: &[u8]) -> Result<OutboundMessage, Box<dyn Error>> {
    Ok(OutboundMessage::new(
        TopicAddress::new(topic)?,
        EventId::new(id)?,
        SystemTime::now(),
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(payload.to_vec()),
            ContentType::new("application/octet-stream")?,
            None,
        )),
    ))
}

/// Builds a durable earliest-position request for `topic` and `subscriber`;
/// returns identifier validation errors without network I/O.
fn request(topic: &str, subscriber: &str) -> Result<SpiSubscriptionRequest, Box<dyn Error>> {
    request_at(topic, subscriber, StartPosition::Earliest)
}

/// Builds a durable request with a caller-selected start position.
fn request_at(
    topic: &str,
    subscriber: &str,
    start_position: StartPosition,
) -> Result<SpiSubscriptionRequest, Box<dyn Error>> {
    Ok(SpiSubscriptionRequest::new(
        Id::new(SUBSCRIPTION_IDS.fetch_add(1, Ordering::Relaxed)),
        TopicAddress::new(topic)?,
        SubscriberId::new(subscriber)?,
        Some(ConsumerGroup::new("workers")?),
        SubscriptionDurability::Durable,
        start_position,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    ))
}

/// Publishes and receives through `bus` on the host executor, verifies bytes
/// and settlement idempotence, then closes; returns metadata/SPI errors or a
/// missing-delivery/token diagnostic. Assertions fail on a broken contract.
async fn verify_message(bus: Arc<dyn AsyncEventBusSpi>) -> Result<(), Box<dyn Error>> {
    let _ = bus
        .publish(message("async-events", "async-1", &[0, 11, 128, 255])?)
        .await?;
    let mut receiver = bus.subscribe(request("async-events", "worker-a")?).await?;
    let ReceiveOutcome::Message(mut received) = receiver.receive(Duration::from_secs(2)).await?
    else {
        return Err("published record was not received".into());
    };
    let TransportPayload::Encoded(payload) = received.payload() else {
        return Err("encoded payload expected".into());
    };
    assert_eq!(payload.bytes(), &[0, 11, 128, 255]);
    let token = received
        .take_settlement()
        .ok_or("missing settlement token")?;
    receiver.settle(&token, DeliveryDisposition::Accept).await?;
    receiver.settle(&token, DeliveryDisposition::Accept).await?;
    assert!(
        receiver
            .settle(&token, DeliveryDisposition::Reject)
            .await
            .is_err()
    );
    receiver.close().await?;
    Ok(())
}

#[test]
fn test_async_spi_runs_on_smol_executor() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    block_on(verify_message(bus))
}

#[test]
#[cfg(feature = "conformance")]
fn test_async_spi_conformance() -> Result<(), Box<dyn Error>> {
    let server = Arc::new(RedisServer::start()?);
    let settlement_server = Arc::clone(&server);
    let settlement: AsyncConformanceCheck = Arc::new(move || {
        let server = Arc::clone(&settlement_server);
        Box::pin(async move { check_async_settlement(&server).await })
    });
    let cancellation_server = Arc::clone(&server);
    let receive_cancellation: AsyncConformanceCheck = Arc::new(move || {
        let server = Arc::clone(&cancellation_server);
        Box::pin(async move { check_async_receive_cancellation(&server).await })
    });
    let recovery_server = Arc::clone(&server);
    let durable_recovery: AsyncConformanceCheck = Arc::new(move || {
        let server = Arc::clone(&recovery_server);
        Box::pin(async move { check_async_durable_recovery(&server).await })
    });
    let settlement_cancel_server = Arc::clone(&server);
    let settlement_cancellation: AsyncConformanceCheck = Arc::new(move || {
        let server = Arc::clone(&settlement_cancel_server);
        Box::pin(async move { check_async_settlement_cancellation(&server).await })
    });
    let close_cancel_server = Arc::clone(&server);
    let close_cancellation: AsyncConformanceCheck = Arc::new(move || {
        let server = Arc::clone(&close_cancel_server);
        Box::pin(async move { check_async_close_cancellation(&server).await })
    });
    let shutdown_cancel_server = Arc::clone(&server);
    let shutdown_cancellation: AsyncConformanceCheck = Arc::new(move || {
        let server = Arc::clone(&shutdown_cancel_server);
        Box::pin(async move { check_async_shutdown_cancellation(&server).await })
    });
    let hooks = AsyncConformanceHooks {
        settlement: Some(settlement),
        receive_cancellation: Some(receive_cancellation),
        settlement_cancellation: Some(settlement_cancellation),
        close_cancellation: Some(close_cancellation),
        shutdown_cancellation: Some(shutdown_cancellation),
        durable_recovery: Some(durable_recovery),
        ..Default::default()
    };
    let report = block_on(run_async_with_profile(
        || {
            let server = Arc::clone(&server);
            async move {
                let options: ProviderOptions = [
                    ("redis.url".into(), server.url().into()),
                    ("redis.namespace".into(), "async-conformance".into()),
                    ("redis.claim_min_idle_ms".into(), "0".into()),
                ]
                .into();
                let config = EventBusConfig::default().with_provider_options(options);
                AsyncRedisEventBusProvider
                    .create_configured(&config)
                    .await
                    .expect("Redis provider should be created")
            }
        },
        &hooks,
        ConformanceProfile::Strict,
    ));
    report.assert_all_passed();
    Ok(())
}

/// Checks repeated Accept and conflicting disposition rejection on `server`
/// through Redis I/O. Returns success or a setup, transport, or contract
/// failure description.
#[cfg(feature = "conformance")]
async fn check_async_settlement(server: &RedisServer) -> Result<(), String> {
    let bus = create_bus(server).map_err(|error| error.to_string())?;
    let _ = bus
        .publish(
            message("conformance-settlement", "settlement", b"payload")
                .map_err(|error| error.to_string())?,
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut receiver = bus
        .subscribe(
            request("conformance-settlement", "settlement-worker")
                .map_err(|error| error.to_string())?,
        )
        .await
        .map_err(|error| error.to_string())?;
    let ReceiveOutcome::Message(mut received) = receiver
        .receive(Duration::from_secs(2))
        .await
        .map_err(|error| error.to_string())?
    else {
        return Err("settlement fixture did not receive the published event".into());
    };
    let token = received
        .take_settlement()
        .ok_or("settlement token is missing")?;
    receiver
        .settle(&token, DeliveryDisposition::Accept)
        .await
        .map_err(|error| error.to_string())?;
    receiver
        .settle(&token, DeliveryDisposition::Accept)
        .await
        .map_err(|error| error.to_string())?;
    if receiver
        .settle(&token, DeliveryDisposition::Reject)
        .await
        .is_ok()
    {
        return Err("conflicting settlement unexpectedly succeeded".into());
    }
    receiver.close().await.map_err(|error| error.to_string())
}

/// Cancels the settlement future after Redis applies XACK on `server` while
/// the proxy gate holds its reply, then checks conflicting dispositions are
/// rejected and the same Accept can be retried through Redis I/O. Returns
/// success or a setup, transport, or contract failure description.
#[cfg(feature = "conformance")]
async fn check_async_settlement_cancellation(server: &RedisServer) -> Result<(), String> {
    let proxy = ControlledRedis::start(server.url()).map_err(|error| error.to_string())?;
    let bus = create_bus_url(&proxy.url()).map_err(|error| error.to_string())?;
    let _ = bus
        .publish(
            message("conformance-settle-cancel", "settle-cancel", b"payload")
                .map_err(|error| error.to_string())?,
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut receiver = bus
        .subscribe(
            request("conformance-settle-cancel", "settle-cancel")
                .map_err(|error| error.to_string())?,
        )
        .await
        .map_err(|error| error.to_string())?;
    let ReceiveOutcome::Message(mut received) = receiver
        .receive(Duration::from_secs(2))
        .await
        .map_err(|error| error.to_string())?
    else {
        return Err("settlement cancellation fixture did not receive its message".into());
    };
    let token = received
        .take_settlement()
        .ok_or("settlement token is missing")?;
    drop(received);
    let gate = proxy.pause_after_reply("XACK");
    // Build the Send SPI future before awaiting: SettlementToken is Send,
    // but its opaque payload is deliberately not required to be Sync.
    let settlement = receiver.settle(&token, DeliveryDisposition::Accept);
    let result = race(async move { Some(settlement.await) }, async {
        gate.wait_applied().await;
        None
    })
    .await;
    // race drops the still-pending settlement future while the proxy holds
    // Redis's applied reply. This is an in-flight cancellation boundary.
    gate.release();
    if result.is_some() {
        return Err("settlement completed before the applied-response cancellation gate".into());
    }
    let mut observer = Client::open(server.url())
        .map_err(|error| error.to_string())?
        .get_connection()
        .map_err(|error| error.to_string())?;
    let pending: StreamPendingCountReply = cmd("XPENDING")
        .arg(stream_key("async-tests", "conformance-settle-cancel"))
        .arg(group_name(
            "async-tests",
            "conformance-settle-cancel",
            "settle-cancel",
            Some("workers"),
        ))
        .arg("-")
        .arg("+")
        .arg(10)
        .query(&mut observer)
        .map_err(|error| error.to_string())?;
    if !pending.ids.is_empty() {
        return Err("XACK must have taken effect before cancelling its pending future".into());
    }
    if receiver
        .settle(&token, DeliveryDisposition::Retry)
        .await
        .is_ok()
        || receiver
            .settle(&token, DeliveryDisposition::Reject)
            .await
            .is_ok()
    {
        return Err(
            "conflicting settlement succeeded while the cancelled Accept outcome was unknown"
                .into(),
        );
    }
    receiver
        .settle(&token, DeliveryDisposition::Accept)
        .await
        .map_err(|error| format!("repeating the applied settlement failed: {error}"))?;
    receiver
        .settle(&token, DeliveryDisposition::Accept)
        .await
        .map_err(|error| error.to_string())?;
    receiver.close().await.map_err(|error| error.to_string())?;
    let _ = bus
        .shutdown(ShutdownMode::Immediate)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// Checks close cancellation boundaries and receiver state on `server` through
/// Redis I/O. Returns success or a setup, transport, or contract failure
/// description.
#[cfg(feature = "conformance")]
async fn check_async_close_cancellation(server: &RedisServer) -> Result<(), String> {
    let bus = create_bus(server).map_err(|error| error.to_string())?;
    let mut receiver = bus
        .subscribe(
            request("conformance-close-cancel", "close-cancel")
                .map_err(|error| error.to_string())?,
        )
        .await
        .map_err(|error| error.to_string())?;
    // close has no suspension point: cancellation can happen before its
    // first poll; once polled, its state transition completes atomically.
    drop(receiver.close());
    if matches!(
        receiver.receive(Duration::ZERO).await,
        Ok(ReceiveOutcome::Closed)
    ) {
        return Err("dropping an unpolled close future closed the receiver".into());
    }
    let result = poll_once(receiver.close())
        .await
        .ok_or("close unexpectedly suspended instead of completing on its first poll")?;
    result.map_err(|error| format!("polled close failed: {error}"))?;
    if !matches!(
        receiver.receive(Duration::ZERO).await,
        Ok(ReceiveOutcome::Closed)
    ) {
        return Err("receiver was not closed after cancellation".into());
    }
    receiver.close().await.map_err(|error| error.to_string())?;
    let _ = bus
        .shutdown(ShutdownMode::Immediate)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// Checks shutdown cancellation boundaries and repeated completion on `server`
/// through Redis I/O. Returns success or a setup, transport, or contract
/// failure description.
#[cfg(feature = "conformance")]
async fn check_async_shutdown_cancellation(server: &RedisServer) -> Result<(), String> {
    let bus = create_bus(server).map_err(|error| error.to_string())?;
    // shutdown also completes in its first poll; do not manufacture a
    // Pending operation after it has already returned Complete.
    drop(bus.shutdown(ShutdownMode::Immediate));
    let _ = bus
        .publish(
            message("conformance-shutdown-cancel", "unpolled-shutdown", b"open")
                .map_err(|error| error.to_string())?,
        )
        .await
        .map_err(|error| error.to_string())?;
    let result = poll_once(bus.shutdown(ShutdownMode::Immediate))
        .await
        .ok_or("shutdown unexpectedly suspended instead of completing on its first poll")?;
    if !matches!(result, Ok(ShutdownOutcome::Complete)) {
        return Err("polled shutdown did not complete".into());
    }
    match bus.shutdown(ShutdownMode::Immediate).await {
        Ok(ShutdownOutcome::Complete) => Ok(()),
        Ok(outcome) => Err(format!("repeated shutdown returned {outcome:?}")),
        Err(error) => Err(format!("repeated shutdown failed: {error}")),
    }
}

/// Checks receiver usability after bounded receive cancellation on `server`
/// through Redis I/O. Returns success or a setup, transport, or contract
/// failure description.
#[cfg(feature = "conformance")]
async fn check_async_receive_cancellation(server: &RedisServer) -> Result<(), String> {
    let bus = create_bus(server).map_err(|error| error.to_string())?;
    let mut receiver = bus
        .subscribe(
            request("conformance-cancellation", "cancellation-worker")
                .map_err(|error| error.to_string())?,
        )
        .await
        .map_err(|error| error.to_string())?;
    if poll_once(receiver.receive(Duration::MAX)).await.is_some() {
        return Err("empty receive unexpectedly completed before cancellation".into());
    }
    let _ = bus
        .publish(
            message("conformance-cancellation", "after-cancel", b"payload")
                .map_err(|error| error.to_string())?,
        )
        .await
        .map_err(|error| error.to_string())?;
    let ReceiveOutcome::Message(received) = receiver
        .receive(Duration::from_secs(2))
        .await
        .map_err(|error| error.to_string())?
    else {
        return Err("subscription did not continue after receive cancellation".into());
    };
    receiver
        .settle(
            received
                .settlement()
                .ok_or("cancelled receive lost its settlement token")?,
            DeliveryDisposition::Accept,
        )
        .await
        .map_err(|error| error.to_string())?;
    receiver.close().await.map_err(|error| error.to_string())
}

/// Checks an unsettled durable event survives receiver replacement on `server`
/// through Redis I/O. Returns success or a setup, transport, or contract
/// failure description.
#[cfg(feature = "conformance")]
async fn check_async_durable_recovery(server: &RedisServer) -> Result<(), String> {
    let bus = create_bus(server).map_err(|error| error.to_string())?;
    let _ = bus
        .publish(
            message("conformance-recovery", "recovery", b"pending")
                .map_err(|error| error.to_string())?,
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut receiver = bus
        .subscribe(
            request("conformance-recovery", "recovery-before-close")
                .map_err(|error| error.to_string())?,
        )
        .await
        .map_err(|error| error.to_string())?;
    let ReceiveOutcome::Message(received) = receiver
        .receive(Duration::from_secs(2))
        .await
        .map_err(|error| error.to_string())?
    else {
        return Err("recovery fixture did not receive the published event".into());
    };
    if received.id().as_str() != "recovery" {
        return Err("recovery fixture received an unexpected event".into());
    }
    receiver.close().await.map_err(|error| error.to_string())?;
    let _ = bus
        .shutdown(ShutdownMode::Immediate)
        .await
        .map_err(|error| error.to_string())?;

    let recovered_bus = create_bus(server).map_err(|error| error.to_string())?;
    let mut recovered = recovered_bus
        .subscribe(
            request("conformance-recovery", "recovery-after-close")
                .map_err(|error| error.to_string())?,
        )
        .await
        .map_err(|error| error.to_string())?;
    let ReceiveOutcome::Message(received) = recovered
        .receive(Duration::from_secs(2))
        .await
        .map_err(|error| error.to_string())?
    else {
        return Err("unsettled durable event was not recovered".into());
    };
    if received.id().as_str() != "recovery" {
        return Err("recovery fixture returned an unexpected event".into());
    }
    recovered
        .settle(
            received
                .settlement()
                .ok_or("recovered settlement token is missing")?,
            DeliveryDisposition::Accept,
        )
        .await
        .map_err(|error| error.to_string())?;
    if !matches!(
        recovered.receive(Duration::ZERO).await,
        Ok(ReceiveOutcome::TimedOut)
    ) {
        return Err("accepted delivery was unexpectedly recovered again".into());
    }
    recovered.close().await.map_err(|error| error.to_string())?;
    let _ = recovered_bus
        .shutdown(ShutdownMode::Immediate)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

#[tokio::test]
async fn test_async_spi_runs_on_tokio_executor() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    verify_message(bus).await
}

#[test]
fn test_async_receive_cancellation_leaves_pending_message_recoverable() -> Result<(), Box<dyn Error>>
{
    let server = RedisServer::start()?;
    let proxy = ControlledRedis::start(server.url())?;
    let gate = proxy.gate();
    let bus = create_bus_url(&proxy.url())?;
    block_on(async {
        let _ = bus
            .publish(message("async-events", "cancel-1", b"recover")?)
            .await?;
        let mut receiver = bus.subscribe(request("async-events", "worker-c")?).await?;
        let cancel = Arc::new(CancelReceive::default());
        let worker_cancel = Arc::clone(&cancel);
        let (result_tx, result_rx) = channel();
        gate.arm();
        scope(|scope| {
            scope.spawn(|| {
                let result = block_on(race(
                    async { Some(receiver.receive(Duration::from_secs(2)).await) },
                    WaitForCancel(Arc::clone(&worker_cancel)),
                ));
                let _ = result_tx.send(result);
            });
            let reached = gate.wait_until_reached(Duration::from_secs(3));
            cancel.cancel();
            let result = result_rx.recv_timeout(Duration::from_secs(2))?;
            assert!(result.is_none(), "receive future should be cancelled");
            gate.release();
            if !reached {
                return Err("XREADGROUP response gate was not reached".into());
            }
            let mut connection = Client::open(server.url())?.get_connection()?;
            let pending: Vec<Value> = cmd("XPENDING")
                .arg(stream_key("async-tests", "async-events"))
                .arg(group_name(
                    "async-tests",
                    "async-events",
                    "worker-c",
                    Some("workers"),
                ))
                .arg("-")
                .arg("+")
                .arg(10)
                .query(&mut connection)?;
            assert_eq!(
                pending.len(),
                1,
                "Redis must have applied XREADGROUP before cancellation"
            );
            Ok::<(), Box<dyn Error>>(())
        })?;
        let ReceiveOutcome::Message(received) = receiver.receive(Duration::from_secs(2)).await?
        else {
            return Err("pending record was not recovered".into());
        };
        let TransportPayload::Encoded(payload) = received.payload() else {
            return Err("encoded payload expected".into());
        };
        assert_eq!(
            received.id().as_str(),
            "cancel-1",
            "cancelled receive must recover the same event"
        );
        assert_eq!(payload.bytes(), b"recover");
        receiver
            .settle(
                received.settlement().ok_or("missing settlement token")?,
                DeliveryDisposition::Accept,
            )
            .await?;
        let mut observer = Client::open(server.url())?.get_connection()?;
        let pending: StreamPendingCountReply = cmd("XPENDING")
            .arg(stream_key("async-tests", "async-events"))
            .arg(group_name(
                "async-tests",
                "async-events",
                "worker-c",
                Some("workers"),
            ))
            .arg("-")
            .arg("+")
            .arg(10)
            .query(&mut observer)?;
        assert!(
            pending.ids.is_empty(),
            "recovered delivery's valid token must ACK its PEL entry"
        );
        receiver.close().await?;
        assert!(matches!(
            receiver.receive(Duration::ZERO).await?,
            ReceiveOutcome::Closed
        ));
        receiver.close().await?;
        Ok(())
    })
}

#[test]
fn test_async_settlement_cancellation_after_xack_is_idempotent() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let proxy = ControlledRedis::start(server.url())?;
    let bus = create_bus_url(&proxy.url())?;
    block_on(async {
        let _ = bus
            .publish(message("async-settle-cancel", "cancel-ack", b"settle")?)
            .await?;
        let mut receiver = bus
            .subscribe(request("async-settle-cancel", "worker-ack-cancel")?)
            .await?;
        let ReceiveOutcome::Message(received) = receiver.receive(Duration::from_secs(2)).await?
        else {
            return Err("published event was not received".into());
        };
        if received.settlement().is_none() {
            return Err("missing settlement token".into());
        }
        let gate = proxy.pause_after_reply("XACK");
        let cancel = Arc::new(CancelReceive::default());
        let worker_cancel = Arc::clone(&cancel);
        let (result_tx, result_rx) = channel();
        let (returned_receiver, returned_message) = scope(|scope| {
            scope.spawn(move || {
                let result = block_on(race(
                    async {
                        Some(
                            receiver
                                .settle(
                                    received.settlement().expect("settlement token is present"),
                                    DeliveryDisposition::Accept,
                                )
                                .await,
                        )
                    },
                    WaitForSettleCancel(Arc::clone(&worker_cancel)),
                ));
                let _ = result_tx.send((result, receiver, received));
            });
            let reached = gate.wait_until_reached(Duration::from_secs(3));
            cancel.cancel();
            let (result, returned_receiver, returned_message) =
                result_rx.recv_timeout(Duration::from_secs(2))?;
            assert!(
                result.is_none(),
                "settlement future should be cancelled after XACK"
            );
            gate.release();
            if !reached {
                return Err("XACK response gate was not reached".into());
            }
            let mut observer = Client::open(server.url())?.get_connection()?;
            let pending: Vec<Value> = cmd("XPENDING")
                .arg(stream_key("async-tests", "async-settle-cancel"))
                .arg(group_name(
                    "async-tests",
                    "async-settle-cancel",
                    "worker-ack-cancel",
                    Some("workers"),
                ))
                .arg("-")
                .arg("+")
                .arg(10)
                .query(&mut observer)?;
            assert!(pending.is_empty(), "Redis applied XACK before cancellation");
            Ok::<_, Box<dyn Error>>((returned_receiver, returned_message))
        })?;
        let mut receiver = returned_receiver;
        let received = returned_message;
        receiver
            .settle(
                received.settlement().ok_or("missing settlement token")?,
                DeliveryDisposition::Accept,
            )
            .await?;
        assert!(
            receiver
                .settle(
                    received.settlement().ok_or("missing settlement token")?,
                    DeliveryDisposition::Reject,
                )
                .await
                .is_err()
        );
        receiver.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn test_async_claim_cancellation_after_owner_transfer_recovers_message()
-> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let setup_bus = create_bus(&server)?;
    block_on(async {
        let _ = setup_bus
            .publish(message("async-claim-cancel", "claim-cancel", b"claim")?)
            .await?;
        let mut old = setup_bus
            .subscribe(request("async-claim-cancel", "worker-before-claim")?)
            .await?;
        let ReceiveOutcome::Message(_pending) = old.receive(Duration::from_secs(2)).await? else {
            return Err("initial consumer did not create a pending delivery".into());
        };
        drop(old);
        let mut observer = Client::open(server.url())?.get_connection()?;
        let before_claim: Vec<Vec<Value>> = cmd("XPENDING")
            .arg(stream_key("async-tests", "async-claim-cancel"))
            .arg(group_name(
                "async-tests",
                "async-claim-cancel",
                "worker-before-claim",
                Some("workers"),
            ))
            .arg("-")
            .arg("+")
            .arg(10)
            .query(&mut observer)?;
        let old_owner = from_redis_value::<String>(&before_claim[0][1])?;

        let proxy = ControlledRedis::start(server.url())?;
        let gate = proxy.pause_after_reply("XAUTOCLAIM");
        let bus = create_bus_url(&proxy.url())?;
        let mut receiver = bus
            .subscribe(request_at(
                "async-claim-cancel",
                "worker-after-claim",
                StartPosition::New,
            )?)
            .await?;
        let cancel = Arc::new(CancelReceive::default());
        let worker_cancel = Arc::clone(&cancel);
        let (result_tx, result_rx) = channel();
        let (result, mut receiver) = scope(|scope| {
            scope.spawn(move || {
                let result = block_on(race(
                    async { Some(receiver.receive(Duration::from_secs(2)).await) },
                    WaitForCancel(Arc::clone(&worker_cancel)),
                ));
                let _ = result_tx.send((result, receiver));
            });
            let reached = gate.wait_until_reached(Duration::from_secs(3));
            cancel.cancel();
            let (result, receiver) = result_rx.recv_timeout(Duration::from_secs(2))?;
            gate.release();
            if !reached {
                return Err("XAUTOCLAIM response gate was not reached".into());
            }
            let mut observer = Client::open(server.url())?.get_connection()?;
            let pending: Vec<Vec<Value>> = cmd("XPENDING")
                .arg(stream_key("async-tests", "async-claim-cancel"))
                .arg(group_name(
                    "async-tests",
                    "async-claim-cancel",
                    "worker-after-claim",
                    Some("workers"),
                ))
                .arg("-")
                .arg("+")
                .arg(10)
                .query(&mut observer)?;
            assert_eq!(pending.len(), 1);
            let owner = from_redis_value::<String>(&pending[0][1])?;
            assert_ne!(
                owner, old_owner,
                "Redis applied ownership transfer before cancellation"
            );
            Ok::<_, Box<dyn Error>>((result, receiver))
        })?;
        assert!(
            result.is_none(),
            "receive future should be cancelled after claim"
        );
        let ReceiveOutcome::Message(received) = receiver.receive(Duration::from_secs(2)).await?
        else {
            return Err("claimed pending record was not recovered".into());
        };
        assert_eq!(
            received.id().as_str(),
            "claim-cancel",
            "cancelled claim must recover the same event"
        );
        receiver
            .settle(
                received.settlement().ok_or("missing settlement token")?,
                DeliveryDisposition::Accept,
            )
            .await?;
        let mut observer = Client::open(server.url())?.get_connection()?;
        let pending: StreamPendingCountReply = cmd("XPENDING")
            .arg(stream_key("async-tests", "async-claim-cancel"))
            .arg(group_name(
                "async-tests",
                "async-claim-cancel",
                "worker-after-claim",
                Some("workers"),
            ))
            .arg("-")
            .arg("+")
            .arg(10)
            .query(&mut observer)?;
        assert!(
            pending.ids.is_empty(),
            "recovered delivery's valid token must ACK its PEL entry"
        );
        receiver.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[derive(Default)]
struct CancelReceive {
    cancelled: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl CancelReceive {
    /// Marks this signal cancelled and wakes its waiter; panics if the local
    /// waker mutex is poisoned, without issuing network operations.
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        if let Some(waker) = self
            .waker
            .lock()
            .expect("cancel waker lock is healthy")
            .take()
        {
            waker.wake();
        }
    }
}

struct WaitForCancel(Arc<CancelReceive>);

impl Future for WaitForCancel {
    type Output = Option<Result<ReceiveOutcome, SpiError>>;

    /// Reads this signal and registers `context` while pending; returns
    /// cancellation as None and panics only if the local mutex is poisoned.
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0.cancelled.load(Ordering::SeqCst) {
            Poll::Ready(None)
        } else {
            *self.0.waker.lock().expect("cancel waker lock is healthy") =
                Some(context.waker().clone());
            if self.0.cancelled.load(Ordering::SeqCst) {
                Poll::Ready(None)
            } else {
                Poll::Pending
            }
        }
    }
}

struct WaitForSettleCancel(Arc<CancelReceive>);

impl Future for WaitForSettleCancel {
    type Output = Option<Result<(), SpiError>>;

    /// Reads this signal and registers `context` while pending; returns
    /// cancellation as None and panics only if the local mutex is poisoned.
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0.cancelled.load(Ordering::SeqCst) {
            Poll::Ready(None)
        } else {
            *self.0.waker.lock().expect("cancel waker lock is healthy") =
                Some(context.waker().clone());
            if self.0.cancelled.load(Ordering::SeqCst) {
                Poll::Ready(None)
            } else {
                Poll::Pending
            }
        }
    }
}

/// Cancels a local signal exactly while its poll clones the context waker.
struct CancelOnWakerClone {
    signal: Weak<CancelReceive>,
    clone_entered: AtomicBool,
    wake_count: AtomicU64,
}

impl CancelOnWakerClone {
    /// Builds a waker retaining `self`; its clone callback cancels the signal.
    ///
    /// Returns an owned waker whose callbacks balance each raw Arc reference.
    fn into_waker(self: Arc<Self>) -> Waker {
        let raw = RawWaker::new(Arc::into_raw(self).cast(), &Self::VTABLE);
        // SAFETY: the vtable maintains the live Arc reference represented by raw.
        unsafe { Waker::from_raw(raw) }
    }

    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        Self::clone_raw,
        Self::wake_raw,
        Self::wake_by_ref_raw,
        Self::drop_raw,
    );

    /// Cancels while cloning `data`, then retains one reference for the clone.
    ///
    /// Requires a live Arc pointer supplied by this type's waker. Cancellation
    /// may briefly lock its mutex and panic if that mutex is poisoned.
    unsafe fn clone_raw(data: *const ()) -> RawWaker {
        let pointer = data.cast::<Self>();
        // SAFETY: each live raw waker owns an Arc reference to this allocation.
        let state = unsafe { &*pointer };
        state.clone_entered.store(true, Ordering::SeqCst);
        state
            .signal
            .upgrade()
            .expect("test signal remains live while polling")
            .cancel();
        // SAFETY: the original raw waker's Arc reference remains live.
        unsafe { Arc::increment_strong_count(pointer) };
        RawWaker::new(data, &Self::VTABLE)
    }

    /// Records a wake and consumes the owned Arc reference at `data`.
    ///
    /// Requires exactly one live Arc reference from this type's raw waker.
    unsafe fn wake_raw(data: *const ()) {
        // SAFETY: wake consumes the reference transferred by into_raw or clone.
        let state = unsafe { Arc::from_raw(data.cast::<Self>()) };
        state.wake_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Records a wake without consuming the reference represented by `data`.
    ///
    /// Requires a live Arc pointer from this type's raw waker.
    unsafe fn wake_by_ref_raw(data: *const ()) {
        // SAFETY: wake_by_ref preserves the caller's live Arc reference.
        let state = unsafe { &*data.cast::<Self>() };
        state.wake_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Drops the single owned Arc reference represented by `data`.
    ///
    /// Requires exactly one live Arc reference from this type's raw waker.
    unsafe fn drop_raw(data: *const ()) {
        // SAFETY: drop consumes the reference transferred by into_raw or clone.
        drop(unsafe { Arc::from_raw(data.cast::<Self>()) });
    }
}

/// Polls `wait` once with a waker that cancels `signal` before registration.
///
/// Requires a fresh signal with no registered waker. Asserts synchronous
/// completion after the forced race and verifies no wake could rescue Pending.
/// Performs no network I/O, thread spawning, or timing waits.
fn assert_cancel_during_waker_registration<T>(
    signal: Arc<CancelReceive>,
    mut wait: impl Future<Output = Option<Result<T, SpiError>>> + Unpin,
) {
    let state = Arc::new(CancelOnWakerClone {
        signal: Arc::downgrade(&signal),
        clone_entered: AtomicBool::new(false),
        wake_count: AtomicU64::new(0),
    });
    let waker = Arc::clone(&state).into_waker();
    let mut context = Context::from_waker(&waker);
    let result = Pin::new(&mut wait).poll(&mut context);
    assert!(state.clone_entered.load(Ordering::SeqCst));
    assert!(signal.cancelled.load(Ordering::SeqCst));
    assert_eq!(state.wake_count.load(Ordering::SeqCst), 0);
    assert!(
        matches!(result, Poll::Ready(None)),
        "cancellation during waker registration must complete the current poll"
    );
}

#[test]
fn test_receive_cancel_during_waker_registration_completes() {
    let signal = Arc::new(CancelReceive::default());
    let wait = WaitForCancel(Arc::clone(&signal));
    assert_cancel_during_waker_registration(signal, wait);
}

#[test]
fn test_settle_cancel_during_waker_registration_completes() {
    let signal = Arc::new(CancelReceive::default());
    let wait = WaitForSettleCancel(Arc::clone(&signal));
    assert_cancel_during_waker_registration(signal, wait);
}

#[test]
fn test_async_existing_consumer_group_is_not_retried() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    block_on(async {
        let _ = bus.publish(message("busy-group", "busy-a", b"A")?).await?;
        let mut first = bus.subscribe(request("busy-group", "worker-a")?).await?;
        let ReceiveOutcome::Message(first_message) = first.receive(Duration::from_secs(2)).await?
        else {
            return Err("first group consumer did not receive A".into());
        };
        assert_eq!(first_message.id().as_str(), "busy-a");
        first
            .settle(
                first_message
                    .settlement()
                    .ok_or("missing settlement token for A")?,
                DeliveryDisposition::Accept,
            )
            .await?;
        first.close().await?;

        let _ = bus.publish(message("busy-group", "busy-b", b"B")?).await?;
        let before_second = xgroup_command_calls(&server)?;
        let second = bus.subscribe(request("busy-group", "worker-b")?).await;
        let error = match second {
            Ok(_) => return Err("existing group unexpectedly accepted Earliest".into()),
            Err(error) => error,
        };
        assert_eq!(error.kind(), "existing_group_start_position_ignored");
        assert_eq!(error.retryable(), Some(false));
        let after_second = xgroup_command_calls(&server)?;
        assert_eq!(
            after_second - before_second,
            1,
            "BUSYGROUP must not trigger a repeated XGROUP CREATE"
        );

        let before_resume = xgroup_command_calls(&server)?;
        let resume_options =
            ProviderOptions::from([("redis.existing_group_start".into(), "resume".into())]);
        let mut resumed = bus
            .subscribe(SpiSubscriptionRequest::new(
                Id::new(SUBSCRIPTION_IDS.fetch_add(1, Ordering::Relaxed)),
                TopicAddress::new("busy-group")?,
                SubscriberId::new("worker-c")?,
                Some(ConsumerGroup::new("workers")?),
                SubscriptionDurability::Durable,
                StartPosition::Earliest,
                resume_options,
                TypeId::of::<Vec<u8>>(),
            ))
            .await?;
        assert_eq!(xgroup_command_calls(&server)? - before_resume, 1);
        let ReceiveOutcome::Message(second_message) =
            resumed.receive(Duration::from_secs(2)).await?
        else {
            return Err("resumed group did not receive B".into());
        };
        assert_eq!(second_message.id().as_str(), "busy-b");
        resumed
            .settle(
                second_message
                    .settlement()
                    .ok_or("missing settlement token for B")?,
                DeliveryDisposition::Accept,
            )
            .await?;
        assert!(matches!(
            resumed.receive(Duration::ZERO).await?,
            ReceiveOutcome::TimedOut
        ));
        resumed.close().await?;

        let before_new = xgroup_command_calls(&server)?;
        let mut newest = bus
            .subscribe(SpiSubscriptionRequest::new(
                Id::new(SUBSCRIPTION_IDS.fetch_add(1, Ordering::Relaxed)),
                TopicAddress::new("busy-group")?,
                SubscriberId::new("worker-d")?,
                Some(ConsumerGroup::new("workers")?),
                SubscriptionDurability::Durable,
                StartPosition::New,
                ProviderOptions::new(),
                TypeId::of::<Vec<u8>>(),
            ))
            .await?;
        assert_eq!(xgroup_command_calls(&server)? - before_new, 1);
        assert!(matches!(
            newest.receive(Duration::ZERO).await?,
            ReceiveOutcome::TimedOut
        ));
        let _ = bus.publish(message("busy-group", "busy-c", b"C")?).await?;
        let ReceiveOutcome::Message(third_message) = newest.receive(Duration::from_secs(2)).await?
        else {
            return Err("New resume did not receive C".into());
        };
        assert_eq!(third_message.id().as_str(), "busy-c");
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn test_async_cancelled_subscribe_releases_receiver_permit() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let proxy = ControlledRedis::start(server.url())?;
    let options = ProviderOptions::from([
        ("redis.url".into(), proxy.url()),
        ("redis.namespace".into(), "async-cancel-subscribe".into()),
        ("redis.max_active_receivers".into(), "1".into()),
    ]);
    let bus = block_on(
        AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(options)),
    )
    .map_err(|failure| failure.into_error())?;
    let gate = proxy.pause_after_reply("XGROUP");
    let cancel = Arc::new(CancelReceive::default());
    let (result_tx, result_rx) = channel();
    scope(|scope| -> Result<(), Box<dyn Error>> {
        let worker_bus = Arc::clone(&bus);
        let worker_cancel = Arc::clone(&cancel);
        scope.spawn(move || {
            let result = block_on(race(
                async {
                    Some(
                        worker_bus
                            .subscribe(group_request("subscribe-cancel", "worker-a", "workers"))
                            .await,
                    )
                },
                async {
                    WaitForCancel(worker_cancel).await;
                    None
                },
            ));
            let _ = result_tx.send(result.is_none());
        });
        let reached = gate.wait_until_reached(Duration::from_secs(3));
        cancel.cancel();
        let cancelled = result_rx.recv_timeout(Duration::from_secs(2))?;
        gate.release();
        assert!(reached, "XGROUP reply gate was not reached");
        assert!(cancelled, "subscribe future was not cancelled");
        Ok(())
    })?;
    let mut resumed = block_on(bus.subscribe(group_request_at(
        "subscribe-cancel",
        "worker-b",
        "workers",
        StartPosition::New,
    )))?;
    block_on(resumed.close())?;
    Ok(())
}

/// Reads the XGROUP call counter from `server` with blocking Redis I/O;
/// returns transport, missing-statistic, and malformed-counter errors.
fn xgroup_command_calls(server: &RedisServer) -> Result<u64, Box<dyn Error>> {
    let client = Client::open(server.url())?;
    let mut connection = client.get_connection()?;
    let info: String = cmd("INFO").arg("commandstats").query(&mut connection)?;
    let line = info
        .lines()
        .find(|line| line.starts_with("cmdstat_xgroup"))
        .ok_or_else(|| format!("missing XGROUP command statistics: {info}"))?;
    let calls = line
        .split("calls=")
        .nth(1)
        .ok_or("missing XGROUP calls count")?;
    Ok(calls
        .split(',')
        .next()
        .ok_or("empty XGROUP calls count")?
        .parse()?)
}

#[test]
fn test_async_unsettled_message_is_claimed_after_consumer_reconnect() -> Result<(), Box<dyn Error>>
{
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    block_on(async {
        let _ = bus
            .publish(message("recovery", "async-recovery", b"resume")?)
            .await?;
        let mut first = bus.subscribe(request("recovery", "worker-one")?).await?;
        let ReceiveOutcome::Message(_) = first.receive(Duration::from_secs(2)).await? else {
            return Err("initial consumer did not receive the event".into());
        };
        first.close().await?;
        let mut second = bus
            .subscribe(request_at("recovery", "worker-two", StartPosition::New)?)
            .await?;
        let ReceiveOutcome::Message(received) = second.receive(Duration::from_secs(2)).await?
        else {
            return Err("reconnected consumer did not claim the pending event".into());
        };
        let TransportPayload::Encoded(payload) = received.payload() else {
            return Err("encoded payload expected".into());
        };
        assert_eq!(payload.bytes(), b"resume");
        Ok(())
    })
}

#[test]
fn test_async_receiver_pauses_at_unsettled_limit() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    block_on(async {
        let _ = bus
            .publish(message("bounded", "async-bound-1", b"one")?)
            .await?;
        let _ = bus
            .publish(message("bounded", "async-bound-2", b"two")?)
            .await?;
        let mut receiver = bus.subscribe(request("bounded", "bounded-worker")?).await?;
        let ReceiveOutcome::Message(first) = receiver.receive(Duration::from_secs(2)).await? else {
            return Err("first message was not received".into());
        };
        assert!(matches!(
            receiver.receive(Duration::ZERO).await?,
            ReceiveOutcome::TimedOut
        ));
        receiver
            .settle(
                first.settlement().ok_or("missing settlement token")?,
                DeliveryDisposition::Accept,
            )
            .await?;
        let ReceiveOutcome::Message(second) = receiver.receive(Duration::from_secs(2)).await?
        else {
            return Err("reading should resume after settlement frees the slot".into());
        };
        let TransportPayload::Encoded(payload) = second.payload() else {
            return Err("encoded payload expected".into());
        };
        assert_eq!(payload.bytes(), b"two");
        Ok(())
    })
}

#[test]
fn test_async_groups_fan_out_and_share_work() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    block_on(async {
        let _ = bus
            .publish(message("async-groups", "async-group-event-1", b"one")?)
            .await?;
        let _ = bus
            .publish(message("async-groups", "async-group-event-2", b"two")?)
            .await?;
        let mut worker_a = bus
            .subscribe(group_request("async-groups", "worker-a", "billing"))
            .await?;
        let mut worker_b = bus
            .subscribe(group_request_at(
                "async-groups",
                "worker-b",
                "billing",
                StartPosition::New,
            ))
            .await?;
        let mut audit = bus
            .subscribe(group_request("async-groups", "audit", "audit"))
            .await?;
        let mut worker_ids = vec![];
        for receiver in [&mut worker_a, &mut worker_b] {
            let ReceiveOutcome::Message(message) = receiver.receive(Duration::from_secs(2)).await?
            else {
                return Err("billing group did not receive both events".into());
            };
            worker_ids.push(message.id().as_str().to_owned());
            receiver
                .settle(
                    message.settlement().ok_or("missing settlement token")?,
                    DeliveryDisposition::Accept,
                )
                .await?;
        }
        worker_ids.sort();
        assert_eq!(worker_ids, ["async-group-event-1", "async-group-event-2"]);
        let ReceiveOutcome::Message(first) = audit.receive(Duration::from_secs(2)).await? else {
            return Err("audit group did not receive the first event".into());
        };
        assert_eq!(first.id().as_str(), "async-group-event-1");
        audit
            .settle(
                first.settlement().ok_or("missing settlement token")?,
                DeliveryDisposition::Accept,
            )
            .await?;
        let ReceiveOutcome::Message(second) = audit.receive(Duration::from_secs(2)).await? else {
            return Err("audit group did not receive the second event".into());
        };
        assert_eq!(second.id().as_str(), "async-group-event-2");
        audit
            .settle(
                second.settlement().ok_or("missing settlement token")?,
                DeliveryDisposition::Accept,
            )
            .await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn test_async_replay_from_stream_position_and_new_tail() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    block_on(async {
        let first = bus
            .publish(message("async-positions", "async-position-1", b"one")?)
            .await?;
        let first_id = match first {
            PublishAcknowledgement::Accepted {
                provider_message_id: Some(id),
                ..
            } => id,
            _ => return Err("Redis publish did not return a stream ID".into()),
        };
        let _ = bus
            .publish(message("async-positions", "async-position-2", b"two")?)
            .await?;
        let mut at = bus
            .subscribe(SpiSubscriptionRequest::new(
                Id::new(SUBSCRIPTION_IDS.fetch_add(1, Ordering::Relaxed)),
                TopicAddress::new("async-positions")?,
                SubscriberId::new("at-position")?,
                Some(ConsumerGroup::new("position-group")?),
                SubscriptionDurability::Durable,
                StartPosition::At(first_id.into()),
                ProviderOptions::new(),
                TypeId::of::<Vec<u8>>(),
            ))
            .await?;
        let ReceiveOutcome::Message(second) = at.receive(Duration::from_secs(2)).await? else {
            return Err("consumer at stream position did not receive a later event".into());
        };
        assert_eq!(second.id().as_str(), "async-position-2");
        at.settle(
            second.settlement().ok_or("missing token")?,
            DeliveryDisposition::Accept,
        )
        .await?;
        assert!(matches!(
            at.receive(Duration::ZERO).await?,
            ReceiveOutcome::TimedOut
        ));

        let mut new = bus
            .subscribe(SpiSubscriptionRequest::new(
                Id::new(SUBSCRIPTION_IDS.fetch_add(1, Ordering::Relaxed)),
                TopicAddress::new("async-positions")?,
                SubscriberId::new("new-position")?,
                Some(ConsumerGroup::new("new-position-group")?),
                SubscriptionDurability::Durable,
                StartPosition::New,
                ProviderOptions::new(),
                TypeId::of::<Vec<u8>>(),
            ))
            .await?;
        let _ = bus
            .publish(message("async-positions", "async-position-3", b"three")?)
            .await?;
        let ReceiveOutcome::Message(third) = new.receive(Duration::from_secs(2)).await? else {
            return Err("New consumer did not receive a new event".into());
        };
        assert_eq!(third.id().as_str(), "async-position-3");
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn test_async_reports_gap_for_removed_pending_entries() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    block_on(async {
        let _ = bus
            .publish(message("async-gaps", "async-removed-event", b"payload")?)
            .await?;
        let mut first = bus
            .subscribe(group_request("async-gaps", "gap-worker-one", "gap-group"))
            .await?;
        let ReceiveOutcome::Message(_) = first.receive(Duration::from_secs(2)).await? else {
            return Err("pending gap fixture was not received".into());
        };
        first.close().await?;
        let mut connection = Client::open(server.url())?.get_connection()?;
        cmd("XTRIM")
            .arg(stream_key("async-tests", "async-gaps"))
            .arg("MAXLEN")
            .arg(0)
            .query::<usize>(&mut connection)?;
        let mut second = bus
            .subscribe(group_request_at(
                "async-gaps",
                "gap-worker-two",
                "gap-group",
                StartPosition::New,
            ))
            .await?;
        let outcome = second.receive(Duration::from_secs(1)).await?;
        assert!(matches!(outcome, ReceiveOutcome::Gap(_)));
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn test_async_reject_acks_and_malformed_wire_is_quarantined() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    block_on(async {
        let _ = bus
            .publish(message("async-malformed", "async-reject-event", b"reject")?)
            .await?;
        let mut receiver = bus
            .subscribe(group_request(
                "async-malformed",
                "reject-worker",
                "reject-group",
            ))
            .await?;
        let ReceiveOutcome::Message(rejected) = receiver.receive(Duration::from_secs(2)).await?
        else {
            return Err("reject test message was not received".into());
        };
        receiver
            .settle(
                rejected.settlement().ok_or("missing settlement token")?,
                DeliveryDisposition::Reject,
            )
            .await?;
        let mut connection = Client::open(server.url())?.get_connection()?;
        cmd("XADD")
            .arg(stream_key("async-tests", "async-malformed"))
            .arg("*")
            .arg("other")
            .arg("value")
            .query::<String>(&mut connection)?;
        assert!(matches!(
            receiver.receive(Duration::from_secs(1)).await?,
            ReceiveOutcome::Gap(_)
        ));
        let stream = stream_key("async-tests", "async-malformed");
        for wire in [
            "not-json",
            r#"{"version":1,"event_id":"event","timestamp_ms":0,"headers_json":"{","ordering_key":null,"content_type":"application/octet-stream","schema_id":null,"payload":[]}"#,
            r#"{"version":1,"event_id":"","timestamp_ms":0,"headers_json":"{}","ordering_key":null,"content_type":"application/octet-stream","schema_id":null,"payload":[]}"#,
        ] {
            cmd("XADD")
                .arg(&stream)
                .arg("*")
                .arg("wire")
                .arg(wire)
                .query::<String>(&mut connection)?;
            assert!(matches!(
                receiver.receive(Duration::from_secs(1)).await?,
                ReceiveOutcome::Gap(_)
            ));
        }
        cmd("XADD")
            .arg(&stream)
            .arg("*")
            .arg("wire")
            .arg(vec![0xff_u8])
            .query::<String>(&mut connection)?;
        assert!(matches!(
            receiver.receive(Duration::from_secs(1)).await?,
            ReceiveOutcome::Gap(_)
        ));
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn test_async_redis_command_failures_are_returned_without_details() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    block_on(async {
        let native_message = OutboundMessage::new(
            TopicAddress::new("async-native")?,
            EventId::new("async-native-event")?,
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            None,
            TransportPayload::Native(Arc::new(7_u8)),
        );
        assert!(bus.publish(native_message).await.is_err());
        let key = stream_key("async-tests", "async-wrong-type");
        let mut connection = Client::open(server.url())?.get_connection()?;
        cmd("SET")
            .arg(&key)
            .arg("not-a-stream")
            .query::<()>(&mut connection)?;
        assert!(
            bus.publish(message("async-wrong-type", "failed-write", b"x")?)
                .await
                .is_err()
        );
        assert!(
            bus.subscribe(group_request(
                "async-wrong-type",
                "failed-subscribe",
                "group"
            ))
            .await
            .is_err()
        );
        let _: usize = cmd("DEL").arg(&key).query(&mut connection)?;
        let mut first = bus
            .subscribe(group_request(
                "async-wrong-type",
                "duplicate-group-worker",
                "group",
            ))
            .await?;
        let mut second = bus
            .subscribe(group_request_at(
                "async-wrong-type",
                "duplicate-group-worker",
                "group",
                StartPosition::New,
            ))
            .await?;
        first.close().await?;
        second.close().await?;

        let _ = bus
            .publish(message("async-failed-ack", "failed-ack-event", b"x")?)
            .await?;
        let mut receiver = bus
            .subscribe(group_request(
                "async-failed-ack",
                "failed-ack-worker",
                "group",
            ))
            .await?;
        let ReceiveOutcome::Message(received) = receiver.receive(Duration::from_secs(2)).await?
        else {
            return Err("valid stream event was not received".into());
        };
        let key = stream_key("async-tests", "async-failed-ack");
        cmd("SET")
            .arg(key)
            .arg("not-a-stream")
            .query::<()>(&mut connection)?;
        let error = receiver
            .settle(
                received.settlement().ok_or("missing settlement token")?,
                DeliveryDisposition::Accept,
            )
            .await
            .unwrap_err();
        assert!(!error.to_string().contains("not-a-stream"));

        let options: ProviderOptions = [
            ("redis.url".into(), "redis://127.0.0.1:1/".into()),
            ("redis.namespace".into(), "async-offline".into()),
        ]
        .into();
        let offline = AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(options))
            .await
            .map_err(|failure| failure.into_error())?;
        assert!(
            offline
                .publish(message("topic", "offline-publish", b"x")?)
                .await
                .is_err()
        );
        assert!(
            offline
                .subscribe(group_request("topic", "offline-subscribe", "group"))
                .await
                .is_err()
        );
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn test_async_client_builds_standalone_and_sentinel_authentication() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let standalone: ProviderOptions = [
            ("redis.url".into(), "redis://127.0.0.1:1/".into()),
            ("redis.username_env".into(), "PATH".into()),
            ("redis.password_env".into(), "HOME".into()),
        ]
        .into();
        let standalone = AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(standalone))
            .await
            .map_err(|failure| failure.into_error())?;
        assert!(
            standalone
                .publish(message("auth", "async-standalone-auth", b"x")?)
                .await
                .is_err()
        );

        let sentinel: ProviderOptions = [
            ("redis.sentinel.nodes".into(), "127.0.0.1:1".into()),
            ("redis.sentinel.service_name".into(), "primary".into()),
            ("redis.username_env".into(), "PATH".into()),
            ("redis.password_env".into(), "HOME".into()),
            ("redis.sentinel.username_env".into(), "PATH".into()),
            ("redis.sentinel.password_env".into(), "HOME".into()),
        ]
        .into();
        let sentinel = AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(sentinel))
            .await
            .map_err(|failure| failure.into_error())?;
        assert!(
            sentinel
                .publish(message("auth", "async-sentinel-auth", b"x")?)
                .await
                .is_err()
        );
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn test_async_recovers_pending_message_after_redis_restart() -> Result<(), Box<dyn Error>> {
    let mut server = RedisServer::start()?;
    let options: ProviderOptions = [
        ("redis.url".into(), server.url().into()),
        ("redis.namespace".into(), "async-tests".into()),
        ("redis.claim_min_idle_ms".into(), "0".into()),
    ]
    .into();
    let config = EventBusConfig::default().with_provider_options(options);
    let bus = block_on(AsyncRedisEventBusProvider.create_configured(&config))
        .map_err(|failure| failure.into_error())?;
    block_on(async {
        let _ = bus
            .publish(message("restart", "async-restart", b"durable")?)
            .await?;
        let mut first = bus
            .subscribe(group_request("restart", "before-restart", "restart-group"))
            .await?;
        let ReceiveOutcome::Message(received) = first.receive(Duration::from_secs(2)).await? else {
            return Err("pre-restart event was not received".into());
        };
        assert_eq!(received.id().as_str(), "async-restart");
        drop(first);

        server.restart()?;
        let mut recovered = bus
            .subscribe(group_request_at(
                "restart",
                "after-restart",
                "restart-group",
                StartPosition::New,
            ))
            .await?;
        let ReceiveOutcome::Message(received) = recovered.receive(Duration::from_secs(3)).await?
        else {
            return Err("pending event was not recovered after Redis restart".into());
        };
        assert_eq!(received.id().as_str(), "async-restart");
        recovered
            .settle(
                received
                    .settlement()
                    .ok_or("recovered event has no settlement token")?,
                DeliveryDisposition::Accept,
            )
            .await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

/// Returns a durable request for `topic`, `subscriber`, and `group` without
/// I/O; panics if a fixed fixture identifier fails validation.
fn group_request(topic: &str, subscriber: &str, group: &str) -> SpiSubscriptionRequest {
    group_request_at(topic, subscriber, group, StartPosition::Earliest)
}

/// Returns a durable group request with a caller-selected start position.
fn group_request_at(
    topic: &str,
    subscriber: &str,
    group: &str,
    start_position: StartPosition,
) -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(SUBSCRIPTION_IDS.fetch_add(1, Ordering::Relaxed)),
        TopicAddress::new(topic).expect("static topic is valid"),
        SubscriberId::new(subscriber).expect("static subscriber is valid"),
        Some(ConsumerGroup::new(group).expect("static group is valid")),
        SubscriptionDurability::Durable,
        start_position,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    )
}
