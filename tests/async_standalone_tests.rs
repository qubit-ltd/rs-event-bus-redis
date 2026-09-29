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
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::SystemTime;

use futures_lite::future::block_on;
use futures_lite::future::race;
use qubit_event_bus::EventBusConfig;
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
use redis::cmd;
use support::controlled_redis::ControlledRedis;
use support::redis_server::RedisServer;

static SUBSCRIPTION_IDS: AtomicU64 = AtomicU64::new(100);

#[test]
fn test_async_close_makes_future_receives_return_closed() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    block_on(async {
        let mut subscription = bus.subscribe(request("async-close-events", "close-worker")?).await?;
        subscription.close().await?;
        assert!(matches!(
            subscription.receive(Duration::ZERO).await?,
            ReceiveOutcome::Closed
        ));
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[test]
fn test_async_approximate_stream_limit_trims_old_entries_when_enabled() -> Result<(), Box<dyn std::error::Error>> {
    use qubit_spi::AsyncServiceProvider;

    let server = RedisServer::start()?;
    let options: ProviderOptions = [
        ("redis.url".into(), server.url().into()),
        ("redis.namespace".into(), "async-limit-tests".into()),
        ("redis.stream_maxlen_approx".into(), "10".into()),
    ]
    .into();
    let bus = block_on(
        AsyncRedisEventBusProvider.create_configured(&EventBusConfig::default().with_provider_options(options)),
    )
    .map_err(|failure| failure.into_error())?;
    block_on(async {
        for index in 0..250 {
            bus.publish(message("trim-events", &format!("trim-{index}"), b"payload")?)
                .await?;
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;

    let mut connection = Client::open(server.url())?.get_connection()?;
    let length: usize = cmd("XLEN")
        .arg(stream_key("async-limit-tests", "trim-events"))
        .query(&mut connection)?;
    assert!(length < 250, "approximate trim left {length} entries");
    Ok(())
}

#[test]
fn test_async_stream_is_untrimmed_by_default() -> Result<(), Box<dyn std::error::Error>> {
    use qubit_spi::AsyncServiceProvider;

    let server = RedisServer::start()?;
    let options: ProviderOptions = [
        ("redis.url".into(), server.url().into()),
        ("redis.namespace".into(), "async-default-retention".into()),
    ]
    .into();
    let bus = block_on(
        AsyncRedisEventBusProvider.create_configured(&EventBusConfig::default().with_provider_options(options)),
    )
    .map_err(|failure| failure.into_error())?;
    block_on(async {
        for index in 0..250 {
            bus.publish(message("default-events", &format!("default-{index}"), b"payload")?)
                .await?;
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;

    let mut connection = Client::open(server.url())?.get_connection()?;
    let length: usize = cmd("XLEN")
        .arg(stream_key("async-default-retention", "default-events"))
        .query(&mut connection)?;
    assert_eq!(length, 250);
    Ok(())
}

fn create_bus(server: &RedisServer) -> Result<Arc<dyn AsyncEventBusSpi>, Box<dyn std::error::Error>> {
    create_bus_url(server.url())
}

fn create_bus_url(url: &str) -> Result<Arc<dyn AsyncEventBusSpi>, Box<dyn std::error::Error>> {
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

fn message(topic: &str, id: &str, payload: &[u8]) -> Result<OutboundMessage, Box<dyn std::error::Error>> {
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

fn request(topic: &str, subscriber: &str) -> Result<SpiSubscriptionRequest, Box<dyn std::error::Error>> {
    Ok(SpiSubscriptionRequest::new(
        Id::new(SUBSCRIPTION_IDS.fetch_add(1, Ordering::Relaxed)),
        TopicAddress::new(topic)?,
        SubscriberId::new(subscriber)?,
        Some(ConsumerGroup::new("workers")?),
        SubscriptionDurability::Durable,
        StartPosition::Earliest,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    ))
}

async fn verify_message(bus: Arc<dyn AsyncEventBusSpi>) -> Result<(), Box<dyn std::error::Error>> {
    bus.publish(message("async-events", "async-1", &[0, 11, 128, 255])?)
        .await?;
    let mut receiver = bus.subscribe(request("async-events", "worker-a")?).await?;
    let ReceiveOutcome::Message(mut received) = receiver.receive(Duration::from_secs(2)).await? else {
        return Err("published record was not received".into());
    };
    let TransportPayload::Encoded(payload) = received.payload() else {
        return Err("encoded payload expected".into());
    };
    assert_eq!(payload.bytes(), &[0, 11, 128, 255]);
    let token = received.take_settlement().ok_or("missing settlement token")?;
    receiver.settle(&token, DeliveryDisposition::Accept).await?;
    receiver.settle(&token, DeliveryDisposition::Accept).await?;
    assert!(receiver.settle(&token, DeliveryDisposition::Reject).await.is_err());
    receiver.close().await?;
    Ok(())
}

#[test]
fn test_async_spi_runs_on_smol_executor() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    block_on(verify_message(bus))
}

#[test]
#[cfg(feature = "conformance")]
fn test_async_spi_conformance() -> Result<(), Box<dyn std::error::Error>> {
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
        ..AsyncConformanceHooks::default()
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

#[cfg(feature = "conformance")]
async fn check_async_settlement(server: &RedisServer) -> Result<(), String> {
    let bus = create_bus(server).map_err(|error| error.to_string())?;
    bus.publish(message("conformance-settlement", "settlement", b"payload").map_err(|error| error.to_string())?)
        .await
        .map_err(|error| error.to_string())?;
    let mut receiver = bus
        .subscribe(request("conformance-settlement", "settlement-worker").map_err(|error| error.to_string())?)
        .await
        .map_err(|error| error.to_string())?;
    let ReceiveOutcome::Message(mut received) = receiver
        .receive(Duration::from_secs(2))
        .await
        .map_err(|error| error.to_string())?
    else {
        return Err("settlement fixture did not receive the published event".into());
    };
    let token = received.take_settlement().ok_or("settlement token is missing")?;
    receiver
        .settle(&token, DeliveryDisposition::Accept)
        .await
        .map_err(|error| error.to_string())?;
    receiver
        .settle(&token, DeliveryDisposition::Accept)
        .await
        .map_err(|error| error.to_string())?;
    if receiver.settle(&token, DeliveryDisposition::Reject).await.is_ok() {
        return Err("conflicting settlement unexpectedly succeeded".into());
    }
    receiver.close().await.map_err(|error| error.to_string())
}

#[cfg(feature = "conformance")]
async fn check_async_settlement_cancellation(server: &RedisServer) -> Result<(), String> {
    let bus = create_bus(server).map_err(|error| error.to_string())?;
    bus.publish(message("conformance-settle-cancel", "settle-cancel", b"payload").map_err(|error| error.to_string())?)
        .await
        .map_err(|error| error.to_string())?;
    let mut receiver = bus
        .subscribe(request("conformance-settle-cancel", "settle-cancel").map_err(|error| error.to_string())?)
        .await
        .map_err(|error| error.to_string())?;
    let ReceiveOutcome::Message(mut received) = receiver
        .receive(Duration::from_secs(2))
        .await
        .map_err(|error| error.to_string())?
    else {
        return Err("settlement cancellation fixture did not receive its message".into());
    };
    let token = received.take_settlement().ok_or("settlement token is missing")?;
    drop(received);
    cancel_after_operation(receiver.settle(&token, DeliveryDisposition::Accept))
        .await
        .map_err(|error| format!("cancelled settlement failed: {error}"))?;
    receiver
        .settle(&token, DeliveryDisposition::Accept)
        .await
        .map_err(|error| format!("repeating the applied settlement failed: {error}"))?;
    if receiver.settle(&token, DeliveryDisposition::Retry).await.is_ok() {
        return Err("conflicting settlement succeeded after cancellation".into());
    }
    receiver.close().await.map_err(|error| error.to_string())?;
    bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

#[cfg(feature = "conformance")]
async fn check_async_close_cancellation(server: &RedisServer) -> Result<(), String> {
    let bus = create_bus(server).map_err(|error| error.to_string())?;
    let mut receiver = bus
        .subscribe(request("conformance-close-cancel", "close-cancel").map_err(|error| error.to_string())?)
        .await
        .map_err(|error| error.to_string())?;
    cancel_after_operation(receiver.close())
        .await
        .map_err(|error| format!("cancelled close failed: {error}"))?;
    if !matches!(receiver.receive(Duration::ZERO).await, Ok(ReceiveOutcome::Closed)) {
        return Err("receiver was not closed after cancellation".into());
    }
    receiver.close().await.map_err(|error| error.to_string())?;
    bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

#[cfg(feature = "conformance")]
async fn check_async_shutdown_cancellation(server: &RedisServer) -> Result<(), String> {
    let bus = create_bus(server).map_err(|error| error.to_string())?;
    cancel_after_operation(bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate))
        .await
        .map_err(|error| format!("cancelled shutdown failed: {error}"))?;
    match bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate).await {
        Ok(qubit_event_bus::spi::ShutdownOutcome::Complete) => Ok(()),
        Ok(outcome) => Err(format!("repeated shutdown returned {outcome:?}")),
        Err(error) => Err(format!("repeated shutdown failed: {error}")),
    }
}

#[cfg(feature = "conformance")]
async fn cancel_after_operation<F: Future>(operation: F) -> F::Output {
    let result = Arc::new(Mutex::new(None));
    let completed = Arc::new(AtomicBool::new(false));
    let driver_result = Arc::clone(&result);
    let driver_completed = Arc::clone(&completed);
    let mut operation = Box::pin(operation);
    let mut driver = Box::pin(futures_lite::future::poll_fn(move |cx| {
        if !driver_completed.load(Ordering::Acquire)
            && let Poll::Ready(output) = operation.as_mut().poll(cx)
        {
            *driver_result.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(output);
            driver_completed.store(true, Ordering::Release);
            cx.waker().wake_by_ref();
        }
        Poll::<()>::Pending
    }));
    futures_lite::future::poll_fn(|cx| {
        let _ = driver.as_mut().poll(cx);
        if completed.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    drop(driver);
    result
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
        .expect("completed operation must retain its result")
}

#[cfg(feature = "conformance")]
async fn check_async_receive_cancellation(server: &RedisServer) -> Result<(), String> {
    let bus = create_bus(server).map_err(|error| error.to_string())?;
    let mut receiver = bus
        .subscribe(request("conformance-cancellation", "cancellation-worker").map_err(|error| error.to_string())?)
        .await
        .map_err(|error| error.to_string())?;
    if futures_lite::future::poll_once(receiver.receive(Duration::MAX))
        .await
        .is_some()
    {
        return Err("empty receive unexpectedly completed before cancellation".into());
    }
    bus.publish(message("conformance-cancellation", "after-cancel", b"payload").map_err(|error| error.to_string())?)
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

#[cfg(feature = "conformance")]
async fn check_async_durable_recovery(server: &RedisServer) -> Result<(), String> {
    let bus = create_bus(server).map_err(|error| error.to_string())?;
    bus.publish(message("conformance-recovery", "recovery", b"pending").map_err(|error| error.to_string())?)
        .await
        .map_err(|error| error.to_string())?;
    let mut receiver = bus
        .subscribe(request("conformance-recovery", "recovery-before-close").map_err(|error| error.to_string())?)
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
    bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate)
        .await
        .map_err(|error| error.to_string())?;

    let recovered_bus = create_bus(server).map_err(|error| error.to_string())?;
    let mut recovered = recovered_bus
        .subscribe(request("conformance-recovery", "recovery-after-close").map_err(|error| error.to_string())?)
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
            received.settlement().ok_or("recovered settlement token is missing")?,
            DeliveryDisposition::Accept,
        )
        .await
        .map_err(|error| error.to_string())?;
    if !matches!(recovered.receive(Duration::ZERO).await, Ok(ReceiveOutcome::TimedOut)) {
        return Err("accepted delivery was unexpectedly recovered again".into());
    }
    recovered.close().await.map_err(|error| error.to_string())?;
    recovered_bus
        .shutdown(qubit_event_bus::spi::ShutdownMode::Immediate)
        .await
        .map_err(|error| error.to_string())?;

    let drop_bus = create_bus(server).map_err(|error| error.to_string())?;
    drop_bus
        .publish(message("conformance-recovery-drop", "recovery-drop", b"pending").map_err(|error| error.to_string())?)
        .await
        .map_err(|error| error.to_string())?;
    let mut dropped = drop_bus
        .subscribe(
            request("conformance-recovery-drop", "recovery-drop-before-drop").map_err(|error| error.to_string())?,
        )
        .await
        .map_err(|error| error.to_string())?;
    let ReceiveOutcome::Message(pending) = dropped
        .receive(Duration::from_secs(2))
        .await
        .map_err(|error| error.to_string())?
    else {
        return Err("drop recovery fixture did not receive its event".into());
    };
    if pending.id().as_str() != "recovery-drop" {
        return Err("drop recovery fixture received an unexpected event".into());
    }
    drop(pending);
    drop(dropped);
    drop_bus
        .shutdown(qubit_event_bus::spi::ShutdownMode::Immediate)
        .await
        .map_err(|error| error.to_string())?;
    let after_drop_bus = create_bus(server).map_err(|error| error.to_string())?;
    let mut after_drop = after_drop_bus
        .subscribe(request("conformance-recovery-drop", "recovery-drop-after-drop").map_err(|error| error.to_string())?)
        .await
        .map_err(|error| error.to_string())?;
    let ReceiveOutcome::Message(recovered) = after_drop
        .receive(Duration::from_secs(2))
        .await
        .map_err(|error| error.to_string())?
    else {
        return Err("unsettled delivery was not recovered after receiver drop".into());
    };
    if recovered.id().as_str() != "recovery-drop" {
        return Err("receiver drop recovery returned an unexpected event".into());
    }
    let token = recovered
        .settlement()
        .ok_or("drop recovery settlement token is missing")?;
    after_drop
        .settle(token, DeliveryDisposition::Accept)
        .await
        .map_err(|error| error.to_string())?;
    after_drop.close().await.map_err(|error| error.to_string())?;
    after_drop_bus
        .shutdown(qubit_event_bus::spi::ShutdownMode::Immediate)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

#[tokio::test]
async fn test_async_spi_runs_on_tokio_executor() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    verify_message(bus).await
}

#[test]
fn test_async_receive_cancellation_leaves_pending_message_recoverable() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let proxy = ControlledRedis::start(server.url())?;
    let gate = proxy.gate();
    let bus = create_bus_url(&proxy.url())?;
    block_on(async {
        bus.publish(message("async-events", "cancel-1", b"recover")?).await?;
        let mut receiver = bus.subscribe(request("async-events", "worker-c")?).await?;
        let cancel = Arc::new(CancelReceive::default());
        let worker_cancel = Arc::clone(&cancel);
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        gate.arm();
        std::thread::scope(|scope| {
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
            let pending: Vec<redis::Value> = cmd("XPENDING")
                .arg(stream_key("async-tests", "async-events"))
                .arg(group_name("async-tests", "async-events", "worker-c", Some("workers")))
                .arg("-")
                .arg("+")
                .arg(10)
                .query(&mut connection)?;
            assert_eq!(
                pending.len(),
                1,
                "Redis must have applied XREADGROUP before cancellation"
            );
            Ok::<(), Box<dyn std::error::Error>>(())
        })?;
        let ReceiveOutcome::Message(received) = receiver.receive(Duration::from_secs(2)).await? else {
            return Err("pending record was not recovered".into());
        };
        let TransportPayload::Encoded(payload) = received.payload() else {
            return Err("encoded payload expected".into());
        };
        assert_eq!(payload.bytes(), b"recover");
        receiver
            .settle(
                received.settlement().ok_or("missing settlement token")?,
                DeliveryDisposition::Accept,
            )
            .await?;
        receiver.close().await?;
        assert!(matches!(
            receiver.receive(Duration::ZERO).await?,
            ReceiveOutcome::Closed
        ));
        receiver.close().await?;
        Ok(())
    })
}

#[derive(Default)]
struct CancelReceive {
    cancelled: std::sync::atomic::AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl CancelReceive {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        if let Some(waker) = self.waker.lock().expect("cancel waker lock is healthy").take() {
            waker.wake();
        }
    }
}

struct WaitForCancel(Arc<CancelReceive>);

impl std::future::Future for WaitForCancel {
    type Output = Option<Result<ReceiveOutcome, qubit_event_bus::error::SpiError>>;

    fn poll(self: std::pin::Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0.cancelled.load(Ordering::SeqCst) {
            Poll::Ready(None)
        } else {
            *self.0.waker.lock().expect("cancel waker lock is healthy") = Some(context.waker().clone());
            Poll::Pending
        }
    }
}

#[test]
fn test_async_existing_consumer_group_is_not_retried() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    block_on(async {
        let first = bus.subscribe(request("busy-group", "worker-a")?).await?;
        let before_second = xgroup_command_calls(&server)?;
        let second = bus.subscribe(request("busy-group", "worker-b")?).await?;
        let after_second = xgroup_command_calls(&server)?;
        assert_eq!(
            after_second - before_second,
            1,
            "BUSYGROUP must not trigger a repeated XGROUP CREATE"
        );
        drop((first, second));
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

fn xgroup_command_calls(server: &RedisServer) -> Result<u64, Box<dyn std::error::Error>> {
    let client = Client::open(server.url())?;
    let mut connection = client.get_connection()?;
    let info: String = cmd("INFO").arg("commandstats").query(&mut connection)?;
    let line = info
        .lines()
        .find(|line| line.starts_with("cmdstat_xgroup"))
        .ok_or_else(|| format!("missing XGROUP command statistics: {info}"))?;
    let calls = line.split("calls=").nth(1).ok_or("missing XGROUP calls count")?;
    Ok(calls.split(',').next().ok_or("empty XGROUP calls count")?.parse()?)
}

#[test]
fn test_async_unsettled_message_is_claimed_after_consumer_reconnect() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    block_on(async {
        bus.publish(message("recovery", "async-recovery", b"resume")?).await?;
        let mut first = bus.subscribe(request("recovery", "worker-one")?).await?;
        let ReceiveOutcome::Message(_) = first.receive(Duration::from_secs(2)).await? else {
            return Err("initial consumer did not receive the event".into());
        };
        first.close().await?;
        let mut second = bus.subscribe(request("recovery", "worker-two")?).await?;
        let ReceiveOutcome::Message(received) = second.receive(Duration::from_secs(2)).await? else {
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
fn test_async_receiver_pauses_at_unsettled_limit() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    block_on(async {
        bus.publish(message("bounded", "async-bound-1", b"one")?).await?;
        bus.publish(message("bounded", "async-bound-2", b"two")?).await?;
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
        let ReceiveOutcome::Message(second) = receiver.receive(Duration::from_secs(2)).await? else {
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
fn test_async_groups_fan_out_and_share_work() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    block_on(async {
        bus.publish(message("async-groups", "async-group-event-1", b"one")?)
            .await?;
        bus.publish(message("async-groups", "async-group-event-2", b"two")?)
            .await?;
        let mut worker_a = bus
            .subscribe(group_request("async-groups", "worker-a", "billing"))
            .await?;
        let mut worker_b = bus
            .subscribe(group_request("async-groups", "worker-b", "billing"))
            .await?;
        let mut audit = bus.subscribe(group_request("async-groups", "audit", "audit")).await?;
        let mut worker_ids = vec![];
        for receiver in [&mut worker_a, &mut worker_b] {
            let ReceiveOutcome::Message(message) = receiver.receive(Duration::from_secs(2)).await? else {
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
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[test]
fn test_async_replay_from_stream_position_and_new_tail() -> Result<(), Box<dyn std::error::Error>> {
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
        bus.publish(message("async-positions", "async-position-2", b"two")?)
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
        at.settle(second.settlement().ok_or("missing token")?, DeliveryDisposition::Accept)
            .await?;
        assert!(matches!(at.receive(Duration::ZERO).await?, ReceiveOutcome::TimedOut));

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
        bus.publish(message("async-positions", "async-position-3", b"three")?)
            .await?;
        let ReceiveOutcome::Message(third) = new.receive(Duration::from_secs(2)).await? else {
            return Err("New consumer did not receive a new event".into());
        };
        assert_eq!(third.id().as_str(), "async-position-3");
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[test]
fn test_async_reports_gap_for_removed_pending_entries() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    block_on(async {
        bus.publish(message("async-gaps", "async-removed-event", b"payload")?)
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
            .subscribe(group_request("async-gaps", "gap-worker-two", "gap-group"))
            .await?;
        let outcome = second.receive(Duration::from_secs(1)).await?;
        assert!(matches!(outcome, ReceiveOutcome::Gap(_)));
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[test]
fn test_async_reject_acks_and_malformed_wire_is_quarantined() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    block_on(async {
        bus.publish(message("async-malformed", "async-reject-event", b"reject")?)
            .await?;
        let mut receiver = bus
            .subscribe(group_request("async-malformed", "reject-worker", "reject-group"))
            .await?;
        let ReceiveOutcome::Message(rejected) = receiver.receive(Duration::from_secs(2)).await? else {
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
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[test]
fn test_async_redis_command_failures_are_returned_without_details() -> Result<(), Box<dyn std::error::Error>> {
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
        cmd("SET").arg(&key).arg("not-a-stream").query::<()>(&mut connection)?;
        assert!(
            bus.publish(message("async-wrong-type", "failed-write", b"x")?)
                .await
                .is_err()
        );
        assert!(
            bus.subscribe(group_request("async-wrong-type", "failed-subscribe", "group"))
                .await
                .is_err()
        );
        let _: usize = cmd("DEL").arg(&key).query(&mut connection)?;
        let mut first = bus
            .subscribe(group_request("async-wrong-type", "duplicate-group-worker", "group"))
            .await?;
        let mut second = bus
            .subscribe(group_request("async-wrong-type", "duplicate-group-worker", "group"))
            .await?;
        first.close().await?;
        second.close().await?;

        bus.publish(message("async-failed-ack", "failed-ack-event", b"x")?)
            .await?;
        let mut receiver = bus
            .subscribe(group_request("async-failed-ack", "failed-ack-worker", "group"))
            .await?;
        let ReceiveOutcome::Message(received) = receiver.receive(Duration::from_secs(2)).await? else {
            return Err("valid stream event was not received".into());
        };
        let key = stream_key("async-tests", "async-failed-ack");
        cmd("SET").arg(key).arg("not-a-stream").query::<()>(&mut connection)?;
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
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[test]
fn test_async_client_builds_standalone_and_sentinel_authentication() -> Result<(), Box<dyn std::error::Error>> {
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
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[test]
fn test_async_recovers_pending_message_after_redis_restart() -> Result<(), Box<dyn std::error::Error>> {
    let mut server = RedisServer::start()?;
    let options: ProviderOptions = [
        ("redis.url".into(), server.url().into()),
        ("redis.namespace".into(), "async-tests".into()),
        ("redis.claim_min_idle_ms".into(), "0".into()),
    ]
    .into();
    let config = EventBusConfig::default().with_provider_options(options);
    let bus =
        block_on(AsyncRedisEventBusProvider.create_configured(&config)).map_err(|failure| failure.into_error())?;
    block_on(async {
        bus.publish(message("restart", "async-restart", b"durable")?).await?;
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
            .subscribe(group_request("restart", "after-restart", "restart-group"))
            .await?;
        let ReceiveOutcome::Message(received) = recovered.receive(Duration::from_secs(3)).await? else {
            return Err("pending event was not recovered after Redis restart".into());
        };
        assert_eq!(received.id().as_str(), "async-restart");
        recovered
            .settle(
                received.settlement().ok_or("recovered event has no settlement token")?,
                DeliveryDisposition::Accept,
            )
            .await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

fn group_request(topic: &str, subscriber: &str, group: &str) -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(SUBSCRIPTION_IDS.fetch_add(1, Ordering::Relaxed)),
        TopicAddress::new(topic).expect("static topic is valid"),
        SubscriberId::new(subscriber).expect("static subscriber is valid"),
        Some(ConsumerGroup::new(group).expect("static group is valid")),
        SubscriptionDurability::Durable,
        StartPosition::Earliest,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    )
}
