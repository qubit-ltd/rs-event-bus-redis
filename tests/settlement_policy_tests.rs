// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Facade settlement regressions against an isolated Redis PEL and real XACK.

#![cfg(any(feature = "sync", feature = "async"))]
mod support;

use std::any::TypeId;
use std::error::Error;
use std::process::id as process_id;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
#[cfg(feature = "sync")]
use std::thread::sleep;
use std::time::Duration;
use std::time::Instant;

#[cfg(feature = "async")]
use futures_channel::oneshot;
#[cfg(feature = "async")]
use futures_lite::future::block_on;
#[cfg(feature = "async")]
use futures_lite::future::race;
#[cfg(feature = "async")]
use futures_lite::future::yield_now;
#[cfg(feature = "async")]
use qubit_event_bus::AsyncEventBus;
#[cfg(feature = "sync")]
use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusConfig;
#[cfg(feature = "sync")]
use qubit_event_bus::Subscription;
use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::CodecError;
#[cfg(feature = "async")]
use qubit_event_bus::error::ReceiveError;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::SettlementTermination;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::model::SubscriptionStopReason;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
#[cfg(feature = "async")]
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
use qubit_event_bus_redis::naming::group_name;
use qubit_event_bus_redis::naming::stream_key;
#[cfg(feature = "sync")]
use qubit_event_bus_redis::sync::RedisEventBusProvider;
use qubit_id::Id;
#[cfg(feature = "async")]
use qubit_spi::AsyncServiceProvider;
#[cfg(feature = "sync")]
use qubit_spi::ServiceProvider;
use redis::Client;
use redis::cmd;
use redis::streams::StreamPendingReply;

use self::support::controlled_redis::proxy::ControlledRedis;
use self::support::redis_server::RedisServer;
#[cfg(feature = "async")]
use self::support::settlement_fault::AsyncObservedBus;
use self::support::settlement_fault::Observation;
#[cfg(feature = "sync")]
use self::support::settlement_fault::SyncObservedBus;

static IDS: AtomicU64 = AtomicU64::new(10_000);

const TOPIC: &str = "settlement.events";
const WATCHDOG: Duration = Duration::from_secs(10);

struct Utf8Codec(ContentType);
impl EventCodec<String> for Utf8Codec {
    fn content_type(&self) -> &ContentType {
        &self.0
    }
    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }
    fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from(value.as_bytes()))
    }
    fn decode(&self, payload: &EncodedPayload) -> Result<String, CodecError> {
        String::from_utf8(payload.bytes().to_vec()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}

fn facade_config() -> Result<EventBusFacadeConfig, Box<dyn Error>> {
    let mut codecs = CodecRegistry::new();
    codecs.register::<String>(Arc::new(Utf8Codec(ContentType::new("text/plain")?)))?;
    let config = EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs));
    let scheduling = config.delivery_scheduling();
    assert_eq!(scheduling.max_running_handlers().get(), 4);
    assert_eq!(scheduling.max_owned_deliveries().get(), 256);
    assert_eq!(scheduling.max_owned_per_subscription().get(), 32);
    assert_eq!(scheduling.max_subscriptions().get(), 256);
    assert_eq!(config.settlement_retry().max_attempts().get(), 5);
    Ok(config)
}

struct Fixture {
    server: RedisServer,
    namespace: String,
    group: String,
}
impl Fixture {
    fn start() -> Result<Self, Box<dyn Error>> {
        // Never fall back to a developer Redis or silently skip unavailable Docker.
        let server = RedisServer::start()?;
        let unique = format!("{}-{}", process_id(), IDS.fetch_add(1, Ordering::Relaxed));
        Ok(Self {
            server,
            namespace: format!("settlement-{unique}"),
            group: format!("workers-{unique}"),
        })
    }
    fn config(&self, url: &str) -> EventBusConfig {
        let options: ProviderOptions = [
            ("redis.url".into(), url.into()),
            ("redis.namespace".into(), self.namespace.clone()),
            // Immediate claim is intentional: a new consumer must recover the closed owner's PEL.
            ("redis.claim_min_idle_ms".into(), "0".into()),
        ]
        .into();
        EventBusConfig::default().with_provider_options(options)
    }
    fn request(&self) -> Result<SubscribeRequest<String>, Box<dyn Error>> {
        Ok(SubscribeRequest::builder()
            .subscriber_id(SubscriberId::new("first-consumer")?)
            .topic(Topic::new(TOPIC)?)
            .consumer_group(ConsumerGroup::new(&self.group)?)
            .durability(SubscriptionDurability::Durable)
            .start_position(StartPosition::Earliest)
            .build()?)
    }
    fn recovery_request(&self) -> Result<SpiSubscriptionRequest, Box<dyn Error>> {
        self.consumer_request("recovery-consumer", ProviderOptions::new())
    }
    fn consumer_request(
        &self,
        consumer: &str,
        options: ProviderOptions,
    ) -> Result<SpiSubscriptionRequest, Box<dyn Error>> {
        Ok(SpiSubscriptionRequest::new(
            Id::new(IDS.fetch_add(1, Ordering::Relaxed)),
            TopicAddress::new(TOPIC)?,
            SubscriberId::new(consumer)?,
            Some(ConsumerGroup::new(&self.group)?),
            SubscriptionDurability::Durable,
            StartPosition::New,
            options,
            TypeId::of::<String>(),
        ))
    }
    fn pending_owner(&self, _message_id: &str) -> Result<Option<String>, Box<dyn Error>> {
        let mut connection = Client::open(self.server.url())?.get_connection()?;
        let rows: Vec<Vec<redis::Value>> = cmd("XPENDING")
            .arg(stream_key(&self.namespace, TOPIC))
            .arg(group_name(
                &self.namespace,
                TOPIC,
                "first-consumer",
                Some(&self.group),
            ))
            .arg("-")
            .arg("+")
            .arg(1)
            .query(&mut connection)?;
        Ok(rows
            .first()
            .map(|row| redis::from_redis_value(row[1].clone()))
            .transpose()?)
    }
    fn pending(&self) -> Result<usize, Box<dyn Error>> {
        let mut connection = Client::open(self.server.url())?.get_connection()?;
        let reply: StreamPendingReply = cmd("XPENDING")
            .arg(stream_key(&self.namespace, TOPIC))
            .arg(group_name(
                &self.namespace,
                TOPIC,
                "first-consumer",
                Some(&self.group),
            ))
            .query(&mut connection)?;
        Ok(reply.count())
    }
}

fn assert_terminal(reason: &SubscriptionStopReason, event_id: &EventId, observation: &Observation) {
    let SubscriptionStopReason::Settlement {
        event_id: actual,
        disposition,
        attempts,
        termination,
        error,
    } = reason
    else {
        panic!("expected structured terminal settlement failure: {reason:?}");
    };
    assert_eq!(actual, event_id);
    assert_eq!(*disposition, DeliveryDisposition::Accept);
    assert_eq!(*attempts, 1);
    assert_eq!(*termination, SettlementTermination::PermanentError);
    assert_eq!(error.retryable(), Some(false));
    assert_eq!(observation.underlying_settles.load(Ordering::SeqCst), 0);
    assert_eq!(
        observation.closes.load(Ordering::SeqCst),
        1,
        "natural terminal stop closed the real receiver"
    );
    assert_eq!(
        observation
            .attempts
            .lock()
            .expect("observations lock")
            .len(),
        1
    );
}

fn assert_retry(observation: &Observation) {
    let attempts = observation.attempts.lock().expect("observations lock");
    assert_eq!(
        attempts.len(),
        2,
        "one lost reply followed by one successful settlement"
    );
    assert_eq!(attempts[0].token_address, attempts[1].token_address);
    assert_eq!(attempts[0].disposition, DeliveryDisposition::Accept);
    assert_eq!(attempts[0].disposition, attempts[1].disposition);
    assert_eq!(
        attempts[0].retryable_error,
        Some(true),
        "actual Redis I/O error must allow retry"
    );
    assert!(!attempts[0].succeeded);
    assert!(attempts[1].succeeded);
    assert_eq!(observation.underlying_settles.load(Ordering::SeqCst), 2);
}

#[cfg(feature = "sync")]
fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + WATCHDOG;
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "settlement observation watchdog expired"
        );
        sleep(Duration::from_millis(5));
    }
}

#[cfg(feature = "sync")]
struct CancelOnDrop<'a>(&'a Subscription);
#[cfg(feature = "sync")]
impl Drop for CancelOnDrop<'_> {
    fn drop(&mut self) {
        let _ = self.0.cancel();
    }
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_terminal_settlement_retains_unacked_redis_entry() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::start()?;
    let inner = RedisEventBusProvider
        .create_configured(&fixture.config(fixture.server.url()))
        .map_err(|e| e.into_error())?;
    let observation = Arc::new(Observation::default());
    let wrapped = Arc::new(SyncObservedBus {
        inner: inner.clone(),
        observation: observation.clone(),
        fail_before_settle: true,
    });
    let bus = EventBus::with_config(ProviderId::new("redis-streams")?, wrapped, facade_config()?)?;
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let subscription = bus.subscribe(fixture.request()?, move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
    })?;
    let _cancel = CancelOnDrop(&subscription);
    let receipt = bus.publish(PublishRequest::new(
        Topic::new(TOPIC)?,
        "durable".to_owned(),
    )?)?;
    wait_until(|| {
        subscription.terminal_failure().is_some() && observation.closes.load(Ordering::SeqCst) == 1
    });
    let reason = subscription
        .terminal_failure()
        .expect("terminal cause retained");
    assert_terminal(&reason, receipt.input_event_id(), &observation);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.pending()?,
        1,
        "close must not implicitly XACK durable delivery"
    );

    let mut recovered = inner.subscribe(fixture.recovery_request()?)?;
    let ReceiveOutcome::Message(message) = recovered.receive(Duration::from_secs(2))? else {
        panic!("closed owner's entry must be claimable by the new consumer")
    };
    assert_eq!(message.id(), receipt.input_event_id());
    recovered.settle(
        message.settlement().expect("Redis settlement token"),
        DeliveryDisposition::Accept,
    )?;
    assert_eq!(fixture.pending()?, 0);
    recovered.close()?;
    subscription.cancel()?;
    assert!(Arc::ptr_eq(
        &reason,
        &subscription
            .terminal_failure()
            .expect("first cause survives cancellation")
    ));
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_settlement_ownership_changed_preserves_new_owner() -> Result<(), Box<dyn Error>> {
    for disposition in [DeliveryDisposition::Accept, DeliveryDisposition::Reject] {
        let fixture = Fixture::start()?;
        let inner = RedisEventBusProvider
            .create_configured(&fixture.config(fixture.server.url()))
            .map_err(|e| e.into_error())?;
        let bus = EventBus::with_config(
            ProviderId::new("redis-streams")?,
            inner.clone(),
            facade_config()?,
        )?;
        let mut a =
            inner.subscribe(fixture.consumer_request("consumer-a", ProviderOptions::new())?)?;
        let receipt = bus.publish(PublishRequest::new(
            Topic::new(TOPIC)?,
            "ownership".to_owned(),
        )?)?;
        let ReceiveOutcome::Message(message_a) = a.receive(Duration::from_secs(2))? else {
            panic!("consumer A must receive the new message")
        };
        let token_a = message_a.settlement().expect("token A");
        let owner_a = fixture
            .pending_owner(message_a.id().as_str())?
            .expect("A owns PEL entry");
        let options =
            ProviderOptions::from([("redis.existing_group_start".into(), "resume".into())]);
        let mut b = inner.subscribe(fixture.consumer_request("consumer-b", options)?)?;
        let ReceiveOutcome::Message(message_b) = b.receive(Duration::from_secs(2))? else {
            panic!("consumer B must claim A's pending message")
        };
        let token_b = message_b.settlement().expect("token B");
        assert_eq!(message_b.id(), receipt.input_event_id());
        let owner_b = fixture
            .pending_owner(message_a.id().as_str())?
            .expect("B owns PEL entry");
        assert_ne!(owner_a, owner_b);

        let error = a
            .settle(token_a, disposition)
            .expect_err("stale owner must not settle");
        assert_eq!(error.kind(), "settlement_ownership_changed");
        assert_eq!(error.retryable(), Some(false));
        assert_eq!(
            fixture.pending_owner(message_a.id().as_str())?.as_deref(),
            Some(owner_b.as_str())
        );
        b.settle(token_b, DeliveryDisposition::Retry)?;
        let ReceiveOutcome::Message(message_again) = b.receive(Duration::from_secs(2))? else {
            panic!("consumer B must recover the retried message")
        };
        assert_eq!(message_again.id(), receipt.input_event_id());
        b.settle(
            message_again.settlement().expect("new B token"),
            DeliveryDisposition::Accept,
        )?;
        assert_eq!(fixture.pending_owner(message_a.id().as_str())?, None);
        a.close()?;
        b.close()?;
    }
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_facade_stops_when_settlement_owner_changes() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::start()?;
    let inner = RedisEventBusProvider
        .create_configured(&fixture.config(fixture.server.url()))
        .map_err(|e| e.into_error())?;
    let observation = Arc::new(Observation::default());
    let wrapped = Arc::new(SyncObservedBus {
        inner: inner.clone(),
        observation: observation.clone(),
        fail_before_settle: false,
    });
    let bus = EventBus::with_config(ProviderId::new("redis-streams")?, wrapped, facade_config()?)?;
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
    let release_signal = release_rx.clone();
    let subscription = bus.subscribe(fixture.request()?, move |_| {
        started_tx.send(()).expect("test observes handler start");
        release_signal
            .lock()
            .expect("release lock")
            .recv()
            .expect("test releases blocked handler");
    })?;
    let receipt = bus.publish(PublishRequest::new(
        Topic::new(TOPIC)?,
        "facade-ownership".to_owned(),
    )?)?;
    started_rx.recv_timeout(WATCHDOG)?;
    let options = ProviderOptions::from([("redis.existing_group_start".into(), "resume".into())]);
    let mut b = inner.subscribe(fixture.consumer_request("consumer-b", options)?)?;
    let ReceiveOutcome::Message(message_b) = b.receive(Duration::from_secs(2))? else {
        panic!("B must claim the blocked handler's pending entry")
    };
    assert_eq!(message_b.id(), receipt.input_event_id());
    let owner_b = fixture
        .pending_owner(message_b.id().as_str())?
        .expect("B owns PEL entry");
    release_tx.send(())?;
    wait_until(|| {
        subscription.terminal_failure().is_some() && observation.closes.load(Ordering::SeqCst) == 1
    });
    let reason = subscription
        .terminal_failure()
        .expect("facade retains terminal settlement error");
    let qubit_event_bus::model::SubscriptionStopReason::Settlement { error, .. } = reason.as_ref()
    else {
        panic!("expected terminal settlement failure, got {reason:?}");
    };
    assert_eq!(error.kind(), "settlement_ownership_changed");
    assert_eq!(observation.underlying_settles.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.pending_owner(message_b.id().as_str())?.as_deref(),
        Some(owner_b.as_str())
    );
    b.close()?;
    subscription.cancel()?;
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_unknown_settlement_keeps_intent_and_checks_later_owner() -> Result<(), Box<dyn Error>>
{
    let fixture = Fixture::start()?;
    let proxy = ControlledRedis::start(fixture.server.url())?;
    let inner = RedisEventBusProvider
        .create_configured(&fixture.config(&proxy.url()))
        .map_err(|e| e.into_error())?;
    let bus = EventBus::with_config(
        ProviderId::new("redis-streams")?,
        inner.clone(),
        facade_config()?,
    )?;
    let mut a = inner.subscribe(fixture.consumer_request("consumer-a", ProviderOptions::new())?)?;
    let receipt = bus.publish(PublishRequest::new(
        Topic::new(TOPIC)?,
        "unknown-owner".to_owned(),
    )?)?;
    let ReceiveOutcome::Message(message_a) = a.receive(Duration::from_secs(2))? else {
        panic!("consumer A must receive the new message")
    };
    let owner_a = fixture
        .pending_owner(message_a.id().as_str())?
        .expect("A owns PEL entry");
    let token_a = message_a.settlement().expect("token A");
    proxy.drop_next_settlement_before_forward();
    let unknown = a
        .settle(token_a, DeliveryDisposition::Accept)
        .expect_err("proxy loses settlement before Redis");
    assert_eq!(unknown.kind(), "outcome_unknown");
    assert_eq!(
        fixture.pending_owner(message_a.id().as_str())?.as_deref(),
        Some(owner_a.as_str())
    );

    let options = ProviderOptions::from([("redis.existing_group_start".into(), "resume".into())]);
    let mut b = inner.subscribe(fixture.consumer_request("consumer-b", options)?)?;
    let ReceiveOutcome::Message(message_b) = b.receive(Duration::from_secs(2))? else {
        panic!("consumer B must claim A's pending message")
    };
    assert_eq!(message_b.id(), receipt.input_event_id());
    let owner_b = fixture
        .pending_owner(message_a.id().as_str())?
        .expect("B owns PEL entry");
    assert_ne!(owner_a, owner_b);
    let changed = a
        .settle(token_a, DeliveryDisposition::Accept)
        .expect_err("same intent must recheck current owner");
    assert_eq!(changed.kind(), "settlement_ownership_changed");
    assert_eq!(changed.retryable(), Some(false));
    assert_eq!(
        fixture.pending_owner(message_a.id().as_str())?.as_deref(),
        Some(owner_b.as_str())
    );
    b.settle(
        message_b.settlement().expect("token B"),
        DeliveryDisposition::Retry,
    )?;
    let ReceiveOutcome::Message(message_again) = b.receive(Duration::from_secs(2))? else {
        panic!("B must recover its retry")
    };
    b.settle(
        message_again.settlement().expect("new B token"),
        DeliveryDisposition::Accept,
    )?;
    assert_eq!(fixture.pending_owner(message_a.id().as_str())?, None);
    a.close()?;
    b.close()?;
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_unknown_unapplied_settlement_retries_with_same_owner() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::start()?;
    let proxy = ControlledRedis::start(fixture.server.url())?;
    let inner = RedisEventBusProvider
        .create_configured(&fixture.config(&proxy.url()))
        .map_err(|e| e.into_error())?;
    let bus = EventBus::with_config(
        ProviderId::new("redis-streams")?,
        inner.clone(),
        facade_config()?,
    )?;
    let mut a = inner.subscribe(fixture.consumer_request("consumer-a", ProviderOptions::new())?)?;
    let receipt = bus.publish(PublishRequest::new(
        Topic::new(TOPIC)?,
        "unknown-same-owner".to_owned(),
    )?)?;
    let ReceiveOutcome::Message(message) = a.receive(Duration::from_secs(2))? else {
        panic!("A receives event")
    };
    let token = message.settlement().expect("token");
    let owner = fixture
        .pending_owner(message.id().as_str())?
        .expect("PEL owner");
    proxy.drop_next_settlement_before_forward();
    let error = a
        .settle(token, DeliveryDisposition::Accept)
        .expect_err("first reply is lost");
    assert_eq!(error.kind(), "outcome_unknown");
    assert_eq!(
        fixture.pending_owner(message.id().as_str())?.as_deref(),
        Some(owner.as_str())
    );
    a.settle(token, DeliveryDisposition::Accept)?;
    assert_eq!(
        fixture.pending_owner(receipt.input_event_id().as_str())?,
        None
    );
    a.close()?;
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_settlement_ownership_changed_preserves_new_owner() -> Result<(), Box<dyn Error>> {
    block_on(async {
        for disposition in [DeliveryDisposition::Accept, DeliveryDisposition::Reject] {
            let fixture = Fixture::start()?;
            let inner = AsyncRedisEventBusProvider
                .create_configured(&fixture.config(fixture.server.url()))
                .await
                .map_err(|e| e.into_error())?;
            let bus = AsyncEventBus::with_config(
                ProviderId::new("redis-streams")?,
                inner.clone(),
                facade_config()?,
            )?;
            let mut a = inner
                .subscribe(fixture.consumer_request("consumer-a", ProviderOptions::new())?)
                .await?;
            let receipt = bus
                .publish(PublishRequest::new(
                    Topic::new(TOPIC)?,
                    "ownership".to_owned(),
                )?)
                .await?;
            let ReceiveOutcome::Message(message_a) = a.receive(Duration::from_secs(2)).await?
            else {
                panic!("consumer A must receive the new message")
            };
            let token_a = message_a.settlement().expect("token A");
            let owner_a = fixture
                .pending_owner(message_a.id().as_str())?
                .expect("A owns PEL entry");
            let options =
                ProviderOptions::from([("redis.existing_group_start".into(), "resume".into())]);
            let mut b = inner
                .subscribe(fixture.consumer_request("consumer-b", options)?)
                .await?;
            let ReceiveOutcome::Message(message_b) = b.receive(Duration::from_secs(2)).await?
            else {
                panic!("consumer B must claim A's pending message")
            };
            let token_b = message_b.settlement().expect("token B");
            assert_eq!(message_b.id(), receipt.input_event_id());
            let owner_b = fixture
                .pending_owner(message_a.id().as_str())?
                .expect("B owns PEL entry");
            assert_ne!(owner_a, owner_b);

            let error = a
                .settle(token_a, disposition)
                .await
                .expect_err("stale owner must not settle");
            assert_eq!(error.kind(), "settlement_ownership_changed");
            assert_eq!(error.retryable(), Some(false));
            assert_eq!(
                fixture.pending_owner(message_a.id().as_str())?.as_deref(),
                Some(owner_b.as_str())
            );
            b.settle(token_b, DeliveryDisposition::Retry).await?;
            let ReceiveOutcome::Message(message_again) = b.receive(Duration::from_secs(2)).await?
            else {
                panic!("consumer B must recover the retried message")
            };
            assert_eq!(message_again.id(), receipt.input_event_id());
            b.settle(
                message_again.settlement().expect("new B token"),
                DeliveryDisposition::Accept,
            )
            .await?;
            assert_eq!(fixture.pending_owner(message_a.id().as_str())?, None);
            a.close().await?;
            b.close().await?;
        }
        Ok::<(), Box<dyn Error>>(())
    })
}

#[cfg(feature = "async")]
#[test]
fn test_async_facade_stops_when_settlement_owner_changes() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let fixture = Fixture::start()?;
        let inner = AsyncRedisEventBusProvider
            .create_configured(&fixture.config(fixture.server.url()))
            .await
            .map_err(|e| e.into_error())?;
        let observation = Arc::new(Observation::default());
        let wrapped = Arc::new(AsyncObservedBus {
            inner: inner.clone(),
            observation: observation.clone(),
            fail_before_settle: false,
        });
        let bus = AsyncEventBus::with_config(
            ProviderId::new("redis-streams")?,
            wrapped,
            facade_config()?,
        )?;
        let mut subscription = bus.subscribe(fixture.request()?).await?;
        let receipt = bus
            .publish(PublishRequest::new(
                Topic::new(TOPIC)?,
                "async-facade-ownership".to_owned(),
            )?)
            .await?;
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let started_tx = Arc::new(std::sync::Mutex::new(Some(started_tx)));
        let release_rx = Arc::new(std::sync::Mutex::new(Some(release_rx)));
        let handler_started = started_tx.clone();
        let handler_release = release_rx.clone();
        let handler = move |_| {
            if let Some(signal) = handler_started.lock().expect("started lock").take() {
                let _ = signal.send(());
            }
            let wait = handler_release
                .lock()
                .expect("release lock")
                .take()
                .expect("one delivery");
            async move {
                wait.await
                    .map_err(|_| qubit_event_bus::error::DeliveryError::Handler {
                        source: Box::new(std::io::Error::other("test gate released")),
                    })?;
                Ok(())
            }
        };
        let competitor = async {
            started_rx
                .await
                .map_err(|_| "facade handler did not start")?;
            let options =
                ProviderOptions::from([("redis.existing_group_start".into(), "resume".into())]);
            let mut b = inner
                .subscribe(fixture.consumer_request("consumer-b", options)?)
                .await?;
            let ReceiveOutcome::Message(message_b) = b.receive(Duration::from_secs(2)).await?
            else {
                return Err("B did not claim blocked handler's message".into());
            };
            assert_eq!(message_b.id(), receipt.input_event_id());
            let owner_b = fixture
                .pending_owner(message_b.id().as_str())?
                .expect("B owns PEL entry");
            release_tx
                .send(())
                .map_err(|_| "could not release facade handler")?;
            b.close().await?;
            Ok::<String, Box<dyn Error>>(owner_b)
        };
        let (run_result, owner_b) =
            futures_lite::future::zip(subscription.run(handler), competitor).await;
        let owner_b = owner_b?;
        assert!(matches!(run_result, Err(ReceiveError::Stopped(_))));
        let reason = subscription
            .terminal_failure()
            .expect("facade retains terminal settlement cause");
        let qubit_event_bus::model::SubscriptionStopReason::Settlement { error, .. } =
            reason.as_ref()
        else {
            panic!("expected settlement stop reason, got {reason:?}");
        };
        assert_eq!(error.kind(), "settlement_ownership_changed");
        assert_eq!(observation.closes.load(Ordering::SeqCst), 1);
        assert_eq!(observation.underlying_settles.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture
                .pending_owner(receipt.input_event_id().as_str())?
                .as_deref(),
            Some(owner_b.as_str())
        );
        subscription.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[cfg(feature = "async")]
#[test]
fn test_async_unknown_settlement_keeps_intent_and_checks_later_owner() -> Result<(), Box<dyn Error>>
{
    block_on(async {
        let fixture = Fixture::start()?;
        let proxy = ControlledRedis::start(fixture.server.url())?;
        let inner = AsyncRedisEventBusProvider
            .create_configured(&fixture.config(&proxy.url()))
            .await
            .map_err(|e| e.into_error())?;
        let bus = AsyncEventBus::with_config(
            ProviderId::new("redis-streams")?,
            inner.clone(),
            facade_config()?,
        )?;
        let mut a = inner
            .subscribe(fixture.consumer_request("consumer-a", ProviderOptions::new())?)
            .await?;
        let receipt = bus
            .publish(PublishRequest::new(
                Topic::new(TOPIC)?,
                "unknown-owner".to_owned(),
            )?)
            .await?;
        let ReceiveOutcome::Message(message_a) = a.receive(Duration::from_secs(2)).await? else {
            panic!("consumer A must receive the new message")
        };
        let owner_a = fixture
            .pending_owner(message_a.id().as_str())?
            .expect("A owns PEL entry");
        let token_a = message_a.settlement().expect("token A");
        proxy.drop_next_settlement_before_forward();
        let unknown = a
            .settle(token_a, DeliveryDisposition::Accept)
            .await
            .expect_err("proxy loses settlement before Redis");
        assert_eq!(unknown.kind(), "outcome_unknown");
        assert_eq!(
            fixture.pending_owner(message_a.id().as_str())?.as_deref(),
            Some(owner_a.as_str())
        );

        let options =
            ProviderOptions::from([("redis.existing_group_start".into(), "resume".into())]);
        let mut b = inner
            .subscribe(fixture.consumer_request("consumer-b", options)?)
            .await?;
        let ReceiveOutcome::Message(message_b) = b.receive(Duration::from_secs(2)).await? else {
            panic!("consumer B must claim A's pending message")
        };
        assert_eq!(message_b.id(), receipt.input_event_id());
        let owner_b = fixture
            .pending_owner(message_a.id().as_str())?
            .expect("B owns PEL entry");
        assert_ne!(owner_a, owner_b);
        let changed = a
            .settle(token_a, DeliveryDisposition::Accept)
            .await
            .expect_err("same intent must recheck current owner");
        assert_eq!(changed.kind(), "settlement_ownership_changed");
        assert_eq!(changed.retryable(), Some(false));
        assert_eq!(
            fixture.pending_owner(message_a.id().as_str())?.as_deref(),
            Some(owner_b.as_str())
        );
        b.settle(
            message_b.settlement().expect("token B"),
            DeliveryDisposition::Retry,
        )
        .await?;
        let ReceiveOutcome::Message(message_again) = b.receive(Duration::from_secs(2)).await?
        else {
            panic!("B must recover its retry")
        };
        b.settle(
            message_again.settlement().expect("new B token"),
            DeliveryDisposition::Accept,
        )
        .await?;
        assert_eq!(fixture.pending_owner(message_a.id().as_str())?, None);
        a.close().await?;
        b.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[cfg(feature = "async")]
#[test]
fn test_async_unknown_unapplied_settlement_retries_with_same_owner() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let fixture = Fixture::start()?;
        let proxy = ControlledRedis::start(fixture.server.url())?;
        let inner = AsyncRedisEventBusProvider
            .create_configured(&fixture.config(&proxy.url()))
            .await
            .map_err(|e| e.into_error())?;
        let bus = AsyncEventBus::with_config(
            ProviderId::new("redis-streams")?,
            inner.clone(),
            facade_config()?,
        )?;
        let mut a = inner
            .subscribe(fixture.consumer_request("consumer-a", ProviderOptions::new())?)
            .await?;
        let receipt = bus
            .publish(PublishRequest::new(
                Topic::new(TOPIC)?,
                "unknown-same-owner".to_owned(),
            )?)
            .await?;
        let ReceiveOutcome::Message(message) = a.receive(Duration::from_secs(2)).await? else {
            panic!("A receives event")
        };
        let token = message.settlement().expect("token");
        let owner = fixture
            .pending_owner(message.id().as_str())?
            .expect("PEL owner");
        proxy.drop_next_settlement_before_forward();
        let error = a
            .settle(token, DeliveryDisposition::Accept)
            .await
            .expect_err("first reply is lost");
        assert_eq!(error.kind(), "outcome_unknown");
        assert_eq!(
            fixture.pending_owner(message.id().as_str())?.as_deref(),
            Some(owner.as_str())
        );
        a.settle(token, DeliveryDisposition::Accept).await?;
        assert_eq!(
            fixture.pending_owner(receipt.input_event_id().as_str())?,
            None
        );
        a.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_retry_after_applied_xack_lost_reply() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::start()?;
    let proxy = ControlledRedis::start(fixture.server.url())?;
    let gate = proxy.pause_after_settlement_reply();
    let inner = RedisEventBusProvider
        .create_configured(&fixture.config(&proxy.url()))
        .map_err(|e| e.into_error())?;
    let observation = Arc::new(Observation::default());
    let wrapped = Arc::new(SyncObservedBus {
        inner,
        observation: observation.clone(),
        fail_before_settle: false,
    });
    let bus = EventBus::with_config(ProviderId::new("redis-streams")?, wrapped, facade_config()?)?;
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let subscription = bus.subscribe(fixture.request()?, move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
    })?;
    let _cancel = CancelOnDrop(&subscription);
    // Release the gate before cancellation even if an assertion unwinds.
    struct ReleaseGate(Arc<dyn Fn() + Send + Sync>);
    impl Drop for ReleaseGate {
        fn drop(&mut self) {
            (self.0)();
        }
    }
    let release = gate.clone();
    let _release = ReleaseGate(Arc::new(move || release.release_without_reply()));
    let _ = bus.publish(PublishRequest::new(
        Topic::new(TOPIC)?,
        "acknowledged".to_owned(),
    )?)?;
    assert!(
        gate.wait_until_reached(WATCHDOG),
        "real Redis settlement EVAL must reach the gate"
    );
    assert_eq!(
        fixture.pending()?,
        0,
        "XACK applied before any reply is delivered"
    );
    gate.release_without_reply();
    wait_until(|| observation.settled());
    subscription.cancel()?;
    assert_retry(&observation);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(subscription.terminal_failure().is_none());
    assert_eq!(fixture.pending()?, 0);
    Ok(())
}

#[cfg(feature = "async")]
async fn watchdog() {
    let deadline = Instant::now() + WATCHDOG;
    while Instant::now() < deadline {
        yield_now().await;
    }
    panic!("asynchronous settlement watchdog expired");
}

#[cfg(feature = "async")]
#[test]
fn test_async_terminal_settlement_retains_unacked_redis_entry() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let fixture = Fixture::start()?;
        let inner = AsyncRedisEventBusProvider
            .create_configured(&fixture.config(fixture.server.url()))
            .await
            .map_err(|e| e.into_error())?;
        let observation = Arc::new(Observation::default());
        let wrapped = Arc::new(AsyncObservedBus {
            inner: inner.clone(),
            observation: observation.clone(),
            fail_before_settle: true,
        });
        let bus = AsyncEventBus::with_config(
            ProviderId::new("redis-streams")?,
            wrapped,
            facade_config()?,
        )?;
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let mut subscription = bus.subscribe(fixture.request()?).await?;
        let receipt = bus
            .publish(PublishRequest::new(
                Topic::new(TOPIC)?,
                "durable".to_owned(),
            )?)
            .await?;
        let result = race(
            subscription.run(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            }),
            async {
                watchdog().await;
                unreachable!()
            },
        )
        .await;
        assert!(matches!(result, Err(ReceiveError::Stopped(_))));
        let reason = subscription
            .terminal_failure()
            .expect("natural stop retains terminal reason");
        assert_terminal(&reason, receipt.input_event_id(), &observation);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.pending()?, 1);
        let mut recovered = inner.subscribe(fixture.recovery_request()?).await?;
        let ReceiveOutcome::Message(message) = recovered.receive(Duration::from_secs(2)).await?
        else {
            panic!("closed owner's entry must be claimable by the new consumer")
        };
        assert_eq!(message.id(), receipt.input_event_id());
        recovered
            .settle(
                message.settlement().expect("Redis settlement token"),
                DeliveryDisposition::Accept,
            )
            .await?;
        assert_eq!(fixture.pending()?, 0);
        recovered.close().await?;
        subscription.close().await?;
        assert!(Arc::ptr_eq(
            &reason,
            &subscription
                .terminal_failure()
                .expect("first cause survives close")
        ));
        Ok(())
    })
}

#[cfg(feature = "async")]
#[test]
fn test_async_retry_after_applied_xack_lost_reply() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let fixture = Fixture::start()?;
        let proxy = ControlledRedis::start(fixture.server.url())?;
        let gate = proxy.pause_after_settlement_reply();
        let inner = AsyncRedisEventBusProvider
            .create_configured(&fixture.config(&proxy.url()))
            .await
            .map_err(|e| e.into_error())?;
        let observation = Arc::new(Observation::default());
        let wrapped = Arc::new(AsyncObservedBus {
            inner,
            observation: observation.clone(),
            fail_before_settle: false,
        });
        let bus = AsyncEventBus::with_config(
            ProviderId::new("redis-streams")?,
            wrapped,
            facade_config()?,
        )?;
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let mut subscription = bus.subscribe(fixture.request()?).await?;
        let _ = bus
            .publish(PublishRequest::new(
                Topic::new(TOPIC)?,
                "acknowledged".to_owned(),
            )?)
            .await?;
        let observe = async {
            gate.wait_applied().await;
            assert_eq!(
                fixture.pending()?,
                0,
                "server PEL cleared while the reply remains gated"
            );
            gate.release_without_reply();
            while !observation.settled() {
                yield_now().await;
            }
            Ok::<(), Box<dyn Error>>(())
        };
        let run = async {
            subscription
                .run(move |_| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    async { Ok(()) }
                })
                .await?;
            Err::<(), Box<dyn Error>>("runner unexpectedly stopped before successful retry".into())
        };
        race(race(run, observe), async {
            watchdog().await;
            unreachable!()
        })
        .await?;
        subscription.close().await?;
        assert_retry(&observation);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(subscription.terminal_failure().is_none());
        assert_eq!(fixture.pending()?, 0);
        Ok(())
    })
}
