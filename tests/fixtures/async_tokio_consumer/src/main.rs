// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Exercises the provider using isolated production async features.

use std::any::TypeId;
use std::env::args;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::CodecError;
use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::model::Topic;
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
use qubit_event_bus_redis::RedisSubscriptionProfile;
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
use qubit_id::Id;
use qubit_spi::AsyncServiceProvider;
use tokio::main as tokio_main;

/// Reads the Redis URL from the first process argument and exercises public
/// SPI.
///
/// Returns success after real publish/receive/ACK/close, or
/// argument/configuration/ transport errors. Requires the host executor and
/// performs Redis network IO; assertions panic for incorrect delivery identity,
/// bytes, or redelivery behavior.
async fn exercise() -> Result<(), Box<dyn Error>> {
    let url = args().nth(1).ok_or("expected Redis URL")?;
    let options: ProviderOptions = [
        ("redis.url".into(), url),
        ("redis.namespace".into(), "isolated-runtime".into()),
        ("redis.max_unsettled_per_subscription".into(), "1".into()),
    ]
    .into();
    let bus = AsyncRedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(options))
        .await
        .map_err(|failure| failure.into_error())?;
    let topic = TopicAddress::new("runtime.events")?;
    let mut receiver = bus
        .subscribe(SpiSubscriptionRequest::new(
            Id::new(101),
            topic.clone(),
            SubscriberId::new("runtime-worker")?,
            Some(ConsumerGroup::new("runtime-group")?),
            SubscriptionDurability::Durable,
            StartPosition::Earliest,
            ProviderOptions::new(),
            TypeId::of::<Vec<u8>>(),
        ))
        .await?;
    let payload: Arc<[u8]> = Arc::from(b"independent production features".as_slice());
    let _ = bus
        .publish(OutboundMessage::new(
            topic,
            EventId::new("isolated-runtime-event")?,
            SystemTime::now(),
            Headers::new(),
            None,
            None,
            TransportPayload::Encoded(EncodedPayload::new(
                payload.clone(),
                ContentType::new("text/plain")?,
                None,
            )),
        ))
        .await?;
    let ReceiveOutcome::Message(received) = receiver.receive(Duration::from_secs(5)).await? else {
        return Err("provider did not deliver under the fixture executor".into());
    };
    assert_eq!(received.id().as_str(), "isolated-runtime-event");
    let TransportPayload::Encoded(encoded) = received.payload() else {
        return Err("expected encoded transport payload".into());
    };
    assert_eq!(encoded.bytes(), payload.as_ref());

    let _ = bus
        .publish(OutboundMessage::new(
            TopicAddress::new("runtime.events")?,
            EventId::new("isolated-runtime-second-event")?,
            SystemTime::now(),
            Headers::new(),
            None,
            None,
            TransportPayload::Encoded(EncodedPayload::new(
                Arc::from(b"second event after settlement".as_slice()),
                ContentType::new("text/plain")?,
                None,
            )),
        ))
        .await?;
    let progressed = Arc::new(AtomicBool::new(false));
    let task_progressed = Arc::clone(&progressed);
    let started = Instant::now();
    let waiting = receiver.receive(Duration::MAX);
    let (outcome, ()) = tokio::join!(waiting, async move {
        tokio::task::yield_now().await;
        task_progressed.store(true, Ordering::Release);
    });
    assert!(matches!(outcome?, ReceiveOutcome::TimedOut));
    assert!(
        started.elapsed() >= Duration::from_millis(5),
        "full-capacity receive should observe the bounded timer"
    );
    assert!(
        progressed.load(Ordering::Acquire),
        "a sibling async task must progress during full-capacity backoff"
    );
    receiver
        .settle(
            received.settlement().ok_or("missing settlement token")?,
            DeliveryDisposition::Accept,
        )
        .await?;
    let ReceiveOutcome::Message(second) = receiver.receive(Duration::from_secs(5)).await? else {
        return Err("second event was not received after settlement".into());
    };
    assert_eq!(second.id().as_str(), "isolated-runtime-second-event");
    receiver
        .settle(
            second
                .settlement()
                .ok_or("missing second settlement token")?,
            DeliveryDisposition::Accept,
        )
        .await?;
    assert!(matches!(
        receiver.receive(Duration::ZERO).await?,
        ReceiveOutcome::TimedOut
    ));
    receiver.close().await?;
    exercise_facade_gate(Arc::clone(&bus)).await?;
    println!("isolated publish/receive/ACK/close passed");
    Ok(())
}

/// Exercises facade-owned capacity greater than the provider's unsettled cap.
/// The first handler is held by an explicit gate while the facade and a sibling
/// future remain polled; releasing it must allow the second event and shutdown.
async fn exercise_facade_gate(provider: Arc<dyn AsyncEventBusSpi>) -> Result<(), Box<dyn Error>> {
    let topic = Topic::<String>::new("runtime.facade.events")?;
    let mut codecs = CodecRegistry::new();
    codecs.register::<String>(Arc::new(Utf8Codec(ContentType::new("text/plain")?)))?;
    let scheduling = DeliverySchedulingConfig::new(
        std::num::NonZeroUsize::new(2).expect("positive handler limit"),
        std::num::NonZeroUsize::new(8).expect("positive owned limit"),
        std::num::NonZeroUsize::new(4).expect("positive subscription-owned limit"),
        std::num::NonZeroUsize::new(1).expect("positive subscription limit"),
    )?;
    let facade = AsyncEventBus::with_config(
        ProviderId::new("runtime-facade")?,
        provider,
        EventBusFacadeConfig::new()
            .with_codec_registry(Arc::new(codecs))
            .with_delivery_scheduling(scheduling),
    )?;
    let profile = RedisSubscriptionProfile::new(StartPosition::Earliest)
        .consumer_group(ConsumerGroup::new("runtime-facade-group")?);
    let subscription = facade
        .subscribe(
            SubscribeRequest::new("runtime-facade-worker", topic.clone())?
                .with_options(profile.options::<String>().build()),
        )
        .await?;
    let _ = facade
        .publish(PublishRequest::new(
            topic.clone(),
            "facade-first".to_owned(),
        )?)
        .await?;
    let _ = facade
        .publish(PublishRequest::new(topic, "facade-second".to_owned())?)
        .await?;

    let first_started = Arc::new(AtomicBool::new(false));
    let release_first = Arc::new(AtomicBool::new(false));
    let sibling_progress = Arc::new(AtomicUsize::new(0));
    let second_seen = Arc::new(AtomicBool::new(false));
    let (handler_started, handler_gate, handler_second) = (
        Arc::clone(&first_started),
        Arc::clone(&release_first),
        Arc::clone(&second_seen),
    );
    let run = subscription.run(move |delivery| {
        let (handler_started, handler_gate, handler_second) = (
            Arc::clone(&handler_started),
            Arc::clone(&handler_gate),
            Arc::clone(&handler_second),
        );
        let payload = delivery.payload().clone();
        async move {
            if payload == "facade-first" {
                handler_started.store(true, Ordering::Release);
                while !handler_gate.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            } else if payload == "facade-second" {
                handler_second.store(true, Ordering::Release);
            } else {
                return Err(CodecError::Decode {
                    source: Box::new(std::io::Error::other("unexpected facade payload")),
                }
                .into());
            }
            Ok(())
        }
    });
    let driver_gate = Arc::clone(&release_first);
    let driver_started = Arc::clone(&first_started);
    let driver_progress = Arc::clone(&sibling_progress);
    let driver_second = Arc::clone(&second_seen);
    let facade_for_shutdown = facade.clone();
    let driver = async move {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !driver_started.load(Ordering::Acquire) {
            if Instant::now() >= deadline {
                let _ = facade_for_shutdown
                    .shutdown(ShutdownMode::Immediate)
                    .await?;
                return Err("facade first handler did not enter before watchdog".into());
            }
            tokio::task::yield_now().await;
        }
        let release_at = Instant::now() + Duration::from_millis(50);
        while Instant::now() < release_at {
            if Instant::now() >= deadline {
                driver_gate.store(true, Ordering::Release);
                let _ = facade_for_shutdown
                    .shutdown(ShutdownMode::Immediate)
                    .await?;
                return Err("facade gate window exceeded watchdog".into());
            }
            driver_progress.fetch_add(1, Ordering::Relaxed);
            tokio::task::yield_now().await;
        }
        driver_gate.store(true, Ordering::Release);
        while !driver_second.load(Ordering::Acquire) {
            if Instant::now() >= deadline {
                let report = facade_for_shutdown
                    .shutdown(ShutdownMode::Immediate)
                    .await?;
                let _ = report;
                return Err("second facade event did not complete before watchdog".into());
            }
            tokio::task::yield_now().await;
        }
        let report = facade_for_shutdown
            .shutdown(ShutdownMode::Graceful {
                timeout: Duration::from_secs(5),
            })
            .await?;
        if report.outcome != ShutdownOutcome::Complete {
            return Err("facade graceful shutdown did not complete".into());
        }
        Ok::<(), Box<dyn Error>>(())
    };
    let (run_result, driver_result) = tokio::join!(run, driver);
    run_result.map_err(|error| -> Box<dyn Error> { Box::new(error) })?;
    driver_result?;
    if sibling_progress.load(Ordering::Relaxed) == 0 {
        return Err("sibling future did not progress while the first handler was gated".into());
    }
    Ok(())
}

/// UTF-8 codec used to publish typed facade strings through Redis.
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

/// Runs the production consumer scenario on a Tokio current-thread runtime.
///
/// Reads the Redis URL from the first process argument. Returns success or
/// propagated argument/configuration/transport errors after real Redis IO.
#[tokio_main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    exercise().await
}
