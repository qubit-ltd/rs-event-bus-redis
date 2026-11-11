// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Verifies bounded receive behavior while locally unsettled capacity is full.

#![cfg(any(feature = "sync", feature = "async"))]

mod support;

use std::any::TypeId;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EncodedPayload;
#[cfg(feature = "sync")]
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
#[cfg(feature = "async")]
use qubit_event_bus_redis::naming::group_name;
#[cfg(feature = "async")]
use qubit_event_bus_redis::naming::stream_key;
#[cfg(feature = "sync")]
use qubit_event_bus_redis::sync::RedisEventBusProvider;
use qubit_id::Id;
#[cfg(feature = "sync")]
use qubit_spi::ServiceProvider;
#[cfg(feature = "async")]
use redis::Client;
#[cfg(feature = "async")]
use redis::cmd;
use support::redis_server::RedisServer;

static SUBSCRIPTION_IDS: AtomicU64 = AtomicU64::new(1);

#[cfg(feature = "sync")]
#[test]
fn test_sync_full_capacity_returns_within_bounded_time_and_can_settle() -> Result<(), Box<dyn Error>> {
    use std::time::Instant;

    let server = RedisServer::start()?;
    let bus = create_bus(server.url(), "sync-full-capacity")?;
    publish_two(bus.as_ref(), "sync-full-capacity")?;
    let mut receiver = bus.subscribe(request("sync-full-capacity", "sync-worker")?)?;
    let (result_sender, result_receiver) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let result = (|| -> Result<(), String> {
            let ReceiveOutcome::Message(first) = receiver
                .receive(Duration::from_secs(1))
                .map_err(|error| error.to_string())?
            else {
                return Err("first event was not received".into());
            };
            let token = first.settlement().ok_or("missing first settlement token")?;
            let started = Instant::now();
            if !matches!(
                receiver.receive(Duration::MAX).map_err(|error| error.to_string())?,
                ReceiveOutcome::TimedOut
            ) {
                return Err("full-capacity receive did not return TimedOut".into());
            }
            if started.elapsed() >= Duration::from_millis(250) {
                return Err("full-capacity receive exceeded watchdog".into());
            }
            let short_started = Instant::now();
            if !matches!(
                receiver
                    .receive(Duration::from_millis(2))
                    .map_err(|error| error.to_string())?,
                ReceiveOutcome::TimedOut
            ) {
                return Err("short full-capacity receive did not return TimedOut".into());
            }
            let short_elapsed = short_started.elapsed();
            if short_elapsed < Duration::from_millis(1) || short_elapsed >= Duration::from_millis(250) {
                return Err("short full-capacity receive did not honor its bounded delay".into());
            }
            let finite_started = Instant::now();
            if !matches!(
                receiver
                    .receive(Duration::from_secs(1))
                    .map_err(|error| error.to_string())?,
                ReceiveOutcome::TimedOut
            ) {
                return Err("finite full-capacity receive did not return TimedOut".into());
            }
            if finite_started.elapsed() >= Duration::from_millis(250) {
                return Err("full-capacity receive waited for the full finite timeout".into());
            }
            receiver
                .settle(token, DeliveryDisposition::Accept)
                .map_err(|error| error.to_string())?;
            let ReceiveOutcome::Message(second) = receiver
                .receive(Duration::from_secs(1))
                .map_err(|error| error.to_string())?
            else {
                return Err("second event was not received after settlement".into());
            };
            receiver
                .settle(
                    second.settlement().ok_or("missing second settlement token")?,
                    DeliveryDisposition::Accept,
                )
                .map_err(|error| error.to_string())?;
            let third_message = message_for("sync-full-capacity", "third").map_err(|error| error.to_string())?;
            let _ = bus.publish(third_message).map_err(|error| error.to_string())?;
            let ReceiveOutcome::Message(third) = receiver
                .receive(Duration::from_secs(1))
                .map_err(|error| error.to_string())?
            else {
                return Err("third event was not received".into());
            };
            receiver.close().map_err(|error| error.to_string())?;
            if !matches!(
                receiver.receive(Duration::MAX).map_err(|error| error.to_string())?,
                ReceiveOutcome::Closed
            ) {
                return Err("closed receiver must return Closed even with unsettled capacity".into());
            }
            drop(third);
            Ok(())
        })();
        let _ = result_sender.send(result);
    });
    let result = result_receiver
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| "sync receive worker exceeded watchdog")?;
    worker.join().map_err(|_| "sync receive worker panicked")?;
    result.map_err(|error| -> Box<dyn Error> { error.into() })?;
    Ok(())
}

#[cfg(feature = "async")]
#[test]
fn test_async_full_capacity_receive_is_pending_before_short_timeout() -> Result<(), Box<dyn Error>> {
    use futures_lite::future::block_on;
    use futures_lite::future::poll_once;
    use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
    use qubit_spi::AsyncServiceProvider;

    let server = RedisServer::start()?;
    block_on(async {
        let options: ProviderOptions = [
            ("redis.url".into(), server.url().into()),
            ("redis.namespace".into(), "async-full-capacity".into()),
            ("redis.claim_min_idle_ms".into(), "0".into()),
            ("redis.max_unsettled_per_subscription".into(), "1".into()),
        ]
        .into();
        let bus = AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(options))
            .await
            .map_err(|failure| failure.into_error())?;
        let topic = TopicAddress::new("async-full-capacity")?;
        for id in ["first", "second"] {
            let payload: Arc<[u8]> = Arc::from(id.as_bytes());
            let _ = bus
                .publish(OutboundMessage::new(
                    topic.clone(),
                    EventId::new(id)?,
                    SystemTime::now(),
                    Headers::new(),
                    None,
                    None,
                    TransportPayload::Encoded(EncodedPayload::new(payload, ContentType::new("text/plain")?, None)),
                ))
                .await?;
        }
        let mut receiver = bus
            .subscribe(SpiSubscriptionRequest::new(
                Id::new(SUBSCRIPTION_IDS.fetch_add(1, Ordering::Relaxed)),
                topic,
                SubscriberId::new("async-worker")?,
                Some(ConsumerGroup::new("async-full-capacity")?),
                SubscriptionDurability::Durable,
                StartPosition::Earliest,
                ProviderOptions::new(),
                TypeId::of::<Vec<u8>>(),
            ))
            .await?;
        let ReceiveOutcome::Message(first) = receiver.receive(Duration::from_secs(1)).await? else {
            return Err("first event was not received".into());
        };

        assert_eq!(
            1,
            pending_count(
                server.url(),
                "async-full-capacity",
                "async-full-capacity",
                "async-worker"
            )?
        );
        let mut waiting = receiver.receive(Duration::MAX);
        assert!(
            poll_once(&mut waiting).await.is_none(),
            "full-capacity receive must suspend"
        );
        drop(waiting);
        assert_eq!(
            1,
            pending_count(
                server.url(),
                "async-full-capacity",
                "async-full-capacity",
                "async-worker"
            )?
        );
        use std::time::Instant;
        let short_started = Instant::now();
        assert!(matches!(
            receiver.receive(Duration::from_millis(2)).await?,
            ReceiveOutcome::TimedOut
        ));
        let short_elapsed = short_started.elapsed();
        assert!(
            (Duration::from_millis(1)..Duration::from_millis(250)).contains(&short_elapsed),
            "short full-capacity receive did not honor its bounded delay: {short_elapsed:?}"
        );
        let finite_started = Instant::now();
        assert!(matches!(
            receiver.receive(Duration::from_secs(1)).await?,
            ReceiveOutcome::TimedOut
        ));
        assert!(
            finite_started.elapsed() < Duration::from_millis(250),
            "full-capacity receive waited for the full finite timeout"
        );
        assert!(matches!(
            receiver.receive(Duration::ZERO).await?,
            ReceiveOutcome::TimedOut
        ));
        receiver
            .settle(
                first.settlement().ok_or("missing first settlement token")?,
                DeliveryDisposition::Accept,
            )
            .await?;
        let ReceiveOutcome::Message(second) = receiver.receive(Duration::from_secs(1)).await? else {
            return Err("second event was not received after settlement".into());
        };
        receiver
            .settle(
                second.settlement().ok_or("missing second settlement token")?,
                DeliveryDisposition::Accept,
            )
            .await?;
        let _ = bus
            .publish(OutboundMessage::new(
                TopicAddress::new("async-full-capacity")?,
                EventId::new("third")?,
                SystemTime::now(),
                Headers::new(),
                None,
                None,
                TransportPayload::Encoded(EncodedPayload::new(
                    Arc::from(b"third event".as_slice()),
                    ContentType::new("text/plain")?,
                    None,
                )),
            ))
            .await?;
        let ReceiveOutcome::Message(third) = receiver.receive(Duration::from_secs(1)).await? else {
            return Err("third event was not received".into());
        };
        receiver.close().await?;
        assert!(matches!(receiver.receive(Duration::MAX).await?, ReceiveOutcome::Closed));
        drop(third);
        Ok::<(), Box<dyn Error>>(())
    })
}

#[cfg(feature = "sync")]
fn create_bus(url: &str, namespace: &str) -> Result<Arc<dyn EventBusSpi>, Box<dyn Error>> {
    let options: ProviderOptions = [
        ("redis.url".into(), url.into()),
        ("redis.namespace".into(), namespace.into()),
        ("redis.claim_min_idle_ms".into(), "0".into()),
        ("redis.max_unsettled_per_subscription".into(), "1".into()),
    ]
    .into();
    RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(options))
        .map_err(|failure| failure.into_error())
        .map_err(Into::into)
}

#[cfg(feature = "sync")]
fn message_for(topic: &str, id: &str) -> Result<OutboundMessage, Box<dyn Error>> {
    Ok(OutboundMessage::new(
        TopicAddress::new(topic)?,
        EventId::new(id)?,
        SystemTime::now(),
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(id.as_bytes()),
            ContentType::new("text/plain")?,
            None,
        )),
    ))
}

#[cfg(feature = "async")]
fn pending_count(url: &str, namespace: &str, topic: &str, subscriber: &str) -> Result<usize, Box<dyn Error>> {
    let mut connection = Client::open(url)?.get_connection()?;
    let (count, _, _, _): (usize, String, String, Vec<(String, usize)>) = cmd("XPENDING")
        .arg(stream_key(namespace, topic))
        .arg(group_name(namespace, topic, subscriber, Some("async-full-capacity")))
        .query(&mut connection)?;
    Ok(count)
}

#[cfg(feature = "sync")]
fn publish_two(bus: &dyn EventBusSpi, topic: &str) -> Result<(), Box<dyn Error>> {
    for id in ["first", "second"] {
        let payload: Arc<[u8]> = Arc::from(id.as_bytes());
        let _ = bus.publish(OutboundMessage::new(
            TopicAddress::new(topic)?,
            EventId::new(id)?,
            SystemTime::now(),
            Headers::new(),
            None,
            None,
            TransportPayload::Encoded(EncodedPayload::new(payload, ContentType::new("text/plain")?, None)),
        ))?;
    }
    Ok(())
}

#[cfg(feature = "sync")]
fn request(topic: &str, subscriber: &str) -> Result<SpiSubscriptionRequest, Box<dyn Error>> {
    Ok(SpiSubscriptionRequest::new(
        Id::new(SUBSCRIPTION_IDS.fetch_add(1, Ordering::Relaxed)),
        TopicAddress::new(topic)?,
        SubscriberId::new(subscriber)?,
        Some(ConsumerGroup::new(subscriber)?),
        SubscriptionDurability::Durable,
        StartPosition::Earliest,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    ))
}
