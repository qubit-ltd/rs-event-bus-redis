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
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;

use futures_lite::future::block_on;
use qubit_event_bus::EventBusConfig;
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
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::ConformanceHooks;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::run_async;
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
use qubit_event_bus_redis::naming::stream_key;
use qubit_spi::AsyncServiceProvider;
use support::redis_server::RedisServer;

static SUBSCRIPTION_IDS: AtomicU64 = AtomicU64::new(100);

fn create_bus(server: &RedisServer) -> Result<Arc<dyn AsyncEventBusSpi>, Box<dyn std::error::Error>> {
    let options: ProviderOptions = [
        ("redis.url".into(), server.url().into()),
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
        qubit_id::Id::new(SUBSCRIPTION_IDS.fetch_add(1, Ordering::Relaxed)),
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
    let hooks = ConformanceHooks::default();
    let report = block_on(run_async(
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
    ));
    report.assert_all_passed();
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
    let bus = create_bus(&server)?;
    block_on(async {
        bus.publish(message("async-events", "cancel-1", b"recover")?).await?;
        let mut receiver = bus.subscribe(request("async-events", "worker-c")?).await?;
        drop(receiver.receive(Duration::from_secs(1)));
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
            qubit_event_bus::model::PublishAcknowledgement::Accepted {
                provider_message_id: Some(id),
                ..
            } => id,
            _ => return Err("Redis publish did not return a stream ID".into()),
        };
        bus.publish(message("async-positions", "async-position-2", b"two")?)
            .await?;
        let mut at = bus
            .subscribe(SpiSubscriptionRequest::new(
                qubit_id::Id::new(SUBSCRIPTION_IDS.fetch_add(1, Ordering::Relaxed)),
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
                qubit_id::Id::new(SUBSCRIPTION_IDS.fetch_add(1, Ordering::Relaxed)),
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
        let mut connection = redis::Client::open(server.url())?.get_connection()?;
        redis::cmd("XTRIM")
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
fn test_async_reject_acks_and_malformed_wire_is_reported() -> Result<(), Box<dyn std::error::Error>> {
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
        let mut connection = redis::Client::open(server.url())?.get_connection()?;
        redis::cmd("XADD")
            .arg(stream_key("async-tests", "async-malformed"))
            .arg("*")
            .arg("other")
            .arg("value")
            .query::<String>(&mut connection)?;
        assert!(receiver.receive(Duration::from_secs(1)).await.is_err());
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[test]
fn test_async_redis_command_failures_are_returned_without_details() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    block_on(async {
        let key = stream_key("async-tests", "async-wrong-type");
        let mut connection = redis::Client::open(server.url())?.get_connection()?;
        redis::cmd("SET")
            .arg(&key)
            .arg("not-a-stream")
            .query::<()>(&mut connection)?;
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

        bus.publish(message("async-failed-ack", "failed-ack-event", b"x")?)
            .await?;
        let mut receiver = bus
            .subscribe(group_request("async-failed-ack", "failed-ack-worker", "group"))
            .await?;
        let ReceiveOutcome::Message(received) = receiver.receive(Duration::from_secs(2)).await? else {
            return Err("valid stream event was not received".into());
        };
        let key = stream_key("async-tests", "async-failed-ack");
        redis::cmd("SET")
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

fn group_request(topic: &str, subscriber: &str, group: &str) -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        qubit_id::Id::new(SUBSCRIPTION_IDS.fetch_add(1, Ordering::Relaxed)),
        TopicAddress::new(topic).expect("static topic is valid"),
        SubscriberId::new(subscriber).expect("static subscriber is valid"),
        Some(ConsumerGroup::new(group).expect("static group is valid")),
        SubscriptionDurability::Durable,
        StartPosition::Earliest,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    )
}
