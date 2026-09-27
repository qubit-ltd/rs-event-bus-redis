// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Verifies synchronous SPI behavior against a disposable Redis service.

#![cfg(feature = "sync")]

mod support;

use std::any::TypeId;
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
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::ConformanceHooks;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::run_sync;
use qubit_event_bus_redis::naming::stream_key;
use qubit_event_bus_redis::sync::RedisEventBusProvider;
use qubit_event_bus_redis::wire::WireFields;
use qubit_id::Id;
use qubit_spi::ServiceProvider;
use redis::Client;
use redis::cmd;
use support::redis_server::RedisServer;

static SUBSCRIPTION_IDS: AtomicU64 = AtomicU64::new(1);

#[test]
fn test_sync_close_makes_future_receives_return_closed() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    let mut subscription = bus.subscribe(request(
        "close-events",
        "close-worker",
        Some("close-group"),
        StartPosition::Earliest,
    )?)?;

    subscription.close()?;
    assert!(matches!(subscription.receive(Duration::ZERO)?, ReceiveOutcome::Closed));
    Ok(())
}

#[test]
fn test_sync_approximate_stream_limit_trims_old_entries_when_enabled() -> Result<(), Box<dyn std::error::Error>> {
    use qubit_spi::ServiceProvider;

    let server = RedisServer::start()?;
    let options: ProviderOptions = [
        ("redis.url".into(), server.url().into()),
        ("redis.namespace".into(), "sync-limit-tests".into()),
        ("redis.stream_maxlen_approx".into(), "10".into()),
    ]
    .into();
    let bus = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(options))
        .map_err(|failure| failure.into_error())?;
    for index in 0..250 {
        bus.publish(message("trim-events", &format!("trim-{index}"), b"payload")?)?;
    }

    let mut connection = Client::open(server.url())?.get_connection()?;
    let length: usize = cmd("XLEN")
        .arg(stream_key("sync-limit-tests", "trim-events"))
        .query(&mut connection)?;
    assert!(length < 250, "approximate trim left {length} entries");
    Ok(())
}

#[test]
fn test_sync_stream_is_untrimmed_by_default() -> Result<(), Box<dyn std::error::Error>> {
    use qubit_spi::ServiceProvider;

    let server = RedisServer::start()?;
    let options: ProviderOptions = [
        ("redis.url".into(), server.url().into()),
        ("redis.namespace".into(), "sync-default-retention".into()),
    ]
    .into();
    let bus = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(options))
        .map_err(|failure| failure.into_error())?;
    for index in 0..250 {
        bus.publish(message("default-events", &format!("default-{index}"), b"payload")?)?;
    }

    let mut connection = Client::open(server.url())?.get_connection()?;
    let length: usize = cmd("XLEN")
        .arg(stream_key("sync-default-retention", "default-events"))
        .query(&mut connection)?;
    assert_eq!(length, 250);
    Ok(())
}

fn create_bus(server: &RedisServer) -> Result<Arc<dyn EventBusSpi>, Box<dyn std::error::Error>> {
    let options: ProviderOptions = [
        ("redis.url".into(), server.url().into()),
        ("redis.namespace".into(), "sync-tests".into()),
        ("redis.claim_min_idle_ms".into(), "0".into()),
        ("redis.max_unsettled_per_subscription".into(), "1".into()),
    ]
    .into();
    let config = EventBusConfig::default().with_provider_options(options);
    RedisEventBusProvider
        .create_configured(&config)
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

fn request(
    topic: &str,
    subscriber: &str,
    group: Option<&str>,
    position: StartPosition,
) -> Result<SpiSubscriptionRequest, Box<dyn std::error::Error>> {
    Ok(SpiSubscriptionRequest::new(
        Id::new(SUBSCRIPTION_IDS.fetch_add(1, Ordering::Relaxed)),
        TopicAddress::new(topic)?,
        SubscriberId::new(subscriber)?,
        group.map(ConsumerGroup::new).transpose()?,
        SubscriptionDurability::Durable,
        position,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    ))
}

#[test]
fn test_sync_publish_receive_and_accept() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    bus.publish(message("orders", "sync-1", &[0, 7, 128, 255])?)?;
    let mut receiver = bus.subscribe(request("orders", "consumer-a", None, StartPosition::Earliest)?)?;
    let ReceiveOutcome::Message(mut received) = receiver.receive(Duration::from_secs(2))? else {
        return Err("published record was not received".into());
    };
    let TransportPayload::Encoded(payload) = received.payload() else {
        return Err("provider did not return encoded bytes".into());
    };
    assert_eq!(payload.bytes(), &[0, 7, 128, 255]);
    let token = received.take_settlement().ok_or("message has no settlement token")?;
    receiver.settle(&token, DeliveryDisposition::Accept)?;
    receiver.settle(&token, DeliveryDisposition::Accept)?;
    assert!(receiver.settle(&token, DeliveryDisposition::Retry).is_err());
    receiver.close()?;
    Ok(())
}

#[test]
#[cfg(feature = "conformance")]
fn test_sync_spi_conformance() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let server = Arc::new(server);
    let report = run_sync(
        || create_bus(&server).expect("Redis provider should be created"),
        &ConformanceHooks::default(),
    );
    report.assert_all_passed();
    Ok(())
}

#[test]
fn test_sync_retry_remains_in_pending_entries() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    bus.publish(message("retries", "sync-retry", b"payload")?)?;
    let mut receiver = bus.subscribe(request("retries", "consumer-b", None, StartPosition::Earliest)?)?;
    let ReceiveOutcome::Message(first) = receiver.receive(Duration::from_secs(2))? else {
        return Err("published record was not received".into());
    };
    receiver.settle(first.settlement().ok_or("missing token")?, DeliveryDisposition::Retry)?;
    let ReceiveOutcome::Message(second) = receiver.receive(Duration::from_secs(2))? else {
        return Err("retried record was not delivered again".into());
    };
    let TransportPayload::Encoded(payload) = second.payload() else {
        return Err("encoded payload expected".into());
    };
    assert_eq!(payload.bytes(), b"payload");
    Ok(())
}

#[test]
fn test_sync_wire_timestamp_and_payload_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let event = message("metadata", "sync-meta", b"bytes")?;
    assert_eq!(WireFields::from_outbound(&event)?.version, 1);
    Ok(())
}

#[test]
fn test_sync_unsettled_message_is_claimed_after_consumer_reconnect() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    bus.publish(message("recovery", "sync-recovery", b"resume")?)?;
    let mut first = bus.subscribe(request(
        "recovery",
        "worker-one",
        Some("stable-group"),
        StartPosition::Earliest,
    )?)?;
    let ReceiveOutcome::Message(_) = first.receive(Duration::from_secs(2))? else {
        return Err("initial consumer did not receive the event".into());
    };
    first.close()?;
    let mut second = bus.subscribe(request(
        "recovery",
        "worker-two",
        Some("stable-group"),
        StartPosition::Earliest,
    )?)?;
    let ReceiveOutcome::Message(received) = second.receive(Duration::from_secs(2))? else {
        return Err("reconnected consumer did not claim the pending event".into());
    };
    let TransportPayload::Encoded(payload) = received.payload() else {
        return Err("encoded payload expected".into());
    };
    assert_eq!(payload.bytes(), b"resume");
    Ok(())
}

#[test]
fn test_sync_receiver_pauses_at_unsettled_limit() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    bus.publish(message("bounded", "sync-bound-1", b"one")?)?;
    bus.publish(message("bounded", "sync-bound-2", b"two")?)?;
    let mut receiver = bus.subscribe(request("bounded", "bounded-consumer", None, StartPosition::Earliest)?)?;
    let ReceiveOutcome::Message(first) = receiver.receive(Duration::from_secs(2))? else {
        return Err("first message was not received".into());
    };
    assert!(matches!(receiver.receive(Duration::ZERO)?, ReceiveOutcome::TimedOut));
    receiver.settle(
        first.settlement().ok_or("missing settlement token")?,
        DeliveryDisposition::Accept,
    )?;
    let ReceiveOutcome::Message(second) = receiver.receive(Duration::from_secs(2))? else {
        return Err("reading should resume after settlement frees the slot".into());
    };
    let TransportPayload::Encoded(payload) = second.payload() else {
        return Err("encoded payload expected".into());
    };
    assert_eq!(payload.bytes(), b"two");
    Ok(())
}

#[test]
fn test_sync_groups_fan_out_and_share_work() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    bus.publish(message("groups", "group-event-1", b"one")?)?;
    bus.publish(message("groups", "group-event-2", b"two")?)?;
    let mut worker_a = bus.subscribe(request("groups", "worker-a", Some("billing"), StartPosition::Earliest)?)?;
    let mut worker_b = bus.subscribe(request("groups", "worker-b", Some("billing"), StartPosition::Earliest)?)?;
    let mut audit = bus.subscribe(request("groups", "audit", Some("audit"), StartPosition::Earliest)?)?;
    let mut worker_ids = vec![];
    for receiver in [&mut worker_a, &mut worker_b] {
        let ReceiveOutcome::Message(message) = receiver.receive(Duration::from_secs(2))? else {
            return Err("billing group did not receive both events".into());
        };
        worker_ids.push(message.id().as_str().to_owned());
        receiver.settle(
            message.settlement().ok_or("missing settlement token")?,
            DeliveryDisposition::Accept,
        )?;
    }
    worker_ids.sort();
    assert_eq!(worker_ids, ["group-event-1", "group-event-2"]);
    let ReceiveOutcome::Message(audit_one) = audit.receive(Duration::from_secs(2))? else {
        return Err("audit group did not receive the first event".into());
    };
    assert_eq!(audit_one.id().as_str(), "group-event-1");
    audit.settle(
        audit_one.settlement().ok_or("missing settlement token")?,
        DeliveryDisposition::Accept,
    )?;
    let ReceiveOutcome::Message(audit_two) = audit.receive(Duration::from_secs(2))? else {
        return Err("audit group did not receive the second event".into());
    };
    assert_eq!(audit_two.id().as_str(), "group-event-2");
    audit.settle(
        audit_two.settlement().ok_or("missing settlement token")?,
        DeliveryDisposition::Accept,
    )?;
    Ok(())
}

#[test]
fn test_sync_replay_from_stream_position_and_new_tail() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    let first = bus.publish(message("positions", "position-1", b"one")?)?;
    let first_id = match first {
        PublishAcknowledgement::Accepted {
            provider_message_id: Some(id),
            ..
        } => id,
        _ => return Err("Redis publish did not return a stream ID".into()),
    };
    bus.publish(message("positions", "position-2", b"two")?)?;
    let mut at = bus.subscribe(request(
        "positions",
        "at-position",
        Some("position-group"),
        StartPosition::At(first_id.into()),
    )?)?;
    let ReceiveOutcome::Message(second) = at.receive(Duration::from_secs(2))? else {
        return Err("consumer at stream position did not receive a later event".into());
    };
    assert_eq!(second.id().as_str(), "position-2");
    at.settle(second.settlement().ok_or("missing token")?, DeliveryDisposition::Accept)?;
    assert!(matches!(at.receive(Duration::ZERO)?, ReceiveOutcome::TimedOut));

    let mut new = bus.subscribe(request("positions", "new-position", None, StartPosition::New)?)?;
    bus.publish(message("positions", "position-3", b"three")?)?;
    let ReceiveOutcome::Message(third) = new.receive(Duration::from_secs(2))? else {
        return Err("New consumer did not receive a new event".into());
    };
    assert_eq!(third.id().as_str(), "position-3");
    Ok(())
}

#[test]
fn test_sync_reports_gap_for_removed_pending_entries() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    bus.publish(message("gaps", "removed-event", b"payload")?)?;
    let mut first = bus.subscribe(request(
        "gaps",
        "gap-worker-one",
        Some("gap-group"),
        StartPosition::Earliest,
    )?)?;
    let ReceiveOutcome::Message(_) = first.receive(Duration::from_secs(2))? else {
        return Err("pending gap fixture was not received".into());
    };
    first.close()?;
    let mut connection = Client::open(server.url())?.get_connection()?;
    cmd("XTRIM")
        .arg(stream_key("sync-tests", "gaps"))
        .arg("MAXLEN")
        .arg(0)
        .query::<usize>(&mut connection)?;
    let mut second = bus.subscribe(request(
        "gaps",
        "gap-worker-two",
        Some("gap-group"),
        StartPosition::Earliest,
    )?)?;
    let outcome = second.receive(Duration::from_secs(1))?;
    assert!(matches!(outcome, ReceiveOutcome::Gap(_)));
    Ok(())
}

#[test]
fn test_sync_reject_acks_and_malformed_wire_is_quarantined() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    bus.publish(message("malformed", "reject-event", b"reject")?)?;
    let mut receiver = bus.subscribe(request(
        "malformed",
        "reject-worker",
        Some("reject-group"),
        StartPosition::Earliest,
    )?)?;
    let ReceiveOutcome::Message(rejected) = receiver.receive(Duration::from_secs(2))? else {
        return Err("reject test message was not received".into());
    };
    receiver.settle(
        rejected.settlement().ok_or("missing settlement token")?,
        DeliveryDisposition::Reject,
    )?;
    let mut connection = Client::open(server.url())?.get_connection()?;
    cmd("XADD")
        .arg(stream_key("sync-tests", "malformed"))
        .arg("*")
        .arg("other")
        .arg("value")
        .query::<String>(&mut connection)?;
    assert!(matches!(
        receiver.receive(Duration::from_secs(1))?,
        ReceiveOutcome::Gap(_)
    ));
    let stream = stream_key("sync-tests", "malformed");
    for wire in [
        "not-json",
        r#"{"version":999,"event_id":"event","timestamp_ms":0,"headers_json":"{}","ordering_key":null,"content_type":"application/octet-stream","schema_id":null,"payload":[]}"#,
        r#"{"version":1,"event_id":"","timestamp_ms":0,"headers_json":"{}","ordering_key":null,"content_type":"application/octet-stream","schema_id":null,"payload":[]}"#,
    ] {
        cmd("XADD")
            .arg(&stream)
            .arg("*")
            .arg("wire")
            .arg(wire)
            .query::<String>(&mut connection)?;
        assert!(matches!(
            receiver.receive(Duration::from_secs(1))?,
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
        receiver.receive(Duration::from_secs(1))?,
        ReceiveOutcome::Gap(_)
    ));
    Ok(())
}

#[test]
fn test_sync_redis_command_failures_are_returned_without_details() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    let native_message = OutboundMessage::new(
        TopicAddress::new("sync-native")?,
        EventId::new("sync-native-event")?,
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        None,
        TransportPayload::Native(Arc::new(7_u8)),
    );
    assert!(bus.publish(native_message).is_err());
    let key = stream_key("sync-tests", "wrong-type");
    let mut connection = Client::open(server.url())?.get_connection()?;
    cmd("SET").arg(&key).arg("not-a-stream").query::<()>(&mut connection)?;
    assert!(bus.publish(message("wrong-type", "failed-write", b"x")?).is_err());
    assert!(
        bus.subscribe(request(
            "wrong-type",
            "failed-subscribe",
            None,
            StartPosition::Earliest
        )?)
        .is_err()
    );
    let _: usize = cmd("DEL").arg(&key).query(&mut connection)?;
    let mut first = bus.subscribe(request(
        "wrong-type",
        "duplicate-group-worker",
        None,
        StartPosition::Earliest,
    )?)?;
    let mut second = bus.subscribe(request(
        "wrong-type",
        "duplicate-group-worker",
        None,
        StartPosition::Earliest,
    )?)?;
    first.close()?;
    second.close()?;

    bus.publish(message("failed-ack", "failed-ack-event", b"x")?)?;
    let mut receiver = bus.subscribe(request(
        "failed-ack",
        "failed-ack-worker",
        None,
        StartPosition::Earliest,
    )?)?;
    let ReceiveOutcome::Message(received) = receiver.receive(Duration::from_secs(2))? else {
        return Err("valid stream event was not received".into());
    };
    let key = stream_key("sync-tests", "failed-ack");
    cmd("SET").arg(key).arg("not-a-stream").query::<()>(&mut connection)?;
    let error = receiver
        .settle(
            received.settlement().ok_or("missing settlement token")?,
            DeliveryDisposition::Accept,
        )
        .unwrap_err();
    assert!(!error.to_string().contains("not-a-stream"));

    let offline_options: ProviderOptions = [
        ("redis.url".into(), "redis://127.0.0.1:1/".into()),
        ("redis.namespace".into(), "offline".into()),
    ]
    .into();
    let offline = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(offline_options))
        .map_err(|failure| failure.into_error())?;
    assert!(offline.publish(message("topic", "offline-publish", b"x")?).is_err());
    assert!(
        offline
            .subscribe(request("topic", "offline-subscribe", None, StartPosition::Earliest)?)
            .is_err()
    );
    Ok(())
}

#[test]
fn test_sync_client_builds_standalone_and_sentinel_authentication() -> Result<(), Box<dyn std::error::Error>> {
    let standalone: ProviderOptions = [
        ("redis.url".into(), "redis://127.0.0.1:1/".into()),
        ("redis.username_env".into(), "PATH".into()),
        ("redis.password_env".into(), "HOME".into()),
    ]
    .into();
    let standalone = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(standalone))
        .map_err(|failure| failure.into_error())?;
    assert!(standalone.publish(message("auth", "standalone-auth", b"x")?).is_err());

    let sentinel: ProviderOptions = [
        ("redis.sentinel.nodes".into(), "127.0.0.1:1".into()),
        ("redis.sentinel.service_name".into(), "primary".into()),
        ("redis.username_env".into(), "PATH".into()),
        ("redis.password_env".into(), "HOME".into()),
        ("redis.sentinel.username_env".into(), "PATH".into()),
        ("redis.sentinel.password_env".into(), "HOME".into()),
    ]
    .into();
    let sentinel = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(sentinel))
        .map_err(|failure| failure.into_error())?;
    assert!(sentinel.publish(message("auth", "sentinel-auth", b"x")?).is_err());
    Ok(())
}

#[test]
fn test_sync_recovers_pending_message_after_redis_restart() -> Result<(), Box<dyn std::error::Error>> {
    let mut server = RedisServer::start()?;
    let bus = create_bus(&server)?;
    bus.publish(message("restart", "sync-restart", b"durable")?)?;
    let mut first = bus.subscribe(request(
        "restart",
        "before-restart",
        Some("restart-group"),
        StartPosition::Earliest,
    )?)?;
    let ReceiveOutcome::Message(received) = first.receive(Duration::from_secs(2))? else {
        return Err("pre-restart event was not received".into());
    };
    assert_eq!(received.id().as_str(), "sync-restart");
    drop(first);

    server.restart()?;
    let mut recovered = bus.subscribe(request(
        "restart",
        "after-restart",
        Some("restart-group"),
        StartPosition::Earliest,
    )?)?;
    let ReceiveOutcome::Message(received) = recovered.receive(Duration::from_secs(3))? else {
        return Err("pending event was not recovered after Redis restart".into());
    };
    assert_eq!(received.id().as_str(), "sync-restart");
    recovered.settle(
        received.settlement().ok_or("recovered event has no settlement token")?,
        DeliveryDisposition::Accept,
    )?;
    Ok(())
}
