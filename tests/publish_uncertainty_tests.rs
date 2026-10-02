// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Publish evidence must distinguish pre-command failure from lost replies.
#![cfg(feature = "sync")]
mod support;
use std::sync::Arc;
use std::time::SystemTime;

use qubit_event_bus::CodecError;
use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::ReceiveError;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::DeadLetterEvent;
use qubit_event_bus::model::DuplicateRiskPolicy;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishEffect;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_event_bus_redis::sync::RedisEventBusProvider;
use qubit_event_bus_redis::wire::WireFields;
use qubit_id::Id;
use qubit_retry::RetryPolicy;
use qubit_spi::ServiceProvider;
use support::scripted_redis::ScriptedRedis;
use support::scripted_redis::Step;

/// Opens only the configured provider; network IO begins with publishing.
fn bus(url: &str) -> Arc<dyn EventBusSpi> {
    let options: ProviderOptions = [
        ("redis.url".into(), url.into()),
        ("redis.namespace".into(), "uncertainty".into()),
        ("redis.claim_min_idle_ms".into(), "0".into()),
    ]
    .into();
    RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(options))
        .unwrap()
}

/// Creates a stable encoded event so SPI tests bypass all facade limits.
fn message() -> OutboundMessage {
    OutboundMessage::new(
        TopicAddress::new("events").unwrap(),
        EventId::new("stable-id").unwrap(),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(&b"payload"[..]),
            ContentType::new("text/plain").unwrap(),
            None,
        )),
    )
}

#[test]
fn opening_connection_failure_has_no_admission() {
    let error = bus("redis://127.0.0.1:1/").publish(message()).unwrap_err();
    assert_eq!(error.publish_effect(), PublishEffect::NotAccepted);
}

#[test]
fn explicit_redis_rejection_has_no_admission() {
    let server = ScriptedRedis::start(vec![Step::reply("XADD", b"-WRONGTYPE wrong kind\r\n")]).unwrap();
    let error = bus(server.url()).publish(message()).unwrap_err();
    assert_eq!(error.publish_effect(), PublishEffect::NotAccepted);
    assert_eq!(error.kind(), "wrong_type");
    server.finish();
}

#[test]
fn type_conversion_failure_after_query_is_uncertain() {
    let server = ScriptedRedis::start(vec![Step::reply("XADD", b"*1\r\n:42\r\n")]).unwrap();
    let error = bus(server.url()).publish(message()).unwrap_err();
    assert_eq!(error.publish_effect(), PublishEffect::MayHaveBeenAccepted);
    assert_eq!(error.kind(), "outcome_unknown");
    server.finish();
}

#[test]
fn query_disconnect_is_uncertain() {
    let server = ScriptedRedis::start(vec![Step::disconnect("XADD", false)]).unwrap();
    let error = bus(server.url()).publish(message()).unwrap_err();
    assert_eq!(error.publish_effect(), PublishEffect::MayHaveBeenAccepted);
    server.finish();
}

/// Small deterministic codec used by the real facade retry tests.
struct BytesCodec(ContentType);
impl EventCodec<Vec<u8>> for BytesCodec {
    /// Returns the bytes MIME type.
    fn content_type(&self) -> &ContentType {
        &self.0
    }
    /// This test wire format carries no schema.
    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }
    /// Shares an owned copy of the supplied application bytes.
    fn encode(&self, value: &Vec<u8>) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from(value.as_slice()))
    }
    /// Copies compatible received bytes for the application.
    fn decode(&self, payload: &EncodedPayload) -> Result<Vec<u8>, CodecError> {
        Ok(payload.bytes().to_vec())
    }
}

/// Executes a real XADD, discards its reply, and observes actual stream
/// records.
fn applied_xadd_reply_loss(policy: DuplicateRiskPolicy) -> Result<(), Box<dyn std::error::Error>> {
    use qubit_event_bus::EventBus;
    use qubit_event_bus::codec::CodecRegistry;
    use qubit_event_bus::facade::EventBusFacadeConfig;
    use qubit_event_bus::model::ProviderId;
    use qubit_event_bus::model::PublishRequest;
    use qubit_event_bus::model::Topic;
    use qubit_event_bus_redis::naming::stream_key;
    use qubit_retry::RetryPolicy;
    use support::controlled_redis::proxy::ControlledRedis;
    use support::redis_server::RedisServer;
    let server = RedisServer::start()?;
    let proxy = ControlledRedis::start(server.url())?;
    let spi = bus(&proxy.url());
    let mut codecs = CodecRegistry::new();
    codecs.register::<Vec<u8>>(Arc::new(BytesCodec(ContentType::new("text/plain")?)))?;
    let facade = EventBus::with_config(
        ProviderId::new("redis-streams")?,
        spi,
        EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs)),
    )?;
    let gate = proxy.pause_after_reply("XADD");
    let worker = std::thread::spawn(move || {
        let request = PublishRequest::builder()
            .topic(Topic::<Vec<u8>>::new("events").unwrap())
            .payload(b"stable bytes".to_vec())
            .duplicate_risk_policy(policy)
            .retry_policy(RetryPolicy::builder().max_attempts(2).build().unwrap())
            .build()
            .unwrap();
        let id = request.envelope().id().clone();
        (id, facade.publish(request))
    });
    assert!(gate.wait_until_reached(std::time::Duration::from_secs(3)));
    let stream = stream_key("uncertainty", "events");
    let mut observer = redis::Client::open(server.url())?.get_connection()?;
    let applied: usize = redis::cmd("XLEN").arg(&stream).query(&mut observer)?;
    assert_eq!(applied, 1, "first XADD actually executed before reply loss");
    gate.release_without_reply();
    let (event_id, result) = worker.join().expect("publish worker completes");
    assert_eq!(result.unwrap_err().effect(), PublishEffect::MayHaveBeenAccepted);
    let records: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
        .arg(&stream)
        .arg("-")
        .arg("+")
        .query(&mut observer)?;
    assert_eq!(records.ids.len(), 1,);
    let wires: Vec<String> = records
        .ids
        .iter()
        .map(|entry| redis::from_redis_value(entry.map.get("wire").unwrap()).unwrap())
        .collect();
    for wire in &wires {
        let fields: WireFields = serde_json::from_str(wire)?;
        assert_eq!(fields.event_id, event_id.as_str());
    }
    assert!(
        wires.windows(2).all(|pair| pair[0] == pair[1]),
        "retry keeps EventId, timestamp, headers, and payload unchanged"
    );
    Ok(())
}

#[test]
fn applied_xadd_lost_reply_forbid_has_one_record() -> Result<(), Box<dyn std::error::Error>> {
    applied_xadd_reply_loss(DuplicateRiskPolicy::Forbid)
}

#[test]
fn applied_xadd_lost_reply_allow_duplicates_does_not_blindly_retry() -> Result<(), Box<dyn std::error::Error>> {
    applied_xadd_reply_loss(DuplicateRiskPolicy::AllowDuplicates)
}

#[cfg(feature = "async")]
#[test]
fn async_query_stages_preserve_admission_evidence() -> Result<(), Box<dyn std::error::Error>> {
    use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
    use qubit_spi::AsyncServiceProvider;
    futures_lite::future::block_on(async {
        for (response, expected) in [
            (b"-WRONGTYPE wrong kind\r\n".as_slice(), PublishEffect::NotAccepted),
            (b"*1\r\n:42\r\n".as_slice(), PublishEffect::MayHaveBeenAccepted),
        ] {
            let server = ScriptedRedis::start(vec![Step::reply("XADD", response)])?;
            let options: ProviderOptions = [("redis.url".into(), server.url().into())].into();
            let spi = AsyncRedisEventBusProvider
                .create_configured(&EventBusConfig::default().with_provider_options(options))
                .await
                .unwrap();
            let error = spi.publish(message()).await.unwrap_err();
            assert_eq!(error.publish_effect(), expected);
            server.finish();
        }
        Ok(())
    })
}

#[cfg(feature = "async")]
#[test]
fn async_applied_xadd_reply_loss_covers_both_duplicate_policies() -> Result<(), Box<dyn std::error::Error>> {
    use qubit_event_bus::AsyncEventBus;
    use qubit_event_bus::codec::CodecRegistry;
    use qubit_event_bus::facade::EventBusFacadeConfig;
    use qubit_event_bus::model::DuplicateRiskPolicy;
    use qubit_event_bus::model::ProviderId;
    use qubit_event_bus::model::PublishRequest;
    use qubit_event_bus::model::Topic;
    use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
    use qubit_event_bus_redis::naming::stream_key;
    use qubit_retry::RetryPolicy;
    use qubit_spi::AsyncServiceProvider;
    use support::controlled_redis::proxy::ControlledRedis;
    use support::redis_server::RedisServer;
    let server = RedisServer::start()?;
    for (index, policy) in [DuplicateRiskPolicy::Forbid, DuplicateRiskPolicy::AllowDuplicates]
        .into_iter()
        .enumerate()
    {
        let proxy = ControlledRedis::start(server.url())?;
        let namespace = format!("async-uncertainty-{index}");
        let options: ProviderOptions = [
            ("redis.url".into(), proxy.url()),
            ("redis.namespace".into(), namespace.clone()),
        ]
        .into();
        let spi = futures_lite::future::block_on(
            AsyncRedisEventBusProvider.create_configured(&EventBusConfig::default().with_provider_options(options)),
        )
        .unwrap();
        let mut codecs = CodecRegistry::new();
        codecs.register::<Vec<u8>>(Arc::new(BytesCodec(ContentType::new("text/plain")?)))?;
        let facade = AsyncEventBus::with_config(
            ProviderId::new("redis-streams")?,
            spi,
            EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs)),
        )?;
        let gate = proxy.pause_after_reply("XADD");
        let worker = std::thread::spawn(move || {
            futures_lite::future::block_on(async move {
                let request = PublishRequest::builder()
                    .topic(Topic::<Vec<u8>>::new("events").unwrap())
                    .payload(b"stable async bytes".to_vec())
                    .duplicate_risk_policy(policy)
                    .retry_policy(RetryPolicy::builder().max_attempts(2).build().unwrap())
                    .build()
                    .unwrap();
                let id = request.envelope().id().clone();
                (id, facade.publish(request).await)
            })
        });
        assert!(gate.wait_until_reached(std::time::Duration::from_secs(3)));
        let stream = stream_key(&namespace, "events");
        let mut observer = redis::Client::open(server.url())?.get_connection()?;
        let applied: usize = redis::cmd("XLEN").arg(&stream).query(&mut observer)?;
        assert_eq!(applied, 1);
        gate.release_without_reply();
        let (event_id, result) = worker.join().expect("async publish completes");
        assert_eq!(result.unwrap_err().effect(), PublishEffect::MayHaveBeenAccepted);
        let records: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
            .arg(&stream)
            .arg("-")
            .arg("+")
            .query(&mut observer)?;
        assert_eq!(records.ids.len(), 1,);
        let wires: Vec<String> = records
            .ids
            .iter()
            .map(|entry| redis::from_redis_value(entry.map.get("wire").unwrap()).unwrap())
            .collect();
        for wire in &wires {
            let fields: WireFields = serde_json::from_str(wire)?;
            assert_eq!(fields.event_id, event_id.as_str());
        }
        assert!(wires.windows(2).all(|pair| pair[0] == pair[1]));
    }
    Ok(())
}

/// Encodes the original bytes inside the DLQ envelope for a deterministic test
/// transport.
struct DeadBytesCodec(ContentType);
impl EventCodec<DeadLetterEvent<Vec<u8>>> for DeadBytesCodec {
    /// Uses the same test MIME type as the source codec.
    fn content_type(&self) -> &ContentType {
        &self.0
    }
    /// No schema is used by this test codec.
    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }
    /// Copies original bytes while leaving DLQ identity and headers to the
    /// facade.
    fn encode(&self, value: &DeadLetterEvent<Vec<u8>>) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from(value.original_event().payload().as_slice()))
    }
    /// Receiving DLQ events is outside this failure-injection fixture.
    fn decode(&self, _: &EncodedPayload) -> Result<DeadLetterEvent<Vec<u8>>, CodecError> {
        Err(CodecError::Decode {
            source: Box::new(std::io::Error::other("unused DLQ decoder")),
        })
    }
}

#[test]
fn applied_dlq_xadd_lost_reply_stops_and_preserves_durable_source() -> Result<(), Box<dyn std::error::Error>> {
    use qubit_event_bus::EventBus;
    use qubit_event_bus::codec::CodecRegistry;
    use qubit_event_bus::facade::EventBusFacadeConfig;
    use qubit_event_bus::model::ConsumerGroup;
    use qubit_event_bus::model::DeadLetterEvent;
    use qubit_event_bus::model::DeadLetterPolicy;
    use qubit_event_bus::model::Delivery;
    use qubit_event_bus::model::FailureDirective;
    use qubit_event_bus::model::ProviderId;
    use qubit_event_bus::model::StartPosition;
    use qubit_event_bus::model::SubscribeRequest;
    use qubit_event_bus::model::SubscriberId;
    use qubit_event_bus::model::SubscriptionDurability;
    use qubit_event_bus::model::Topic;
    use qubit_event_bus::pipeline::Diagnostic;
    use qubit_event_bus::spi::ReceiveOutcome;
    use qubit_event_bus::spi::ShutdownMode;
    use qubit_event_bus::spi::SpiSubscriptionRequest;
    use qubit_event_bus_redis::naming::group_name;
    use qubit_event_bus_redis::naming::stream_key;
    use qubit_retry::RetryPolicy;
    use support::controlled_redis::proxy::ControlledRedis;
    use support::redis_server::RedisServer;
    let server = RedisServer::start()?;
    let proxy = ControlledRedis::start(server.url())?;
    let spi = bus(&proxy.url());
    let _ = spi.publish(message())?;
    let mut codecs = CodecRegistry::new();
    codecs.register::<Vec<u8>>(Arc::new(BytesCodec(ContentType::new("text/plain")?)))?;
    codecs.register::<DeadLetterEvent<Vec<u8>>>(Arc::new(DeadBytesCodec(ContentType::new("text/plain")?)))?;
    let facade = EventBus::with_config(
        ProviderId::new("redis-streams")?,
        Arc::clone(&spi),
        EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs)),
    )?;
    let (tx, rx) = std::sync::mpsc::channel();
    let _observer = facade.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::DeliveryFailed { .. }) {
            let _ = tx.send(());
        }
    });
    let gate = proxy.pause_after_reply("XADD");
    let subscription = facade.subscribe(
        SubscribeRequest::builder()
            .subscriber_id(SubscriberId::new("worker")?)
            .topic(Topic::<Vec<u8>>::new("events")?)
            .consumer_group(ConsumerGroup::new("group")?)
            .durability(SubscriptionDurability::Durable)
            .start_position(StartPosition::Earliest)
            .retry_policy(RetryPolicy::builder().max_attempts(3).build()?)
            .error_handler(|_, _| FailureDirective::DeadLetter)
            .dead_letter(DeadLetterPolicy::with_topic_name("dead")?)
            .build()?,
        |_: Delivery<Vec<u8>>| {
            Err(DeliveryError::Handler {
                source: Box::new(std::io::Error::other("controlled handler failure")),
            })
        },
    )?;
    assert!(gate.wait_until_reached(std::time::Duration::from_secs(3)));
    let mut observer = redis::Client::open(server.url())?.get_connection()?;
    let dead = stream_key("uncertainty", "dead");
    let applied: usize = redis::cmd("XLEN").arg(&dead).query(&mut observer)?;
    assert_eq!(applied, 1, "DLQ XADD has really executed before its reply is lost");
    gate.release_without_reply();
    rx.recv_timeout(std::time::Duration::from_secs(3))?;
    assert!(
        subscription.is_cancelled(),
        "uncertain DLQ forwarding stops the source subscription"
    );
    subscription.cancel()?;
    let _ = facade.shutdown(ShutdownMode::Graceful {
        timeout: std::time::Duration::from_secs(3),
    })?;
    let count: usize = redis::cmd("XLEN").arg(&dead).query(&mut observer)?;
    assert_eq!(count, 1, "DLQ uncertainty must not trigger blind resend");
    let source = stream_key("uncertainty", "events");
    let group = group_name("uncertainty", "events", "worker", Some("group"));
    let pending: Vec<redis::Value> = redis::cmd("XPENDING")
        .arg(&source)
        .arg(&group)
        .arg("-")
        .arg("+")
        .arg(10)
        .query(&mut observer)?;
    assert_eq!(pending.len(), 1);
    let request = SpiSubscriptionRequest::new(
        Id::new(500),
        TopicAddress::new("events")?,
        SubscriberId::new("worker")?,
        Some(ConsumerGroup::new("group")?),
        SubscriptionDurability::Durable,
        StartPosition::Earliest,
        ProviderOptions::new(),
        std::any::TypeId::of::<Vec<u8>>(),
    );
    let ReceiveOutcome::Message(recovered) = spi.subscribe(request)?.receive(std::time::Duration::from_secs(2))? else {
        panic!("unsettled source recovers")
    };
    assert_eq!(recovered.id().as_str(), "stable-id");
    Ok(())
}

#[test]
fn invalid_xadd_id_reply_does_not_claim_acceptance() {
    let server = ScriptedRedis::start(vec![Step::reply("XADD", b"+OK\r\n")]).unwrap();
    let error = bus(server.url())
        .publish(message())
        .expect_err("malformed XADD id is an uncertain protocol failure");
    assert_eq!(error.publish_effect(), PublishEffect::MayHaveBeenAccepted);
    assert_eq!(error.kind(), "outcome_unknown");
    server.finish();
}

#[cfg(feature = "async")]
#[test]
fn async_applied_dlq_reply_loss_preserves_durable_source() -> Result<(), Box<dyn std::error::Error>> {
    use qubit_event_bus::AsyncEventBus;
    use qubit_event_bus::codec::CodecRegistry;
    use qubit_event_bus::facade::EventBusFacadeConfig;
    use qubit_event_bus::model::ConsumerGroup;
    use qubit_event_bus::model::DeadLetterEvent;
    use qubit_event_bus::model::DeadLetterPolicy;
    use qubit_event_bus::model::FailureDirective;
    use qubit_event_bus::model::ProviderId;
    use qubit_event_bus::model::StartPosition;
    use qubit_event_bus::model::SubscribeRequest;
    use qubit_event_bus::model::SubscriberId;
    use qubit_event_bus::model::SubscriptionDurability;
    use qubit_event_bus::model::Topic;
    use qubit_event_bus::spi::ReceiveOutcome;
    use qubit_event_bus::spi::ShutdownMode;
    use qubit_event_bus::spi::SpiSubscriptionRequest;
    use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
    use qubit_event_bus_redis::naming::group_name;
    use qubit_event_bus_redis::naming::stream_key;
    use qubit_spi::AsyncServiceProvider;
    use support::controlled_redis::proxy::ControlledRedis;
    use support::redis_server::RedisServer;
    let server = RedisServer::start()?;
    let proxy = ControlledRedis::start(server.url())?;
    let namespace = "async-dlq-uncertainty";
    let options: ProviderOptions = [
        ("redis.url".into(), proxy.url()),
        ("redis.namespace".into(), namespace.into()),
        ("redis.claim_min_idle_ms".into(), "0".into()),
    ]
    .into();
    let spi = futures_lite::future::block_on(
        AsyncRedisEventBusProvider.create_configured(&EventBusConfig::default().with_provider_options(options)),
    )
    .unwrap();
    let _ = futures_lite::future::block_on(spi.publish(message()))?;
    let mut codecs = CodecRegistry::new();
    codecs.register::<Vec<u8>>(Arc::new(BytesCodec(ContentType::new("text/plain")?)))?;
    codecs.register::<DeadLetterEvent<Vec<u8>>>(Arc::new(DeadBytesCodec(ContentType::new("text/plain")?)))?;
    let facade = AsyncEventBus::with_config(
        ProviderId::new("redis-streams")?,
        Arc::clone(&spi),
        EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs)),
    )?;
    let gate = proxy.pause_after_reply("XADD");
    let worker = std::thread::spawn(move || {
        futures_lite::future::block_on(async move {
            let mut subscription = facade
                .subscribe(
                    SubscribeRequest::builder()
                        .subscriber_id(SubscriberId::new("worker").unwrap())
                        .topic(Topic::<Vec<u8>>::new("events").unwrap())
                        .consumer_group(ConsumerGroup::new("group").unwrap())
                        .durability(SubscriptionDurability::Durable)
                        .start_position(StartPosition::Earliest)
                        .retry_policy(RetryPolicy::builder().max_attempts(3).build().unwrap())
                        .error_handler(|_, _| FailureDirective::DeadLetter)
                        .dead_letter(DeadLetterPolicy::with_topic_name("dead").unwrap())
                        .build()
                        .unwrap(),
                )
                .await
                .unwrap();
            let failure = subscription
                .run(|_| async {
                    Err(DeliveryError::Handler {
                        source: Box::new(std::io::Error::other("controlled async handler failure")),
                    })
                })
                .await
                .unwrap_err();
            assert!(matches!(failure, ReceiveError::DeadLetterForwardFailed { .. }));
            subscription.close().await.unwrap();
            let _ = facade
                .shutdown(ShutdownMode::Graceful {
                    timeout: std::time::Duration::from_secs(3),
                })
                .await
                .unwrap();
        })
    });
    assert!(gate.wait_until_reached(std::time::Duration::from_secs(3)));
    let mut observer = redis::Client::open(server.url())?.get_connection()?;
    let dead = stream_key(namespace, "dead");
    let count: usize = redis::cmd("XLEN").arg(&dead).query(&mut observer)?;
    assert_eq!(count, 1);
    gate.release_without_reply();
    worker.join().expect("async DLQ run terminates after uncertainty");
    let count: usize = redis::cmd("XLEN").arg(&dead).query(&mut observer)?;
    assert_eq!(count, 1, "async DLQ is not blindly resent");
    let source = stream_key(namespace, "events");
    let group = group_name(namespace, "events", "worker", Some("group"));
    let pending: Vec<redis::Value> = redis::cmd("XPENDING")
        .arg(&source)
        .arg(&group)
        .arg("-")
        .arg("+")
        .arg(10)
        .query(&mut observer)?;
    assert_eq!(pending.len(), 1);
    futures_lite::future::block_on(async {
        let request = SpiSubscriptionRequest::new(
            Id::new(600),
            TopicAddress::new("events")?,
            SubscriberId::new("worker")?,
            Some(ConsumerGroup::new("group")?),
            SubscriptionDurability::Durable,
            StartPosition::Earliest,
            ProviderOptions::new(),
            std::any::TypeId::of::<Vec<u8>>(),
        );
        let mut receiver = spi.subscribe(request).await?;
        let ReceiveOutcome::Message(recovered) = receiver.receive(std::time::Duration::from_secs(2)).await? else {
            panic!("async durable source recovers")
        };
        assert_eq!(recovered.id().as_str(), "stable-id");
        receiver.close().await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}
