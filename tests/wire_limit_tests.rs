// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Redis configuration and direct wire allocation boundaries.

mod support;

use std::sync::Arc;
use std::time::SystemTime;

use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_event_bus_redis::config::RedisEventBusConfig;
use qubit_event_bus_redis::wire::WireFields;

/// Creates a transport message with a deterministic payload length.
fn message(bytes: usize) -> OutboundMessage {
    OutboundMessage::new(
        TopicAddress::new("limits").unwrap(),
        EventId::new("limit-event").unwrap(),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(vec![42; bytes]),
            ContentType::new("application/octet-stream").unwrap(),
            None,
        )),
    )
}

#[test]
fn positive_wire_options_are_supported() {
    let options: ProviderOptions = [
        ("redis.max_wire_bytes".into(), "1024".into()),
        ("redis.max_payload_bytes".into(), "128".into()),
        ("redis.max_headers_bytes".into(), "64".into()),
    ]
    .into();
    let config = RedisEventBusConfig::from_provider_options(&options).unwrap();
    assert_eq!(config.max_wire_bytes(), 1024);
    assert_eq!(config.max_payload_bytes(), 128);
    assert_eq!(config.max_headers_bytes(), 64);
    let defaults = RedisEventBusConfig::default();
    assert_eq!(
        (
            defaults.max_wire_bytes(),
            defaults.max_payload_bytes(),
            defaults.max_headers_bytes()
        ),
        (8_388_608, 1_048_576, 65_536)
    );
}

#[test]
fn public_wire_constructor_copies_payload_without_provider_limits() {
    assert_eq!(
        WireFields::from_outbound(&message(1_048_577)).unwrap().payload.len(),
        1_048_577
    );
    assert_eq!(
        WireFields::from_outbound(&message(1_048_576)).unwrap().payload.len(),
        1_048_576
    );
}

#[test]
fn invalid_wire_options_are_secret_safe() {
    for key in [
        "redis.max_wire_bytes",
        "redis.max_payload_bytes",
        "redis.max_headers_bytes",
    ] {
        for value in ["0", "password=secret", "99999999999999999999999999999999"] {
            let options: ProviderOptions = [(key.into(), value.into())].into();
            let error = RedisEventBusConfig::from_provider_options(&options).unwrap_err();
            assert!(!error.to_string().contains(value));
        }
    }
}

#[cfg(feature = "sync")]
mod durable {
    use std::any::TypeId;
    use std::time::Duration;

    use qubit_event_bus::CodecError;
    use qubit_event_bus::EventBusConfig;
    use qubit_event_bus::codec::EventCodec;
    use qubit_event_bus::model::ConsumerGroup;
    use qubit_event_bus::model::SchemaId;
    use qubit_event_bus::model::StartPosition;
    use qubit_event_bus::model::SubscriberId;
    use qubit_event_bus::model::SubscriptionDurability;
    use qubit_event_bus::spi::EventBusSpi;
    use qubit_event_bus::spi::ReceiveOutcome;
    use qubit_event_bus::spi::SpiSubscriptionRequest;
    use qubit_event_bus_redis::naming::group_name;
    use qubit_event_bus_redis::naming::poison_key;
    use qubit_event_bus_redis::naming::stream_key;
    use qubit_event_bus_redis::sync::RedisEventBusProvider;
    use qubit_id::Id;
    use qubit_retry_025::RetryPolicy;
    use qubit_spi::ServiceProvider;

    use super::Arc;
    use super::ContentType;
    use super::EncodedPayload;
    use super::ProviderOptions;
    use super::TopicAddress;
    use super::WireFields;
    use super::message;
    use crate::support::redis_server::RedisServer;
    use crate::support::scripted_redis::ScriptedRedis;
    use crate::support::scripted_redis::Step;

    /// Constructs direct SPI settings with one independently restricted
    /// component.
    fn bus(url: &str, namespace: &str, key: &str, limit: usize) -> Arc<dyn EventBusSpi> {
        let mut options: ProviderOptions = [
            ("redis.url".into(), url.into()),
            ("redis.namespace".into(), namespace.into()),
            ("redis.claim_min_idle_ms".into(), "0".into()),
            (key.into(), limit.to_string()),
        ]
        .into();
        if key == "redis.max_wire_bytes" {
            options.insert("redis.max_payload_bytes".into(), "5".into());
        }
        RedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(options))
            .unwrap()
    }

    /// Reuses one durable group while giving each receiver a fresh subscription
    /// ID.
    fn request(id: u64) -> SpiSubscriptionRequest {
        request_at(id, StartPosition::Earliest)
    }

    /// Builds a durable request at `start_position` without Redis I/O.
    fn request_at(id: u64, start_position: StartPosition) -> SpiSubscriptionRequest {
        SpiSubscriptionRequest::new(
            Id::new(id),
            TopicAddress::new("limits").unwrap(),
            SubscriberId::new("worker").unwrap(),
            Some(ConsumerGroup::new("group").unwrap()),
            SubscriptionDurability::Durable,
            start_position,
            ProviderOptions::new(),
            TypeId::of::<Vec<u8>>(),
        )
    }

    fn response_limited_bus(url: &str, namespace: &str) -> Arc<dyn EventBusSpi> {
        let options: ProviderOptions = [
            ("redis.url".into(), url.into()),
            ("redis.namespace".into(), namespace.into()),
            ("redis.max_wire_bytes".into(), "1024".into()),
            ("redis.max_payload_bytes".into(), "1024".into()),
            ("redis.claim_min_idle_ms".into(), "0".into()),
            ("redis.command_timeout_ms".into(), "250".into()),
        ]
        .into();
        RedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(options))
            .unwrap()
    }

    fn oversized_stream_response(stream: &str) -> Vec<u8> {
        format!(
            "*1\r\n*2\r\n${}\r\n{}\r\n*1\r\n*2\r\n$3\r\n1-0\r\n*2\r\n$4\r\nwire\r\n$999999999\r\n",
            stream.len(),
            stream
        )
        .into_bytes()
    }

    #[test]
    fn scripted_oversized_receive_frame_stops_the_subscription() {
        let namespace = "scripted-response-limit";
        let stream = stream_key(namespace, "limits");
        let server = ScriptedRedis::start(vec![
            Step::reply("XGROUP", b"+OK\r\n"),
            Step::reply("XAUTOCLAIM", b"*2\r\n$3\r\n0-0\r\n*0\r\n"),
            Step::reply("XREADGROUP", b"*0\r\n"),
            Step::reply("XREADGROUP", &oversized_stream_response(&stream)),
        ])
        .unwrap();
        let spi = response_limited_bus(server.url(), namespace);
        let mut receiver = spi.subscribe(request_at(80, StartPosition::New)).unwrap();
        let error = match receiver.receive(Duration::from_secs(2)) {
            Err(error) => error,
            Ok(_) => panic!("oversized RESP frame must be rejected before timeout"),
        };
        assert_eq!(error.kind(), "receive_response_too_large");
        assert_eq!(error.retryable(), Some(true));
        assert!(matches!(receiver.receive(Duration::ZERO), Ok(ReceiveOutcome::Closed)));
        assert_eq!(server.finish().len(), 4, "the failed receiver socket is never reused");
    }

    #[test]
    fn real_oversized_receive_frame_keeps_record_pending_for_recovery() -> Result<(), Box<dyn std::error::Error>> {
        let server = RedisServer::start()?;
        let mut observer = redis::Client::open(server.url())?.get_connection()?;
        let namespace = "real-response-limit";
        let stream = stream_key(namespace, "limits");
        redis::cmd("XADD")
            .arg(&stream)
            .arg("*")
            .arg("wire")
            .arg(vec![b'x'; 70_000])
            .query::<String>(&mut observer)?;
        let spi = response_limited_bus(server.url(), namespace);
        let mut receiver = spi.subscribe(request(81))?;
        let error = match receiver.receive(Duration::from_secs(2)) {
            Err(error) => error,
            Ok(_) => panic!("oversized Redis reply must stop this receiver"),
        };
        assert_eq!(error.kind(), "receive_response_too_large");
        receiver.close()?;

        let group = group_name(namespace, "limits", "worker", Some("group"));
        let pending: Vec<redis::Value> = redis::cmd("XPENDING")
            .arg(&stream)
            .arg(&group)
            .arg("-")
            .arg("+")
            .arg(10)
            .query(&mut observer)?;
        assert_eq!(pending.len(), 1, "the oversized entry remains in the PEL");

        let mut reopened = spi.subscribe(request_at(82, StartPosition::New))?;
        let second_error = match reopened.receive(Duration::from_secs(2)) {
            Err(error) => error,
            Ok(_) => panic!("new subscription must observe the still-pending oversized entry"),
        };
        assert_eq!(second_error.kind(), "receive_response_too_large");
        let pending_after_reopen: Vec<redis::Value> = redis::cmd("XPENDING")
            .arg(&stream)
            .arg(&group)
            .arg("-")
            .arg("+")
            .arg(10)
            .query(&mut observer)?;
        assert_eq!(pending_after_reopen.len(), 1);
        reopened.close()?;
        Ok(())
    }

    #[test]
    fn receive_limits_preserve_pending_entries_and_allow_recovery() -> Result<(), Box<dyn std::error::Error>> {
        let server = RedisServer::start()?;
        let mut observer = redis::Client::open(server.url())?.get_connection()?;
        for (index, key) in [
            "redis.max_wire_bytes",
            "redis.max_payload_bytes",
            "redis.max_headers_bytes",
        ]
        .iter()
        .enumerate()
        {
            let namespace = format!("receive-limits-{index}");
            let fields = WireFields::from_outbound(&message(5))?;
            let wire = serde_json::to_string(&fields)?;
            let exact = match index {
                0 => wire.len(),
                1 => fields.payload.len(),
                _ => fields.headers_json.len(),
            };
            let stream = stream_key(&namespace, "limits");
            let id: String = redis::cmd("XADD")
                .arg(&stream)
                .arg("*")
                .arg("wire")
                .arg(&wire)
                .query(&mut observer)?;
            let restricted = bus(server.url(), &namespace, key, exact - 1);
            let mut receiver = restricted.subscribe(request(10 + index as u64))?;
            let error = receiver
                .receive(Duration::from_secs(2))
                .err()
                .expect("limit+1 is rejected before delivering");
            assert_eq!(error.kind(), "receive_limit_exceeded");
            assert_eq!(error.retryable(), Some(false));
            receiver.close()?;
            let group = group_name(&namespace, "limits", "worker", Some("group"));
            let pending: Vec<redis::Value> = redis::cmd("XPENDING")
                .arg(&stream)
                .arg(&group)
                .arg("-")
                .arg("+")
                .arg(10)
                .query(&mut observer)?;
            assert_eq!(
                pending.len(),
                1,
                "over-limit record was not acknowledged or quarantined"
            );
            let count: usize = redis::cmd("XLEN").arg(&stream).query(&mut observer)?;
            assert_eq!(count, 1, "over-limit record was not deleted");
            let widened = bus(server.url(), &namespace, key, exact);
            let mut recovered = widened.subscribe(request_at(20 + index as u64, StartPosition::New))?;
            let ReceiveOutcome::Message(record) = recovered.receive(Duration::from_secs(2))? else {
                panic!("record recovers at exact limit")
            };
            assert_eq!(record.id().as_str(), "limit-event");
            assert!(!id.is_empty());
            recovered.close()?;
        }
        Ok(())
    }
    #[test]
    fn future_version_is_preserved_while_malformed_wire_is_quarantined() -> Result<(), Box<dyn std::error::Error>> {
        let server = RedisServer::start()?;
        let mut observer = redis::Client::open(server.url())?.get_connection()?;
        for (namespace, wire, future) in [
            (
                "future-limit",
                r#"{"version":2,"payload":[0,1,2,3,4],"headers_json":"not JSON"}"#,
                true,
            ),
            ("malformed-limit", r#"{"version":1,"payload":broken}"#, false),
        ] {
            let stream = stream_key(namespace, "limits");
            redis::cmd("XADD")
                .arg(&stream)
                .arg("*")
                .arg("wire")
                .arg(wire)
                .query::<String>(&mut observer)?;
            let spi = bus(server.url(), namespace, "redis.max_payload_bytes", 1);
            let mut receiver = spi.subscribe(request(if future { 40 } else { 41 }))?;
            if future {
                let error = receiver
                    .receive(Duration::from_secs(2))
                    .err()
                    .expect("future version is not decoded");
                assert_eq!(error.kind(), "unsupported_wire_version");
                let length: usize = redis::cmd("XLEN").arg(&stream).query(&mut observer)?;
                assert_eq!(length, 1);
                let group = group_name(namespace, "limits", "worker", Some("group"));
                let pending: Vec<redis::Value> = redis::cmd("XPENDING")
                    .arg(&stream)
                    .arg(&group)
                    .arg("-")
                    .arg("+")
                    .arg(10)
                    .query(&mut observer)?;
                assert_eq!(pending.len(), 1);
            } else {
                assert!(matches!(
                    receiver.receive(Duration::from_secs(2))?,
                    ReceiveOutcome::Gap(_)
                ));
                let length: usize = redis::cmd("XLEN").arg(&stream).query(&mut observer)?;
                assert_eq!(length, 1, "quarantine preserves source stream history");
                let group = group_name(namespace, "limits", "worker", Some("group"));
                let pending: Vec<redis::Value> = redis::cmd("XPENDING")
                    .arg(&stream)
                    .arg(&group)
                    .arg("-")
                    .arg("+")
                    .arg(10)
                    .query(&mut observer)?;
                assert!(pending.is_empty(), "malformed record was acknowledged atomically");
                let quarantine = poison_key(namespace, "limits", &group);
                let count: usize = redis::cmd("XLEN").arg(quarantine).query(&mut observer)?;
                assert_eq!(count, 1, "malformed record was quarantined");
            }
            receiver.close()?;
        }
        Ok(())
    }

    #[test]
    fn direct_spi_publish_checks_all_components_before_xadd() -> Result<(), Box<dyn std::error::Error>> {
        use qubit_event_bus::model::PublishEffect;

        use crate::support::scripted_redis::ScriptedRedis;
        for (index, key) in [
            "redis.max_wire_bytes",
            "redis.max_payload_bytes",
            "redis.max_headers_bytes",
        ]
        .iter()
        .enumerate()
        {
            let server = ScriptedRedis::start(vec![])?;
            let fields = WireFields::from_outbound(&message(5))?;
            let wire = serde_json::to_string(&fields)?;
            let exact = match index {
                0 => wire.len(),
                1 => fields.payload.len(),
                _ => fields.headers_json.len(),
            };
            let restricted = bus(server.url(), "direct-publish-limits", key, exact - 1);
            let error = restricted.publish(message(5)).unwrap_err();
            assert_eq!(
                error.kind(),
                if index == 1 {
                    "payload_too_large"
                } else {
                    "wire_too_large"
                }
            );
            assert_eq!(error.publish_effect(), PublishEffect::NotAccepted);
            assert_eq!(error.retryable(), Some(false));
            assert!(
                server.finish().is_empty(),
                "no connection command or XADD was submitted"
            );
        }
        Ok(())
    }

    /// Fails permanently at the real core codec boundary after Redis delivery.
    struct FailingCodec {
        content: ContentType,
        calls: Arc<std::sync::atomic::AtomicUsize>,
        panic: bool,
    }
    impl EventCodec<Vec<u8>> for FailingCodec {
        /// Returns the accepted encoded MIME type.
        fn content_type(&self) -> &ContentType {
            &self.content
        }
        /// Requires no schema metadata.
        fn schema_id(&self) -> Option<&SchemaId> {
            None
        }
        /// Shares bytes for transport.
        fn encode(&self, value: &Vec<u8>) -> Result<Arc<[u8]>, CodecError> {
            Ok(Arc::from(value.as_slice()))
        }
        /// Records calls and creates the selected permanent codec failure.
        fn decode(&self, _: &EncodedPayload) -> Result<Vec<u8>, CodecError> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            assert!(!self.panic, "controlled permanent codec panic");
            Err(CodecError::Decode {
                source: Box::new(std::io::Error::other("controlled permanent decode failure")),
            })
        }
    }

    #[test]
    fn real_core_codec_failures_leave_redis_durable_record_recoverable() -> Result<(), Box<dyn std::error::Error>> {
        use qubit_event_bus::EventBus;
        use qubit_event_bus::codec::CodecRegistry;
        use qubit_event_bus::facade::EventBusFacadeConfig;
        use qubit_event_bus::model::ProviderId;
        use qubit_event_bus::model::SubscribeRequest;
        use qubit_event_bus::model::SubscriptionStopReason;
        use qubit_event_bus::model::Topic;
        use qubit_event_bus::spi::ShutdownMode;
        let server = RedisServer::start()?;
        for (index, panic) in [false, true].into_iter().enumerate() {
            let namespace = format!("core-codec-recovery-{index}");
            let spi = bus(server.url(), &namespace, "redis.max_payload_bytes", 5);
            let _ = spi.publish(message(5))?;
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let handlers = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let mut codecs = CodecRegistry::new();
            codecs.register::<Vec<u8>>(Arc::new(FailingCodec {
                content: ContentType::new(if panic {
                    "application/octet-stream"
                } else {
                    "text/plain"
                })?,
                calls: Arc::clone(&calls),
                panic,
            }))?;
            let facade = EventBus::with_config(
                ProviderId::new("redis-streams")?,
                Arc::clone(&spi),
                EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs)),
            )?;
            let handler_calls = Arc::clone(&handlers);
            let subscription = facade.subscribe(
                SubscribeRequest::builder()
                    .subscriber_id(SubscriberId::new("worker")?)
                    .topic(Topic::<Vec<u8>>::new("limits")?)
                    .consumer_group(ConsumerGroup::new("group")?)
                    .durability(SubscriptionDurability::Durable)
                    .start_position(StartPosition::Earliest)
                    .retry_policy(RetryPolicy::builder().max_attempts(3).build()?)
                    .build()?,
                move |_| {
                    handler_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                },
            )?;
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            while subscription.terminal_failure().is_none() && std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(5));
            }
            let reason = subscription
                .terminal_failure()
                .expect("real Redis delivery stops after codec failure");
            assert!(matches!(&*reason, SubscriptionStopReason::Codec { .. }));
            assert_eq!(
                calls.load(std::sync::atomic::Ordering::SeqCst),
                usize::from(panic),
                "metadata is validated before decode; panic is never retried"
            );
            assert_eq!(handlers.load(std::sync::atomic::Ordering::SeqCst), 0);
            subscription.cancel()?;
            let _ = facade.shutdown(ShutdownMode::Graceful {
                timeout: Duration::from_secs(3),
            })?;
            let mut recovered = spi.subscribe(request_at(70 + index as u64, StartPosition::New))?;
            let ReceiveOutcome::Message(record) = recovered.receive(Duration::from_secs(2))? else {
                panic!("durable source remains recoverable")
            };
            assert_eq!(record.id().as_str(), "limit-event");
            recovered.close()?;
        }
        Ok(())
    }

    #[cfg(feature = "async")]
    #[test]
    fn async_receive_limits_preserve_pending_entries_and_recover() -> Result<(), Box<dyn std::error::Error>> {
        use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
        use qubit_spi::AsyncServiceProvider;
        let server = RedisServer::start()?;
        let mut observer = redis::Client::open(server.url())?.get_connection()?;
        for (index, key) in [
            "redis.max_wire_bytes",
            "redis.max_payload_bytes",
            "redis.max_headers_bytes",
        ]
        .iter()
        .enumerate()
        {
            let namespace = format!("async-receive-limits-{index}");
            let fields = WireFields::from_outbound(&message(5))?;
            let wire = serde_json::to_string(&fields)?;
            let exact = match index {
                0 => wire.len(),
                1 => fields.payload.len(),
                _ => fields.headers_json.len(),
            };
            let stream = stream_key(&namespace, "limits");
            redis::cmd("XADD")
                .arg(&stream)
                .arg("*")
                .arg("wire")
                .arg(&wire)
                .query::<String>(&mut observer)?;
            futures_lite::future::block_on(async {
                for (attempt, limit) in [exact - 1, exact].into_iter().enumerate() {
                    let mut options: ProviderOptions = [
                        ("redis.url".into(), server.url().into()),
                        ("redis.namespace".into(), namespace.clone()),
                        ("redis.claim_min_idle_ms".into(), "0".into()),
                        (key.to_string(), limit.to_string()),
                    ]
                    .into();
                    if *key == "redis.max_wire_bytes" {
                        options.insert("redis.max_payload_bytes".into(), "5".into());
                    }
                    let spi = AsyncRedisEventBusProvider
                        .create_configured(&EventBusConfig::default().with_provider_options(options))
                        .await
                        .unwrap();
                    let start_position = if attempt == 0 {
                        StartPosition::Earliest
                    } else {
                        StartPosition::New
                    };
                    let mut receiver = spi
                        .subscribe(request_at(100 + index as u64 * 2 + attempt as u64, start_position))
                        .await?;
                    if attempt == 0 {
                        let error = receiver
                            .receive(Duration::from_secs(2))
                            .await
                            .err()
                            .expect("async limit+1 rejected");
                        assert_eq!(error.kind(), "receive_limit_exceeded");
                    } else {
                        let ReceiveOutcome::Message(record) = receiver.receive(Duration::from_secs(2)).await? else {
                            panic!("async record recovers at exact limit")
                        };
                        assert_eq!(record.id().as_str(), "limit-event");
                    }
                    receiver.close().await?;
                }
                Ok::<(), Box<dyn std::error::Error>>(())
            })?;
            let group = group_name(&namespace, "limits", "worker", Some("group"));
            let pending: Vec<redis::Value> = redis::cmd("XPENDING")
                .arg(&stream)
                .arg(&group)
                .arg("-")
                .arg("+")
                .arg(10)
                .query(&mut observer)?;
            assert_eq!(pending.len(), 1);
            let length: usize = redis::cmd("XLEN").arg(&stream).query(&mut observer)?;
            assert_eq!(length, 1);
        }
        Ok(())
    }

    #[test]
    fn direct_spi_publish_accepts_exact_custom_component_limits() -> Result<(), Box<dyn std::error::Error>> {
        use crate::support::scripted_redis::ScriptedRedis;
        use crate::support::scripted_redis::Step;
        for (index, key) in [
            "redis.max_wire_bytes",
            "redis.max_payload_bytes",
            "redis.max_headers_bytes",
        ]
        .iter()
        .enumerate()
        {
            let server = ScriptedRedis::start(vec![Step::reply("XADD", b"$3\r\n1-0\r\n")])?;
            let fields = WireFields::from_outbound(&message(5))?;
            let wire = serde_json::to_string(&fields)?;
            let exact = match index {
                0 => wire.len(),
                1 => fields.payload.len(),
                _ => fields.headers_json.len(),
            };
            let spi = bus(server.url(), "exact-publish-limits", key, exact);
            let _ = spi.publish(message(5))?;
            let commands = server.finish();
            assert_eq!(commands.len(), 1);
            assert_eq!(commands[0][0], "XADD");
        }
        Ok(())
    }
}

#[cfg(feature = "async")]
mod async_response_limit {
    use std::any::TypeId;
    use std::time::Duration;

    use futures_lite::future::block_on;
    use qubit_event_bus::EventBusConfig;
    use qubit_event_bus::model::ConsumerGroup;
    use qubit_event_bus::model::ProviderOptions;
    use qubit_event_bus::model::StartPosition;
    use qubit_event_bus::model::SubscriberId;
    use qubit_event_bus::model::SubscriptionDurability;
    use qubit_event_bus::spi::ReceiveOutcome;
    use qubit_event_bus::spi::SpiSubscriptionRequest;
    use qubit_event_bus::spi::TopicAddress;
    use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
    use qubit_id::Id;
    use qubit_spi::AsyncServiceProvider;

    use crate::support::scripted_redis::ScriptedRedis;
    use crate::support::scripted_redis::Step;

    fn request() -> SpiSubscriptionRequest {
        SpiSubscriptionRequest::new(
            Id::new(90),
            TopicAddress::new("limits").unwrap(),
            SubscriberId::new("worker").unwrap(),
            Some(ConsumerGroup::new("group").unwrap()),
            SubscriptionDurability::Durable,
            StartPosition::New,
            ProviderOptions::new(),
            TypeId::of::<Vec<u8>>(),
        )
    }

    fn oversized_stream_response(stream: &str) -> Vec<u8> {
        format!(
            "*1\r\n*2\r\n${}\r\n{}\r\n*1\r\n*2\r\n$3\r\n1-0\r\n*2\r\n$4\r\nwire\r\n$999999999\r\n",
            stream.len(),
            stream
        )
        .into_bytes()
    }

    #[test]
    fn scripted_oversized_async_receive_frame_stops_the_subscription() {
        block_on(async {
            let namespace = "scripted-async-response-limit";
            let stream = format!("{namespace}:limits");
            let server = ScriptedRedis::start(vec![
                Step::reply("XGROUP", b"+OK\r\n"),
                Step::reply("XAUTOCLAIM", b"*2\r\n$3\r\n0-0\r\n*0\r\n"),
                Step::reply("XREADGROUP", b"*0\r\n"),
                Step::reply("XREADGROUP", &oversized_stream_response(&stream)),
            ])
            .unwrap();
            let options: ProviderOptions = [
                ("redis.url".into(), server.url().into()),
                ("redis.namespace".into(), namespace.into()),
                ("redis.max_wire_bytes".into(), "1024".into()),
                ("redis.max_payload_bytes".into(), "1024".into()),
                ("redis.claim_min_idle_ms".into(), "0".into()),
                ("redis.command_timeout_ms".into(), "250".into()),
            ]
            .into();
            let spi = AsyncRedisEventBusProvider
                .create_configured(&EventBusConfig::default().with_provider_options(options))
                .await
                .unwrap();
            let mut receiver = spi.subscribe(request()).await.unwrap();
            let error = match receiver.receive(Duration::from_secs(2)).await {
                Err(error) => error,
                Ok(_) => panic!("oversized RESP frame must be rejected before timeout"),
            };
            assert_eq!(error.kind(), "receive_response_too_large");
            assert_eq!(error.retryable(), Some(true));
            assert!(matches!(
                receiver.receive(Duration::ZERO).await,
                Ok(ReceiveOutcome::Closed)
            ));
            assert_eq!(server.finish().len(), 4, "the failed receiver socket is never reused");
        });
    }
}
