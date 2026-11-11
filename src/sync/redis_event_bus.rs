// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Synchronous Redis Streams operations and secret-safe error conversion.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishEffect;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use redis::ConnectionLike;
use redis::RedisError;
use redis::Value;
use redis::cmd;

use super::subscription::Subscription;
use crate::client::Client;
use crate::client::CommandClass;
use crate::config::RedisEventBusConfig;
use crate::consumer_identity::new_consumer_name;
use crate::diagnostics::RedisDiagnosticCounter;
use crate::error::RedisProviderError;
use crate::error::invalid_publish_reply;
use crate::error::query_publish_error;
use crate::error::to_publish_error;
use crate::existing_group_start::ensure_existing_group_start;
use crate::existing_group_start::parse_existing_group_policy;
use crate::internal::RecoveryState;
use crate::internal::WireLimits;
use crate::internal::redis_capabilities;
use crate::internal::valid_stream_id;
use crate::naming::group_name;
use crate::naming::poison_key;
use crate::naming::stream_key;
use crate::redis_provider_error::from_redis_error;
use crate::wire_fields::check_publish_payload;
use crate::wire_fields::encode_bounded;

/// Validated Redis settings and client shared by publishers and subscribers.
pub(super) struct RedisEventBus {
    /// Connection factory used by each blocking SPI call.
    pub(super) client: Arc<Client>,
    /// Settings used to derive keys and receiver limits.
    pub(super) settings: RedisEventBusConfig,
}

impl EventBusSpi for RedisEventBus {
    /// Declares the encoded, durable capabilities available through Redis
    /// Streams.
    ///
    /// # Returns
    ///
    /// The static capability set supported by this backend.
    #[inline]
    fn capabilities(&self) -> EventBusCapabilities {
        redis_capabilities()
    }

    /// Appends one encoded event to its stream and returns Redis's stream ID.
    ///
    /// Acceptance means Redis replied successfully to `XADD`; it does not
    /// establish that the record was persisted to disk or processed. If the
    /// connection fails before the reply, the outcome may be unknown.
    ///
    /// # Parameters
    ///
    /// - `message`: Event with an encoded payload and transport metadata.
    ///
    /// # Returns
    ///
    /// An accepted acknowledgement containing the Redis stream ID.
    ///
    /// # Errors
    ///
    /// Returns an SPI error if the payload is not encoded, serialization fails,
    /// admission is exhausted, a connection cannot be opened, or Redis rejects
    /// `XADD`. Payload type and payload size are checked before admission;
    /// admission then covers wire encoding and connection setup. A missing or
    /// malformed reply after send returns outcome-unknown without replay.
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        let limits = WireLimits::from_config(&self.settings);
        check_publish_payload(&message, limits)
            .map_err(|error| to_publish_error(message.topic(), error, PublishEffect::NotAccepted))?;
        let permit = self
            .client
            .try_command_permit(CommandClass::General)
            .map_err(|error| to_publish_error(message.topic(), error, PublishEffect::NotAccepted))?;
        let payload = encode_bounded(&message, limits)
            .map_err(|error| to_publish_error(message.topic(), error, PublishEffect::NotAccepted))?;
        let key = stream_key(self.settings.namespace(), message.topic().as_str());
        let mut connection = self
            .client
            .get_connection_with_permit(permit)
            .map_err(|error| to_publish_error(message.topic(), error, PublishEffect::NotAccepted))?;
        let mut command = cmd("XADD");
        command.arg(&key);
        if let Some(maxlen) = self.settings.stream_maxlen_approx() {
            command.arg("MAXLEN").arg("~").arg(maxlen.get());
        }
        command.arg("*").arg("wire").arg(payload);
        let result = connection.req_command(&command);
        let message_id = match result {
            Ok(Value::BulkString(bytes)) => match String::from_utf8(bytes) {
                Ok(id) if valid_stream_id(&id) && id != "0-0" => id,
                _ => {
                    connection.discard();
                    let error = invalid_publish_reply(message.topic());
                    if let Some(diagnostics) = self.client.diagnostics() {
                        diagnostics.increment(RedisDiagnosticCounter::PublishUnknown);
                    }
                    return Err(error);
                }
            },
            Ok(Value::ServerError(error)) => {
                let error: RedisError = error.into();
                let error = query_publish_error(message.topic(), &error);
                if error.kind() == "outcome_unknown"
                    && let Some(diagnostics) = self.client.diagnostics()
                {
                    diagnostics.increment(RedisDiagnosticCounter::PublishUnknown);
                }
                return Err(error);
            }
            Ok(_) => {
                connection.discard();
                let error = invalid_publish_reply(message.topic());
                if let Some(diagnostics) = self.client.diagnostics() {
                    diagnostics.increment(RedisDiagnosticCounter::PublishUnknown);
                }
                return Err(error);
            }
            Err(error) => {
                connection.discard();
                let error = query_publish_error(message.topic(), &error);
                if error.kind() == "outcome_unknown"
                    && let Some(diagnostics) = self.client.diagnostics()
                {
                    diagnostics.increment(RedisDiagnosticCounter::PublishUnknown);
                }
                return Err(error);
            }
        };
        if let Some(diagnostics) = self.client.diagnostics() {
            diagnostics.increment(RedisDiagnosticCounter::PublishAccepted);
        }
        Ok(PublishAcknowledgement::Accepted {
            provider_message_id: Some(message_id),
            metadata: Default::default(),
        })
    }

    /// Creates the group if needed and returns a blocking stream receiver.
    ///
    /// Redis retains an existing group's cursor. A later explicit start
    /// position requires `redis.existing_group_start=resume`; `New` resumes
    /// without the option. Closing does not acknowledge pending entries.
    ///
    /// # Parameters
    ///
    /// - `request`: Topic, group, start position, and identity for the
    ///   receiver.
    ///
    /// # Returns
    ///
    /// A boxed blocking subscription bound to the requested group.
    ///
    /// # Errors
    ///
    /// Returns an SPI error for invalid options, an incompatible existing
    /// group cursor, or a failure to connect or create the consumer group.
    /// Only the RESP `+OK` reply confirms `XGROUP CREATE`; malformed replies
    /// are reported as `subscribe/outcome_unknown` without another attempt.
    /// After a sent command has an unknown result, a retry connection failure
    /// or rejection also remains `subscribe/outcome_unknown`. The single
    /// retry can succeed on `+OK`, or on `BUSYGROUP` when the request accepts
    /// the existing cursor (`New` or explicit `resume`).
    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        let topic = request.topic().clone();
        if request.durability() != SubscriptionDurability::Durable {
            return Err(SpiError::Operation {
                provider_id: "redis-streams".into(),
                operation: "subscribe",
                resource: Some(topic.as_str().into()),
                kind: "unsupported_subscription_durability",
                retryable: Some(false),
                source: Box::new(RedisProviderError::Configuration(
                    "Redis Streams requires durable subscriptions",
                )),
            });
        }
        let key = stream_key(self.settings.namespace(), topic.as_str());
        let group = group_name(
            self.settings.namespace(),
            topic.as_str(),
            request.subscriber_id().as_str(),
            request.group().map(|value| value.as_str()),
        );
        let quarantine = poison_key(self.settings.namespace(), topic.as_str(), &group);
        let start = match request.start_position() {
            StartPosition::Earliest => "0-0",
            StartPosition::At(id) => id,
            StartPosition::New => "$",
            _ => "$",
        };
        if matches!(request.start_position(), StartPosition::At(id) if !valid_stream_id(id)) {
            return Err(SpiError::Operation {
                provider_id: "redis-streams".into(),
                operation: "subscribe",
                resource: Some(topic.as_str().into()),
                kind: "invalid_start_position",
                retryable: Some(false),
                source: Box::new(RedisProviderError::Configuration("invalid Redis stream ID")),
            });
        }
        let policy = parse_existing_group_policy(&request)?;
        let receiver_permit = self
            .client
            .try_receiver()
            .map_err(|error| spi_error("subscribe", Some(&topic), error))?;
        let receive_connection = self
            .client
            .get_dedicated_connection()
            .map_err(|error| spi_error("subscribe", Some(&topic), error))?;
        let mut connection = self
            .client
            .get_connection(CommandClass::General)
            .map_err(|error| spi_error("subscribe", Some(&topic), error))?;
        let mut create_command = cmd("XGROUP");
        create_command
            .arg("CREATE")
            .arg(&key)
            .arg(&group)
            .arg(start)
            .arg("MKSTREAM");
        let (result, malformed_reply): (Result<(), RedisError>, bool) = match connection.req_command(&create_command) {
            Ok(Value::Okay) => (Ok(()), false),
            Ok(Value::ServerError(error)) => (Err(error.into()), false),
            Ok(_) => {
                connection.discard();
                (Ok(()), true)
            }
            Err(error) if error.is_io_error() || error.is_timeout() => (Err(error), false),
            Err(error) => {
                connection.discard();
                (Err(error), true)
            }
        };
        let first_result_unknown = result
            .as_ref()
            .is_err_and(|error| error.is_io_error() || error.is_timeout());
        let (result, malformed_reply) = if malformed_reply {
            (result, true)
        } else if first_result_unknown {
            connection.discard();
            drop(connection);
            let mut retry_connection = self.client.get_connection(CommandClass::General).map_err(|_| {
                spi_error(
                    "subscribe",
                    Some(&topic),
                    RedisProviderError::OutcomeUnknown { operation: "subscribe" },
                )
            })?;
            let (retry_result, malformed_reply) = match retry_connection.req_command(&create_command) {
                Ok(Value::Okay) => (Ok(()), false),
                Ok(Value::ServerError(error)) => (Err(error.into()), false),
                Ok(_) => {
                    retry_connection.discard();
                    (Ok(()), true)
                }
                Err(error) => {
                    retry_connection.discard();
                    (Err(error), true)
                }
            };
            (retry_result, malformed_reply)
        } else {
            (result, false)
        };
        if malformed_reply {
            return Err(spi_error(
                "subscribe",
                Some(&topic),
                RedisProviderError::OutcomeUnknown { operation: "subscribe" },
            ));
        }
        if matches!(&result, Err(error) if error.code() == Some("BUSYGROUP")) {
            if let Err(error) = ensure_existing_group_start(&request, policy) {
                return Err(if first_result_unknown {
                    spi_error(
                        "subscribe",
                        Some(&topic),
                        RedisProviderError::OutcomeUnknown { operation: "subscribe" },
                    )
                } else {
                    error
                });
            }
        } else if let Err(error) = result {
            if first_result_unknown {
                return Err(spi_error(
                    "subscribe",
                    Some(&topic),
                    RedisProviderError::OutcomeUnknown { operation: "subscribe" },
                ));
            }
            return Err(spi_error(
                "subscribe",
                Some(&topic),
                from_redis_error("subscribe", &error),
            ));
        }
        let consumer = new_consumer_name().map_err(|_| SpiError::Operation {
            provider_id: "redis-streams".into(),
            operation: "subscribe",
            resource: Some(topic.as_str().into()),
            kind: "consumer_identity_unavailable",
            retryable: Some(false),
            source: Box::new(RedisProviderError::Operation("generate consumer identity")),
        })?;
        Ok(Box::new(Subscription {
            client: Arc::clone(&self.client),
            wire_limits: WireLimits::from_config(&self.settings),
            receive_connection: Some(receive_connection),
            receiver_permit: Some(receiver_permit),
            key,
            group,
            quarantine,
            consumer,
            topic,
            subscription_id: request.subscription_id(),
            closed: false,
            claim_min_idle_ms: self.settings.claim_min_idle_ms(),
            recovery_interval: Duration::from_millis(self.settings.recovery_interval_ms() as u64),
            max_unsettled: self.settings.max_unsettled_per_subscription(),
            recovery: Arc::new(Mutex::new(RecoveryState::new())),
        }))
    }

    /// Completes shutdown without acknowledging pending messages.
    ///
    /// # Parameters
    ///
    /// - `_mode`: Facade shutdown request; both modes complete without extra
    ///   Redis I/O.
    ///
    /// # Returns
    ///
    /// `Complete`; any unsettled records remain in the Redis pending entries
    /// list.
    ///
    /// # Errors
    ///
    /// This implementation always succeeds without sending a Redis command.
    #[inline]
    fn shutdown(&self, _mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

/// Wraps a provider failure without retaining raw Redis diagnostics.
///
/// # Parameters
///
/// - `operation`: Stable SPI operation name.
/// - `topic`: Optional topic identifying the affected stream.
/// - `source`: Sanitized provider error category.
///
/// # Returns
///
/// A classified SPI operation error without raw Redis diagnostics; retryability
/// reflects both the failure category and whether that operation can safely
/// recover.
#[inline]
pub(super) fn spi_error(operation: &'static str, topic: Option<&TopicAddress>, source: RedisProviderError) -> SpiError {
    crate::error::to_spi_error(operation, topic, source)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::thread;
    use std::time::Duration;
    use std::time::SystemTime;

    use qubit_event_bus::error::SpiError;
    use qubit_event_bus::model::ContentType;
    use qubit_event_bus::model::EventId;
    use qubit_event_bus::model::Headers;
    use qubit_event_bus::model::ProviderOptions;
    use qubit_event_bus::model::PublishEffect;
    use qubit_event_bus::spi::EncodedPayload;
    use qubit_event_bus::spi::EventBusSpi;
    use qubit_event_bus::spi::EventSubscriptionSpi;
    use qubit_event_bus::spi::OutboundMessage;
    use qubit_event_bus::spi::TopicAddress;
    use qubit_event_bus::spi::TransportPayload;
    use qubit_id::Id;

    use super::RedisEventBus;
    use crate::client::Client;
    use crate::config::RedisEventBusConfig;
    use crate::internal::RecoveryState;
    use crate::internal::WireLimits;

    fn outbound(payload: Vec<u8>) -> OutboundMessage {
        OutboundMessage::new(
            TopicAddress::new("events").expect("topic"),
            EventId::new("event-1").expect("event id"),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            None,
            TransportPayload::Encoded(EncodedPayload::new(
                Arc::from(payload),
                ContentType::new("application/octet-stream").expect("content type"),
                None,
            )),
        )
    }

    fn outbound_with_large_headers() -> OutboundMessage {
        OutboundMessage::new(
            TopicAddress::new("events").expect("topic"),
            EventId::new("event-headers").expect("event id"),
            SystemTime::UNIX_EPOCH,
            [("large".into(), "x".repeat(256))].into(),
            None,
            None,
            TransportPayload::Encoded(EncodedPayload::new(
                Arc::from(&b"payload"[..]),
                ContentType::new("application/octet-stream").expect("content type"),
                None,
            )),
        )
    }

    fn outbound_native() -> OutboundMessage {
        OutboundMessage::new(
            TopicAddress::new("events").expect("topic"),
            EventId::new("event-native").expect("event id"),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            None,
            TransportPayload::Native(Arc::new(vec![1, 2, 3])),
        )
    }

    fn small_header_settings() -> RedisEventBusConfig {
        let options: ProviderOptions = [
            ("redis.url".into(), "redis://127.0.0.1:1/".into()),
            ("redis.namespace".into(), "publish-wire-limit".into()),
            ("redis.max_payload_bytes".into(), "128".into()),
            ("redis.max_wire_bytes".into(), "1024".into()),
            ("redis.max_headers_bytes".into(), "8".into()),
        ]
        .into();
        RedisEventBusConfig::from_provider_options(&options).expect("valid limits")
    }

    fn small_wire_settings() -> RedisEventBusConfig {
        let options: ProviderOptions = [
            ("redis.url".into(), "redis://127.0.0.1:1/".into()),
            ("redis.namespace".into(), "publish-wire-limit".into()),
            ("redis.max_payload_bytes".into(), "128".into()),
            ("redis.max_wire_bytes".into(), "128".into()),
            ("redis.max_headers_bytes".into(), "64".into()),
        ]
        .into();
        RedisEventBusConfig::from_provider_options(&options).expect("valid limits")
    }

    fn one_general_slot_settings() -> RedisEventBusConfig {
        let options: ProviderOptions = [
            ("redis.url".into(), "redis://127.0.0.1:1/".into()),
            ("redis.namespace".into(), "publish-encode-gate".into()),
            ("redis.max_concurrent_commands".into(), "2".into()),
            ("redis.reserved_settlement_commands".into(), "1".into()),
            ("redis.max_idle_connections".into(), "1".into()),
        ]
        .into();
        RedisEventBusConfig::from_provider_options(&options).expect("one general slot")
    }

    fn gated_outbound() -> OutboundMessage {
        OutboundMessage::new(
            TopicAddress::new("events").expect("topic"),
            EventId::new("test-encoding-gate").expect("event id"),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            None,
            TransportPayload::Encoded(EncodedPayload::new(
                Arc::from(&b"payload"[..]),
                ContentType::new("application/octet-stream").expect("content type"),
                None,
            )),
        )
    }

    fn assert_publish_error(error: SpiError, expected_kind: &str) {
        match error {
            SpiError::Publish { kind, effect, .. } => {
                assert_eq!(kind, expected_kind);
                assert_eq!(effect, PublishEffect::NotAccepted);
            }
            other => panic!("expected publish error, got {other:?}"),
        }
    }

    #[test]
    fn test_publish_rejects_full_admission_before_wire_encoding() {
        let settings = RedisEventBusConfig::default();
        let capacity = settings.max_concurrent_commands() - settings.reserved_settlement_commands();
        let client = Arc::new(Client::new(&settings).expect("client"));
        let permits: Vec<_> = (0..capacity)
            .map(|_| {
                client
                    .try_command(crate::client::CommandClass::General)
                    .expect("fill general admission")
            })
            .collect();
        crate::wire_fields::reset_encode_count();
        let bus = RedisEventBus {
            client: Arc::clone(&client),
            settings,
        };

        let result = bus.publish(outbound(vec![1; 1024]));

        assert_publish_error(
            result.expect_err("full admission must reject publish"),
            "resource_limit",
        );
        assert_eq!(crate::wire_fields::encode_count(), 0);
        drop(permits);
        let permits: Vec<_> = (0..capacity)
            .map(|_| {
                client
                    .try_command(crate::client::CommandClass::General)
                    .expect("admission was released")
            })
            .collect();
        assert_eq!(permits.len(), capacity);
    }

    #[test]
    fn test_publish_reports_oversized_payload_before_full_admission() {
        let settings = RedisEventBusConfig::default();
        let capacity = settings.max_concurrent_commands() - settings.reserved_settlement_commands();
        let client = Arc::new(Client::new(&settings).expect("client"));
        let permits: Vec<_> = (0..capacity)
            .map(|_| {
                client
                    .try_command(crate::client::CommandClass::General)
                    .expect("fill general admission")
            })
            .collect();
        crate::wire_fields::reset_encode_count();
        let bus = RedisEventBus { client, settings };

        let result = bus.publish(outbound(vec![1; 1_048_577]));

        assert_publish_error(result.expect_err("oversized payload must fail"), "payload_too_large");
        assert_eq!(crate::wire_fields::encode_count(), 0);
        drop(permits);
    }

    #[test]
    fn test_publish_preserves_native_payload_error_when_admission_is_full() {
        let settings = RedisEventBusConfig::default();
        let capacity = settings.max_concurrent_commands() - settings.reserved_settlement_commands();
        let client = Arc::new(Client::new(&settings).expect("client"));
        let permits: Vec<_> = (0..capacity)
            .map(|_| {
                client
                    .try_command(crate::client::CommandClass::General)
                    .expect("fill general admission")
            })
            .collect();
        crate::wire_fields::reset_encode_count();
        let bus = RedisEventBus { client, settings };

        assert_publish_error(
            bus.publish(outbound_native()).expect_err("native payload is invalid"),
            "configuration",
        );

        assert_eq!(crate::wire_fields::encode_count(), 0);
        drop(permits);
    }

    #[test]
    fn test_publish_connection_failure_releases_its_admission() {
        let settings = RedisEventBusConfig::new("redis://127.0.0.1:1/", "publish-connect-failure")
            .expect("valid offline endpoint");
        let capacity = settings.max_concurrent_commands() - settings.reserved_settlement_commands();
        let client = Arc::new(Client::new(&settings).expect("client"));
        let bus = RedisEventBus {
            client: Arc::clone(&client),
            settings,
        };
        crate::wire_fields::reset_encode_count();

        assert!(bus.publish(outbound(vec![1; 16])).is_err());

        assert_eq!(crate::wire_fields::encode_count(), 1);
        let permits: Vec<_> = (0..capacity)
            .map(|_| {
                client
                    .try_command(crate::client::CommandClass::General)
                    .expect("failed setup released permit")
            })
            .collect();
        assert_eq!(permits.len(), capacity);
    }

    #[test]
    fn test_publish_header_limit_is_checked_after_admission() {
        let settings = small_header_settings();
        let capacity = settings.max_concurrent_commands() - settings.reserved_settlement_commands();
        let client = Arc::new(Client::new(&settings).expect("client"));
        let bus = RedisEventBus {
            client: Arc::clone(&client),
            settings,
        };

        crate::wire_fields::reset_encode_count();
        assert_publish_error(
            bus.publish(outbound_with_large_headers()).expect_err("header limit"),
            "wire_too_large",
        );
        assert_eq!(crate::wire_fields::encode_count(), 1);
        let permits: Vec<_> = (0..capacity)
            .map(|_| {
                client
                    .try_command(crate::client::CommandClass::General)
                    .expect("encoding failure released permit")
            })
            .collect();
        drop(permits);

        let permits: Vec<_> = (0..capacity)
            .map(|_| {
                client
                    .try_command(crate::client::CommandClass::General)
                    .expect("fill general admission")
            })
            .collect();
        crate::wire_fields::reset_encode_count();
        assert_publish_error(
            bus.publish(outbound_with_large_headers()).expect_err("full admission"),
            "resource_limit",
        );
        assert_eq!(crate::wire_fields::encode_count(), 0);
        drop(permits);
    }

    #[test]
    fn test_publish_wire_limit_is_checked_after_admission() {
        let settings = small_wire_settings();
        let capacity = settings.max_concurrent_commands() - settings.reserved_settlement_commands();
        let client = Arc::new(Client::new(&settings).expect("client"));
        let bus = RedisEventBus {
            client: Arc::clone(&client),
            settings,
        };

        crate::wire_fields::reset_encode_count();
        assert_publish_error(
            bus.publish(outbound(vec![42; 100])).expect_err("wire limit"),
            "wire_too_large",
        );
        assert_eq!(crate::wire_fields::encode_count(), 1);
        let permits: Vec<_> = (0..capacity)
            .map(|_| {
                client
                    .try_command(crate::client::CommandClass::General)
                    .expect("encoding failure released permit")
            })
            .collect();
        drop(permits);

        let permits: Vec<_> = (0..capacity)
            .map(|_| {
                client
                    .try_command(crate::client::CommandClass::General)
                    .expect("fill general admission")
            })
            .collect();
        crate::wire_fields::reset_encode_count();
        assert_publish_error(
            bus.publish(outbound(vec![42; 100])).expect_err("full admission"),
            "resource_limit",
        );
        assert_eq!(crate::wire_fields::encode_count(), 0);
        drop(permits);
    }

    #[test]
    fn test_publish_holds_one_general_permit_during_concurrent_encoding() {
        let _gate_lock = crate::wire_fields::encode_gate_test_lock();
        let settings = one_general_slot_settings();
        let client = Arc::new(Client::new(&settings).expect("client"));
        let bus = Arc::new(RedisEventBus {
            client: Arc::clone(&client),
            settings,
        });
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        crate::wire_fields::install_encode_gate("test-encoding-gate", entered_tx, release_rx);
        let first_bus = Arc::clone(&bus);
        let first = thread::spawn(move || {
            crate::wire_fields::reset_encode_count();
            let result = first_bus.publish(gated_outbound());
            (result, crate::wire_fields::encode_count())
        });

        if entered_rx.recv_timeout(Duration::from_secs(5)).is_err() {
            let _ = release_tx.send(());
            crate::wire_fields::clear_encode_gate();
            let _ = first.join();
            panic!("publisher did not reach the encoding gate");
        }
        let second_bus = Arc::clone(&bus);
        let second = thread::spawn(move || {
            crate::wire_fields::reset_encode_count();
            let result = second_bus.publish(gated_outbound());
            (result, crate::wire_fields::encode_count())
        });
        let (second_result, second_encode_count) = second.join().expect("second publisher thread");
        let _ = release_tx.send(());
        let (first_result, first_encode_count) = first.join().expect("first publisher thread");
        crate::wire_fields::clear_encode_gate();
        assert_publish_error(
            second_result.expect_err("occupied encoding slot rejects another publish"),
            "resource_limit",
        );
        assert_eq!(second_encode_count, 0);
        assert!(first_result.is_err(), "offline endpoint fails after encoding");
        assert_eq!(first_encode_count, 1);
        let permit = client
            .try_command(crate::client::CommandClass::General)
            .expect("first publisher releases its permit after setup failure");
        drop(permit);
    }

    #[test]
    fn test_stream_id_validation_rejects_malformed_components() {
        assert!(crate::internal::valid_stream_id("123-0"));
        for value in ["", "123", "-0", "123-", "a-0", "1-b", "1-2-3"] {
            assert!(
                !crate::internal::valid_stream_id(value),
                "accepted malformed ID {value:?}"
            );
        }
    }

    #[test]
    fn test_receiver_connection_failure_is_returned_before_delivery() {
        let settings =
            RedisEventBusConfig::new("redis://127.0.0.1:1/", "connection-errors").expect("valid test configuration");
        let bus = RedisEventBus {
            client: Arc::new(Client::new(&settings).expect("unreachable Redis URL is syntactically valid")),
            settings,
        };
        let mut receiver = super::Subscription {
            client: Arc::clone(&bus.client),
            wire_limits: WireLimits::from_config(&bus.settings),
            receive_connection: None,
            receiver_permit: None,
            key: "connection-errors:events".into(),
            group: "workers".into(),
            quarantine: "connection-errors:quarantine".into(),
            consumer: "worker".into(),
            topic: TopicAddress::new("events").expect("topic is valid"),
            subscription_id: Id::new(2),
            closed: false,
            claim_min_idle_ms: 0,
            recovery_interval: Duration::from_secs(1),
            max_unsettled: 1,
            recovery: Arc::new(Mutex::new(RecoveryState::new())),
        };
        assert!(receiver.receive(Duration::ZERO).is_err());
    }
}
