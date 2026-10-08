// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Standalone Redis TLS publish, receive, ACK, and identity-verification tests.

#![cfg(any(feature = "sync", feature = "async"))]

mod support;

use std::error::Error;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_id::Id;
use support::tls_redis_server::TlsRedisServer;

static IDS: AtomicU64 = AtomicU64::new(1);

#[cfg(feature = "sync")]
mod synchronous {
    use std::error::Error;
    use std::sync::Arc;
    use std::time::Duration;

    use qubit_event_bus::EventBusConfig;
    use qubit_event_bus::model::ProviderOptions;
    use qubit_event_bus::spi::DeliveryDisposition;
    use qubit_event_bus::spi::EventBusSpi;
    use qubit_event_bus::spi::ReceiveOutcome;
    use qubit_event_bus::spi::TransportPayload;
    use qubit_event_bus_redis::sync::RedisEventBusProvider;
    use qubit_spi::ServiceProvider;

    use super::TlsRedisServer;
    use super::message;
    use super::request;

    /// Verifies a CA-validated mTLS publish, receive, and ACK round trip.
    #[test]
    fn test_sync_tls_publish_receive_and_ack() -> Result<(), Box<dyn Error>> {
        let server = TlsRedisServer::start_mtls()?;
        let bus = create_bus(
            server.url(),
            &[
                ("redis.tls_ca_cert_path", server.ca_path()),
                ("redis.tls_client_cert_path", server.client_cert_path()),
                ("redis.tls_client_key_path", server.client_key_path()),
            ],
        )?;
        verify_round_trip(bus)
    }

    /// Verifies that an untrusted CA and a mismatched endpoint name both fail.
    #[test]
    fn test_sync_tls_rejects_wrong_ca_and_hostname() -> Result<(), Box<dyn Error>> {
        let server = TlsRedisServer::start()?;
        for (url, ca_path) in [
            (server.url().to_owned(), server.wrong_ca_path().to_owned()),
            (
                server.url().replace("localhost", "127.0.0.1"),
                server.ca_path().to_owned(),
            ),
        ] {
            let bus = create_bus(&url, &[("redis.tls_ca_cert_path", &ca_path)])?;
            let failure = bus
                .publish(message("tls-negative", "not-delivered", b"secret")?)
                .expect_err("TLS identity verification must reject the connection");
            let diagnostic = format!("{failure:?} {failure}");
            assert!(!diagnostic.contains(&ca_path));
        }
        Ok(())
    }

    /// Ensures provider creation errors hide invalid TLS paths and PEM bytes.
    #[test]
    fn test_sync_tls_certificate_configuration_errors_are_sanitized() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("private-ca.pem");
        std::fs::write(&path, b"TEST-PRIVATE-CERTIFICATE-CONTENT")?;
        let ca_path = path.to_string_lossy().into_owned();
        let error = create_bus("rediss://localhost:6379/", &[("redis.tls_ca_cert_path", &ca_path)])
            .err()
            .ok_or("invalid PEM must fail during provider construction")?;
        let diagnostic = format!("{error:?} {error}");
        assert!(!diagnostic.contains(&ca_path));
        assert!(!diagnostic.contains("TEST-PRIVATE-CERTIFICATE-CONTENT"));
        Ok(())
    }

    /// Builds a synchronous public provider with the supplied TLS options.
    fn create_bus(url: &str, tls_options: &[(&str, &str)]) -> Result<Arc<dyn EventBusSpi>, Box<dyn Error>> {
        let mut options = ProviderOptions::new();
        options.insert("redis.url".into(), url.into());
        options.insert("redis.namespace".into(), "sync-tls-tests".into());
        for (key, value) in tls_options {
            options.insert((*key).into(), (*value).into());
        }
        RedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(options))
            .map_err(|failure| failure.into_error())
            .map_err(Into::into)
    }

    /// Publishes one event, receives its bytes, ACKs it, and closes the lease.
    fn verify_round_trip(bus: Arc<dyn EventBusSpi>) -> Result<(), Box<dyn Error>> {
        let _ = bus.publish(message("tls-orders", "sync-tls-event", b"verified tls")?)?;
        let mut receiver = bus.subscribe(request("tls-orders", "sync-tls-worker")?)?;
        let ReceiveOutcome::Message(mut received) = receiver.receive(Duration::from_secs(3))? else {
            return Err("TLS event was not received".into());
        };
        let TransportPayload::Encoded(payload) = received.payload() else {
            return Err("TLS event payload was not encoded".into());
        };
        assert_eq!(payload.bytes(), b"verified tls");
        let token = received.take_settlement().ok_or("missing ACK token")?;
        receiver.settle(&token, DeliveryDisposition::Accept)?;
        receiver.close()?;
        Ok(())
    }
}

#[cfg(feature = "async")]
mod asynchronous {
    use std::error::Error;
    use std::sync::Arc;
    use std::time::Duration;

    use futures_lite::future::block_on;
    use qubit_event_bus::EventBusConfig;
    use qubit_event_bus::model::ProviderOptions;
    use qubit_event_bus::spi::AsyncEventBusSpi;
    use qubit_event_bus::spi::DeliveryDisposition;
    use qubit_event_bus::spi::ReceiveOutcome;
    use qubit_event_bus::spi::TransportPayload;
    use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
    use qubit_spi::AsyncServiceProvider;

    use super::TlsRedisServer;
    use super::message;
    use super::request;

    /// Verifies a CA-validated async TLS publish, receive, and ACK round trip.
    #[test]
    fn test_async_tls_publish_receive_and_ack() -> Result<(), Box<dyn Error>> {
        let server = TlsRedisServer::start()?;
        let bus = create_bus(server.url(), server.ca_path())?;
        block_on(async move { verify_round_trip(bus).await })
    }

    /// Verifies an async connection rejects an endpoint name absent from SAN.
    #[test]
    fn test_async_tls_rejects_hostname_mismatch() -> Result<(), Box<dyn Error>> {
        let server = TlsRedisServer::start()?;
        let url = server.url().replace("localhost", "127.0.0.1");
        let bus = create_bus(&url, server.ca_path())?;
        block_on(async move {
            assert!(
                bus.publish(message("tls-negative", "async-not-delivered", b"secret")?)
                    .await
                    .is_err()
            );
            Ok::<(), Box<dyn Error>>(())
        })
    }

    /// Builds an async public provider with its standalone CA path.
    fn create_bus(url: &str, ca_path: &str) -> Result<Arc<dyn AsyncEventBusSpi>, Box<dyn Error>> {
        let options: ProviderOptions = [
            ("redis.url".into(), url.into()),
            ("redis.namespace".into(), "async-tls-tests".into()),
            ("redis.tls_ca_cert_path".into(), ca_path.into()),
        ]
        .into();
        let config = EventBusConfig::default().with_provider_options(options);
        block_on(AsyncRedisEventBusProvider.create_configured(&config))
            .map_err(|failure| failure.into_error())
            .map_err(Into::into)
    }

    /// Publishes, receives, ACKs, and closes one async TLS delivery.
    async fn verify_round_trip(bus: Arc<dyn AsyncEventBusSpi>) -> Result<(), Box<dyn Error>> {
        let _ = bus
            .publish(message("tls-orders", "async-tls-event", b"verified tls")?)
            .await?;
        let mut receiver = bus.subscribe(request("tls-orders", "async-tls-worker")?).await?;
        let ReceiveOutcome::Message(mut received) = receiver.receive(Duration::from_secs(3)).await? else {
            return Err("async TLS event was not received".into());
        };
        let TransportPayload::Encoded(payload) = received.payload() else {
            return Err("async TLS event payload was not encoded".into());
        };
        assert_eq!(payload.bytes(), b"verified tls");
        let token = received.take_settlement().ok_or("missing async ACK token")?;
        receiver.settle(&token, DeliveryDisposition::Accept).await?;
        receiver.close().await?;
        Ok(())
    }
}

/// Builds an encoded event without network IO; returns identifier or content
/// type validation errors.
fn message(topic: &str, id: &str, payload: &[u8]) -> Result<OutboundMessage, Box<dyn Error>> {
    Ok(OutboundMessage::new(
        TopicAddress::new(topic)?,
        EventId::new(format!("{id}-{}", IDS.fetch_add(1, Ordering::Relaxed)))?,
        SystemTime::now(),
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            std::sync::Arc::from(payload.to_vec()),
            ContentType::new("application/octet-stream")?,
            None,
        )),
    ))
}

/// Builds a durable earliest-position request; returns identifier validation
/// errors without network IO.
fn request(topic: &str, subscriber: &str) -> Result<SpiSubscriptionRequest, Box<dyn Error>> {
    Ok(SpiSubscriptionRequest::new(
        Id::new(IDS.fetch_add(1, Ordering::Relaxed)),
        TopicAddress::new(topic)?,
        SubscriberId::new(subscriber)?,
        None,
        SubscriptionDurability::Durable,
        StartPosition::Earliest,
        ProviderOptions::new(),
        std::any::TypeId::of::<Vec<u8>>(),
    ))
}
