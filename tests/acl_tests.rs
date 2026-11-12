// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Real restricted-user ACL workflows against isolated Redis containers.

#![cfg(any(feature = "sync", feature = "async"))]

mod acl_support;
mod support;

use acl_support::documentation_tests;

#[test]
fn test_documented_acl_parameters_match_both_guides() {
    let english = documentation_tests::documented_arguments(documentation_tests::GUIDES[0]);
    let chinese = documentation_tests::documented_arguments(documentation_tests::GUIDES[1]);
    assert_eq!(english, chinese, "bilingual ACL commands drifted");
    let namespace = "guide-acl";
    let topic = "events";
    let group = qubit_event_bus_redis::naming::group_name(namespace, topic, "billing-worker", Some("billing"));
    let stream = qubit_event_bus_redis::naming::stream_key(namespace, topic);
    let quarantine = qubit_event_bus_redis::naming::poison_key(namespace, topic, &group);
    for guide in documentation_tests::GUIDES {
        let contents = std::fs::read_to_string(guide).unwrap();
        for expected_key in [&stream, &group, &quarantine] {
            assert!(
                contents.contains(expected_key),
                "{guide} is missing documented key {expected_key}"
            );
        }
    }
    let expected = [
        "ACL".to_owned(),
        "SETUSER".to_owned(),
        "event-worker".to_owned(),
        "reset".to_owned(),
        "on".to_owned(),
        ">replace-with-managed-secret".to_owned(),
        format!("~{stream}"),
        format!("~{quarantine}"),
        "+ping".into(),
        "+xadd".into(),
        "+xgroup|create".into(),
        "+xreadgroup".into(),
        "+xautoclaim".into(),
        "+xpending".into(),
        "+xrange".into(),
        "+xack".into(),
        "+eval".into(),
        "+type".into(),
    ];
    assert_eq!(english, expected);
}

#[cfg(feature = "sync")]
#[test]
fn test_documented_acl_command_runs_in_isolated_redis() {
    use std::process::Command;

    use redis::cmd;

    let mut args = documentation_tests::documented_arguments(documentation_tests::GUIDES[0]);
    let server = support::redis_server::RedisServer::start().unwrap();
    let url = server.url();
    let mut admin = redis::Client::open(url).unwrap().get_connection().unwrap();
    args[2] = "acl_docs_fixture".into();
    args[5] = ">one-time-fixture-secret".into();
    let mut acl = cmd("ACL");
    for argument in args.iter().skip(1) {
        acl.arg(argument);
    }
    let _: () = acl.query(&mut admin).unwrap();

    let authenticated_url = format!(
        "redis://acl_docs_fixture:one-time-fixture-secret@{}",
        url.trim_start_matches("redis://")
    );
    let authenticated = redis::Client::open(authenticated_url.as_str()).unwrap();
    let mut connection = authenticated.get_connection().unwrap();
    let _: String = cmd("PING").query(&mut connection).unwrap();

    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "test_sync_acl_child", "--nocapture"])
        .env("RER_ACL_URL", url)
        .env("RER_ACL_NAMESPACE", "guide-acl")
        .env("RER_ACL_CASE", "documented")
        .env("RER_ACL_USER", "acl_docs_fixture")
        .env("RER_ACL_PASSWORD", "one-time-fixture-secret")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "documented ACL provider workflow failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

use std::any::TypeId;
use std::error::Error;
use std::fs;
use std::thread::sleep;
use std::time::Duration;
use std::time::Instant;

#[cfg(any(feature = "sync", feature = "async"))]
use acl_support::run_sentinel_checks;
use acl_support::run_workflow;
#[cfg(feature = "async")]
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
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
#[cfg(feature = "async")]
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
#[cfg(feature = "sync")]
use qubit_event_bus_redis::sync::RedisEventBusProvider;
use qubit_id::Id;
#[cfg(feature = "async")]
use qubit_spi::AsyncServiceProvider;
#[cfg(feature = "sync")]
use qubit_spi::ServiceProvider;
#[cfg(any(feature = "sync", feature = "async"))]
use support::sentinel::SentinelServer;

const CHILD_TIMEOUT: Duration = Duration::from_secs(20);

fn image() -> String {
    std::env::var("RER_ACL_REDIS_IMAGE").unwrap_or_else(|_| "7-alpine".into())
}

fn parent(case: &str, helper: &str) {
    run_workflow(&image(), case, helper).unwrap_or_else(|error| panic!("restricted ACL workflow failed: {error}"));
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_acl_workflow() {
    parent("workflow", "test_sync_acl_child");
}
#[cfg(feature = "sync")]
#[test]
fn test_sync_acl_missing_eval() {
    parent("missing_eval", "test_sync_acl_child");
}
#[cfg(feature = "sync")]
#[test]
fn test_sync_acl_missing_quarantine_key() {
    parent("missing_quarantine_key", "test_sync_acl_child");
}
#[cfg(feature = "sync")]
#[test]
fn test_sync_acl_quarantine_xack_denial_is_unknown_after_copy() {
    parent("quarantine_missing_xack", "test_sync_acl_child");
}
#[cfg(feature = "sync")]
#[test]
fn test_sync_acl_settlement_missing_eval() {
    parent("settlement_missing_eval", "test_sync_acl_child");
}
#[cfg(feature = "sync")]
#[test]
fn test_sync_acl_settlement_missing_xpending() {
    parent("settlement_missing_xpending", "test_sync_acl_child");
}
#[cfg(feature = "sync")]
#[test]
fn test_sync_acl_settlement_missing_xack() {
    parent("settlement_missing_xack", "test_sync_acl_child");
}
#[cfg(feature = "async")]
#[test]
fn test_async_acl_workflow() {
    parent("workflow", "test_async_acl_child");
}
#[cfg(feature = "async")]
#[test]
fn test_async_acl_missing_eval() {
    parent("missing_eval", "test_async_acl_child");
}
#[cfg(feature = "async")]
#[test]
fn test_async_acl_missing_quarantine_key() {
    parent("missing_quarantine_key", "test_async_acl_child");
}
#[cfg(feature = "async")]
#[test]
fn test_async_acl_quarantine_xack_denial_is_unknown_after_copy() {
    parent("quarantine_missing_xack", "test_async_acl_child");
}
#[cfg(feature = "async")]
#[test]
fn test_async_acl_settlement_missing_eval() {
    parent("settlement_missing_eval", "test_async_acl_child");
}
#[cfg(feature = "async")]
#[test]
fn test_async_acl_settlement_missing_xpending() {
    parent("settlement_missing_xpending", "test_async_acl_child");
}
#[cfg(feature = "async")]
#[test]
fn test_async_acl_settlement_missing_xack() {
    parent("settlement_missing_xack", "test_async_acl_child");
}

#[cfg(feature = "sync")]
#[test]
#[ignore = "launched by its parent restricted-ACL test with isolated credentials"]
fn test_sync_acl_child() {
    sync_child().unwrap_or_else(|error| panic!("ACL child failed: {error}"));
}
#[cfg(feature = "async")]
#[test]
#[ignore = "launched by its parent restricted-ACL test with isolated credentials"]
fn test_async_acl_child() {
    block_on(async_child()).unwrap_or_else(|error| panic!("ACL child failed: {error}"));
}

fn config(namespace: &str) -> EventBusConfig {
    let mut options = ProviderOptions::new();
    options.insert(
        "redis.url".into(),
        std::env::var("RER_ACL_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into()),
    );
    options.insert("redis.namespace".into(), namespace.into());
    options.insert("redis.username_env".into(), "RER_ACL_USER".into());
    options.insert("redis.password_env".into(), "RER_ACL_PASSWORD".into());
    if let Ok(nodes) = std::env::var("RER_SENTINEL_NODES") {
        options.insert("redis.sentinel.nodes".into(), nodes);
        options.insert("redis.sentinel.service_name".into(), "qeventbus".into());
        if std::env::var_os("RER_SENTINEL_CREDENTIALS").is_none() {
            options.insert("redis.sentinel.username_env".into(), "RER_SENTINEL_USER".into());
            options.insert("redis.sentinel.password_env".into(), "RER_SENTINEL_PASSWORD".into());
        }
    }
    EventBusConfig::default().with_provider_options(options)
}

fn request() -> Result<SpiSubscriptionRequest, Box<dyn Error>> {
    Ok(SpiSubscriptionRequest::new(
        Id::new(8123),
        TopicAddress::new("events")?,
        SubscriberId::new("worker")?,
        Some(ConsumerGroup::new("workers")?),
        SubscriptionDurability::Durable,
        StartPosition::New,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    ))
}

pub(crate) fn message(topic: &str, id: &str, bytes: &[u8]) -> Result<OutboundMessage, Box<dyn Error>> {
    Ok(OutboundMessage::new(
        TopicAddress::new(topic)?,
        EventId::new(id)?,
        std::time::SystemTime::now(),
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            std::sync::Arc::from(bytes.to_vec()),
            ContentType::new("application/octet-stream")?,
            None,
        )),
    ))
}

fn marker(name: &str) -> std::path::PathBuf {
    std::env::var_os(name).expect("parent sets marker path").into()
}

fn signal_ready_and_wait() -> Result<(), Box<dyn Error>> {
    fs::write(marker("RER_ACL_READY"), b"ready")?;
    let proceed = marker("RER_ACL_PROCEED");
    let deadline = Instant::now() + CHILD_TIMEOUT;
    while Instant::now() < deadline {
        if proceed.exists() {
            return Ok(());
        }
        sleep(Duration::from_millis(20));
    }
    Err("parent did not release ACL helper after poison injection".into())
}

#[cfg(feature = "sync")]
fn sync_child() -> Result<(), Box<dyn Error>> {
    use qubit_event_bus::error::SpiError;
    let namespace = std::env::var("RER_ACL_NAMESPACE")?;
    let case = std::env::var("RER_ACL_CASE")?;
    let bus = RedisEventBusProvider
        .create_configured(&config(&namespace))
        .map_err(|failure| failure.into_error())?;
    let mut subscription = bus.subscribe(request()?)?;
    let _ = bus.publish(message("events", "restricted-publish", b"normal")?)?;
    let ReceiveOutcome::Message(message) = subscription.receive(Duration::from_secs(3))? else {
        return Err("restricted account did not receive its published event".into());
    };
    let settle = subscription.settle(
        message.settlement().ok_or("missing settlement token")?,
        DeliveryDisposition::Accept,
    );
    if case.starts_with("settlement_") {
        let error = settle.expect_err("restricted settlement must fail");
        assert_eq!(error.kind(), "authentication");
        assert_eq!(error.retryable(), Some(false));
        assert!(!error.to_string().contains("ephemeral-acl-test-password"));
        signal_ready_and_wait()?;
        subscription.close()?;
        return Ok(());
    }
    settle?;
    if case == "documented" {
        subscription.close()?;
        return Ok(());
    }
    assert_outside_namespace_denied_sync(&namespace)?;
    signal_ready_and_wait()?;
    let outcome = subscription.receive(Duration::from_secs(3));
    if case == "workflow" {
        assert!(
            matches!(outcome?, ReceiveOutcome::Gap(_)),
            "malformed record must be isolated"
        );
    } else {
        match outcome {
            Err(error @ SpiError::Operation { .. }) => {
                let expected = if matches!(case.as_str(), "missing_eval" | "missing_quarantine_key") {
                    "authentication"
                } else {
                    "outcome_unknown"
                };
                assert_eq!(error.kind(), expected, "ACL denial category for {case}");
                assert_eq!(error.retryable(), Some(false));
                assert!(!error.to_string().contains("ephemeral-acl-test-password"));
            }
            Err(error) => {
                return Err(format!("ACL denial had unexpected error variant: {error}").into());
            }
            Ok(ReceiveOutcome::Gap(_)) => {
                return Err("ACL denial was incorrectly reported as quarantine success".into());
            }
            Ok(_) => return Err("ACL denial unexpectedly returned a delivery result".into()),
        }
    }
    Ok(())
}

#[cfg(feature = "sync")]
fn assert_outside_namespace_denied_sync(namespace: &str) -> Result<(), Box<dyn Error>> {
    let bus = RedisEventBusProvider
        .create_configured(&config(&format!("{namespace}-unrelated")))
        .map_err(|failure| failure.into_error())?;
    assert!(bus.publish(message("events", "unrelated-write", b"x")?).is_err());
    let _ = bus
        .subscribe(request()?)
        .err()
        .ok_or("unrelated read unexpectedly succeeded")?;
    Ok(())
}

#[cfg(feature = "async")]
async fn async_child() -> Result<(), Box<dyn Error>> {
    use qubit_event_bus::error::SpiError;
    let namespace = std::env::var("RER_ACL_NAMESPACE")?;
    let case = std::env::var("RER_ACL_CASE")?;
    let bus = AsyncRedisEventBusProvider
        .create_configured(&config(&namespace))
        .await
        .map_err(|failure| failure.into_error())?;
    let mut subscription = bus.subscribe(request()?).await?;
    let _ = bus.publish(message("events", "restricted-publish", b"normal")?).await?;
    let ReceiveOutcome::Message(message) = subscription.receive(Duration::from_secs(3)).await? else {
        return Err("restricted account did not receive its published event".into());
    };
    let settle = subscription
        .settle(
            message.settlement().ok_or("missing settlement token")?,
            DeliveryDisposition::Accept,
        )
        .await;
    if case.starts_with("settlement_") {
        let error = settle.expect_err("restricted settlement must fail");
        assert_eq!(error.kind(), "authentication");
        assert_eq!(error.retryable(), Some(false));
        assert!(!error.to_string().contains("ephemeral-acl-test-password"));
        signal_ready_and_wait()?;
        subscription.close().await?;
        return Ok(());
    }
    settle?;
    assert_outside_namespace_denied_async(&namespace).await?;
    signal_ready_and_wait()?;
    let outcome = subscription.receive(Duration::from_secs(3)).await;
    if case == "workflow" {
        assert!(
            matches!(outcome?, ReceiveOutcome::Gap(_)),
            "malformed record must be isolated"
        );
    } else {
        match outcome {
            Err(error @ SpiError::Operation { .. }) => {
                let expected = if matches!(case.as_str(), "missing_eval" | "missing_quarantine_key") {
                    "authentication"
                } else {
                    "outcome_unknown"
                };
                assert_eq!(error.kind(), expected, "ACL denial category for {case}");
                assert_eq!(error.retryable(), Some(false));
                assert!(!error.to_string().contains("ephemeral-acl-test-password"));
            }
            Err(error) => {
                return Err(format!("ACL denial had unexpected error variant: {error}").into());
            }
            Ok(ReceiveOutcome::Gap(_)) => {
                return Err("ACL denial was incorrectly reported as quarantine success".into());
            }
            Ok(_) => return Err("ACL denial unexpectedly returned a delivery result".into()),
        }
    }
    Ok(())
}

#[cfg(feature = "async")]
async fn assert_outside_namespace_denied_async(namespace: &str) -> Result<(), Box<dyn Error>> {
    let bus = AsyncRedisEventBusProvider
        .create_configured(&config(&format!("{namespace}-unrelated")))
        .await
        .map_err(|failure| failure.into_error())?;
    assert!(bus.publish(message("events", "unrelated-write", b"x")?).await.is_err());
    let _ = bus
        .subscribe(request()?)
        .await
        .err()
        .ok_or("unrelated read unexpectedly succeeded")?;
    Ok(())
}

#[cfg(feature = "sync")]
#[test]
fn test_concurrent_sentinel_fixture_starts_use_distinct_ports() {
    let start = || std::thread::spawn(|| SentinelServer::start().map_err(|error| error.to_string()));
    let first_thread = start();
    let second_thread = start();
    let first = first_thread
        .join()
        .expect("first fixture thread")
        .expect("first Sentinel fixture");
    let second = second_thread
        .join()
        .expect("second fixture thread")
        .expect("second Sentinel fixture");
    let first_ports: Vec<_> = first
        .endpoints()
        .split(',')
        .map(|endpoint| endpoint.rsplit(':').next().expect("endpoint port").to_owned())
        .collect();
    let second_ports: Vec<_> = second
        .endpoints()
        .split(',')
        .map(|endpoint| endpoint.rsplit(':').next().expect("endpoint port").to_owned())
        .collect();

    assert!(
        first_ports.iter().all(|port| !second_ports.contains(port)),
        "concurrent Sentinel fixtures must own disjoint ports"
    );
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_sentinel_acl_workflow() {
    let sentinel = SentinelServer::start().expect("isolated Sentinel fixture");
    run_sentinel_checks(&sentinel, "test_sync_sentinel_acl_child")
        .unwrap_or_else(|error| panic!("Sentinel ACL checks failed: {error}"));
}

#[cfg(feature = "async")]
#[test]
fn test_async_sentinel_acl_workflow() {
    let sentinel = SentinelServer::start().expect("isolated Sentinel fixture");
    run_sentinel_checks(&sentinel, "test_async_sentinel_acl_child")
        .unwrap_or_else(|error| panic!("Sentinel ACL checks failed: {error}"));
}

#[cfg(feature = "sync")]
#[test]
#[ignore = "launched by the parent Sentinel ACL integration test"]
fn test_sync_sentinel_acl_child() {
    sync_sentinel_child().unwrap_or_else(|error| panic!("Sentinel ACL child failed: {error}"));
}

#[cfg(feature = "async")]
#[test]
#[ignore = "launched by the parent Sentinel ACL integration test"]
fn test_async_sentinel_acl_child() {
    block_on(async_sentinel_child()).unwrap_or_else(|error| panic!("Sentinel ACL child failed: {error}"));
}

#[cfg(feature = "sync")]
fn sync_sentinel_child() -> Result<(), Box<dyn Error>> {
    use qubit_event_bus::error::SpiError;
    let namespace = std::env::var("RER_ACL_NAMESPACE")?;
    let case = std::env::var("RER_SENTINEL_CASE")?;
    let bus = RedisEventBusProvider
        .create_configured(&config(&namespace))
        .map_err(|failure| failure.into_error())?;
    let result = bus.publish(message("events", "sentinel-acl", b"x")?);
    if case == "success" {
        let _ = result?;
    } else if case == "wrong_password" || case == "missing_auth" {
        let error = result.expect_err("invalid Sentinel setup credentials must fail");
        match error {
            SpiError::Publish {
                kind: "authentication",
                retryable: Some(false),
                ..
            } => {}
            error => {
                return Err(format!("Sentinel setup failure had unexpected category: {error}").into());
            }
        }
        let display = error.to_string();
        assert!(
            !display.contains("qubit_sentinel_test"),
            "error leaked username: {display}"
        );
        assert!(
            !display.contains("ephemeral-acl-test-password"),
            "error leaked password: {display}"
        );
        assert!(
            !display.contains("incorrect-sentinel-password"),
            "error leaked incorrect password: {display}"
        );
        for endpoint in std::env::var("RER_SENTINEL_NODES")?.split(',') {
            assert!(!display.contains(endpoint), "error leaked Sentinel endpoint: {display}");
        }
    } else {
        match result {
            Err(SpiError::Publish {
                kind: "authentication",
                retryable: Some(false),
                ..
            }) => {}
            Err(error) => {
                return Err(format!("Sentinel ACL denial had unexpected category: {error}").into());
            }
            Ok(_) => return Err("Sentinel ACL denial unexpectedly published".into()),
        }
    }
    Ok(())
}

#[cfg(feature = "async")]
async fn async_sentinel_child() -> Result<(), Box<dyn Error>> {
    use qubit_event_bus::error::SpiError;
    let namespace = std::env::var("RER_ACL_NAMESPACE")?;
    let case = std::env::var("RER_SENTINEL_CASE")?;
    let bus = AsyncRedisEventBusProvider
        .create_configured(&config(&namespace))
        .await
        .map_err(|failure| failure.into_error())?;
    let result = bus.publish(message("events", "sentinel-acl", b"x")?).await;
    if case == "success" {
        let _ = result?;
    } else if case == "wrong_password" || case == "missing_auth" {
        let error = result.expect_err("invalid Sentinel setup credentials must fail");
        match error {
            SpiError::Publish {
                kind: "authentication",
                retryable: Some(false),
                ..
            } => {}
            error => {
                return Err(format!("Sentinel setup failure had unexpected category: {error}").into());
            }
        }
        let display = error.to_string();
        assert!(
            !display.contains("qubit_sentinel_test"),
            "error leaked username: {display}"
        );
        assert!(
            !display.contains("ephemeral-acl-test-password"),
            "error leaked password: {display}"
        );
        assert!(
            !display.contains("incorrect-sentinel-password"),
            "error leaked incorrect password: {display}"
        );
        for endpoint in std::env::var("RER_SENTINEL_NODES")?.split(',') {
            assert!(!display.contains(endpoint), "error leaked Sentinel endpoint: {display}");
        }
    } else {
        match result {
            Err(SpiError::Publish {
                kind: "authentication",
                retryable: Some(false),
                ..
            }) => {}
            Err(error) => {
                return Err(format!("Sentinel ACL denial had unexpected category: {error}").into());
            }
            Ok(_) => return Err("Sentinel ACL denial unexpectedly published".into()),
        }
    }
    Ok(())
}
