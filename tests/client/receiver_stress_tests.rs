// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Controlled stress test for dedicated receivers and short-command admission.

#![cfg(feature = "sync")]

use std::any::TypeId;
use std::sync::Arc;
use std::thread::spawn;
use std::time::Duration;

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus_redis::sync::RedisEventBusProvider;
use qubit_id::Id;
use qubit_spi::ServiceProvider;

use super::message;
use super::options;
use crate::support::controlled_redis::proxy::ControlledRedis;
use crate::support::redis_server::RedisServer;

/// Creates one receiver request for the controlled shared consumer group.
fn request() -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(1),
        TopicAddress::new("events").expect("topic"),
        SubscriberId::new("admission-worker").expect("subscriber"),
        None,
        SubscriptionDurability::Durable,
        StartPosition::Earliest,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    )
}

/// Exercises many dedicated sockets while a controlled publish owns the only
/// general short-command slot.
#[test]
fn test_one_hundred_sync_receivers_poll_without_general_command_admission() {
    let server = RedisServer::start().expect("isolated Redis");
    let proxy = ControlledRedis::start(server.url()).expect("controlled Redis");
    let mut settings = options(&proxy.url(), 2);
    settings.insert("redis.max_active_receivers".into(), "100".into());
    settings.insert("redis.command_timeout_ms".into(), "2000".into());
    settings.insert("redis.connect_timeout_ms".into(), "2000".into());
    let bus = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(settings))
        .expect("provider");
    let mut receivers: Vec<_> = (0..100)
        .map(|_| bus.subscribe(request()).expect("receiver lease"))
        .collect();
    let gate = proxy.pause_after_reply("XADD");
    let publisher = Arc::clone(&bus);
    let publishing = spawn(move || publisher.publish(message()));
    let reached = gate.wait_until_reached(Duration::from_secs(2));
    let observations = if reached {
        receivers
            .iter_mut()
            .map(|receiver| receiver.receive(Duration::ZERO))
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    gate.release();
    assert!(reached, "publish reply must be paused after Redis accepted XADD");
    let _ = publishing.join().expect("publish worker").expect("publish reply");
    for (index, observation) in observations.into_iter().enumerate() {
        assert!(
            matches!(observation, Ok(ReceiveOutcome::Message(_) | ReceiveOutcome::TimedOut)),
            "receiver {index} must poll without a command resource limit"
        );
    }
    for receiver in &mut receivers {
        receiver.close().expect("close receiver");
    }
}
