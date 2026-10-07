// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Publishes one order event, consumes it, then explicitly cancels and shuts
//! down.

mod support;

use std::env::args;
use std::error::Error;
use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus_redis::RedisSubscriptionProfile;
use qubit_spi::ProviderSelection;

fn main() -> Result<(), Box<dyn Error>> {
    let redis_url = args()
        .nth(1)
        .unwrap_or_else(|| "redis://127.0.0.1/".to_owned());
    let namespace = args()
        .nth(2)
        .unwrap_or_else(|| "sync-orders-example".to_owned());
    let config = EventBusConfig::default()
        .with_selection(ProviderSelection::named("redis-streams")?)
        .with_provider_options(support::provider_options(&redis_url, &namespace, None))
        .with_facade_config(support::facade_config()?);
    let registry = EventBusRegistry::discover()?;
    let bus = registry.create(&config)?;
    let topic = Topic::<String>::new("orders.created")?;
    let (sender, receiver) = mpsc::channel();
    let subscription = bus.subscribe(
        SubscribeRequest::new("sync-orders-example", topic.clone())?.with_options(
            RedisSubscriptionProfile::new(StartPosition::Earliest)
                .consumer_group(ConsumerGroup::new("billing")?)
                .options()
                .build(),
        ),
        move |delivery| {
            sender
                .send(delivery.payload().clone())
                .map_err(|error| DeliveryError::Handler {
                    source: Box::new(error),
                })
        },
    )?;
    let _ = bus.publish(PublishRequest::new(topic, "order-42".to_owned())?)?;
    let received = receiver.recv_timeout(Duration::from_secs(5))?;
    println!("consumed order event: {received}");
    subscription.cancel()?;
    let report = bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(5),
    })?;
    if report.outcome != ShutdownOutcome::Complete {
        return Err("Redis provider did not complete graceful shutdown".into());
    }
    Ok(())
}
