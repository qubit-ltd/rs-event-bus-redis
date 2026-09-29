// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runs one runtime-neutral async receive and explicitly closes the session.

mod support;

use std::error::Error;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use futures_channel::oneshot;
use futures_lite::future;
use qubit_event_bus::AsyncEventBusRegistry;
use qubit_event_bus::SubscriberId;
use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::model::Topic;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus_redis as _;
use qubit_spi::ProviderSelection;

fn main() -> Result<(), Box<dyn Error>> {
    future::block_on(async {
        let redis_url = std::env::args()
            .nth(1)
            .unwrap_or_else(|| "redis://127.0.0.1/".to_owned());
        let namespace = std::env::args()
            .nth(2)
            .unwrap_or_else(|| "async-orders-example".to_owned());
        let config = EventBusConfig::default()
            .with_selection(ProviderSelection::named("redis-streams")?)
            .with_provider_options(support::provider_options(&redis_url, &namespace, None))
            .with_facade_config(support::facade_config()?);
        let registry = AsyncEventBusRegistry::discover()?;
        let bus = registry.create(&config).await?;
        let topic = Topic::<String>::new("orders.created")?;
        let mut subscription = bus
            .subscribe(
                SubscribeRequest::builder()
                    .subscriber_id(SubscriberId::new("async-orders-example")?)
                    .topic(topic.clone())
                    .consumer_group(ConsumerGroup::new("billing")?)
                    .durability(SubscriptionDurability::Durable)
                    .start_position(StartPosition::Earliest)
                    .build()?,
            )
            .await?;
        bus.publish(PublishRequest::new(topic, "order-43".to_owned())?).await?;

        let received = Arc::new(Mutex::new(None::<String>));
        let handler_received = Arc::clone(&received);
        let run = subscription.run(move |delivery| {
            let value = delivery.payload().clone();
            let handler_received = Arc::clone(&handler_received);
            async move {
                *handler_received.lock().expect("receive lock poisoned") = Some(value.clone());
                println!("consumed order event: {value}");
                Ok(())
            }
        });
        let run = async move {
            run.await.map_err(|error| -> Box<dyn Error> { Box::new(error) })?;
            Ok::<(), Box<dyn Error>>(())
        };
        let (stop_sender, stop_receiver) = oneshot::channel::<()>();
        thread::spawn(move || {
            let mut line = String::new();
            let _ = std::io::stdin().read_line(&mut line);
            let _ = stop_sender.send(());
        });
        println!("press Enter after the event is consumed to stop the runner");
        future::race(run, async move {
            stop_receiver
                .await
                .map_err(|error| -> Box<dyn Error> { Box::new(error) })
        })
        .await?;
        if received.lock().expect("receive lock poisoned").is_none() {
            return Err("subscription stopped before consuming an event".into());
        }
        subscription.close().await?;
        let report = bus
            .shutdown(ShutdownMode::Graceful {
                timeout: Duration::from_secs(5),
            })
            .await?;
        if report.outcome != qubit_event_bus::spi::ShutdownOutcome::Complete {
            return Err("Redis provider did not complete graceful shutdown".into());
        }
        Ok(())
    })
}
