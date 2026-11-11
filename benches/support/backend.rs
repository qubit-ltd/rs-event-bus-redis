// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public provider creation and single publication attempts.

use std::any::TypeId;
use std::error::Error;
use std::hint::black_box;
use std::sync::Arc;

use futures_lite::future::block_on;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
use qubit_event_bus_redis::sync::RedisEventBusProvider;
use qubit_id::Id;
use qubit_spi::AsyncServiceProvider;
use qubit_spi::ServiceProvider;

use super::receiver::Receiver;

/// Runtime choice, driven by equivalent independent worker threads.
#[derive(Clone)]
pub enum Backend {
    Sync(Arc<dyn EventBusSpi>),
    Async(Arc<dyn AsyncEventBusSpi>),
}

impl Backend {
    /// Creates a lazy public provider from identical before/after settings.
    pub fn new(mode: &str, options: ProviderOptions) -> Result<Self, Box<dyn Error>> {
        let config = EventBusConfig::default().with_provider_options(options);
        if mode == "sync" {
            Ok(Self::Sync(RedisEventBusProvider.create_configured(&config)?))
        } else {
            Ok(Self::Async(block_on(
                AsyncRedisEventBusProvider.create_configured(&config),
            )?))
        }
    }

    /// Prepares a receiver outside the measured worker region.
    pub fn subscribe(&self, topic: &str, worker: usize) -> Result<Receiver, Box<dyn Error>> {
        let request = SpiSubscriptionRequest::new(
            Id::new(worker as u64 + 1),
            TopicAddress::new(topic)?,
            SubscriberId::new(format!("worker-{worker}"))?,
            Some(ConsumerGroup::new(&format!("group-{worker}"))?),
            SubscriptionDurability::Durable,
            StartPosition::New,
            ProviderOptions::new(),
            TypeId::of::<Vec<u8>>(),
        );
        match self {
            Self::Sync(bus) => Ok(Receiver::Sync(bus.subscribe(request)?)),
            Self::Async(bus) => Ok(Receiver::Async(block_on(bus.subscribe(request))?)),
        }
    }

    /// Publishes exactly once, preserving phase and error certainty in CSV.
    pub fn publish(&self, message: OutboundMessage) -> String {
        match self {
            Self::Sync(bus) => bus.publish(black_box(message)).map_or_else(
                |error| format!("{}:{}", error.operation(), error.kind()),
                |_| "ok".into(),
            ),
            Self::Async(bus) => block_on(bus.publish(black_box(message))).map_or_else(
                |error| format!("{}:{}", error.operation(), error.kind()),
                |_| "ok".into(),
            ),
        }
    }

    /// Measures one publish/receive/accept attempt or idle receive.
    pub fn attempt(&self, receiver: &mut Receiver, message: Option<OutboundMessage>) -> String {
        if let Some(message) = message {
            let expected_id = message.id().clone();
            let result = self.publish(message);
            if result != "ok" {
                return result;
            }
            receiver.receive_accept(&expected_id)
        } else {
            receiver.poll()
        }
    }
}
