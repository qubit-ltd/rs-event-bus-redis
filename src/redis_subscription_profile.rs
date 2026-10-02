// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Explicit durable subscription policy for Redis Streams consumers.

use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeOptionsBuilder;
use qubit_event_bus::model::SubscriptionDurability;

/// Redis subscription settings shared across typed subscriber requests.
///
/// A start position is required for each profile. Redis subscriptions are
/// durable; the returned builder can add ACK, retry, and middleware policies.
/// Changing its durability to `Ephemeral` is rejected by the Redis provider's
/// capability check when subscribing.
#[derive(Clone, Debug)]
#[must_use]
pub struct RedisSubscriptionProfile {
    start_position: StartPosition,
    consumer_group: Option<ConsumerGroup>,
}

impl RedisSubscriptionProfile {
    /// Creates a durable profile with the specified starting position and no
    /// consumer group.
    ///
    /// `start_position` is passed unchanged to every options builder.
    #[must_use]
    pub fn new(start_position: StartPosition) -> Self {
        Self {
            start_position,
            consumer_group: None,
        }
    }

    /// Assigns a validated Redis consumer group and returns the updated
    /// profile.
    #[must_use = "Use the returned profile."]
    pub fn consumer_group(mut self, group: ConsumerGroup) -> Self {
        self.consumer_group = Some(group);
        self
    }

    /// Builds durable options for payload type `T` with the profile's start
    /// position and optional consumer group.
    ///
    /// Callers can append ACK, retry, and middleware settings before `build`.
    /// A later `durability(Ephemeral)` call is rejected by the Redis provider
    /// when the resulting request is subscribed.
    #[must_use = "Build and apply these options to a subscription request."]
    pub fn options<T: 'static>(&self) -> SubscribeOptionsBuilder<T> {
        let options = SubscribeOptions::<T>::builder()
            .durability(SubscriptionDurability::Durable)
            .start_position(self.start_position.clone());
        match &self.consumer_group {
            Some(group) => options.consumer_group(group.clone()),
            None => options,
        }
    }
}
