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
///
/// ```
/// use qubit_event_bus::model::ConsumerGroup;
/// use qubit_event_bus::model::StartPosition;
/// use qubit_event_bus::model::SubscriptionDurability;
/// use qubit_event_bus_redis::RedisSubscriptionProfile;
///
/// let group = ConsumerGroup::new("billing")?;
/// let options = RedisSubscriptionProfile::new(StartPosition::Earliest)
///     .consumer_group(group.clone())
///     .options::<String>()
///     .build();
/// assert_eq!(options.durability(), SubscriptionDurability::Durable);
/// assert_eq!(options.start_position(), &StartPosition::Earliest);
/// assert_eq!(options.consumer_group(), Some(&group));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug)]
#[must_use]
pub struct RedisSubscriptionProfile {
    /// Required start position copied into each options builder.
    start_position: StartPosition,
    /// Optional consumer group applied to each options builder.
    consumer_group: Option<ConsumerGroup>,
}

impl RedisSubscriptionProfile {
    /// Creates a durable profile with the specified starting position and no
    /// consumer group.
    ///
    /// `start_position` is passed unchanged to every options builder.
    ///
    /// # Parameters
    ///
    /// - `start_position`: Required position from which a new group should
    ///   begin consuming, or the requested position when using an existing
    ///   group.
    ///
    /// # Returns
    ///
    /// A durable profile that uses this start position and has no consumer
    /// group configured.
    pub fn new(start_position: StartPosition) -> Self {
        Self {
            start_position,
            consumer_group: None,
        }
    }

    /// Assigns a validated Redis consumer group and returns the updated
    /// profile.
    ///
    /// # Parameters
    ///
    /// - `group`: Consumer group to include in options built from this profile.
    ///
    /// # Returns
    ///
    /// The updated profile with `group` configured.
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
    ///
    /// # Type Parameters
    ///
    /// - `T`: Payload type delivered by the subscription.
    ///
    /// # Returns
    ///
    /// A builder with durable delivery, this profile's start position, and its
    /// optional consumer group. Additional subscription policies can be set
    /// before building the options.
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
