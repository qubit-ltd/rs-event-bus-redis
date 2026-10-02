// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public Redis subscription profile behavior without Redis I/O.

use qubit_event_bus::model::AckMode;
use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus_redis::RedisSubscriptionProfile;

#[test]
fn test_options_preserves_explicit_start_position_without_group() {
    for start_position in [StartPosition::Earliest, StartPosition::New] {
        let options = RedisSubscriptionProfile::new(start_position.clone())
            .options::<String>()
            .build();

        assert_eq!(options.durability(), SubscriptionDurability::Durable);
        assert_eq!(options.start_position(), &start_position);
        assert_eq!(options.consumer_group(), None);
    }
}

#[test]
fn test_options_preserves_group_and_allows_ack_configuration() {
    let group = ConsumerGroup::new("billing").expect("valid group name");
    for start_position in [StartPosition::Earliest, StartPosition::New] {
        let options = RedisSubscriptionProfile::new(start_position.clone())
            .consumer_group(group.clone())
            .options::<String>()
            .ack_mode(AckMode::Manual)
            .build();

        assert_eq!(options.durability(), SubscriptionDurability::Durable);
        assert_eq!(options.start_position(), &start_position);
        assert_eq!(options.consumer_group(), Some(&group));
        assert_eq!(options.ack_mode(), AckMode::Manual);
    }
}
