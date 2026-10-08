// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Deterministic collision properties for UTF-8 Redis key components.

use std::collections::HashSet;

use qubit_event_bus_redis::naming::group_name;
use qubit_event_bus_redis::naming::poison_key;
use qubit_event_bus_redis::naming::stream_key;

#[test]
fn test_length_prefixed_components_are_injective_for_unicode_and_delimiters() {
    let components = ["", "a", ":", "a:b", "b:c", "订单", "🙂", "6:订单", "::"];
    let mut streams = HashSet::new();
    let mut groups = HashSet::new();
    let mut quarantines = HashSet::new();
    for namespace in components {
        for topic in components {
            let stream = stream_key(namespace, topic);
            assert!(
                streams.insert(stream.clone()),
                "colliding stream for {namespace:?}/{topic:?}"
            );
            assert_eq!(stream, stream_key(namespace, topic));
            for identity in components {
                let group = group_name(namespace, topic, identity, None);
                assert_eq!(
                    group,
                    group_name(namespace, topic, "unused", Some(identity))
                );
                assert!(
                    groups.insert(group),
                    "colliding group for {namespace:?}/{topic:?}/{identity:?}"
                );
                assert!(
                    quarantines.insert(poison_key(namespace, topic, identity)),
                    "colliding quarantine key"
                );
            }
        }
    }
    assert_eq!(streams.len(), components.len().pow(2));
    assert_eq!(groups.len(), components.len().pow(3));
    assert_eq!(quarantines.len(), components.len().pow(3));
}

#[test]
fn test_poison_key_delimits_utf8_components_without_collisions() {
    assert_ne!(poison_key("a:b", "c", "d"), poison_key("a", "b:c", "d"));
    assert_eq!(
        poison_key("订单", "创建", "消费组"),
        "qubit:poison:6:订单:6:创建:9:消费组"
    );
}

#[test]
fn test_stream_key_delimits_components_without_collisions() {
    assert_ne!(stream_key("app", "a:b"), stream_key("app:a", "b"));
}

#[test]
fn test_consumer_group_name_is_stable_for_same_identity() {
    assert_eq!(
        group_name("app", "events", "worker", None),
        group_name("app", "events", "worker", None)
    );
    assert_ne!(
        group_name("app", "events", "worker", None),
        group_name("app", "events", "worker", Some("blue"))
    );
}
