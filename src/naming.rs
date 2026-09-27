// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Collision-safe Redis key construction.

/// Builds a Redis stream key whose length-prefixed components cannot collide.
///
/// The namespace and topic are encoded as UTF-8 byte lengths followed by their
/// original contents. This keeps component boundaries unambiguous even when a
/// value contains `:`. The function does not validate either input; callers
/// that accept user configuration should validate it before key construction.
///
/// # Parameters
///
/// - `namespace`: Application scope shared by related providers.
/// - `topic`: Logical topic whose events are stored in the stream.
///
/// # Returns
///
/// A stable Redis key for this namespace and topic pair.
///
/// # Examples
///
/// ```
/// use qubit_event_bus_redis::naming::stream_key;
///
/// let key = stream_key("orders", "created");
/// assert_eq!(key, "qubit:stream:6:orders:7:created");
/// ```
pub fn stream_key(namespace: &str, topic: &str) -> String {
    format!("qubit:stream:{}:{namespace}:{}:{topic}", namespace.len(), topic.len())
}

/// Builds a stable Redis consumer-group name from subscription identity.
///
/// When `group` is `None`, the subscriber ID becomes the group identity. An
/// explicit group lets several subscribers share work while a different group
/// receives its own copy of every stream entry. Component lengths count UTF-8
/// bytes, so values containing `:` remain unambiguous.
///
/// # Parameters
///
/// - `namespace`: Application scope used by the corresponding stream key.
/// - `topic`: Logical topic consumed by the group.
/// - `subscriber`: Stable subscriber identity used when no group is supplied.
/// - `group`: Optional shared group identity.
///
/// # Returns
///
/// A deterministic Redis consumer-group name.
///
/// # Examples
///
/// ```
/// use qubit_event_bus_redis::naming::group_name;
///
/// assert_eq!(
///     group_name("orders", "created", "billing", None),
///     group_name("orders", "created", "billing", Some("billing")),
/// );
/// ```
pub fn group_name(namespace: &str, topic: &str, subscriber: &str, group: Option<&str>) -> String {
    let group = group.unwrap_or(subscriber);
    format!(
        "qubit:group:{}:{namespace}:{}:{topic}:{}:{group}",
        namespace.len(),
        topic.len(),
        group.len()
    )
}

/// Builds a group-specific Redis stream key for malformed-entry quarantine.
///
/// Length-prefixed UTF-8 byte counts keep namespace, topic, and group
/// boundaries unambiguous even when a component contains `:`.
///
/// # Parameters
///
/// - `namespace`: Application scope shared by related providers.
/// - `topic`: Topic whose malformed records are quarantined.
/// - `group`: Consumer group whose delivery reached a terminal decode error.
///
/// # Returns
///
/// A stable Redis key isolated to one namespace, topic, and consumer group.
///
/// # Examples
///
/// ```
/// use qubit_event_bus_redis::naming::poison_key;
///
/// assert_eq!(poison_key("orders", "created", "billing"),
///     "qubit:poison:6:orders:7:created:7:billing");
/// ```
pub fn poison_key(namespace: &str, topic: &str, group: &str) -> String {
    format!(
        "qubit:poison:{}:{namespace}:{}:{topic}:{}:{group}",
        namespace.len(),
        topic.len(),
        group.len()
    )
}

#[cfg(test)]
mod tests {
    use super::poison_key;

    #[test]
    fn test_poison_key_delimits_utf8_components_without_collisions() {
        assert_ne!(poison_key("a:b", "c", "d"), poison_key("a", "b:c", "d"));
        assert_eq!(
            poison_key("订单", "创建", "消费组"),
            "qubit:poison:6:订单:6:创建:9:消费组"
        );
    }
}
