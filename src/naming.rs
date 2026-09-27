// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Collision-safe Redis key construction.

/// Builds a namespaced stream key from length-prefixed components.
pub fn stream_key(namespace: &str, topic: &str) -> String {
    format!("qubit:stream:{}:{namespace}:{}:{topic}", namespace.len(), topic.len())
}

/// Builds a stable Redis consumer-group name.
pub fn group_name(namespace: &str, topic: &str, subscriber: &str, group: Option<&str>) -> String {
    let group = group.unwrap_or(subscriber);
    format!(
        "qubit:group:{}:{namespace}:{}:{topic}:{}:{group}",
        namespace.len(),
        topic.len(),
        group.len()
    )
}
