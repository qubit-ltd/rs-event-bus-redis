// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared configuration helpers for runnable provider examples.

use qubit_event_bus::model::ProviderOptions;

/// Returns options for `redis_url` and `namespace` without I/O.
/// `sentinel` adds nodes/service for `Some`, and uses standalone mode for
/// `None`.
#[must_use]
pub(crate) fn provider_options(redis_url: &str, namespace: &str, sentinel: Option<(&str, &str)>) -> ProviderOptions {
    let mut entries = vec![
        ("redis.url".into(), redis_url.into()),
        ("redis.namespace".into(), namespace.into()),
    ];
    if let Some((nodes, service_name)) = sentinel {
        entries.push(("redis.sentinel.nodes".into(), nodes.into()));
        entries.push(("redis.sentinel.service_name".into(), service_name.into()));
    }
    entries.into_iter().collect()
}
