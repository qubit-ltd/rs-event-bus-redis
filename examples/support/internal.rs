// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared configuration helpers for runnable provider examples.

use std::error::Error;
use std::sync::Arc;

use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::ProviderOptions;

use super::utf8_codec::Utf8Codec;

/// Returns a facade configured with the UTF-8 string codec without I/O;
/// returns the content-type validation error if the fixed MIME type is
/// rejected.
pub(crate) fn facade_config() -> Result<EventBusFacadeConfig, Box<dyn Error>> {
    let mut codecs = CodecRegistry::new();
    codecs.register::<String>(Arc::new(Utf8Codec(ContentType::new("text/plain")?)));
    Ok(EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs)))
}

/// Returns options for `redis_url` and `namespace` without I/O.
/// `sentinel` adds nodes/service for `Some`, and uses standalone mode for
/// `None`.
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
