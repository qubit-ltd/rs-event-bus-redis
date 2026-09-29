// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Crate-private recovery and unsettled-delivery configuration.

use qubit_event_bus::model::ProviderOptions;

use crate::config::RedisEventBusConfig;

#[test]
#[cfg(any(feature = "sync", feature = "async"))]
fn test_provider_options_validate_pool_and_stream_limits() {
    let options: ProviderOptions = [
        ("redis.max_idle_connections".into(), "64".into()),
        ("redis.max_unsettled_per_subscription".into(), "10000".into()),
        ("redis.stream_maxlen_approx".into(), "42".into()),
    ]
    .into();
    let config = RedisEventBusConfig::from_provider_options(&options).unwrap();
    assert_eq!(config.max_idle_connections(), 64);
    assert_eq!(config.max_unsettled_per_subscription(), 10_000);
    assert_eq!(config.stream_maxlen_approx().unwrap().get(), 42);
    for (key, value) in [
        ("redis.max_idle_connections", "65"),
        ("redis.max_unsettled_per_subscription", "10001"),
        ("redis.stream_maxlen_approx", "0"),
    ] {
        let options: ProviderOptions = [(key.into(), value.into())].into();
        assert!(
            RedisEventBusConfig::from_provider_options(&options).is_err(),
            "{key}={value}"
        );
    }
}

#[test]
#[cfg(any(feature = "sync", feature = "async"))]
fn test_provider_options_accept_claim_idle_threshold() {
    let options: ProviderOptions = [("redis.claim_min_idle_ms".into(), "125".into())].into();
    let config = RedisEventBusConfig::from_provider_options(&options).unwrap();
    assert_eq!(config.claim_min_idle_ms(), 125);
}
