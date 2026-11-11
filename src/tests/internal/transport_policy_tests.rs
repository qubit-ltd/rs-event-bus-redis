// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Pure waiting-budget and byte-budget boundary regressions.

use std::time::Duration;

use crate::config::RedisEventBusConfig;
use crate::error::RedisProviderError;
use crate::internal::TransportPolicy;

#[test]
fn test_transport_policy_response_timeout_checked_add() {
    let policy = TransportPolicy::from_config(&RedisEventBusConfig::default());
    assert_eq!(policy.connect_timeout, Duration::from_secs(2));
    assert_eq!(
        policy.response_timeout(None).expect("short command"),
        Duration::from_secs(2)
    );
    assert_eq!(
        policy
            .response_timeout(Some(Duration::from_secs(1)))
            .expect("BLOCK margin"),
        Duration::from_secs(3)
    );
    assert!(matches!(
        policy.response_timeout(Some(Duration::MAX)),
        Err(RedisProviderError::Configuration(
            "response timeout overflow"
        ))
    ));
}
