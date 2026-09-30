// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider limits applied before copying or decoding wire components.

use crate::redis_event_bus_config::RedisEventBusConfig;
use crate::redis_provider_error::RedisProviderError;

/// Independent bounds for one Redis record and its decoded components.
#[derive(Clone, Copy, Debug)]
pub(crate) struct WireLimits {
    /// Maximum UTF-8 JSON bytes in one stored wire field.
    pub(crate) wire: usize,
    /// Maximum decoded payload byte-vector length.
    pub(crate) payload: usize,
    /// Maximum decoded headers JSON byte length.
    pub(crate) headers: usize,
}

impl Default for WireLimits {
    /// Returns the finite default wire, payload, and header limits.
    fn default() -> Self {
        Self {
            wire: 8_388_608,
            payload: 1_048_576,
            headers: 65_536,
        }
    }
}

impl WireLimits {
    /// Copies the validated byte budgets from provider configuration.
    #[must_use]
    pub(crate) fn from_config(config: &RedisEventBusConfig) -> Self {
        Self {
            wire: config.max_wire_bytes(),
            payload: config.max_payload_bytes(),
            headers: config.max_headers_bytes(),
        }
    }

    /// Checks one inclusive wire byte budget.
    pub(crate) fn check_wire(self, actual: usize) -> Result<(), RedisProviderError> {
        if actual > self.wire {
            Err(RedisProviderError::WireTooLarge)
        } else {
            Ok(())
        }
    }

    /// Checks one inclusive payload byte budget.
    pub(crate) fn check_payload(self, actual: usize) -> Result<(), RedisProviderError> {
        if actual > self.payload {
            Err(RedisProviderError::PayloadTooLarge)
        } else {
            Ok(())
        }
    }
}
