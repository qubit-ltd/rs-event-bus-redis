// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Pure connection and command waiting-budget policy.

use std::time::Duration;

use crate::config::RedisEventBusConfig;
use crate::error::RedisProviderError;

/// Finite waiting budgets shared by synchronous and asynchronous adapters.
///
/// These budgets bound individual waits; they are not a synchronous wall-clock
/// deadline.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TransportPolicy {
    /// Single-endpoint TCP and Redis setup waiting budget.
    pub(crate) connect_timeout: Duration,
    /// Short-command response or socket waiting budget.
    pub(crate) command_timeout: Duration,
}

impl TransportPolicy {
    /// Copies the validated waiting budgets from `config` without performing
    /// I/O.
    ///
    /// # Parameters
    ///
    /// - `config`: Validated provider settings whose timeouts are copied.
    ///
    /// # Returns
    ///
    /// The transport policy for this client.
    #[must_use]
    #[inline]
    pub(crate) fn from_config(config: &RedisEventBusConfig) -> Self {
        Self {
            connect_timeout: config.connect_timeout(),
            command_timeout: config.command_timeout(),
        }
    }

    /// Calculates a response budget for an optional actual Redis BLOCK
    /// duration.
    ///
    /// # Parameters
    ///
    /// `block`: `Some` adds the server wait; `None` selects a short command.
    ///
    /// # Returns
    ///
    /// The command budget plus the actual BLOCK duration, when present.
    ///
    /// # Errors
    ///
    /// Returns a configuration error if the duration addition overflows.
    #[inline]
    pub(crate) fn response_timeout(&self, block: Option<Duration>) -> Result<Duration, RedisProviderError> {
        match block {
            None => Ok(self.command_timeout),
            Some(block) => block
                .checked_add(self.command_timeout)
                .ok_or(RedisProviderError::Configuration("response timeout overflow")),
        }
    }
}
