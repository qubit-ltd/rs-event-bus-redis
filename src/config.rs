// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Non-secret settings for Redis Streams providers.

/// Redis provider connection settings.
#[derive(Clone)]
pub struct RedisEventBusConfig {
    /// Redis connection URL, including credentials where required.
    connection_url: String,
    /// Namespace prepended to generated stream names.
    namespace: String,
}

impl std::fmt::Debug for RedisEventBusConfig {
    /// Formats the configuration without exposing connection credentials.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RedisEventBusConfig")
            .field("connection_url", &"<redacted>")
            .field("namespace", &self.namespace)
            .finish()
    }
}

impl Default for RedisEventBusConfig {
    /// Creates settings using the local Redis default and `qubit` namespace.
    fn default() -> Self {
        Self {
            connection_url: "redis://127.0.0.1/".into(),
            namespace: "qubit".into(),
        }
    }
}

impl RedisEventBusConfig {
    /// Creates configuration from a Redis URL and namespace.
    pub fn new(connection_url: &str, namespace: &str) -> Self {
        Self {
            connection_url: connection_url.into(),
            namespace: namespace.into(),
        }
    }

    /// Returns the configured Redis URL for connection setup.
    pub fn connection_url(&self) -> &str {
        &self.connection_url
    }

    /// Returns the stream namespace.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}
