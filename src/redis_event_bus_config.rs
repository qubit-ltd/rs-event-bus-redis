// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Non-secret settings for Redis Streams providers.

use std::env;

use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::registry::EventBusConfig;

use crate::error::RedisProviderError;

/// Redis provider connection settings.
#[derive(Clone)]
pub struct RedisEventBusConfig {
    /// Redis connection URL, including credentials where required.
    connection_url: String,
    /// Namespace prepended to generated stream names.
    namespace: String,
    /// Optional Sentinel endpoints.
    sentinel_nodes: Option<Vec<String>>,
    /// Sentinel master service name.
    sentinel_service: Option<String>,
    /// ACL credentials loaded from environment variable references.
    credentials: RedisCredentials,
    /// Sentinel ACL credentials loaded from environment variable references.
    sentinel_credentials: RedisCredentials,
    /// Minimum idle milliseconds before another Redis consumer can claim a
    /// pending delivery.
    claim_min_idle_ms: usize,
    /// Maximum unsettled messages held by one SPI subscription.
    max_unsettled_per_subscription: usize,
}

impl std::fmt::Debug for RedisEventBusConfig {
    /// Formats the configuration without exposing connection credentials.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RedisEventBusConfig")
            .field("connection_url", &"<redacted>")
            .field("namespace", &self.namespace)
            .field("sentinel_nodes", &self.sentinel_nodes)
            .field("sentinel_service", &self.sentinel_service)
            .field("credentials", &self.credentials)
            .field("sentinel_credentials", &self.sentinel_credentials)
            .field("claim_min_idle_ms", &self.claim_min_idle_ms)
            .field("max_unsettled_per_subscription", &self.max_unsettled_per_subscription)
            .finish()
    }
}

impl Default for RedisEventBusConfig {
    /// Creates settings using the local Redis default and `qubit` namespace.
    fn default() -> Self {
        Self {
            connection_url: "redis://127.0.0.1/".into(),
            namespace: "qubit".into(),
            sentinel_nodes: None,
            sentinel_service: None,
            credentials: RedisCredentials::default(),
            sentinel_credentials: RedisCredentials::default(),
            claim_min_idle_ms: 30_000,
            max_unsettled_per_subscription: 100,
        }
    }
}

impl RedisEventBusConfig {
    /// Creates configuration from a Redis URL and namespace.
    pub fn new(connection_url: &str, namespace: &str) -> Self {
        Self {
            connection_url: connection_url.into(),
            namespace: namespace.into(),
            sentinel_nodes: None,
            sentinel_service: None,
            credentials: RedisCredentials::default(),
            sentinel_credentials: RedisCredentials::default(),
            claim_min_idle_ms: 30_000,
            max_unsettled_per_subscription: 100,
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

    /// Returns configured Sentinel endpoints, if present.
    pub fn sentinel_nodes(&self) -> Option<&[String]> {
        self.sentinel_nodes.as_deref()
    }

    /// Returns the configured Sentinel master name, if present.
    pub fn sentinel_service(&self) -> Option<&str> {
        self.sentinel_service.as_deref()
    }

    /// Returns the idle delay before a pending record can be claimed by another
    /// consumer.
    #[cfg(any(feature = "sync", feature = "async"))]
    pub(crate) const fn claim_min_idle_ms(&self) -> usize {
        self.claim_min_idle_ms
    }

    /// Returns the Redis username and password loaded from environment
    /// references.
    #[cfg(any(feature = "sync", feature = "async"))]
    pub(crate) fn credentials(&self) -> (&Option<String>, &Option<String>) {
        (&self.credentials.username, &self.credentials.password)
    }

    /// Returns the Sentinel username and password loaded from environment
    /// references.
    #[cfg(any(feature = "sync", feature = "async"))]
    pub(crate) fn sentinel_credentials(&self) -> (&Option<String>, &Option<String>) {
        (&self.sentinel_credentials.username, &self.sentinel_credentials.password)
    }

    /// Returns the maximum number of unsettled records held by one receiver.
    #[cfg(any(feature = "sync", feature = "async"))]
    pub(crate) const fn max_unsettled_per_subscription(&self) -> usize {
        self.max_unsettled_per_subscription
    }

    /// Creates settings from a facade provider-options map.
    pub fn from_provider_options(options: &ProviderOptions) -> Result<Self, RedisProviderError> {
        let connection_url = options
            .get("redis.url")
            .map(String::as_str)
            .unwrap_or("redis://127.0.0.1/");
        let namespace = options.get("redis.namespace").map(String::as_str).unwrap_or("qubit");
        validate_url(connection_url)?;
        validate_namespace(namespace)?;
        let sentinel_nodes = options.get("redis.sentinel.nodes").map(|nodes| {
            nodes
                .split(',')
                .map(str::trim)
                .filter(|node| !node.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        });
        let sentinel_service = options.get("redis.sentinel.service_name").cloned();
        let credentials = RedisCredentials::from_env_references(options, "redis")?;
        let sentinel_credentials = RedisCredentials::from_env_references(options, "redis.sentinel")?;
        let claim_min_idle_ms = options
            .get("redis.claim_min_idle_ms")
            .map(|value| {
                value
                    .parse::<usize>()
                    .map_err(|_| RedisProviderError::Configuration("invalid redis.claim_min_idle_ms"))
            })
            .transpose()?
            .unwrap_or(30_000);
        let max_unsettled_per_subscription = options
            .get("redis.max_unsettled_per_subscription")
            .map(|value| {
                value
                    .parse::<usize>()
                    .ok()
                    .filter(|value| (1..=10_000).contains(value))
                    .ok_or(RedisProviderError::Configuration(
                        "invalid redis.max_unsettled_per_subscription",
                    ))
            })
            .transpose()?
            .unwrap_or(100);
        if sentinel_nodes.as_ref().is_some_and(Vec::is_empty) {
            return Err(RedisProviderError::Configuration(
                "redis.sentinel.nodes must contain endpoints",
            ));
        }
        if sentinel_nodes.is_some() != sentinel_service.is_some() {
            return Err(RedisProviderError::Configuration(
                "Sentinel nodes and service name must be configured together",
            ));
        }
        let mut config = Self::new(connection_url, namespace);
        config.sentinel_nodes = sentinel_nodes;
        config.sentinel_service = sentinel_service;
        config.credentials = credentials;
        config.sentinel_credentials = sentinel_credentials;
        config.claim_min_idle_ms = claim_min_idle_ms;
        config.max_unsettled_per_subscription = max_unsettled_per_subscription;
        Ok(config)
    }

    /// Extracts Redis settings from the facade configuration.
    pub fn from_event_bus_config(config: &EventBusConfig) -> Result<Self, RedisProviderError> {
        Self::from_provider_options(config.provider_options())
    }
}

/// Rejects URLs containing inline credentials so debug and option metadata stay
/// non-secret.
fn validate_url(connection_url: &str) -> Result<(), RedisProviderError> {
    let client =
        redis::Client::open(connection_url).map_err(|_| RedisProviderError::Configuration("invalid redis.url"))?;
    let parsed = client.get_connection_info();
    if parsed.redis.password.is_some() || parsed.redis.username.is_some() {
        return Err(RedisProviderError::Configuration(
            "credentials must be supplied using environment variables",
        ));
    }
    Ok(())
}

/// Validates the namespace used to derive stream names.
fn validate_namespace(namespace: &str) -> Result<(), RedisProviderError> {
    if namespace.is_empty() || namespace.len() > 128 || namespace.chars().any(char::is_control) {
        return Err(RedisProviderError::Configuration("invalid redis.namespace"));
    }
    Ok(())
}

/// Holds Redis credentials without exposing their values through debug output.
#[derive(Clone, Default)]
struct RedisCredentials {
    /// Optional Redis ACL username.
    username: Option<String>,
    /// Optional Redis ACL password.
    password: Option<String>,
}

impl std::fmt::Debug for RedisCredentials {
    /// Formats whether each credential exists without revealing its value.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RedisCredentials")
            .field("username", &self.username.as_ref().map(|_| "<redacted>"))
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl RedisCredentials {
    /// Loads credentials from the non-secret environment variable names in
    /// options.
    fn from_env_references(options: &ProviderOptions, prefix: &str) -> Result<Self, RedisProviderError> {
        let read = |key: &str| -> Result<Option<String>, RedisProviderError> {
            let option = format!("{prefix}.{key}_env");
            let Some(name) = options.get(&option) else {
                return Ok(None);
            };
            env::var(name)
                .map(Some)
                .map_err(|_| RedisProviderError::Configuration("credential environment variable is unavailable"))
        };
        Ok(Self {
            username: read("username")?,
            password: read("password")?,
        })
    }
}
