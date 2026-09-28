// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Non-secret settings for Redis Streams providers.

mod redis_credentials;

use std::num::NonZeroUsize;

use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::registry::EventBusConfig;
use redis::Client as RedisClient;

use self::redis_credentials::RedisCredentials;
use crate::error::RedisProviderError;

/// Validated connection, namespace, recovery, and in-flight limits for Redis.
///
/// Configuration parsed from provider options rejects inline URL credentials
/// and resolves credential environment-variable references immediately. Its
/// `Debug` output redacts both the URL and credential values.
///
/// # Examples
///
/// ```
/// use qubit_event_bus_redis::config::RedisEventBusConfig;
///
/// let config = RedisEventBusConfig::new("redis://127.0.0.1/", "orders").expect("valid config");
/// assert_eq!(config.namespace(), "orders");
/// assert_eq!(config.connection_url(), "redis://127.0.0.1/");
/// ```
#[derive(Clone)]
pub struct RedisEventBusConfig {
    /// Redis connection URL without embedded credentials.
    connection_url: String,
    /// Application namespace used when deriving stream and group keys.
    namespace: String,
    /// Optional Sentinel endpoint list after provider-option parsing.
    sentinel_nodes: Option<Vec<String>>,
    /// Sentinel monitored master name, paired with `sentinel_nodes`.
    sentinel_service: Option<String>,
    /// Redis ACL credentials resolved from configured environment variables.
    credentials: RedisCredentials,
    /// Sentinel ACL credentials resolved from configured environment variables.
    sentinel_credentials: RedisCredentials,
    /// Minimum pending idle time, in milliseconds, before another consumer may
    /// claim a delivery with `XAUTOCLAIM`.
    claim_min_idle_ms: usize,
    /// Interval, in milliseconds, between recovery scans during a long wait.
    recovery_interval_ms: usize,
    /// Maximum delivered but unsettled messages retained by one SPI receiver.
    max_unsettled_per_subscription: usize,
    /// Maximum number of idle synchronous standalone connections retained.
    max_idle_connections: usize,
    /// Optional approximate maximum length for each Redis stream.
    stream_maxlen_approx: Option<NonZeroUsize>,
}

impl std::fmt::Debug for RedisEventBusConfig {
    /// Formats the configuration without exposing connection credentials.
    ///
    /// # Returns
    ///
    /// The formatter result produced while writing the redacted debug view.
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
            .field("recovery_interval_ms", &self.recovery_interval_ms)
            .field("max_unsettled_per_subscription", &self.max_unsettled_per_subscription)
            .field("max_idle_connections", &self.max_idle_connections)
            .field("stream_maxlen_approx", &self.stream_maxlen_approx)
            .finish()
    }
}

impl Default for RedisEventBusConfig {
    /// Creates settings for local Redis with the default `qubit` namespace.
    fn default() -> Self {
        Self {
            connection_url: "redis://127.0.0.1/".into(),
            namespace: "qubit".into(),
            sentinel_nodes: None,
            sentinel_service: None,
            credentials: RedisCredentials::default(),
            sentinel_credentials: RedisCredentials::default(),
            claim_min_idle_ms: 30_000,
            recovery_interval_ms: 1_000,
            max_unsettled_per_subscription: 100,
            max_idle_connections: 8,
            stream_maxlen_approx: None,
        }
    }
}

impl RedisEventBusConfig {
    /// Creates configuration from a credential-free Redis URL and namespace.
    ///
    /// URL and namespace validation uses the same rules as provider options.
    ///
    /// # Parameters
    ///
    /// - `connection_url`: Redis URL used for standalone connection setup.
    /// - `namespace`: Prefix scope used to derive stream and group keys.
    ///
    /// # Returns
    ///
    /// A validated configuration with default recovery limits and no
    /// Sentinel endpoints.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the URL or namespace is invalid.
    pub fn new(connection_url: &str, namespace: &str) -> Result<Self, RedisProviderError> {
        let options: ProviderOptions = [
            ("redis.url".to_owned(), connection_url.to_owned()),
            ("redis.namespace".to_owned(), namespace.to_owned()),
        ]
        .into();
        Self::from_provider_options(&options)
    }

    /// Borrows the Redis URL used when creating a client.
    ///
    /// Provider-option parsing rejects a username or password embedded in the
    /// URL; credentials are configured separately through environment
    /// references.
    ///
    /// # Returns
    ///
    /// The configured URL without allocating or exposing a mutable reference.
    #[must_use]
    #[inline]
    pub fn connection_url(&self) -> &str {
        &self.connection_url
    }

    /// Borrows the namespace used to derive Redis stream and group keys.
    ///
    /// # Returns
    ///
    /// The namespace string stored by this configuration.
    #[must_use]
    #[inline]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Borrows the Sentinel endpoints when Sentinel mode is configured.
    ///
    /// # Returns
    ///
    /// `Some` contains the parsed endpoint list; `None` selects standalone
    /// Redis connection setup.
    #[must_use]
    #[inline]
    pub fn sentinel_nodes(&self) -> Option<&[String]> {
        self.sentinel_nodes.as_deref()
    }

    /// Borrows the monitored Sentinel master service name, if configured.
    ///
    /// # Returns
    ///
    /// `Some` is present together with
    /// [`sentinel_nodes`](Self::sentinel_nodes); `None` selects standalone
    /// Redis mode.
    #[must_use]
    #[inline]
    pub fn sentinel_service(&self) -> Option<&str> {
        self.sentinel_service.as_deref()
    }

    /// Returns the pending idle delay, in milliseconds, before a claim is
    /// allowed.
    ///
    /// # Returns
    ///
    /// The configured Redis `XAUTOCLAIM` minimum idle time.
    #[cfg(any(feature = "sync", feature = "async"))]
    #[must_use]
    #[inline]
    pub(crate) const fn claim_min_idle_ms(&self) -> usize {
        self.claim_min_idle_ms
    }

    /// Returns the interval between recovery scans during a receive call.
    ///
    /// # Returns
    ///
    /// The validated interval in milliseconds, between 50 and 60,000.
    #[must_use]
    #[inline]
    pub const fn recovery_interval_ms(&self) -> usize {
        self.recovery_interval_ms
    }

    /// Borrows the resolved Redis ACL username and password for client setup.
    ///
    /// # Returns
    ///
    /// References to the optional secret values; callers must not log them.
    #[cfg(any(feature = "sync", feature = "async"))]
    #[must_use]
    #[inline]
    pub(crate) fn credentials(&self) -> (&Option<String>, &Option<String>) {
        (&self.credentials.username, &self.credentials.password)
    }

    /// Borrows the resolved Sentinel ACL username and password for client
    /// setup.
    ///
    /// # Returns
    ///
    /// References to the optional secret values; callers must not log them.
    #[cfg(any(feature = "sync", feature = "async"))]
    #[must_use]
    #[inline]
    pub(crate) fn sentinel_credentials(&self) -> (&Option<String>, &Option<String>) {
        (&self.sentinel_credentials.username, &self.sentinel_credentials.password)
    }

    /// Returns the per-subscription bound for delivered, unsettled records.
    ///
    /// Receivers pause reading new records at this limit and resume after a
    /// settlement or retry releases an in-flight slot.
    ///
    /// # Returns
    ///
    /// The positive configured maximum.
    #[cfg(any(feature = "sync", feature = "async"))]
    #[must_use]
    #[inline]
    pub(crate) const fn max_unsettled_per_subscription(&self) -> usize {
        self.max_unsettled_per_subscription
    }

    /// Returns the maximum number of idle synchronous connections retained.
    #[must_use]
    #[inline]
    pub const fn max_idle_connections(&self) -> usize {
        self.max_idle_connections
    }

    /// Returns the optional approximate maximum entry count per stream.
    ///
    /// Redis may trim entries that are still needed by consumers when this
    /// option is enabled.
    #[must_use]
    pub const fn stream_maxlen_approx(&self) -> Option<NonZeroUsize> {
        self.stream_maxlen_approx
    }

    /// Parses and validates Redis settings from facade provider options.
    ///
    /// Credential options contain environment-variable names, not secret
    /// values. Sentinel endpoints and service names must be supplied together.
    ///
    /// # Parameters
    ///
    /// - `options`: Provider-specific key/value settings from the event-bus
    ///   facade.
    ///
    /// # Returns
    ///
    /// Validated settings with defaults applied to omitted optional values.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for malformed URLs, invalid limits,
    /// unavailable credential variables, empty namespaces, or incomplete
    /// Sentinel settings.
    pub fn from_provider_options(options: &ProviderOptions) -> Result<Self, RedisProviderError> {
        const KNOWN_OPTIONS: &[&str] = &[
            "redis.url",
            "redis.namespace",
            "redis.username_env",
            "redis.password_env",
            "redis.sentinel.username_env",
            "redis.sentinel.password_env",
            "redis.sentinel.nodes",
            "redis.sentinel.service_name",
            "redis.claim_min_idle_ms",
            "redis.recovery_interval_ms",
            "redis.max_unsettled_per_subscription",
            "redis.max_idle_connections",
            "redis.stream_maxlen_approx",
        ];
        if options
            .keys()
            .any(|key| key.starts_with("redis.") && !KNOWN_OPTIONS.contains(&key.as_str()))
        {
            return Err(RedisProviderError::Configuration("unknown Redis provider option"));
        }
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
        let recovery_interval_ms = options
            .get("redis.recovery_interval_ms")
            .map(|value| {
                value
                    .parse::<usize>()
                    .ok()
                    .filter(|value| (50..=60_000).contains(value))
                    .ok_or(RedisProviderError::Configuration("invalid redis.recovery_interval_ms"))
            })
            .transpose()?
            .unwrap_or(1_000);
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
        let max_idle_connections = options
            .get("redis.max_idle_connections")
            .map(|value| {
                value
                    .parse::<usize>()
                    .ok()
                    .filter(|value| (1..=64).contains(value))
                    .ok_or(RedisProviderError::Configuration("invalid redis.max_idle_connections"))
            })
            .transpose()?
            .unwrap_or(8);
        let stream_maxlen_approx = options
            .get("redis.stream_maxlen_approx")
            .map(|value| {
                value
                    .parse::<usize>()
                    .ok()
                    .and_then(NonZeroUsize::new)
                    .ok_or(RedisProviderError::Configuration("invalid redis.stream_maxlen_approx"))
            })
            .transpose()?;
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
        Ok(Self {
            connection_url: connection_url.into(),
            namespace: namespace.into(),
            sentinel_nodes,
            sentinel_service,
            credentials,
            sentinel_credentials,
            claim_min_idle_ms,
            recovery_interval_ms,
            max_unsettled_per_subscription,
            max_idle_connections,
            stream_maxlen_approx,
        })
    }

    /// Parses Redis provider options carried by an event-bus configuration.
    ///
    /// # Parameters
    ///
    /// - `config`: Facade configuration whose provider options contain Redis
    ///   settings.
    ///
    /// # Returns
    ///
    /// Validated Redis settings with defaults applied.
    ///
    /// # Errors
    ///
    /// Returns the same validation errors as
    /// [`from_provider_options`](Self::from_provider_options).
    pub fn from_event_bus_config(config: &EventBusConfig) -> Result<Self, RedisProviderError> {
        Self::from_provider_options(config.provider_options())
    }
}

/// Parses a Redis URL and rejects embedded credentials before retaining it.
///
/// The caller receives a stable configuration error instead of the Redis
/// parser's diagnostic, which could include sensitive connection details.
///
/// # Parameters
///
/// - `connection_url`: URL supplied in the `redis.url` provider option.
///
/// # Returns
///
/// Success when the URL is valid and contains no username or password.
///
/// # Errors
///
/// Returns a generic configuration error for malformed URLs or embedded
/// credentials, without preserving the parser diagnostic.
fn validate_url(connection_url: &str) -> Result<(), RedisProviderError> {
    let client =
        RedisClient::open(connection_url).map_err(|_| RedisProviderError::Configuration("invalid redis.url"))?;
    let parsed = client.get_connection_info();
    if parsed.redis.password.is_some() || parsed.redis.username.is_some() {
        return Err(RedisProviderError::Configuration(
            "credentials must be supplied using environment variables",
        ));
    }
    Ok(())
}

/// Ensures a namespace is non-empty, bounded, and free from control characters.
///
/// # Parameters
///
/// - `namespace`: String used as the scope component in generated Redis keys.
///
/// # Returns
///
/// Success when the namespace is no longer than 128 UTF-8 bytes and contains
/// no control characters.
///
/// # Errors
///
/// Returns a configuration error when the namespace is empty, oversized, or
/// contains a control character.
fn validate_namespace(namespace: &str) -> Result<(), RedisProviderError> {
    if namespace.is_empty() || namespace.len() > 128 || namespace.chars().any(char::is_control) {
        return Err(RedisProviderError::Configuration("invalid redis.namespace"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use qubit_event_bus::model::ProviderOptions;

    use super::RedisEventBusConfig;

    #[test]
    fn config_defaults_and_debug_redact_connection_details() {
        let config = RedisEventBusConfig::default();
        assert_eq!(config.connection_url(), "redis://127.0.0.1/");
        assert_eq!(config.namespace(), "qubit");
        assert_eq!(config.recovery_interval_ms(), 1_000);
        assert!(config.sentinel_nodes().is_none());
        assert!(config.sentinel_service().is_none());
        assert!(config.stream_maxlen_approx().is_none());
        assert!(!format!("{config:?}").contains("127.0.0.1"));
    }

    #[test]
    fn new_config_exposes_supplied_values_and_defaults() {
        let config = RedisEventBusConfig::new("redis://localhost/", "orders").unwrap();
        assert_eq!(config.connection_url(), "redis://localhost/");
        assert_eq!(config.namespace(), "orders");
        assert_eq!(config.recovery_interval_ms(), 1_000);
        assert_eq!(config.max_idle_connections(), 8);
    }

    #[test]
    fn new_rejects_invalid_url_credentials_and_namespace() {
        assert!(RedisEventBusConfig::new("not a URL", "orders").is_err());
        assert!(RedisEventBusConfig::new("redis://user:secret@localhost/", "orders").is_err());
        assert!(RedisEventBusConfig::new("redis://localhost/", "").is_err());
        assert!(RedisEventBusConfig::new("redis://localhost/", &"x".repeat(129)).is_err());
    }

    #[test]
    fn provider_options_accept_recovery_interval_boundaries_and_limits() {
        for interval in [50, 60_000] {
            let options: ProviderOptions = [("redis.recovery_interval_ms".into(), interval.to_string())].into();
            assert_eq!(
                RedisEventBusConfig::from_provider_options(&options)
                    .unwrap()
                    .recovery_interval_ms(),
                interval
            );
        }
        for interval in [49, 60_001] {
            let options: ProviderOptions = [("redis.recovery_interval_ms".into(), interval.to_string())].into();
            assert!(RedisEventBusConfig::from_provider_options(&options).is_err());
        }
    }

    #[test]
    #[cfg(any(feature = "sync", feature = "async"))]
    fn provider_options_validate_pool_and_stream_limits() {
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
    fn provider_options_accept_claim_idle_threshold() {
        let options: ProviderOptions = [("redis.claim_min_idle_ms".into(), "125".into())].into();
        let config = RedisEventBusConfig::from_provider_options(&options).unwrap();
        assert_eq!(config.claim_min_idle_ms(), 125);
    }

    #[test]
    fn event_bus_config_parsing_reads_provider_options() {
        let options: ProviderOptions = [("redis.namespace".into(), "billing".into())].into();
        let event_bus_config = qubit_event_bus::EventBusConfig::default().with_provider_options(options);
        assert_eq!(
            RedisEventBusConfig::from_event_bus_config(&event_bus_config)
                .unwrap()
                .namespace(),
            "billing"
        );
    }

    #[test]
    fn provider_options_reject_inline_credentials_invalid_namespaces_and_partial_sentinel() {
        for options in [
            [("redis.url".into(), "redis://user:secret@localhost/".into())].into(),
            [("redis.namespace".into(), "bad\nnamespace".into())].into(),
            [("redis.sentinel.nodes".into(), "127.0.0.1:26379".into())].into(),
        ] {
            assert!(RedisEventBusConfig::from_provider_options(&options).is_err());
        }
        let error =
            RedisEventBusConfig::from_provider_options(&[("redis.url".into(), "not a redis url".into())].into())
                .unwrap_err();
        assert!(!error.to_string().contains("secret"));
    }

    #[test]
    fn provider_options_reject_unknown_redis_keys_without_echoing_values() {
        let options: ProviderOptions = [("redis.passwrod".into(), "secret-value".into())].into();
        let error = RedisEventBusConfig::from_provider_options(&options).unwrap_err();
        assert_eq!(
            error.to_string(),
            "invalid Redis provider configuration: unknown Redis provider option"
        );
        assert!(!error.to_string().contains("secret-value"));
    }
}
