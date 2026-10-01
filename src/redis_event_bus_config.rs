// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Non-secret settings for Redis Streams providers.

/// Stores resolved ACL credentials with redacted Debug formatting.
#[path = "redis_event_bus_config/internal/redis_credentials.rs"]
mod redis_credentials;

use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Result as FmtResult;
use std::net::Ipv6Addr;
use std::num::NonZeroUsize;
use std::time::Duration;

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
    /// The single-endpoint connection and setup waiting budget.
    connect_timeout: Duration,
    /// The non-blocking command I/O waiting budget.
    command_timeout: Duration,
    /// The maximum concurrently admitted short commands per client.
    max_concurrent_commands: usize,
    /// The maximum active receivers per client.
    max_active_receivers: usize,
    /// The maximum raw encoded payload size in bytes.
    max_payload_bytes: usize,
    /// The maximum complete JSON wire size in bytes.
    max_wire_bytes: usize,
    /// The maximum serialized headers JSON size in bytes.
    max_headers_bytes: usize,
    /// Optional approximate maximum length for each Redis stream.
    stream_maxlen_approx: Option<NonZeroUsize>,
}

impl Debug for RedisEventBusConfig {
    /// Formats the configuration without exposing connection credentials.
    ///
    /// # Parameters
    ///
    /// - `formatter`: Destination for the redacted debug representation.
    ///
    /// # Returns
    ///
    /// The formatter result produced while writing the redacted debug view.
    ///
    /// # Errors
    ///
    /// Returns a formatting error if the destination rejects a write.
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
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
            .field("connect_timeout", &self.connect_timeout)
            .field("command_timeout", &self.command_timeout)
            .field("max_concurrent_commands", &self.max_concurrent_commands)
            .field("max_active_receivers", &self.max_active_receivers)
            .field("max_payload_bytes", &self.max_payload_bytes)
            .field("max_wire_bytes", &self.max_wire_bytes)
            .field("max_headers_bytes", &self.max_headers_bytes)
            .field("stream_maxlen_approx", &self.stream_maxlen_approx)
            .finish()
    }
}

impl Default for RedisEventBusConfig {
    /// Creates local standalone settings with the `qubit` namespace and finite
    /// defaults.
    ///
    /// # Returns
    ///
    /// A lazy configuration; no environment lookup or network I/O is performed.
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
            connect_timeout: Duration::from_millis(2_000),
            command_timeout: Duration::from_millis(2_000),
            max_concurrent_commands: 64,
            max_active_receivers: 256,
            max_payload_bytes: 1_048_576,
            max_wire_bytes: 8_388_608,
            max_headers_bytes: 65_536,
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
        /// Redis-owned option names accepted by this parser; values are never
        /// echoed.
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
            "redis.connect_timeout_ms",
            "redis.command_timeout_ms",
            "redis.max_concurrent_commands",
            "redis.max_active_receivers",
            "redis.max_payload_bytes",
            "redis.max_wire_bytes",
            "redis.max_headers_bytes",
            "redis.stream_maxlen_approx",
            "redis.allow_lossy_retention",
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
        let sentinel_nodes = options
            .get("redis.sentinel.nodes")
            .map(|nodes| parse_sentinel_nodes(nodes))
            .transpose()?;
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
        let connect_timeout = parse_limit(options, "redis.connect_timeout_ms", 2000, 60000)?;
        let connect_timeout = Duration::from_millis(
            u64::try_from(connect_timeout)
                .map_err(|_| RedisProviderError::Configuration("invalid redis.connect_timeout_ms"))?,
        );
        let command_timeout = parse_limit(options, "redis.command_timeout_ms", 2000, 60000)?;
        let command_timeout = Duration::from_millis(
            u64::try_from(command_timeout)
                .map_err(|_| RedisProviderError::Configuration("invalid redis.command_timeout_ms"))?,
        );
        let max_concurrent_commands = parse_limit(options, "redis.max_concurrent_commands", 64, 4096)?;
        let max_active_receivers = parse_limit(options, "redis.max_active_receivers", 256, 4096)?;
        let max_payload_bytes = parse_limit(options, "redis.max_payload_bytes", 1048576, 67108864)?;
        let max_wire_bytes = parse_limit(options, "redis.max_wire_bytes", 8388608, 268435456)?;
        let max_headers_bytes = parse_limit(options, "redis.max_headers_bytes", 65536, 67108864)?;
        if max_idle_connections > max_concurrent_commands {
            return Err(RedisProviderError::Configuration(
                "redis.max_idle_connections exceeds redis.max_concurrent_commands",
            ));
        }
        if max_wire_bytes < max_payload_bytes {
            return Err(RedisProviderError::Configuration(
                "redis.max_wire_bytes is smaller than redis.max_payload_bytes",
            ));
        }
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
        let allow_lossy_retention = match options.get("redis.allow_lossy_retention").map(String::as_str) {
            None | Some("false") => false,
            Some("true") => true,
            Some(_) => return Err(RedisProviderError::Configuration("invalid redis.allow_lossy_retention")),
        };
        if stream_maxlen_approx.is_some() != allow_lossy_retention {
            return Err(RedisProviderError::Configuration(
                "redis.stream_maxlen_approx requires redis.allow_lossy_retention=true",
            ));
        }
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
            connect_timeout,
            command_timeout,
            max_concurrent_commands,
            max_active_receivers,
            max_payload_bytes,
            max_wire_bytes,
            max_headers_bytes,
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

    /// Returns the maximum number of idle synchronous short-command sockets
    /// retained.
    ///
    /// # Returns
    ///
    /// The positive idle cap, separate from active receiver admission.
    #[must_use]
    #[inline]
    pub const fn max_idle_connections(&self) -> usize {
        self.max_idle_connections
    }

    /// Returns the single-endpoint connection and setup waiting budget.
    ///
    /// # Returns
    ///
    /// The validated waiting budget from 1 through 60,000 milliseconds.
    #[must_use]
    #[inline]
    pub const fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Returns the non-blocking command I/O waiting budget.
    ///
    /// # Returns
    ///
    /// The validated waiting budget from 1 through 60,000 milliseconds.
    #[must_use]
    #[inline]
    pub const fn command_timeout(&self) -> Duration {
        self.command_timeout
    }

    /// Returns the maximum concurrently admitted short commands per client.
    ///
    /// # Returns
    ///
    /// The validated admission limit from 1 through 4,096 commands.
    #[must_use]
    #[inline]
    pub const fn max_concurrent_commands(&self) -> usize {
        self.max_concurrent_commands
    }

    /// Returns the maximum active receivers per client.
    ///
    /// # Returns
    ///
    /// The validated admission limit from 1 through 4,096 receivers.
    #[must_use]
    #[inline]
    pub const fn max_active_receivers(&self) -> usize {
        self.max_active_receivers
    }

    /// Returns the maximum raw encoded payload size in bytes.
    ///
    /// # Returns
    ///
    /// The inclusive encoded payload limit from 1 through 67,108,864 bytes.
    #[must_use]
    #[inline]
    pub const fn max_payload_bytes(&self) -> usize {
        self.max_payload_bytes
    }

    /// Returns the maximum complete JSON wire size in bytes.
    ///
    /// # Returns
    ///
    /// The inclusive complete JSON limit from 1 through 268,435,456 bytes;
    /// it is at least the configured raw payload limit.
    #[must_use]
    #[inline]
    pub const fn max_wire_bytes(&self) -> usize {
        self.max_wire_bytes
    }

    /// Returns the maximum serialized headers JSON size in bytes.
    pub const fn max_headers_bytes(&self) -> usize {
        self.max_headers_bytes
    }

    /// Returns the optional approximate maximum entry count per stream.
    ///
    /// Redis may trim entries that are still needed by consumers when this
    /// option is enabled.
    ///
    /// # Returns
    ///
    /// `Some` is the configured approximate length; `None` disables
    /// provider-side trim.
    #[must_use]
    #[inline]
    pub const fn stream_maxlen_approx(&self) -> Option<NonZeroUsize> {
        self.stream_maxlen_approx
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

    /// Borrows the resolved Redis ACL username and password for client setup.
    ///
    /// # Returns
    ///
    /// `Some` is a resolved secret; `None` means that ACL component was not
    /// configured. The borrowed values remain owned by this configuration
    /// and must not be logged.
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
    /// `Some` is a resolved secret; `None` means that ACL component was not
    /// configured. The borrowed values remain owned by this configuration
    /// and must not be logged.
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
}

/// Parses a positive decimal provider limit without accepting signs or
/// overflow.
///
/// # Parameters
///
/// `options` supplies `key`; `default` applies when absent; `maximum` is
/// inclusive.
///
/// # Returns
///
/// The validated positive number.
///
/// # Errors
///
/// Returns a secret-safe configuration error for malformed or out-of-range
/// values.
fn parse_limit(
    options: &ProviderOptions,
    key: &'static str,
    default: usize,
    maximum: usize,
) -> Result<usize, RedisProviderError> {
    let Some(value) = options.get(key) else {
        return Ok(default);
    };
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(RedisProviderError::Configuration(key));
    }
    value
        .parse::<usize>()
        .ok()
        .filter(|value| (1..=maximum).contains(value))
        .ok_or(RedisProviderError::Configuration(key))
}

/// Parses at most sixteen non-empty Sentinel host/port endpoints.
///
/// # Parameters
///
/// `nodes` contains comma-separated endpoints, including bracketed IPv6 hosts.
///
/// # Returns
///
/// Validated endpoints in their configured order.
///
/// # Errors
///
/// Returns a configuration error for an empty host, invalid port, or excess
/// nodes.
fn parse_sentinel_nodes(nodes: &str) -> Result<Vec<String>, RedisProviderError> {
    let endpoints: Vec<_> = nodes.split(',').map(str::trim).collect();
    if endpoints.len() > 16 {
        return Err(RedisProviderError::Configuration("invalid redis.sentinel.nodes"));
    }
    for endpoint in &endpoints {
        let Some((host, port)) = endpoint.rsplit_once(':') else {
            return Err(RedisProviderError::Configuration("invalid redis.sentinel.nodes"));
        };
        let valid_host = !host.is_empty()
            && !host
                .chars()
                .any(|character| character.is_whitespace() || character.is_control())
            && if host.starts_with('[') {
                host.ends_with(']') && host[1..host.len() - 1].parse::<Ipv6Addr>().is_ok()
            } else {
                !host.contains([':', '/', '@', '?', '#', '\\', '[', ']'])
            };
        if !valid_host
            || port.is_empty()
            || !port.bytes().all(|byte| byte.is_ascii_digit())
            || port.parse::<u16>().ok().is_none_or(|port| port == 0)
        {
            return Err(RedisProviderError::Configuration("invalid redis.sentinel.nodes"));
        }
    }
    Ok(endpoints.into_iter().map(str::to_owned).collect())
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
