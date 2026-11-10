// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded Sentinel discovery without cached master sockets or write replay.
use std::str::from_utf8;
#[cfg(feature = "sync")]
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
#[cfg(feature = "sync")]
use std::time::Duration;

#[cfg(feature = "async")]
use redis::AsyncConnectionConfig;
use redis::Client;
#[cfg(feature = "sync")]
use redis::Connection;
use redis::ConnectionAddr;
#[cfg(feature = "sync")]
use redis::ConnectionConfig;
use redis::ConnectionInfo;
#[cfg(feature = "sync")]
use redis::ConnectionLike;
use redis::ErrorKind;
use redis::RedisConnectionInfo;
use redis::RedisError;
use redis::TlsCertificates;
use redis::Value;
#[cfg(feature = "async")]
use redis::aio::MultiplexedConnection;
use redis::cmd;

#[cfg(feature = "async")]
use super::async_connection_cache::AsyncConnectionCache;
use super::tls::build_client;
use super::tls::load_certificates;
use crate::config::RedisEventBusConfig;
#[cfg(feature = "async")]
use crate::error::RedisProviderError;
use crate::internal::TransportPolicy;

/// Finite RESP frame budget for Sentinel discovery and non-receiver master
/// sockets.
const DISCOVERY_MAX_RESPONSE_BYTES: usize = 1024 * 1024;

/// Probes each configured endpoint once, preferring the last successful node.
pub(crate) struct SentinelResolver {
    /// Configured Sentinel endpoints with independent ACL credentials.
    endpoints: Vec<Client>,
    /// One healthy idle sync control socket per endpoint. Concurrent lookups
    /// use temporary connections when the slot is checked out.
    #[cfg(feature = "sync")]
    sync_control_connections: Vec<Mutex<Option<Connection>>>,
    /// Single-flight async Sentinel control sockets, separate from master
    /// command connections.
    #[cfg(feature = "async")]
    async_control_connections: Vec<AsyncConnectionCache>,
    /// Monitored master service name used for discovery queries.
    service: String,
    /// Master ACL and database settings preserved for discovered addresses.
    master_info: RedisConnectionInfo,
    /// Parsed master connection template, preserving TCP transport settings.
    master_connection_template: ConnectionInfo,
    /// Whether the discovered master address must use verified TLS.
    master_tls_enabled: bool,
    /// Master-specific CA and mTLS identity, independent from Sentinel TLS.
    master_certificates: Option<TlsCertificates>,
    /// Index of the last successful endpoint; relaxed ordering only affects
    /// probe preference.
    preferred: AtomicUsize,
    /// Finite setup and per-command waiting budgets used for every probe.
    policy: TransportPolicy,
}
impl SentinelResolver {
    /// Parses endpoints and preserves independent Sentinel/master ACL and DB
    /// setup. Returns parse errors without connecting to the network.
    ///
    /// # Parameters
    ///
    /// - `config`: Validated endpoint list, service name, ACL/DB settings, and
    ///   timeouts.
    ///
    /// # Returns
    ///
    /// A resolver with the first endpoint preferred and no cached master
    /// socket.
    ///
    /// # Errors
    ///
    /// Returns a Redis endpoint parsing error without preserving it in public
    /// SPI diagnostics.
    pub(crate) fn new(config: &RedisEventBusConfig) -> Result<Self, RedisError> {
        let mut endpoints = Vec::new();
        let (username, password) = config.sentinel_credentials();
        let sentinel_certificates = if config.sentinel_tls_enabled() {
            load_certificates(
                config.sentinel_tls_ca_cert_path(),
                config.sentinel_tls_client_cert_path(),
                config.sentinel_tls_client_key_path(),
            )?
        } else {
            None
        };
        for node in config.sentinel_nodes().unwrap_or_default() {
            let scheme = if config.sentinel_tls_enabled() {
                "rediss"
            } else {
                "redis"
            };
            let info = Client::open(format!("{scheme}://{node}/"))?
                .get_connection_info()
                .clone();
            let mut redis_settings = info.redis_settings().clone();
            if let Some(username) = username {
                redis_settings = redis_settings.set_username(username);
            }
            if let Some(password) = password {
                redis_settings = redis_settings.set_password(password);
            }
            let info = info.set_redis_settings(redis_settings);
            endpoints.push(build_client(info, sentinel_certificates.as_ref())?);
        }
        let master_connection_info = Client::open(config.connection_url())?.get_connection_info().clone();
        let master_tls_enabled = matches!(master_connection_info.addr(), ConnectionAddr::TcpTls { .. });
        let (username, password) = config.credentials();
        let mut master_info = master_connection_info.redis_settings().clone();
        if let Some(username) = username {
            master_info = master_info.set_username(username);
        }
        if let Some(password) = password {
            master_info = master_info.set_password(password);
        }
        let master_connection_info = master_connection_info.set_redis_settings(master_info.clone());
        let master_certificates = load_certificates(
            config.tls_ca_cert_path(),
            config.tls_client_cert_path(),
            config.tls_client_key_path(),
        )?;
        if let Some(certificates) = &master_certificates {
            build_client(master_connection_info.clone(), Some(certificates))?;
        }
        let endpoint_count = endpoints.len();
        Ok(Self {
            #[cfg(feature = "sync")]
            sync_control_connections: (0..endpoint_count).map(|_| Mutex::new(None)).collect(),
            #[cfg(feature = "async")]
            async_control_connections: (0..endpoint_count).map(|_| AsyncConnectionCache::new()).collect(),
            endpoints,
            service: config.sentinel_service().unwrap_or_default().into(),
            master_info,
            master_connection_template: master_connection_info,
            master_tls_enabled,
            master_certificates,
            preferred: AtomicUsize::new(0),
            policy: TransportPolicy::from_config(config),
        })
    }

    /// Opens bounded sync Sentinel/setup/ROLE connections; never pools the
    /// target.
    ///
    /// # Parameters
    ///
    /// - `max_response_bytes`: Optional response-size cap for the target
    ///   socket.
    ///
    /// # Returns
    ///
    /// A master socket after probing each configured endpoint at most once.
    ///
    /// # Errors
    ///
    /// Returns the last endpoint, timeout, role, or protocol failure when
    /// discovery fails. Individual I/O waits are bounded; DNS and
    /// multi-address work have no hard wall-clock deadline.
    #[cfg(feature = "sync")]
    pub(crate) fn connect_sync(&self, max_response_bytes: Option<usize>) -> Result<Connection, RedisError> {
        let mut last_error = discovery_error();
        let start = self.preferred.load(Ordering::Relaxed);
        for offset in 0..self.endpoints.len() {
            let index = (start + offset) % self.endpoints.len();
            let attempt = (|| {
                let target = self.query_target_sync(index)?;
                let mut connection = open_sync(
                    &target,
                    self.policy,
                    Some(max_response_bytes.unwrap_or(DISCOVERY_MAX_RESPONSE_BYTES)),
                )?;
                let role = connection.req_command(&cmd("ROLE"))?;
                if let Value::ServerError(error) = role {
                    return Err(error.into());
                }
                if !is_master(&role) {
                    return Err(discovery_error());
                }
                Ok(connection)
            })();
            match attempt {
                Ok(connection) => {
                    self.preferred.store(index, Ordering::Relaxed);
                    return Ok(connection);
                }
                Err(error) => last_error = error,
            }
        }
        Err(last_error)
    }
    /// Uses the host executor for bounded Sentinel/setup/ROLE probes, one per
    /// node.
    ///
    /// # Parameters
    ///
    /// - `max_response_bytes`: Optional response-size cap for the target
    ///   socket.
    ///
    /// # Returns
    ///
    /// A master connection after bounded setup and ROLE validation without
    /// cached sockets.
    ///
    /// # Errors
    ///
    /// Returns the last endpoint, timeout, role, or protocol failure.
    /// Cancellation drops the active probe; no hidden election, write
    /// replay, or replacement task is started.
    #[cfg(feature = "async")]
    pub(crate) async fn connect_async(
        &self,
        max_response_bytes: Option<usize>,
    ) -> Result<MultiplexedConnection, RedisError> {
        let mut last_error = discovery_error();
        let start = self.preferred.load(Ordering::Relaxed);
        for offset in 0..self.endpoints.len() {
            let index = (start + offset) % self.endpoints.len();
            let attempt = async {
                let target = self.query_target_async(index).await?;
                let mut connection = open_async(
                    &target,
                    self.policy,
                    Some(max_response_bytes.unwrap_or(DISCOVERY_MAX_RESPONSE_BYTES)),
                )
                .await?;
                let role = connection.send_packed_command(&cmd("ROLE")).await?;
                if let Value::ServerError(error) = role {
                    return Err(error.into());
                }
                if !is_master(&role) {
                    return Err(discovery_error());
                }
                Ok(connection)
            }
            .await;
            match attempt {
                Ok(connection) => {
                    self.preferred.store(index, Ordering::Relaxed);
                    return Ok(connection);
                }
                Err(error) => last_error = error,
            }
        }
        Err(last_error)
    }

    /// Queries and validates a Sentinel response, returning only a healthy
    /// connection to the endpoint's bounded idle slot.
    ///
    /// # Parameters
    ///
    /// - `index`: Endpoint index selected by the resolver's discovery loop.
    #[cfg(feature = "sync")]
    fn query_target_sync(&self, index: usize) -> Result<Client, RedisError> {
        let cached = self.sync_control_connections[index]
            .lock()
            .map_err(|_| discovery_error())?
            .take();
        let mut sentinel = match cached {
            Some(connection) => connection,
            None => open_sync(&self.endpoints[index], self.policy, Some(DISCOVERY_MAX_RESPONSE_BYTES))?,
        };
        configure_sync(&sentinel, self.policy.command_timeout)?;
        let raw = sentinel.req_command(cmd("SENTINEL").arg("get-master-addr-by-name").arg(&self.service))?;
        let target = self.target(raw)?;
        if let Ok(mut slot) = self.sync_control_connections[index].lock()
            && slot.is_none()
        {
            *slot = Some(sentinel);
        }
        Ok(target)
    }

    /// Runs one Sentinel query through a single-flight async control socket.
    /// Redis errors are sanitized at this boundary before returning to the
    /// resolver's discovery loop.
    ///
    /// # Parameters
    ///
    /// - `index`: Endpoint index selected by the resolver's discovery loop.
    #[cfg(feature = "async")]
    async fn query_target_async(&self, index: usize) -> Result<Client, RedisError> {
        let cache = &self.async_control_connections[index];
        let (generation, mut sentinel) = cache
            .get_or_connect(async {
                open_async(&self.endpoints[index], self.policy, Some(DISCOVERY_MAX_RESPONSE_BYTES))
                    .await
                    .map_err(|_| RedisProviderError::Operation("Sentinel control connection"))
            })
            .await
            .map_err(|_| RedisError::from((ErrorKind::Io, "Sentinel control connection failed")))?;
        let result = async {
            let raw = sentinel
                .send_packed_command(cmd("SENTINEL").arg("get-master-addr-by-name").arg(&self.service))
                .await?;
            self.target(raw)
        }
        .await;
        if result.is_err() {
            cache.invalidate_if_current(generation);
        }
        result
    }
    /// Builds a target client only from a validated Sentinel host/port reply.
    /// Preserves top-level Redis rejections for sanitized error classification.
    ///
    /// # Parameters
    ///
    /// - `value`: Owned Sentinel host/port reply.
    ///
    /// # Returns
    ///
    /// A parsed target using master credentials and database; no network I/O is
    /// issued.
    ///
    /// # Errors
    ///
    /// Returns a top-level server error or a stable discovery error for
    /// malformed addresses.
    fn target(&self, value: Value) -> Result<Client, RedisError> {
        if let Value::ServerError(error) = value {
            return Err(error.into());
        }
        let Value::Array(address) = value else {
            return Err(discovery_error());
        };
        let [Value::BulkString(host), Value::BulkString(port)] = address.as_slice() else {
            return Err(discovery_error());
        };
        let host = from_utf8(host).map_err(|_| discovery_error())?;
        let port: u16 = from_utf8(port)
            .ok()
            .and_then(|port| port.parse().ok())
            .filter(|port| *port != 0)
            .ok_or_else(discovery_error)?;
        if host.is_empty()
            || host
                .bytes()
                .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        {
            return Err(discovery_error());
        }
        let addr = if self.master_tls_enabled {
            ConnectionAddr::TcpTls {
                host: host.into(),
                port,
                insecure: false,
                tls_params: None,
            }
        } else {
            ConnectionAddr::Tcp(host.into(), port)
        };
        build_client(
            self.master_connection_template
                .clone()
                .set_addr(addr)
                .set_redis_settings(self.master_info.clone()),
            self.master_certificates.as_ref(),
        )
    }
}
/// Checks the top-level ROLE master tag without allocation or I/O.
///
/// # Parameters
///
/// - `value`: Raw ROLE reply whose first field is inspected without cloning.
///
/// # Returns
///
/// `true` only when the first array field is the bulk-string master role.
#[must_use]
#[inline]
fn is_master(value: &Value) -> bool {
    matches!(value, Value::Array(values) if matches!(values.first(), Some(Value::BulkString(role)) if role == b"master"))
}
/// Returns a static master-down diagnostic without endpoint, ACL, or response
/// data.
///
/// # Returns
///
/// A fresh Redis error category used when discovery cannot identify a master.
fn discovery_error() -> RedisError {
    RedisError::from((
        ErrorKind::Server(redis::ServerErrorKind::MasterDown),
        "Sentinel master discovery failed",
    ))
}
/// Bounds TCP/setup first, then reapplies short-command read/write socket
/// waits.
///
/// # Parameters
///
/// - `client`: Parsed endpoint to connect.
/// - `policy`: Finite setup and short-command waits.
///
/// # Returns
///
/// A newly initialized blocking socket; DNS may exceed a strict overall
/// deadline.
///
/// # Errors
///
/// Returns Redis connection/setup, timeout, or socket-option failures.
#[cfg(feature = "sync")]
pub(crate) fn open_sync(
    client: &Client,
    policy: TransportPolicy,
    max_response_bytes: Option<usize>,
) -> Result<Connection, RedisError> {
    let config = ConnectionConfig::new().set_max_response_bytes(max_response_bytes);
    let connection = client.get_connection_with_timeout_and_config(policy.connect_timeout, &config)?;
    configure_sync(&connection, policy.command_timeout)?;
    Ok(connection)
}
/// Resets both socket waits on every checkout, including dedicated connections.
///
/// # Parameters
///
/// - `connection`: Live socket whose options are updated without a Redis
///   command.
/// - `timeout`: Finite read and write waiting budget.
///
/// # Returns
///
/// Success after both socket options are applied.
///
/// # Errors
///
/// Returns the socket-option failure; the first option may already have
/// changed.
#[cfg(feature = "sync")]
pub(crate) fn configure_sync(connection: &Connection, timeout: Duration) -> Result<(), RedisError> {
    connection.set_read_timeout(Some(timeout))?;
    connection.set_write_timeout(Some(timeout))
}
/// Configures connection/setup and response waits using the selected host
/// runtime.
///
/// # Parameters
///
/// - `client`: Parsed endpoint whose transport setup is polled by the host.
/// - `policy`: Finite setup and short-command response waiting budgets.
///
/// # Returns
///
/// A new controlled multiplexed connection without starting a separate
/// executor.
///
/// # Errors
///
/// Returns Redis connection/setup or timeout failures. Cancellation can abandon
/// setup.
#[cfg(feature = "async")]
pub(crate) async fn open_async(
    client: &Client,
    policy: TransportPolicy,
    max_response_bytes: Option<usize>,
) -> Result<MultiplexedConnection, RedisError> {
    let config = AsyncConnectionConfig::new()
        .set_connection_timeout(Some(policy.connect_timeout))
        .set_response_timeout(Some(policy.command_timeout))
        .set_max_response_bytes(max_response_bytes);
    client.get_multiplexed_async_connection_with_config(&config).await
}

#[cfg(all(test, feature = "sync"))]
mod tests {
    #[cfg(feature = "async")]
    use std::time::Duration;

    use qubit_event_bus::model::ProviderOptions;
    use redis::ErrorKind;

    use super::SentinelResolver;
    use crate::config::RedisEventBusConfig;
    use crate::tests::support::redis_support::scripted_redis::ScriptedRedis;
    use crate::tests::support::redis_support::scripted_redis::Step;

    fn resolver_settings(sentinel: &ScriptedRedis, service: &str) -> ProviderOptions {
        let node = sentinel
            .url()
            .strip_prefix("redis://")
            .expect("scripted Sentinel URL")
            .trim_end_matches('/');
        [
            ("redis.url".into(), "redis://127.0.0.1:1/0".into()),
            ("redis.sentinel.nodes".into(), node.into()),
            ("redis.sentinel.service_name".into(), service.into()),
            ("redis.connect_timeout_ms".into(), "1000".into()),
            ("redis.command_timeout_ms".into(), "1000".into()),
        ]
        .into()
    }

    fn discovery_reply(master: &ScriptedRedis) -> Vec<u8> {
        let address = master
            .url()
            .strip_prefix("redis://")
            .expect("scripted master URL")
            .trim_end_matches('/');
        let (host, port) = address.split_once(':').expect("master host and port");
        format!("*2\r\n${}\r\n{host}\r\n${}\r\n{port}\r\n", host.len(), port.len()).into_bytes()
    }

    fn role_steps(count: usize) -> Vec<Step> {
        (0..count)
            .map(|_| Step::reply("ROLE", b"*1\r\n$6\r\nmaster\r\n"))
            .collect()
    }

    #[test]
    fn test_invalid_utf8_discovered_host_is_rejected_before_target_connection() {
        let server = ScriptedRedis::start(vec![Step::reply("SENTINEL", b"*2\r\n$1\r\n\xff\r\n$4\r\n6379\r\n")])
            .expect("scripted Sentinel");
        let node = server
            .url()
            .strip_prefix("redis://")
            .expect("Redis URL")
            .trim_end_matches('/');
        let settings: ProviderOptions = [
            ("redis.url".into(), "redis://127.0.0.1:1/3".into()),
            ("redis.sentinel.nodes".into(), node.into()),
            ("redis.sentinel.service_name".into(), "master".into()),
            ("redis.connect_timeout_ms".into(), "100".into()),
            ("redis.command_timeout_ms".into(), "100".into()),
        ]
        .into();
        let config = RedisEventBusConfig::from_provider_options(&settings).expect("valid settings");
        let resolver = SentinelResolver::new(&config).expect("resolver");
        let error = match resolver.connect_sync(None) {
            Err(error) => error,
            Ok(_) => panic!("invalid host cannot become a target"),
        };
        assert_eq!(error.kind(), ErrorKind::Server(redis::ServerErrorKind::MasterDown));
        assert!(error.to_string().contains("Sentinel master discovery failed"));
        assert!(
            !error.to_string().contains(node),
            "endpoint data stays out of diagnostics"
        );
        let commands = server.finish();
        assert_eq!(
            commands,
            [vec![
                "SENTINEL".to_owned(),
                "get-master-addr-by-name".to_owned(),
                "master".to_owned()
            ]]
        );
    }

    #[test]
    fn test_sync_reuses_healthy_sentinel_socket_but_rediscovers_master_each_time() {
        const DISCOVERIES: usize = 4;
        let master = ScriptedRedis::start(role_steps(DISCOVERIES)).expect("scripted master");
        let sentinel = ScriptedRedis::start(
            (0..DISCOVERIES)
                .map(|_| Step::reply("SENTINEL", &discovery_reply(&master)))
                .collect(),
        )
        .expect("scripted Sentinel");
        let config = RedisEventBusConfig::from_provider_options(&resolver_settings(&sentinel, "master"))
            .expect("valid settings");
        let resolver = SentinelResolver::new(&config).expect("resolver");

        for _ in 0..DISCOVERIES {
            drop(resolver.connect_sync(None).expect("master connection"));
        }

        sentinel.finish();
        master.finish();
        assert_eq!(sentinel.connection_count(), 1, "healthy control socket is reused");
        assert_eq!(
            master.connection_count(),
            DISCOVERIES,
            "master sockets remain per operation"
        );
    }

    #[test]
    fn test_sync_drops_sentinel_socket_after_server_rejection() {
        let master = ScriptedRedis::start(role_steps(1)).expect("scripted master");
        let sentinel = ScriptedRedis::start(vec![
            Step::reply("SENTINEL", b"-ERR master unavailable\r\n"),
            Step::reply("SENTINEL", &discovery_reply(&master)),
        ])
        .expect("scripted Sentinel");
        let config = RedisEventBusConfig::from_provider_options(&resolver_settings(&sentinel, "master"))
            .expect("valid settings");
        let resolver = SentinelResolver::new(&config).expect("resolver");

        assert!(resolver.connect_sync(None).is_err());
        drop(resolver.connect_sync(None).expect("recovered master connection"));

        sentinel.finish();
        master.finish();
        assert_eq!(sentinel.connection_count(), 2, "a rejected response is not pooled");
        assert_eq!(master.connection_count(), 1);
    }

    #[cfg(feature = "async")]
    #[test]
    fn test_async_reuses_healthy_sentinel_socket_but_rediscovers_master_each_time() {
        use futures_lite::future::block_on;

        const DISCOVERIES: usize = 4;
        let master = ScriptedRedis::start(role_steps(DISCOVERIES)).expect("scripted master");
        let sentinel = ScriptedRedis::start(
            (0..DISCOVERIES)
                .map(|_| Step::reply("SENTINEL", &discovery_reply(&master)))
                .collect(),
        )
        .expect("scripted Sentinel");
        let config = RedisEventBusConfig::from_provider_options(&resolver_settings(&sentinel, "master"))
            .expect("valid settings");
        let resolver = SentinelResolver::new(&config).expect("resolver");

        for _ in 0..DISCOVERIES {
            drop(block_on(resolver.connect_async(None)).expect("master connection"));
        }

        sentinel.finish();
        master.finish();
        assert_eq!(sentinel.connection_count(), 1, "healthy control socket is reused");
        assert_eq!(
            master.connection_count(),
            DISCOVERIES,
            "master sockets remain per operation"
        );
    }

    #[cfg(feature = "async")]
    #[test]
    fn test_async_drops_sentinel_generation_after_server_rejection() {
        use futures_lite::future::block_on;

        let master = ScriptedRedis::start(role_steps(1)).expect("scripted master");
        let sentinel = ScriptedRedis::start(vec![
            Step::reply("SENTINEL", b"-ERR master unavailable\r\n"),
            Step::reply("SENTINEL", &discovery_reply(&master)),
        ])
        .expect("scripted Sentinel");
        let config = RedisEventBusConfig::from_provider_options(&resolver_settings(&sentinel, "master"))
            .expect("valid settings");
        let resolver = SentinelResolver::new(&config).expect("resolver");

        assert!(block_on(resolver.connect_async(None)).is_err());
        drop(block_on(resolver.connect_async(None)).expect("recovered master connection"));

        sentinel.finish();
        master.finish();
        assert_eq!(sentinel.connection_count(), 2, "a rejected generation is not reused");
        assert_eq!(master.connection_count(), 1);
    }

    #[cfg(feature = "async")]
    #[test]
    fn test_async_canceled_sentinel_query_keeps_multiplexed_connection_usable() {
        use futures_lite::future::block_on;
        use futures_lite::future::or;

        let master = ScriptedRedis::start(role_steps(1)).expect("scripted master");
        let sentinel = ScriptedRedis::start(vec![
            Step::delayed_reply("SENTINEL", &discovery_reply(&master), Duration::from_millis(100)),
            Step::reply("SENTINEL", &discovery_reply(&master)),
        ])
        .expect("scripted Sentinel");
        let config = RedisEventBusConfig::from_provider_options(&resolver_settings(&sentinel, "master"))
            .expect("valid settings");
        let resolver = SentinelResolver::new(&config).expect("resolver");
        let (cancel, canceled) = futures_channel::oneshot::channel();
        let query = std::thread::spawn(move || {
            block_on(or(
                async {
                    let _ = resolver.connect_async(None).await;
                },
                async {
                    let _ = canceled.await;
                },
            ));
            drop(block_on(resolver.connect_async(None)).expect("next query uses a live connection"));
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while sentinel.finish_allow_remaining().is_empty() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            sentinel.finish_allow_remaining().len(),
            1,
            "query reached Sentinel before cancellation"
        );
        cancel.send(()).expect("cancel signal receiver is alive");
        query.join().expect("query worker completes");

        sentinel.finish();
        master.finish();
        assert_eq!(sentinel.connection_count(), 1);
    }
}
