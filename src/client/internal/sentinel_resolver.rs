// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded Sentinel discovery without cached master sockets or write replay.
use std::str::from_utf8;
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
use redis::ConnectionInfo;
#[cfg(feature = "sync")]
use redis::ConnectionLike;
use redis::ErrorKind;
use redis::RedisConnectionInfo;
use redis::RedisError;
use redis::Value;
#[cfg(feature = "async")]
use redis::aio::MultiplexedConnection;
use redis::cmd;

use crate::config::RedisEventBusConfig;
use crate::internal::TransportPolicy;

/// Probes each configured endpoint once, preferring the last successful node.
pub(crate) struct SentinelResolver {
    /// Configured Sentinel endpoints with independent ACL credentials.
    endpoints: Vec<Client>,
    /// Monitored master service name used for discovery queries.
    service: String,
    /// Master ACL and database settings preserved for discovered addresses.
    master_info: RedisConnectionInfo,
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
        for node in config.sentinel_nodes().unwrap_or_default() {
            let mut info = Client::open(format!("redis://{node}/"))?
                .get_connection_info()
                .clone();
            info.redis.username.clone_from(username);
            info.redis.password.clone_from(password);
            endpoints.push(Client::open(info)?);
        }
        let mut master_info = Client::open(config.connection_url())?
            .get_connection_info()
            .redis
            .clone();
        let (username, password) = config.credentials();
        master_info.username.clone_from(username);
        master_info.password.clone_from(password);
        Ok(Self {
            endpoints,
            service: config.sentinel_service().unwrap_or_default().into(),
            master_info,
            preferred: AtomicUsize::new(0),
            policy: TransportPolicy::from_config(config),
        })
    }

    /// Opens bounded sync Sentinel/setup/ROLE connections; never pools the
    /// target.
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
    pub(crate) fn connect_sync(&self) -> Result<Connection, RedisError> {
        let mut last_error = discovery_error();
        let start = self.preferred.load(Ordering::Relaxed);
        for offset in 0..self.endpoints.len() {
            let index = (start + offset) % self.endpoints.len();
            let attempt = (|| {
                let mut sentinel = open_sync(&self.endpoints[index], self.policy)?;
                let raw = sentinel.req_command(
                    cmd("SENTINEL")
                        .arg("get-master-addr-by-name")
                        .arg(&self.service),
                )?;
                let target = self.target(raw)?;
                let mut connection = open_sync(&target, self.policy)?;
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
    pub(crate) async fn connect_async(&self) -> Result<MultiplexedConnection, RedisError> {
        let mut last_error = discovery_error();
        let start = self.preferred.load(Ordering::Relaxed);
        for offset in 0..self.endpoints.len() {
            let index = (start + offset) % self.endpoints.len();
            let attempt = async {
                let mut sentinel = open_async(&self.endpoints[index], self.policy).await?;
                let raw = sentinel
                    .send_packed_command(
                        cmd("SENTINEL")
                            .arg("get-master-addr-by-name")
                            .arg(&self.service),
                    )
                    .await?;
                let target = self.target(raw)?;
                let mut connection = open_async(&target, self.policy).await?;
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
        Client::open(ConnectionInfo {
            addr: ConnectionAddr::Tcp(host.into(), port),
            redis: self.master_info.clone(),
        })
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
    RedisError::from((ErrorKind::MasterDown, "Sentinel master discovery failed"))
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
) -> Result<Connection, RedisError> {
    let connection = client.get_connection_with_timeout(policy.connect_timeout)?;
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
) -> Result<MultiplexedConnection, RedisError> {
    let config = AsyncConnectionConfig::new()
        .set_connection_timeout(policy.connect_timeout)
        .set_response_timeout(policy.command_timeout);
    client
        .get_multiplexed_async_connection_with_config(&config)
        .await
}

#[cfg(all(test, feature = "sync"))]
mod tests {
    use qubit_event_bus::model::ProviderOptions;
    use redis::ErrorKind;

    use super::SentinelResolver;
    use crate::config::RedisEventBusConfig;
    use crate::tests::support::redis_support::scripted_redis::ScriptedRedis;
    use crate::tests::support::redis_support::scripted_redis::Step;

    #[test]
    fn test_invalid_utf8_discovered_host_is_rejected_before_target_connection() {
        let server = ScriptedRedis::start(vec![Step::reply(
            "SENTINEL",
            b"*2\r\n$1\r\n\xff\r\n$4\r\n6379\r\n",
        )])
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
        let error = match resolver.connect_sync() {
            Err(error) => error,
            Ok(_) => panic!("invalid host cannot become a target"),
        };
        assert_eq!(error.kind(), ErrorKind::MasterDown);
        assert!(
            error
                .to_string()
                .contains("Sentinel master discovery failed")
        );
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
}
