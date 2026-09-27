// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Redis clients for standalone and Sentinel deployments.

use std::sync::Mutex as StdMutex;

#[cfg(feature = "async")]
use async_lock::Mutex as AsyncMutex;
use redis::Client as RedisConnectionClient;
use redis::Connection;
use redis::ErrorKind;
use redis::IntoConnectionInfo;
use redis::RedisError;
use redis::aio::MultiplexedConnection;
use redis::sentinel::SentinelClient;
use redis::sentinel::SentinelClientBuilder;
use redis::sentinel::SentinelServerType;

use crate::config::RedisEventBusConfig;

/// Redis connection factory with optional Sentinel master discovery.
///
/// Standalone mode opens a reusable client handle; Sentinel mode resolves the
/// current master for each new connection. The factory is shared across
/// publishers and receivers, while each SPI operation obtains its own
/// connection.
pub(crate) struct Client {
    /// Direct standalone client when Sentinel is disabled.
    standalone: Option<RedisConnectionClient>,
    /// Synchronous Sentinel resolver.
    #[cfg(feature = "sync")]
    sync_sentinel: Option<StdMutex<SentinelClient>>,
    /// Async Sentinel resolver, protected by an executor-neutral mutex.
    #[cfg(feature = "async")]
    async_sentinel: Option<AsyncMutex<SentinelClient>>,
}

impl Client {
    /// Builds a standalone or Sentinel connection factory from validated
    /// settings.
    ///
    /// # Parameters
    ///
    /// - `config`: Redis settings whose URL and credentials have already been
    ///   validated.
    ///
    /// # Returns
    ///
    /// A reusable factory for obtaining blocking or asynchronous connections.
    ///
    /// # Errors
    ///
    /// Returns a Redis client error when a URL, Sentinel endpoint, or Sentinel
    /// client configuration cannot be parsed.
    pub(crate) fn new(config: &RedisEventBusConfig) -> Result<Self, RedisError> {
        let Some(nodes) = config.sentinel_nodes() else {
            let base = RedisConnectionClient::open(config.connection_url())?;
            let mut connection_info = base.get_connection_info().clone();
            let (username, password) = config.credentials();
            connection_info.redis.username.clone_from(username);
            connection_info.redis.password.clone_from(password);
            return Ok(Self {
                standalone: Some(RedisConnectionClient::open(connection_info)?),
                #[cfg(feature = "sync")]
                sync_sentinel: None,
                #[cfg(feature = "async")]
                async_sentinel: None,
            });
        };
        let endpoints = nodes.iter().map(|node| format!("redis://{node}/")).collect::<Vec<_>>();
        #[cfg(feature = "sync")]
        let sync_sentinel = Some(StdMutex::new(build_sentinel(config, endpoints.clone())?));
        #[cfg(feature = "async")]
        let async_sentinel = Some(AsyncMutex::new(build_sentinel(config, endpoints)?));
        Ok(Self {
            standalone: None,
            #[cfg(feature = "sync")]
            sync_sentinel,
            #[cfg(feature = "async")]
            async_sentinel,
        })
    }

    /// Opens a blocking connection, resolving the current Sentinel master if
    /// needed.
    ///
    /// # Returns
    ///
    /// A ready Redis connection for one blocking SPI operation.
    ///
    /// # Errors
    ///
    /// Returns a connection error if the standalone server is unavailable, the
    /// Sentinel lock is poisoned, or Sentinel cannot resolve a master.
    #[cfg(feature = "sync")]
    pub(crate) fn get_connection(&self) -> Result<Connection, RedisError> {
        if let Some(client) = &self.standalone {
            return client.get_connection();
        }
        self.sync_sentinel
            .as_ref()
            .ok_or_else(|| RedisError::from((ErrorKind::IoError, "missing Sentinel client")))?
            .lock()
            .map_err(|_| RedisError::from((ErrorKind::IoError, "Sentinel lock poisoned")))?
            .get_connection()
    }

    /// Opens a multiplexed connection using the host's selected executor
    /// adapter.
    ///
    /// # Returns
    ///
    /// A Redis async connection suitable for concurrent commands on the host
    /// runtime.
    ///
    /// # Errors
    ///
    /// Returns a connection error if the standalone server is unavailable or
    /// Sentinel cannot resolve the current master.
    #[cfg(feature = "async")]
    pub(crate) async fn get_async_connection(&self) -> Result<MultiplexedConnection, RedisError> {
        if let Some(client) = &self.standalone {
            return client.get_multiplexed_async_connection().await;
        }
        let sentinel = self
            .async_sentinel
            .as_ref()
            .ok_or_else(|| RedisError::from((ErrorKind::IoError, "missing Sentinel client")))?;
        sentinel.lock().await.get_async_connection().await
    }
}

/// Constructs the authenticated Sentinel resolver used for master discovery.
///
/// # Type Parameters
///
/// - `T`: Input endpoint type convertible to Redis connection information.
///
/// # Parameters
///
/// - `config`: Validated service name and Redis/Sentinel ACL credentials.
/// - `endpoints`: Sentinel endpoints parsed into Redis connection information.
///
/// # Returns
///
/// A resolver configured to connect to the monitored master.
///
/// # Errors
///
/// Returns a Redis error if an endpoint or Sentinel builder option is invalid.
fn build_sentinel<T: IntoConnectionInfo>(
    config: &RedisEventBusConfig,
    endpoints: Vec<T>,
) -> Result<SentinelClient, RedisError> {
    let mut builder = SentinelClientBuilder::new(
        endpoints
            .into_iter()
            .map(|endpoint| endpoint.into_connection_info().map(|info| info.addr))
            .collect::<Result<Vec<_>, _>>()?,
        config.sentinel_service().unwrap_or_default().to_owned(),
        SentinelServerType::Master,
    )?;
    let (username, password) = config.credentials();
    if let Some(username) = username {
        builder = builder.set_client_to_redis_username(username.clone());
    }
    if let Some(password) = password {
        builder = builder.set_client_to_redis_password(password.clone());
    }
    let (sentinel_username, sentinel_password) = config.sentinel_credentials();
    if let Some(username) = sentinel_username {
        builder = builder.set_client_to_sentinel_username(username.clone());
    }
    if let Some(password) = sentinel_password {
        builder = builder.set_client_to_sentinel_password(password.clone());
    }
    builder.build()
}
