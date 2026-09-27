// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Redis clients for standalone and Sentinel deployments.

use crate::config::RedisEventBusConfig;

/// Redis connection handle with optional Sentinel discovery.
pub(crate) struct RedisClient {
    /// Direct standalone client when Sentinel is disabled.
    standalone: Option<redis::Client>,
    /// Synchronous Sentinel resolver.
    #[cfg(feature = "sync")]
    sync_sentinel: Option<std::sync::Mutex<redis::sentinel::SentinelClient>>,
    /// Async Sentinel resolver, protected by an executor-neutral mutex.
    #[cfg(feature = "async")]
    async_sentinel: Option<async_lock::Mutex<redis::sentinel::SentinelClient>>,
}

impl RedisClient {
    /// Builds standalone and/or Sentinel client handles from validated
    /// settings.
    pub(crate) fn new(config: &RedisEventBusConfig) -> Result<Self, redis::RedisError> {
        let Some(nodes) = config.sentinel_nodes() else {
            let base = redis::Client::open(config.connection_url())?;
            let mut connection_info = base.get_connection_info().clone();
            let (username, password) = config.credentials();
            connection_info.redis.username.clone_from(username);
            connection_info.redis.password.clone_from(password);
            return Ok(Self {
                standalone: Some(redis::Client::open(connection_info)?),
                #[cfg(feature = "sync")]
                sync_sentinel: None,
                #[cfg(feature = "async")]
                async_sentinel: None,
            });
        };
        let endpoints = nodes.iter().map(|node| format!("redis://{node}/")).collect::<Vec<_>>();
        #[cfg(feature = "sync")]
        let sync_sentinel = Some(std::sync::Mutex::new(build_sentinel(config, endpoints.clone())?));
        #[cfg(feature = "async")]
        let async_sentinel = Some(async_lock::Mutex::new(build_sentinel(config, endpoints)?));
        Ok(Self {
            standalone: None,
            #[cfg(feature = "sync")]
            sync_sentinel,
            #[cfg(feature = "async")]
            async_sentinel,
        })
    }

    /// Opens a blocking connection, resolving the current Sentinel master when
    /// configured.
    #[cfg(feature = "sync")]
    pub(crate) fn get_connection(&self) -> Result<redis::Connection, redis::RedisError> {
        if let Some(client) = &self.standalone {
            return client.get_connection();
        }
        self.sync_sentinel
            .as_ref()
            .ok_or_else(|| redis::RedisError::from((redis::ErrorKind::IoError, "missing Sentinel client")))?
            .lock()
            .map_err(|_| redis::RedisError::from((redis::ErrorKind::IoError, "Sentinel lock poisoned")))?
            .get_connection()
    }

    /// Opens a multiplexed async connection using the selected host's executor
    /// adapter.
    #[cfg(feature = "async")]
    pub(crate) async fn get_async_connection(&self) -> Result<redis::aio::MultiplexedConnection, redis::RedisError> {
        if let Some(client) = &self.standalone {
            return client.get_multiplexed_async_connection().await;
        }
        let sentinel = self
            .async_sentinel
            .as_ref()
            .ok_or_else(|| redis::RedisError::from((redis::ErrorKind::IoError, "missing Sentinel client")))?;
        sentinel.lock().await.get_async_connection().await
    }
}

/// Builds an authenticated Sentinel master client from validated settings.
fn build_sentinel<T: redis::IntoConnectionInfo>(
    config: &RedisEventBusConfig,
    endpoints: Vec<T>,
) -> Result<redis::sentinel::SentinelClient, redis::RedisError> {
    let mut builder = redis::sentinel::SentinelClientBuilder::new(
        endpoints
            .into_iter()
            .map(|endpoint| endpoint.into_connection_info().map(|info| info.addr))
            .collect::<Result<Vec<_>, _>>()?,
        config.sentinel_service().unwrap_or_default().to_owned(),
        redis::sentinel::SentinelServerType::Master,
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
