// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Redis clients for standalone and Sentinel deployments.

#[cfg(feature = "sync")]
use std::sync::Arc;
#[cfg(feature = "sync")]
use std::sync::Mutex as StdMutex;

#[cfg(feature = "async")]
use async_lock::Mutex as AsyncMutex;
use redis::Client as RedisConnectionClient;
use redis::ErrorKind;
use redis::IntoConnectionInfo;
use redis::RedisError;
#[cfg(feature = "async")]
use redis::aio::MultiplexedConnection;
use redis::sentinel::SentinelClient;
use redis::sentinel::SentinelClientBuilder;
use redis::sentinel::SentinelServerType;

use crate::config::RedisEventBusConfig;

#[path = "client/internal.rs"]
mod internal;
#[cfg(feature = "sync")]
pub(crate) use internal::PooledConnection;
#[cfg(feature = "sync")]
use internal::SyncConnectionPool;

/// Redis connection factory with optional Sentinel master discovery.
///
/// Standalone mode opens a reusable client handle; Sentinel mode resolves the
/// current master for each new connection. Async standalone short commands
/// share a multiplexed connection. Receiver reads use dedicated connections.
pub(crate) struct Client {
    /// Direct standalone client when Sentinel is disabled.
    standalone: Option<RedisConnectionClient>,
    /// Bounded pool for short synchronous standalone commands.
    #[cfg(feature = "sync")]
    sync_pool: Arc<SyncConnectionPool>,
    /// Shared async short-command connection for standalone Redis.
    #[cfg(feature = "async")]
    async_standalone_connection: AsyncMutex<Option<MultiplexedConnection>>,
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
                sync_pool: Arc::new(SyncConnectionPool::new(config.max_idle_connections())),
                #[cfg(feature = "async")]
                async_standalone_connection: AsyncMutex::new(None),
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
            sync_pool: Arc::new(SyncConnectionPool::new(config.max_idle_connections())),
            #[cfg(feature = "async")]
            async_standalone_connection: AsyncMutex::new(None),
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
    pub(crate) fn get_connection(&self) -> Result<PooledConnection, RedisError> {
        if let Some(client) = &self.standalone {
            let connection = self
                .sync_pool
                .idle
                .lock()
                .map_err(|_| RedisError::from((ErrorKind::IoError, "connection pool lock poisoned")))?
                .pop();
            return match connection {
                Some(connection) => Ok(PooledConnection::new(connection, Some(Arc::clone(&self.sync_pool)))),
                None => client
                    .get_connection()
                    .map(|connection| PooledConnection::new(connection, Some(Arc::clone(&self.sync_pool)))),
            };
        }
        self.sync_sentinel
            .as_ref()
            .ok_or_else(|| RedisError::from((ErrorKind::IoError, "missing Sentinel client")))?
            .lock()
            .map_err(|_| RedisError::from((ErrorKind::IoError, "Sentinel lock poisoned")))?
            .get_connection()
            .map(|connection| PooledConnection::new(connection, None))
    }

    /// Opens a receiver-only blocking connection outside the idle pool.
    #[cfg(feature = "sync")]
    pub(crate) fn get_dedicated_connection(&self) -> Result<PooledConnection, RedisError> {
        let connection = if let Some(client) = &self.standalone {
            client.get_connection()?
        } else {
            self.sync_sentinel
                .as_ref()
                .ok_or_else(|| RedisError::from((ErrorKind::IoError, "missing Sentinel client")))?
                .lock()
                .map_err(|_| RedisError::from((ErrorKind::IoError, "Sentinel lock poisoned")))?
                .get_connection()?
        };
        Ok(PooledConnection::new(connection, None))
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
            if let Some(connection) = self.async_standalone_connection.lock().await.as_ref() {
                return Ok(connection.clone());
            }
            let connection = client.get_multiplexed_async_connection().await?;
            let mut cached = self.async_standalone_connection.lock().await;
            if cached.is_none() {
                *cached = Some(connection.clone());
            }
            return Ok(cached.as_ref().expect("connection was initialized").clone());
        }
        let sentinel = self
            .async_sentinel
            .as_ref()
            .ok_or_else(|| RedisError::from((ErrorKind::IoError, "missing Sentinel client")))?;
        sentinel.lock().await.get_async_connection().await
    }

    /// Opens a connection dedicated to a receiver's potentially blocking read.
    #[cfg(feature = "async")]
    pub(crate) async fn get_async_dedicated_connection(&self) -> Result<MultiplexedConnection, RedisError> {
        if let Some(client) = &self.standalone {
            return client.get_multiplexed_async_connection().await;
        }
        let sentinel = self
            .async_sentinel
            .as_ref()
            .ok_or_else(|| RedisError::from((ErrorKind::IoError, "missing Sentinel client")))?;
        sentinel.lock().await.get_async_connection().await
    }

    /// Clears a cached standalone async connection after a command failure.
    #[cfg(feature = "async")]
    pub(crate) async fn invalidate_async_connection(&self) {
        if self.standalone.is_some() {
            *self.async_standalone_connection.lock().await = None;
        }
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

#[cfg(test)]
mod tests {
    use super::Client;
    #[cfg(feature = "sync")]
    use super::SyncConnectionPool;
    #[cfg(any(feature = "sync", feature = "async"))]
    use crate::config::RedisEventBusConfig;

    #[cfg(feature = "sync")]
    #[test]
    fn pooled_connections_are_reused_bounded_and_discardable() {
        use std::io::Read;
        use std::io::Write;
        use std::net::TcpListener;

        use redis::ConnectionLike;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral TCP port");
        let address = listener.local_addr().expect("listener has an address");
        let accept_thread = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("Redis client connects");
            let mut bytes = [0; 4096];
            while let Ok(count) = stream.read(&mut bytes) {
                if count == 0 {
                    break;
                }
                let commands = bytes[..count].iter().filter(|byte| **byte == b'*').count();
                let response = if bytes[..count].windows(4).any(|window| window == b"PING") {
                    b"+PONG\r\n".as_slice()
                } else {
                    b"+OK\r\n".as_slice()
                };
                for _ in 0..commands {
                    stream.write_all(response).expect("reply to Redis setup command");
                }
            }
        });
        let settings =
            RedisEventBusConfig::new(&format!("redis://{address}/"), "pool-test").expect("valid test configuration");
        let client = Client::new(&settings).expect("standalone client configuration is valid");

        let first = client.get_connection().expect("first connection opens");
        assert!(first.is_open());
        drop(first);
        assert_eq!(client.sync_pool.idle.lock().unwrap().len(), 1);

        let mut reused = client.get_connection().expect("idle connection is reused");
        assert!(reused.is_open());
        let pong: String = redis::cmd("PING")
            .query(&mut reused)
            .expect("connection forwards commands to Redis");
        assert_eq!(pong, "PONG");
        drop(reused);
        assert_eq!(client.sync_pool.idle.lock().unwrap().len(), 1);

        let mut discarded = client.get_connection().expect("released connection is reused");
        discarded.discard();
        drop(discarded);
        assert!(client.sync_pool.idle.lock().unwrap().is_empty());

        drop(client);
        accept_thread.join().expect("accept thread completes");
    }

    #[cfg(feature = "sync")]
    fn sentinel_client() -> Client {
        let options: qubit_event_bus::model::ProviderOptions = [
            ("redis.sentinel.nodes".into(), "127.0.0.1:26379".into()),
            ("redis.sentinel.service_name".into(), "test-master".into()),
            ("redis.sentinel.username_env".into(), "PATH".into()),
            ("redis.sentinel.password_env".into(), "HOME".into()),
        ]
        .into();
        let settings = RedisEventBusConfig::from_provider_options(&options)
            .expect("Sentinel settings are valid with existing environment variables");
        Client::new(&settings).expect("Sentinel client configuration is valid")
    }

    fn client_without_sentinel() -> Client {
        Client {
            standalone: None,
            #[cfg(feature = "sync")]
            sync_pool: std::sync::Arc::new(SyncConnectionPool::new(1)),
            #[cfg(feature = "async")]
            async_standalone_connection: async_lock::Mutex::new(None),
            #[cfg(feature = "sync")]
            sync_sentinel: None,
            #[cfg(feature = "async")]
            async_sentinel: None,
        }
    }

    #[cfg(feature = "sync")]
    #[test]
    fn sync_connection_methods_report_a_missing_sentinel() {
        let client = client_without_sentinel();
        assert!(client.get_connection().is_err());
        assert!(client.get_dedicated_connection().is_err());
    }

    #[cfg(feature = "sync")]
    #[test]
    fn sync_connection_methods_report_poisoned_locks() {
        let client = Client::new(&RedisEventBusConfig::default()).expect("standalone client configuration is valid");
        let pool = std::sync::Arc::clone(&client.sync_pool);
        let _ = std::thread::spawn(move || {
            let _guard = pool.idle.lock().expect("pool lock is initially healthy");
            panic!("poison connection pool lock for error-path coverage");
        })
        .join();
        let error = match client.get_connection() {
            Err(error) => error,
            Ok(_) => panic!("poisoned standalone pool must reject a connection checkout"),
        };
        assert!(error.to_string().contains("connection pool lock poisoned"));

        let client = sentinel_client();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = client
                .sync_sentinel
                .as_ref()
                .expect("Sentinel is configured")
                .lock()
                .expect("Sentinel lock is initially healthy");
            panic!("poison Sentinel lock for error-path coverage");
        }));
        assert!(client.get_connection().is_err());
        assert!(client.get_dedicated_connection().is_err());
    }

    #[cfg(feature = "async")]
    #[test]
    fn async_connection_methods_report_a_missing_sentinel() {
        futures_lite::future::block_on(async {
            let client = client_without_sentinel();
            assert!(client.get_async_connection().await.is_err());
            assert!(client.get_async_dedicated_connection().await.is_err());
            client.invalidate_async_connection().await;

            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a local port");
            let address = listener.local_addr().expect("listener has an address");
            drop(listener);
            let settings = RedisEventBusConfig::new(&format!("redis://{address}/"), "unavailable")
                .expect("valid test configuration");
            let standalone = Client::new(&settings).expect("standalone client configuration is valid");
            assert!(standalone.get_async_connection().await.is_err());
            assert!(standalone.get_async_dedicated_connection().await.is_err());
            standalone.invalidate_async_connection().await;
        });
    }
}
