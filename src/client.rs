// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Controlled Redis connection factories and application resource admission.
use std::sync::Arc;
use std::time::Duration;

use redis::Client as RedisConnectionClient;
#[cfg(feature = "sync")]
use redis::Connection;
use redis::RedisError;
#[cfg(feature = "async")]
use redis::aio::MultiplexedConnection;

use crate::config::RedisEventBusConfig;
use crate::diagnostics::RedisDiagnosticsState;
use crate::diagnostics::RedisProviderMode;
use crate::error::RedisProviderError;
use crate::internal::TransportPolicy;
use crate::redis_provider_error::from_redis_error;

/// Connection leases, admission accounting, and bounded endpoint discovery.
mod internal;
#[cfg(feature = "async")]
use self::internal::AsyncCommandConnection;
#[cfg(feature = "async")]
use self::internal::AsyncConnectionCache;
pub(crate) use self::internal::CommandClass;
#[cfg(test)]
use self::internal::CommandPermit;
#[cfg(feature = "sync")]
pub(crate) use self::internal::PooledConnection;
pub(crate) use self::internal::ReceiverPermit;
pub(crate) use self::internal::ResourceBudget;
use self::internal::SentinelResolver;
#[cfg(feature = "sync")]
use self::internal::SyncConnectionPool;
#[cfg(feature = "sync")]
use self::internal::sentinel_resolver::configure_sync;
#[cfg(feature = "async")]
use self::internal::sentinel_resolver::open_async as open_async_connection;
#[cfg(feature = "sync")]
use self::internal::sentinel_resolver::open_sync as open_sync_connection;

/// Shares finite admission caps; receiver reads always use dedicated
/// connections.
pub(crate) struct Client {
    /// Parsed standalone endpoint, or `None` when Sentinel discovery is
    /// selected.
    standalone: Option<RedisConnectionClient>,
    /// Bounded resolver, present only when Sentinel endpoint settings were
    /// supplied.
    sentinel: Option<SentinelResolver>,
    /// Admission counters shared by short operations and receiver leases.
    budget: Arc<ResourceBudget>,
    /// Instance diagnostics, attached by a provider before it shares the client.
    diagnostics: Option<Arc<RedisDiagnosticsState>>,
    /// Finite per-endpoint setup and per-command response waiting budgets.
    policy: TransportPolicy,
    /// Standalone idle short-command connections; receiver sockets are
    /// excluded.
    #[cfg(feature = "sync")]
    sync_pool: Arc<SyncConnectionPool>,
    /// Single-flight standalone short-command connection cache with
    /// generations.
    #[cfg(feature = "async")]
    async_cache: AsyncConnectionCache,
}
impl Client {
    /// Parses validated endpoint/ACL options without performing network I/O.
    ///
    /// # Parameters
    ///
    /// - `config`: Validated endpoint, credential, timeout, and admission
    ///   settings.
    ///
    /// # Returns
    ///
    /// A client with empty connection caches and no admitted operations.
    ///
    /// # Errors
    ///
    /// Returns a Redis configuration error if a stored endpoint cannot be
    /// parsed.
    pub(crate) fn new(config: &RedisEventBusConfig) -> Result<Self, RedisError> {
        let (standalone, sentinel) = if config.sentinel_nodes().is_some() {
            (None, Some(SentinelResolver::new(config)?))
        } else {
            let mut info = RedisConnectionClient::open(config.connection_url())?
                .get_connection_info()
                .clone();
            let (username, password) = config.credentials();
            info.redis.username.clone_from(username);
            info.redis.password.clone_from(password);
            (Some(RedisConnectionClient::open(info)?), None)
        };
        Ok(Self {
            standalone,
            sentinel,
            budget: Arc::new(ResourceBudget::new(
                config.max_concurrent_commands(),
                config.reserved_settlement_commands(),
                config.max_active_receivers(),
            )),
            diagnostics: None,
            policy: TransportPolicy::from_config(config),
            #[cfg(feature = "sync")]
            sync_pool: Arc::new(SyncConnectionPool::new(config.max_idle_connections())),
            #[cfg(feature = "async")]
            async_cache: AsyncConnectionCache::new(),
        })
    }
    /// Registers diagnostics exactly once before a provider shares this client.
    ///
    /// Returns an error if diagnostics were already attached or the process ID
    /// space is exhausted; it performs no Redis network I/O.
    pub(crate) fn attach_diagnostics(
        &mut self,
        mode: RedisProviderMode,
        namespace: &str,
    ) -> Result<(), RedisProviderError> {
        if self.diagnostics.is_some() {
            return Err(RedisProviderError::Operation("Redis diagnostics already attached"));
        }
        self.diagnostics = Some(RedisDiagnosticsState::register(
            mode,
            namespace,
            Arc::clone(&self.budget),
        )?);
        Ok(())
    }

    /// Borrows the attached instance counters, if a provider created this client.
    ///
    /// Internal clients built directly by tests may have no diagnostics.
    #[allow(dead_code)] // T2 and T3 attach the counter update sites.
    pub(crate) fn diagnostics(&self) -> Option<&RedisDiagnosticsState> {
        self.diagnostics.as_deref()
    }
    /// Reserves a short-command slot before I/O.
    ///
    /// # Returns
    ///
    /// A non-cloneable permit whose drop releases application admission.
    ///
    /// # Errors
    ///
    /// Returns `ResourceLimit` immediately when the shared command cap is
    /// reached.
    #[inline]
    #[cfg(test)]
    pub(crate) fn try_command(&self, class: CommandClass) -> Result<CommandPermit, RedisProviderError> {
        self.budget.try_command(class)
    }
    /// Reserves receiver capacity before subscription setup I/O.
    ///
    /// # Returns
    ///
    /// A non-cloneable permit retained until setup failure, close, or drop.
    ///
    /// # Errors
    ///
    /// Returns `ResourceLimit` immediately when the receiver cap is reached.
    #[inline]
    pub(crate) fn try_receiver(&self) -> Result<ReceiverPermit, RedisProviderError> {
        self.budget.try_receiver()
    }
    /// Returns the configured short-command response waiting budget.
    ///
    /// # Returns
    ///
    /// The validated command timeout without duration conversion or I/O. This
    /// is an individual transport wait, not a wall-clock operation deadline.
    #[must_use]
    #[inline]
    pub(crate) fn command_timeout(&self) -> Duration {
        self.policy.command_timeout
    }
    /// Calculates response waiting including the actual Redis BLOCK duration.
    ///
    /// # Parameters
    ///
    /// - `block_ms`: `Some` includes that server wait; `None` uses the
    ///   short-command budget.
    ///
    /// # Returns
    ///
    /// A finite individual I/O wait budget, not a synchronous wall-clock
    /// deadline.
    ///
    /// # Errors
    ///
    /// Returns a configuration error if milliseconds or checked duration
    /// addition overflow.
    pub(crate) fn response_timeout(&self, block_ms: Option<usize>) -> Result<Duration, RedisProviderError> {
        let block = block_ms
            .map(|ms| u64::try_from(ms).map(Duration::from_millis))
            .transpose()
            .map_err(|_| RedisProviderError::Configuration("BLOCK duration overflow"))?;
        self.policy.response_timeout(block)
    }
    /// Reserves admission and checks out a bounded short-command connection.
    ///
    /// Standalone connections reuse the idle pool; Sentinel operations resolve
    /// the current master afresh. Network setup runs outside the pool mutex.
    ///
    /// # Returns
    ///
    /// A lease owning its command permit until drop, with short socket waits
    /// restored.
    ///
    /// # Errors
    ///
    /// Returns resource exhaustion, pool poisoning, or a sanitized
    /// endpoint/setup failure.
    #[cfg(feature = "sync")]
    pub(crate) fn get_connection(&self, class: CommandClass) -> Result<PooledConnection, RedisProviderError> {
        let permit = self.budget.try_command(class)?;
        if self.standalone.is_some() {
            let idle = self
                .sync_pool
                .idle
                .lock()
                .map_err(|_| RedisProviderError::Operation("connection pool lock poisoned"))?
                .pop();
            let connection = match idle {
                Some(connection) => connection,
                None => self.open_sync()?,
            };
            configure_sync(&connection, self.policy.command_timeout)
                .map_err(|error| from_redis_error("connect", &error))?;
            Ok(PooledConnection::new(
                connection,
                Some(Arc::clone(&self.sync_pool)),
                Some(permit),
            ))
        } else {
            Ok(PooledConnection::new(self.open_sync()?, None, Some(permit)))
        }
    }
    /// Opens a receiver socket outside the idle pool with bounded setup waits.
    ///
    /// # Returns
    ///
    /// A dedicated lease; its caller separately owns receiver admission.
    ///
    /// # Errors
    ///
    /// Returns sanitized endpoint discovery, connection, or timeout setup
    /// failures.
    #[cfg(feature = "sync")]
    pub(crate) fn get_dedicated_connection(&self) -> Result<PooledConnection, RedisProviderError> {
        Ok(PooledConnection::new(self.open_sync()?, None, None))
    }

    /// Reserves admission and single-flights standalone cold connection setup.
    ///
    /// The host executor polls all I/O. Cancellation releases application
    /// admission but does not guarantee cancellation of an already sent Redis
    /// request.
    ///
    /// # Returns
    ///
    /// A permit-owning connection generation; Sentinel leases use generation
    /// zero.
    ///
    /// # Errors
    ///
    /// Returns resource exhaustion, generation overflow, or sanitized setup
    /// failures.
    #[cfg(feature = "async")]
    pub(crate) async fn get_async_connection(
        &self,
        class: CommandClass,
    ) -> Result<AsyncCommandConnection, RedisProviderError> {
        let permit = self.budget.try_command(class)?;
        let (generation, connection) = if self.standalone.is_some() {
            self.async_cache.get_or_connect(self.open_async()).await?
        } else {
            (0, self.open_async().await?)
        };
        Ok(AsyncCommandConnection {
            generation,
            connection,
            _permit: permit,
        })
    }
    /// Opens host-polled receiver transport independent of the command cache.
    ///
    /// # Returns
    ///
    /// A dedicated connection whose caller separately retains receiver
    /// admission.
    ///
    /// # Errors
    ///
    /// Returns sanitized endpoint discovery, connection, or setup timeout
    /// failures.
    #[cfg(feature = "async")]
    pub(crate) async fn get_async_dedicated_connection(&self) -> Result<MultiplexedConnection, RedisProviderError> {
        self.open_async().await
    }

    /// Clears only the failing lease generation while holding the async cache
    /// lock.
    ///
    /// # Parameters
    ///
    /// - `generation`: Identity carried by the failed short-command lease.
    ///
    /// A stale generation leaves a replacement connection intact. No network
    /// I/O is issued.
    #[cfg(feature = "async")]
    pub(crate) async fn invalidate_async_connection(&self, generation: u64) {
        self.async_cache.invalidate_if_current(generation).await;
    }
    /// Opens controlled standalone or freshly discovered Sentinel transport.
    ///
    /// # Returns
    ///
    /// A newly connected socket with finite short-command read/write waits.
    ///
    /// # Errors
    ///
    /// Returns a sanitized discovery/setup failure or missing-factory invariant
    /// error.
    #[cfg(feature = "sync")]
    fn open_sync(&self) -> Result<Connection, RedisProviderError> {
        let result = match (&self.standalone, &self.sentinel) {
            (Some(client), _) => open_sync_connection(client, self.policy),
            (_, Some(resolver)) => resolver.connect_sync(),
            _ => return Err(RedisProviderError::Operation("missing Redis client")),
        };
        result.map_err(|error| from_redis_error("connect", &error))
    }
    /// Opens bounded async standalone or Sentinel transport on the host
    /// executor.
    ///
    /// # Returns
    ///
    /// A newly initialized connection with the short-command response budget.
    ///
    /// # Errors
    ///
    /// Returns sanitized discovery/setup failures or a missing-factory
    /// invariant error.
    #[cfg(feature = "async")]
    async fn open_async(&self) -> Result<MultiplexedConnection, RedisProviderError> {
        let result = match (&self.standalone, &self.sentinel) {
            (Some(client), _) => open_async_connection(client, self.policy).await,
            (_, Some(resolver)) => resolver.connect_async().await,
            _ => return Err(RedisProviderError::Operation("missing Redis client")),
        };
        result.map_err(|error| from_redis_error("connect", &error))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "sync")]
    use std::io::BufRead;
    #[cfg(feature = "sync")]
    use std::io::BufReader;
    #[cfg(feature = "sync")]
    use std::io::ErrorKind;
    #[cfg(feature = "sync")]
    use std::io::Read;
    #[cfg(feature = "sync")]
    use std::io::Result as IoResult;
    #[cfg(feature = "sync")]
    use std::io::Write;
    #[cfg(feature = "sync")]
    use std::net::Shutdown;
    #[cfg(feature = "sync")]
    use std::net::TcpListener;
    #[cfg(feature = "sync")]
    use std::net::TcpStream;
    #[cfg(feature = "sync")]
    use std::sync::Arc;
    #[cfg(feature = "sync")]
    use std::sync::Mutex;
    #[cfg(feature = "sync")]
    use std::sync::atomic::AtomicBool;
    #[cfg(feature = "sync")]
    use std::sync::atomic::AtomicUsize;
    #[cfg(feature = "sync")]
    use std::sync::atomic::Ordering;
    #[cfg(feature = "sync")]
    use std::thread::JoinHandle;
    #[cfg(feature = "sync")]
    use std::thread::sleep;
    #[cfg(feature = "sync")]
    use std::thread::spawn;
    use std::time::Duration;

    #[cfg(feature = "async")]
    use futures_lite::future::block_on;
    #[cfg(feature = "sync")]
    use qubit_event_bus::model::ProviderOptions;
    #[cfg(feature = "sync")]
    use redis::ConnectionLike;
    #[cfg(feature = "sync")]
    use redis::Value;
    #[cfg(feature = "sync")]
    use redis::cmd;
    #[cfg(feature = "sync")]
    use redis::pipe;

    use super::Client;
    use super::CommandClass;
    use crate::config::RedisEventBusConfig;
    #[cfg(feature = "sync")]
    use crate::error::RedisProviderError;
    #[cfg(feature = "sync")]
    #[test]
    fn test_pooled_connections_are_reused_bounded_and_discardable() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral TCP port");
        let address = listener.local_addr().expect("listener has an address");
        let accept_thread = spawn(move || {
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

        let first = client
            .get_connection(CommandClass::General)
            .expect("first connection opens");
        assert!(first.is_open());
        drop(first);
        assert_eq!(client.sync_pool.idle.lock().unwrap().len(), 1);

        let mut reused = client
            .get_connection(CommandClass::General)
            .expect("idle connection is reused");
        assert!(reused.is_open());
        let pong: String = cmd("PING")
            .query(&mut reused)
            .expect("connection forwards commands to Redis");
        assert_eq!(pong, "PONG");
        drop(reused);
        assert_eq!(client.sync_pool.idle.lock().unwrap().len(), 1);

        let mut discarded = client
            .get_connection(CommandClass::General)
            .expect("released connection is reused");
        discarded.discard();
        drop(discarded);
        assert!(client.sync_pool.idle.lock().unwrap().is_empty());

        drop(client);
        accept_thread.join().expect("accept thread completes");
    }

    #[cfg(feature = "sync")]
    #[test]
    fn test_poisoned_pool_fails_without_network_io() {
        let client = Client::new(&RedisEventBusConfig::default()).expect("client");
        let pool = Arc::clone(&client.sync_pool);
        let _ = spawn(move || {
            let _guard = pool.idle.lock().expect("pool");
            panic!("poison pool for regression");
        })
        .join();
        assert!(matches!(
            client.get_connection(CommandClass::General),
            Err(RedisProviderError::Operation("connection pool lock poisoned"))
        ));
    }
    #[test]
    fn test_missing_factory_reports_sanitized_error() {
        let mut client = Client::new(&RedisEventBusConfig::default()).expect("client");
        client.standalone = None;
        client.sentinel = None;
        #[cfg(feature = "sync")]
        {
            assert!(client.get_connection(CommandClass::General).is_err());
            assert!(client.get_dedicated_connection().is_err());
        }
        #[cfg(feature = "async")]
        block_on(async {
            assert!(client.get_async_connection(CommandClass::General).await.is_err());
            assert!(client.get_async_dedicated_connection().await.is_err());
            client.invalidate_async_connection(0).await;
        });
    }
    #[cfg(feature = "sync")]
    #[test]
    fn test_pool_retention_cap_one_discards_extra_idle_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("TCP endpoint");
        let address = listener.local_addr().expect("address");
        let worker = spawn(move || {
            let mut workers = Vec::new();
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().expect("connection");
                workers.push(spawn(move || {
                    let mut bytes = [0; 4096];
                    while let Ok(count) = stream.read(&mut bytes) {
                        if count == 0 {
                            break;
                        }
                        for _ in 0..bytes[..count].iter().filter(|byte| **byte == b'*').count() {
                            if stream.write_all(b"+OK\r\n").is_err() {
                                return;
                            }
                        }
                    }
                }));
            }
            for worker in workers {
                worker.join().expect("socket worker");
            }
        });
        let options: ProviderOptions = [
            ("redis.url".into(), format!("redis://{address}/")),
            ("redis.max_idle_connections".into(), "1".into()),
            ("redis.max_concurrent_commands".into(), "3".into()),
            ("redis.reserved_settlement_commands".into(), "1".into()),
        ]
        .into();
        let client =
            Client::new(&RedisEventBusConfig::from_provider_options(&options).expect("settings")).expect("client");
        let first = client.get_connection(CommandClass::General).expect("first checkout");
        let second = client.get_connection(CommandClass::General).expect("second checkout");
        drop(first);
        drop(second);
        assert_eq!(
            client.sync_pool.idle.lock().expect("pool").len(),
            1,
            "extra idle connection is dropped"
        );
        drop(client);
        worker.join().expect("listener worker");
    }
    /// Local RESP endpoint recording setup and pipeline traffic; modes inject
    /// non-PONG, ECHO disconnect, and PING disconnect without changing
    /// production.
    #[cfg(feature = "sync")]
    struct LeaseEndpoint {
        url: String,
        stop: Arc<AtomicBool>,
        mode: Arc<AtomicUsize>,
        accepted: Arc<AtomicUsize>,
        commands: Arc<Mutex<Vec<Vec<String>>>>,
        sockets: Arc<Mutex<Vec<TcpStream>>>,
        worker: Option<JoinHandle<()>>,
    }

    #[cfg(feature = "sync")]
    impl LeaseEndpoint {
        /// Starts a local RESP endpoint recording lease health and pipeline
        /// traffic.
        ///
        /// # Returns
        ///
        /// An owned endpoint with TCP workers that are joined on drop. Binding,
        /// accepting, and serving requests perform blocking socket I/O.
        ///
        /// # Panics
        ///
        /// Panics if fixture socket setup or thread creation fails; workers
        /// also panic on malformed or unexpected fixture commands.
        fn new() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("lease endpoint");
            let url = format!("redis://{}/3", listener.local_addr().expect("address"));
            listener.set_nonblocking(true).expect("nonblocking listener");
            let stop = Arc::new(AtomicBool::new(false));
            let mode = Arc::new(AtomicUsize::new(0));
            let accepted = Arc::new(AtomicUsize::new(0));
            let commands = Arc::new(Mutex::new(Vec::new()));
            let sockets = Arc::new(Mutex::new(Vec::new()));
            let thread_stop = Arc::clone(&stop);
            let thread_mode = Arc::clone(&mode);
            let thread_accepted = Arc::clone(&accepted);
            let thread_commands = Arc::clone(&commands);
            let thread_sockets = Arc::clone(&sockets);
            let worker = spawn(move || {
                let mut workers = Vec::new();
                while !thread_stop.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            thread_accepted.fetch_add(1, Ordering::SeqCst);
                            stream
                                .set_read_timeout(Some(Duration::from_secs(3)))
                                .expect("fixture socket wait");
                            thread_sockets
                                .lock()
                                .expect("sockets")
                                .push(stream.try_clone().expect("socket clone"));
                            let mode = Arc::clone(&thread_mode);
                            let commands = Arc::clone(&thread_commands);
                            workers.push(spawn(move || {
                                let mut reader = BufReader::new(stream);
                                while let Ok(Some(command)) = read_lease_command(&mut reader) {
                                    commands.lock().expect("commands").push(command.clone());
                                    let current_mode = mode.load(Ordering::SeqCst);
                                    if (command[0] == "ECHO" && current_mode == 2)
                                        || (command[0] == "PING" && current_mode == 3)
                                    {
                                        let _ = reader.get_mut().shutdown(Shutdown::Both);
                                        break;
                                    }
                                    let reply = match command[0].as_str() {
                                        "PING" if current_mode == 1 => b"+NOT_PONG\r\n".to_vec(),
                                        "PING" => b"+PONG\r\n".to_vec(),
                                        "ECHO" => format!("${}\r\n{}\r\n", command[1].len(), command[1]).into_bytes(),
                                        "CLIENT" | "SELECT" => b"+OK\r\n".to_vec(),
                                        other => panic!("unexpected fixture command {other}"),
                                    };
                                    if reader.get_mut().write_all(&reply).is_err() {
                                        break;
                                    }
                                }
                            }));
                        }
                        Err(error) if error.kind() == ErrorKind::WouldBlock => sleep(Duration::from_millis(1)),
                        Err(_) => break,
                    }
                }
                for worker in workers {
                    worker.join().expect("lease socket worker");
                }
            });
            Self {
                url,
                stop,
                mode,
                accepted,
                commands,
                sockets,
                worker: Some(worker),
            }
        }

        /// Constructs a one-general-slot client using this endpoint's
        /// database-three URL.
        ///
        /// # Returns
        ///
        /// A lazy client with finite setup/socket waits; no Redis I/O occurs.
        ///
        /// # Panics
        ///
        /// Panics if the fixed fixture configuration cannot be validated or
        /// parsed.
        fn client(&self) -> Client {
            let options: ProviderOptions = [
                ("redis.url".into(), self.url.clone()),
                ("redis.max_concurrent_commands".into(), "2".into()),
                ("redis.max_idle_connections".into(), "1".into()),
                ("redis.connect_timeout_ms".into(), "100".into()),
                ("redis.command_timeout_ms".into(), "100".into()),
            ]
            .into();
            Client::new(&RedisEventBusConfig::from_provider_options(&options).expect("valid finite settings"))
                .expect("client")
        }
    }

    #[cfg(feature = "sync")]
    impl Drop for LeaseEndpoint {
        /// Stops acceptance and unblocks every socket before joining fixture
        /// workers.
        ///
        /// Shutdown performs socket I/O and joining blocks until workers
        /// finish.
        ///
        /// # Panics
        ///
        /// Panics if the fixture socket registry is poisoned or a worker
        /// panicked.
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            for socket in self.sockets.lock().expect("sockets").iter() {
                let _ = socket.shutdown(Shutdown::Both);
            }
            if let Some(worker) = self.worker.take() {
                worker.join().expect("lease listener");
            }
        }
    }

    /// Reads one RESP request while preserving command boundaries and
    /// arguments.
    ///
    /// # Parameters
    ///
    /// - `reader`: Fixture socket reader used for blocking I/O.
    ///
    /// # Returns
    ///
    /// Some contains the complete command; None means EOF at a request or
    /// argument header, before a complete request was read.
    ///
    /// # Errors
    ///
    /// Returns socket read errors, including incomplete argument payloads.
    ///
    /// # Panics
    ///
    /// Panics on malformed RESP headers, invalid UTF-8, or a missing CRLF.
    #[cfg(feature = "sync")]
    fn read_lease_command(reader: &mut BufReader<TcpStream>) -> IoResult<Option<Vec<String>>> {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            return Ok(None);
        }
        let count = line
            .trim()
            .strip_prefix('*')
            .expect("RESP array")
            .parse::<usize>()
            .expect("array count");
        let mut command = Vec::with_capacity(count);
        for _ in 0..count {
            line.clear();
            if reader.read_line(&mut line)? == 0 {
                return Ok(None);
            }
            let length = line
                .trim()
                .strip_prefix('$')
                .expect("RESP bulk")
                .parse::<usize>()
                .expect("bulk length");
            let mut bytes = vec![0; length + 2];
            reader.read_exact(&mut bytes)?;
            assert_eq!(&bytes[length..], b"\r\n");
            bytes.truncate(length);
            command.push(String::from_utf8(bytes).expect("ASCII fixture command"));
        }
        Ok(Some(command))
    }

    #[cfg(feature = "sync")]
    #[test]
    fn test_sync_pipeline_database_and_health_failures_discard_idle_leases() {
        let server = LeaseEndpoint::new();
        let client = server.client();
        let mut connection = client.get_connection(CommandClass::General).expect("first checkout");
        assert_eq!(connection.get_db(), 3);
        assert!(connection.check_connection(), "complete PONG confirms healthy lease");
        let mut pipeline = pipe();
        pipeline
            .cmd("ECHO")
            .arg("skip")
            .cmd("ECHO")
            .arg("first")
            .cmd("ECHO")
            .arg("second");
        let replies = connection
            .req_packed_commands(&pipeline.get_packed_pipeline(), 1, 2)
            .expect("three-command pipeline");
        assert_eq!(
            replies,
            [
                Value::BulkString(b"first".to_vec()),
                Value::BulkString(b"second".to_vec())
            ]
        );
        let observed = server.commands.lock().expect("commands").clone();
        assert!(observed.contains(&vec!["SELECT".into(), "3".into()]));
        assert_eq!(observed.iter().filter(|command| command[0] == "ECHO").count(), 3);
        server.mode.store(1, Ordering::SeqCst);
        assert!(
            !connection.check_connection(),
            "a complete non-PONG reply is not healthy"
        );
        assert!(connection.is_open(), "discard cannot rely on the transport's open flag");
        drop(connection);
        assert!(client.sync_pool.idle.lock().expect("pool").is_empty());

        let mut connection = client
            .get_connection(CommandClass::General)
            .expect("non-PONG failure releases cap-one permit");
        server.mode.store(2, Ordering::SeqCst);
        let error = connection
            .req_packed_commands(&pipeline.get_packed_pipeline(), 1, 2)
            .expect_err("pipeline reply lost");
        assert!(error.is_io_error());
        drop(connection);
        assert!(client.sync_pool.idle.lock().expect("pool").is_empty());

        let mut connection = client
            .get_connection(CommandClass::General)
            .expect("pipeline failure releases cap-one permit");
        server.mode.store(3, Ordering::SeqCst);
        assert!(!connection.check_connection(), "PING disconnect must discard the lease");
        drop(connection);
        assert!(client.sync_pool.idle.lock().expect("pool").is_empty());
        server.mode.store(0, Ordering::SeqCst);
        let mut connection = client
            .get_connection(CommandClass::General)
            .expect("health failure releases cap-one permit");
        assert!(connection.check_connection());
        drop(connection);
        assert_eq!(client.sync_pool.idle.lock().expect("pool").len(), 1);
        assert_eq!(server.accepted.load(Ordering::SeqCst), 4);
    }

    #[cfg(feature = "sync")]
    #[test]
    fn test_internal_socket_configuration_failure_drops_idle_and_releases_admission() {
        let server = LeaseEndpoint::new();
        let mut client = server.client();
        drop(
            client
                .get_connection(CommandClass::General)
                .expect("initial normal socket"),
        );
        assert_eq!(client.sync_pool.idle.lock().expect("pool").len(), 1);
        // Internal OS/socket-configuration failure injection only: public options
        // reject zero. This tests failed checkout cleanup, not a valid user setting.
        let valid_timeout = client.policy.command_timeout;
        client.policy.command_timeout = Duration::ZERO;
        let error = match client.get_connection(CommandClass::General) {
            Err(error) => error,
            Ok(_) => panic!("OS must reject a zero socket waiting duration"),
        };
        assert!(matches!(
            error,
            RedisProviderError::Transport {
                operation: "connect",
                kind: "transport",
                retryable: Some(true)
            }
        ));
        assert!(!error.to_string().contains(server.url.as_str()));
        assert!(
            client.sync_pool.idle.lock().expect("pool").is_empty(),
            "failed checkout cannot retain the idle socket"
        );
        client.policy.command_timeout = valid_timeout;
        let replacement = client
            .get_connection(CommandClass::General)
            .expect("configuration failure releases cap-one command permit");
        assert_eq!(replacement.get_db(), 3);
        assert_eq!(
            server.accepted.load(Ordering::SeqCst),
            2,
            "failed idle socket must be replaced"
        );
    }

    #[test]
    fn test_command_timeout_is_the_exact_policy_budget_without_io() {
        let settings = RedisEventBusConfig::new("redis://127.0.0.1:1/", "budget").expect("settings");
        let mut client = Client::new(&settings).expect("client construction performs no I/O");
        assert_eq!(client.command_timeout(), client.policy.command_timeout);
        // Internal boundary injection only: public configuration rejects these
        // budgets. The getter must preserve the policy without arithmetic.
        for timeout in [Duration::ZERO, Duration::MAX] {
            client.policy.command_timeout = timeout;
            assert_eq!(client.command_timeout(), timeout);
        }
    }
}
