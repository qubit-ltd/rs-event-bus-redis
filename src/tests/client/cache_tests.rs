// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Generation invariants observed through the production client lease contract.
#![cfg(feature = "async")]
use std::io::BufRead;
use std::io::BufReader;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Result as IoResult;
use std::io::Write;
use std::net::Shutdown;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::thread::sleep;
use std::thread::spawn;
use std::time::Duration;

use futures_lite::future::block_on;
use qubit_event_bus::model::ProviderOptions;
use redis::Value;
use redis::aio::ConnectionLike;
use redis::pipe;

use crate::client::Client;
use crate::client::CommandClass;
use crate::config::RedisEventBusConfig;

/// Owns setup/ECHO workers and records database/pipeline traffic for generation
/// tests.
struct SetupEndpoint {
    url: String,
    stop: Arc<AtomicBool>,
    sockets: Arc<Mutex<Vec<TcpStream>>>,
    accepted: Arc<AtomicUsize>,
    worker: Option<JoinHandle<()>>,
    commands: Arc<Mutex<Vec<Vec<String>>>>,
    disconnect_echo: Arc<AtomicBool>,
}
impl SetupEndpoint {
    /// Starts setup/ECHO workers that record RESP boundaries and database
    /// setup.
    ///
    /// # Returns
    ///
    /// An owned TCP endpoint with disconnect control; serving requests performs
    /// blocking socket I/O, and drop stops and joins its workers.
    ///
    /// # Panics
    ///
    /// Panics if fixture socket setup or thread creation fails; workers also
    /// panic on malformed or unexpected fixture commands.
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind endpoint");
        let url = format!("redis://{}/", listener.local_addr().expect("address"));
        listener.set_nonblocking(true).expect("nonblocking");
        let stop = Arc::new(AtomicBool::new(false));
        let sockets = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(AtomicUsize::new(0));
        let commands = Arc::new(Mutex::new(Vec::new()));
        let disconnect_echo = Arc::new(AtomicBool::new(false));
        let shared_commands = Arc::clone(&commands);
        let shared_disconnect = Arc::clone(&disconnect_echo);
        let shared_stop = Arc::clone(&stop);
        let shared_sockets = Arc::clone(&sockets);
        let shared_accepted = Arc::clone(&accepted);
        let worker = spawn(move || {
            let mut workers = Vec::new();
            while !shared_stop.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((socket, _)) => {
                        socket
                            .set_read_timeout(Some(Duration::from_secs(3)))
                            .expect("finite fixture read wait");
                        socket
                            .set_write_timeout(Some(Duration::from_secs(3)))
                            .expect("finite fixture write wait");
                        shared_accepted.fetch_add(1, Ordering::SeqCst);
                        shared_sockets
                            .lock()
                            .expect("sockets")
                            .push(socket.try_clone().expect("clone"));
                        let commands = Arc::clone(&shared_commands);
                        let disconnect = Arc::clone(&shared_disconnect);
                        workers.push(spawn(move || {
                            let mut reader = BufReader::new(socket);
                            while let Ok(Some(command)) = read_command(&mut reader) {
                                commands.lock().expect("commands").push(command.clone());
                                if command[0] == "ECHO" && disconnect.load(Ordering::SeqCst) {
                                    let _ = reader.get_mut().shutdown(Shutdown::Both);
                                    break;
                                }
                                let reply = match command[0].as_str() {
                                    "CLIENT" | "SELECT" => b"+OK\r\n".to_vec(),
                                    "ECHO" => {
                                        format!("${}\r\n{}\r\n", command[1].len(), command[1])
                                            .into_bytes()
                                    }
                                    other => panic!("unexpected fixture command {other}"),
                                };
                                if reader.get_mut().write_all(&reply).is_err() {
                                    break;
                                }
                            }
                        }));
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        sleep(Duration::from_millis(1))
                    }
                    Err(_) => break,
                }
            }
            for worker in workers {
                worker.join().expect("setup worker");
            }
        });
        Self {
            url,
            stop,
            sockets,
            accepted,
            worker: Some(worker),
            commands,
            disconnect_echo,
        }
    }
}
impl Drop for SetupEndpoint {
    /// Stops acceptance and shuts down every socket before joining fixture
    /// threads.
    ///
    /// Shutdown performs socket I/O and joining blocks until workers finish.
    ///
    /// # Panics
    ///
    /// Panics if the fixture socket registry is poisoned or a worker panicked.
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        for socket in self.sockets.lock().expect("sockets").iter() {
            let _ = socket.shutdown(Shutdown::Both);
        }
        if let Some(worker) = self.worker.take() {
            worker.join().expect("listener worker");
        }
    }
}
#[test]
fn test_stale_generation_failure_preserves_new_connection() {
    let server = SetupEndpoint::new();
    let settings = RedisEventBusConfig::new(&server.url, "generation").expect("settings");
    let client = Client::new(&settings).expect("client");
    block_on(async {
        let old = client
            .get_async_connection(CommandClass::General)
            .await
            .expect("first generation");
        client.invalidate_async_connection(old.generation).await;
        let replacement = client
            .get_async_connection(CommandClass::General)
            .await
            .expect("replacement generation");
        assert!(replacement.generation > old.generation);
        client.invalidate_async_connection(old.generation).await;
        let still_current = client
            .get_async_connection(CommandClass::General)
            .await
            .expect("replacement survives stale failure");
        assert_eq!(still_current.generation, replacement.generation);
        assert_eq!(server.accepted.load(Ordering::SeqCst), 2);
    });
}

/// Reads a complete RESP command even when a pipeline spans TCP reads.
///
/// # Parameters
///
/// - `reader`: Fixture socket reader used for blocking I/O.
///
/// # Returns
///
/// Some contains the complete command; None means EOF at a request or argument
/// header before a complete command was read.
///
/// # Errors
///
/// Returns socket read errors, including incomplete argument payloads.
///
/// # Panics
///
/// Panics on malformed RESP headers, invalid UTF-8, or a missing CRLF.
fn read_command(reader: &mut BufReader<TcpStream>) -> IoResult<Option<Vec<String>>> {
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

#[test]
fn test_async_pipeline_database_and_disconnect_generation_replacement() {
    let server = SetupEndpoint::new();
    let settings: ProviderOptions = [
        ("redis.url".into(), format!("{}3", server.url)),
        ("redis.max_concurrent_commands".into(), "2".into()),
        ("redis.max_idle_connections".into(), "1".into()),
        ("redis.connect_timeout_ms".into(), "100".into()),
        ("redis.command_timeout_ms".into(), "100".into()),
    ]
    .into();
    let client =
        Client::new(&RedisEventBusConfig::from_provider_options(&settings).expect("settings"))
            .expect("client");
    block_on(async {
        let mut old = client
            .get_async_connection(CommandClass::General)
            .await
            .expect("first generation");
        assert_eq!(old.get_db(), 3);
        let mut pipeline = pipe();
        pipeline
            .cmd("ECHO")
            .arg("skip")
            .cmd("ECHO")
            .arg("first")
            .cmd("ECHO")
            .arg("second");
        let replies = old
            .req_packed_commands(&pipeline, 1, 2)
            .await
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
        assert_eq!(
            observed
                .iter()
                .filter(|command| command[0] == "ECHO")
                .count(),
            3
        );
        server.disconnect_echo.store(true, Ordering::SeqCst);
        let error = old
            .req_packed_commands(&pipeline, 1, 2)
            .await
            .expect_err("lost pipeline reply");
        assert!(error.is_io_error());
        let failed_generation = old.generation;
        client.invalidate_async_connection(failed_generation).await;
        drop(old);
        server.disconnect_echo.store(false, Ordering::SeqCst);
        let replacement = client
            .get_async_connection(CommandClass::General)
            .await
            .expect("failed lease releases cap-one permit");
        assert!(replacement.generation > failed_generation);
        assert_eq!(replacement.get_db(), 3);
        let replacement_generation = replacement.generation;
        client.invalidate_async_connection(failed_generation).await;
        drop(replacement);
        let current = client
            .get_async_connection(CommandClass::General)
            .await
            .expect("stale invalidation preserves replacement");
        assert_eq!(current.generation, replacement_generation);
        assert_eq!(server.accepted.load(Ordering::SeqCst), 2);
    });
}
