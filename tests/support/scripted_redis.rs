// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Scripted RESP2 endpoint for deterministic transport and protocol failures.

use std::collections::VecDeque;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Error;
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
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::thread::sleep;
use std::thread::spawn;
use std::time::Duration;

/// One command's response, including loss of the connection before a reply.
pub enum Reply {
    Bytes(Vec<u8>),
    DelayedBytes(Vec<u8>, Duration),
    PendingEntry { id: String, deliveries: u64 },
    Disconnect,
    DisconnectAndRefuseConnections,
}

/// Command names are checked in order across all connections to the endpoint.
pub struct Step {
    command: &'static str,
    reply: Reply,
}

impl Step {
    /// Constructs a step expecting `command` and returning a copy of raw RESP
    /// `reply`.
    pub fn reply(command: &'static str, reply: &[u8]) -> Self {
        Self {
            command,
            reply: Reply::Bytes(reply.to_vec()),
        }
    }

    /// Constructs a step expecting `command` and returning raw RESP `reply`
    /// after `delay`.
    pub fn delayed_reply(command: &'static str, reply: &[u8], delay: Duration) -> Self {
        Self {
            command,
            reply: Reply::DelayedBytes(reply.to_vec(), delay),
        }
    }

    /// Constructs an XPENDING row owned by the most recent XAUTOCLAIM consumer.
    pub fn pending_entry(id: &str, deliveries: u64) -> Self {
        Self {
            command: "XPENDING",
            reply: Reply::PendingEntry {
                id: id.to_owned(),
                deliveries,
            },
        }
    }

    /// Constructs a step expecting `command` and closing without a response.
    ///
    /// If `refuse_connections` is true, subsequent connections are also
    /// refused.
    pub fn disconnect(command: &'static str, refuse_connections: bool) -> Self {
        Self {
            command,
            reply: if refuse_connections {
                Reply::DisconnectAndRefuseConnections
            } else {
                Reply::Disconnect
            },
        }
    }
}

struct State {
    steps: Mutex<VecDeque<Step>>,
    commands: Mutex<Vec<Vec<String>>>,
    errors: Mutex<Vec<String>>,
    connections: Mutex<Vec<TcpStream>>,
    stopping: AtomicBool,
    refusing: AtomicBool,
    consumer: Mutex<Option<String>>,
}

/// Owns an ephemeral TCP listener and joins all connection workers on drop.
///
/// Redis client setup commands are answered automatically. Every application
/// command must match the next scripted step; unexpected commands fail the
/// test instead of being silently accepted.
pub struct ScriptedRedis {
    url: String,
    state: Arc<State>,
    worker: Option<JoinHandle<()>>,
}

impl ScriptedRedis {
    /// Starts a local scripted endpoint and takes ownership of `steps`.
    ///
    /// Returns the listener/worker fixture, or an IO error for bind/nonblocking
    /// setup. Spawns threads that perform blocking socket IO; thread
    /// creation may panic. Unexpected traffic is recorded for finish, and
    /// worker panics surface on drop.
    pub fn start(steps: Vec<Step>) -> IoResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let state = Arc::new(State {
            steps: Mutex::new(steps.into()),
            commands: Mutex::new(Vec::new()),
            errors: Mutex::new(Vec::new()),
            connections: Mutex::new(Vec::new()),
            stopping: AtomicBool::new(false),
            refusing: AtomicBool::new(false),
            consumer: Mutex::new(None),
        });
        let shared = Arc::clone(&state);
        let worker = spawn(move || {
            let mut workers = Vec::new();
            while !shared.stopping.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        if shared.refusing.load(Ordering::SeqCst) || shared.stopping.load(Ordering::SeqCst) {
                            let _ = stream.shutdown(Shutdown::Both);
                            continue;
                        }
                        stream
                            .set_read_timeout(Some(Duration::from_secs(3)))
                            .expect("set read timeout");
                        stream
                            .set_write_timeout(Some(Duration::from_secs(3)))
                            .expect("set write timeout");
                        shared
                            .connections
                            .lock()
                            .expect("connections lock")
                            .push(stream.try_clone().expect("clone connection for shutdown"));
                        let state = Arc::clone(&shared);
                        workers.push(spawn(move || serve_connection(stream, &state)));
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        sleep(Duration::from_millis(1));
                    }
                    Err(error) => {
                        shared.errors.lock().expect("errors lock").push(error.to_string());
                        break;
                    }
                }
            }
            for worker in workers {
                worker.join().expect("RESP connection worker completes");
            }
        });
        Ok(Self {
            url: format!("redis://{address}/"),
            state,
            worker: Some(worker),
        })
    }

    /// Returns the endpoint URL borrowed from this fixture without allocating.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns a snapshot of observed commands, leaving unused script steps
    /// allowed.
    ///
    /// Locks shared state briefly and panics for recorded protocol errors or
    /// poisoned mutexes. Each returned vector contains one command's arguments.
    pub fn finish_allow_remaining(&self) -> Vec<Vec<String>> {
        let errors = self.state.errors.lock().expect("errors lock").clone();
        assert!(errors.is_empty(), "unexpected RESP traffic: {errors:?}");
        self.state.commands.lock().expect("commands lock").clone()
    }

    /// Returns a snapshot of observed command arguments after checking all
    /// steps.
    ///
    /// Locks shared state briefly and panics for recorded errors, poisoned
    /// mutexes, or any unused scripted step. Does not consume the recorded
    /// command history.
    pub fn finish(&self) -> Vec<Vec<String>> {
        let errors = self.state.errors.lock().expect("errors lock").clone();
        assert!(errors.is_empty(), "unexpected RESP traffic: {errors:?}");
        assert!(
            self.state.steps.lock().expect("steps lock").is_empty(),
            "script was not fully consumed"
        );
        self.state.commands.lock().expect("commands lock").clone()
    }
}

impl Drop for ScriptedRedis {
    /// Stops the listener, closes owned socket connections, and joins its
    /// workers.
    ///
    /// Performs blocking socket/thread IO; panics for poisoned state or worker
    /// failure.
    fn drop(&mut self) {
        self.state.stopping.store(true, Ordering::SeqCst);
        for stream in self.state.connections.lock().expect("connections lock").iter() {
            let _ = stream.shutdown(Shutdown::Both);
        }
        if let Some(worker) = self.worker.take() {
            worker.join().expect("RESP listener worker completes");
        }
    }
}

/// Consumes owned `stream` requests against the script shared through `state`.
///
/// Performs blocking reads/writes, records invalid traffic, and returns on EOF
/// or a scripted disconnect. Panics for poisoned state or failed response
/// writes.
fn serve_connection(stream: TcpStream, state: &State) {
    let mut reader = BufReader::new(stream);
    while !state.stopping.load(Ordering::SeqCst) {
        let command = match read_command(&mut reader) {
            Ok(Some(command)) => command,
            Ok(None) => return,
            Err(error) => {
                if !state.stopping.load(Ordering::SeqCst)
                    && !matches!(error.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock)
                {
                    state.errors.lock().expect("errors lock").push(error.to_string());
                }
                return;
            }
        };
        let name = command.first().expect("command has a name");
        if name == "CLIENT" {
            reader.get_mut().write_all(b"+OK\r\n").expect("reply to CLIENT SETINFO");
            continue;
        }
        state.commands.lock().expect("commands lock").push(command.clone());
        if name == "XAUTOCLAIM"
            && let Some(consumer) = command.get(3)
        {
            *state.consumer.lock().expect("consumer lock") = Some(consumer.clone());
        }
        let step = state.steps.lock().expect("steps lock").pop_front();
        let reply = match step {
            Some(step) if step.command == name => step.reply,
            step => {
                let expected = step.map(|step| step.command);
                state
                    .errors
                    .lock()
                    .expect("errors lock")
                    .push(format!("expected {expected:?}, got {command:?}"));
                Reply::Bytes(b"-ERR unexpected scripted command\r\n".to_vec())
            }
        };
        match reply {
            Reply::Bytes(bytes) => reader.get_mut().write_all(&bytes).expect("write scripted response"),
            Reply::DelayedBytes(bytes, delay) => {
                sleep(delay);
                let _ = reader.get_mut().write_all(&bytes);
            }
            Reply::PendingEntry { id, deliveries } => {
                let consumer = state
                    .consumer
                    .lock()
                    .expect("consumer lock")
                    .clone()
                    .expect("XPENDING follows XAUTOCLAIM");
                let response = format!(
                    "*1\r\n*4\r\n${}\r\n{id}\r\n${}\r\n{consumer}\r\n:0\r\n:{deliveries}\r\n",
                    id.len(),
                    consumer.len()
                );
                reader
                    .get_mut()
                    .write_all(response.as_bytes())
                    .expect("write scripted XPENDING response");
            }
            Reply::Disconnect => {
                let _ = reader.get_mut().shutdown(Shutdown::Both);
                return;
            }
            Reply::DisconnectAndRefuseConnections => {
                state.refusing.store(true, Ordering::SeqCst);
                let _ = reader.get_mut().shutdown(Shutdown::Both);
                return;
            }
        }
    }
}

/// Consumes one bounded RESP2 bulk-string command from `reader`.
///
/// Returns Some(argument strings), or None for clean initial EOF. Performs
/// blocking socket reads. Returns IO errors for invalid headers/terminators,
/// argument counts outside 1..=64, bulk lengths over 64 KiB, invalid UTF-8,
/// or truncated/failed reads.
fn read_command(reader: &mut BufReader<TcpStream>) -> IoResult<Option<Vec<String>>> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(None);
    }
    let count = parse_length(&line, '*')?;
    if count == 0 || count > 64 {
        return Err(Error::other("unexpected command argument count"));
    }
    let mut command = Vec::with_capacity(count);
    for _ in 0..count {
        line.clear();
        reader.read_line(&mut line)?;
        let length = parse_length(&line, '$')?;
        if length > 64 * 1024 {
            return Err(Error::other("scripted command argument too long"));
        }
        let mut bytes = vec![0; length + 2];
        reader.read_exact(&mut bytes)?;
        if &bytes[length..] != b"\r\n" {
            return Err(Error::other("invalid RESP argument terminator"));
        }
        bytes.truncate(length);
        command.push(String::from_utf8(bytes).map_err(Error::other)?);
    }
    Ok(Some(command))
}

/// Parses an unsigned length from RESP line `line` with expected `prefix`.
///
/// Returns the length, or an IO error for a missing prefix/CRLF terminator
/// or invalid unsigned integer text. Does not perform IO itself.
fn parse_length(line: &str, prefix: char) -> IoResult<usize> {
    line.strip_prefix(prefix)
        .and_then(|value| value.strip_suffix("\r\n"))
        .ok_or_else(|| Error::other("invalid RESP length header"))?
        .parse()
        .map_err(Error::other)
}
