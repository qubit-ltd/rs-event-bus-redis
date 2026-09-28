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
use std::io::Read;
use std::io::Write;
use std::net::Shutdown;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::time::Duration;

/// One command's response, including loss of the connection before a reply.
pub enum Reply {
    Bytes(Vec<u8>),
    Disconnect,
    DisconnectAndRefuseConnections,
}

/// Command names are checked in order across all connections to the endpoint.
pub struct Step {
    command: &'static str,
    reply: Reply,
}

impl Step {
    /// Expects `command` and writes the supplied raw RESP `reply`.
    pub fn reply(command: &'static str, reply: &[u8]) -> Self {
        Self {
            command,
            reply: Reply::Bytes(reply.to_vec()),
        }
    }

    /// Expects `command`, then closes its connection without writing a reply.
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
    /// Starts a local endpoint for `steps`; returns an error on socket failure.
    pub fn start(steps: Vec<Step>) -> std::io::Result<Self> {
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
        });
        let shared = Arc::clone(&state);
        let worker = std::thread::spawn(move || {
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
                        workers.push(std::thread::spawn(move || serve_connection(stream, &state)));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
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

    /// Returns the endpoint URL for configuring the real Redis provider.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Asserts that every step matched and returns the observed command args.
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
    /// Stops the listener, unblocks socket reads, and joins its worker threads.
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

/// Reads requests and executes the shared script until the peer disconnects.
fn serve_connection(stream: TcpStream, state: &State) {
    let mut reader = BufReader::new(stream);
    while !state.stopping.load(Ordering::SeqCst) {
        let command = match read_command(&mut reader) {
            Ok(Some(command)) => command,
            Ok(None) => return,
            Err(error) => {
                if !state.stopping.load(Ordering::SeqCst) {
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

/// Parses one bounded RESP2 bulk-string command; EOF means a closed client.
fn read_command(reader: &mut BufReader<TcpStream>) -> std::io::Result<Option<Vec<String>>> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(None);
    }
    let count = parse_length(&line, '*')?;
    if count == 0 || count > 64 {
        return Err(std::io::Error::other("unexpected command argument count"));
    }
    let mut command = Vec::with_capacity(count);
    for _ in 0..count {
        line.clear();
        reader.read_line(&mut line)?;
        let length = parse_length(&line, '$')?;
        if length > 64 * 1024 {
            return Err(std::io::Error::other("scripted command argument too long"));
        }
        let mut bytes = vec![0; length + 2];
        reader.read_exact(&mut bytes)?;
        if &bytes[length..] != b"\r\n" {
            return Err(std::io::Error::other("invalid RESP argument terminator"));
        }
        bytes.truncate(length);
        command.push(String::from_utf8(bytes).map_err(std::io::Error::other)?);
    }
    Ok(Some(command))
}

/// Parses the length of an array or bulk string, rejecting invalid RESP input.
fn parse_length(line: &str, prefix: char) -> std::io::Result<usize> {
    line.strip_prefix(prefix)
        .and_then(|value| value.strip_suffix("\r\n"))
        .ok_or_else(|| std::io::Error::other("invalid RESP length header"))?
        .parse()
        .map_err(std::io::Error::other)
}
