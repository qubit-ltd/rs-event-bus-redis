// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! TCP forwarding proxy and its lifecycle.

use std::io::BufReader;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result as IoResult;
use std::io::Write;
use std::net::Shutdown;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::thread::sleep;
use std::thread::spawn;
use std::time::Duration;
use std::time::Instant;

use redis::Client;
use redis::ConnectionAddr;

use super::gate::ReplyGate;
#[path = "proxy/internal.rs"]
mod internal;

/// Pending raw response substitution: Some holds the uppercase command name
/// and replacement RESP bytes; None preserves the upstream response.
type ReplyReplacement = Option<(Vec<u8>, Vec<u8>, bool)>;

/// A command forwarded through a proxy connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestObservation {
    /// Proxy-assigned identity of the client TCP connection.
    pub connection_id: usize,
    /// Uppercase RESP command name.
    pub command: Vec<u8>,
}

#[derive(Default)]
struct RequestLog {
    entries: Mutex<Vec<RequestObservation>>,
    changed: Condvar,
}

/// Forwards RESP traffic and pauses after the selected command is applied.
pub struct ControlledRedis {
    address: String,
    stop: Arc<AtomicBool>,
    gate: Arc<ReplyGate>,
    replacement: Arc<Mutex<ReplyReplacement>>,
    drop_before_settlement: Arc<AtomicBool>,
    workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
    requests: Arc<RequestLog>,
    next_connection_id: Arc<AtomicUsize>,
    listener_thread: Option<JoinHandle<()>>,
}

impl ControlledRedis {
    /// Starts a TCP proxy in front of the Redis URL `upstream`.
    ///
    /// Returns the owned listener and forwarding workers; socket setup performs
    /// IO. Returns an IO error for an invalid URL, unsupported address,
    /// bind failure, or nonblocking-listener configuration failure. Thread
    /// creation may panic.
    pub fn start(upstream: &str) -> IoResult<Self> {
        let connection_info = Client::open(upstream)
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "invalid Redis URL"))?
            .get_connection_info()
            .clone();
        let ConnectionAddr::Tcp(host, port) = connection_info.addr().clone() else {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "controlled Redis proxy supports TCP Redis endpoints",
            ));
        };
        let upstream = format!("{host}:{port}");
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?.to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(ReplyGate::default());
        let replacement = Arc::new(Mutex::new(None));
        let drop_before_settlement = Arc::new(AtomicBool::new(false));
        let thread_drop_before_settlement = Arc::clone(&drop_before_settlement);
        let thread_replacement = Arc::clone(&replacement);
        let thread_stop = Arc::clone(&stop);
        let thread_gate = Arc::clone(&gate);
        let workers = Arc::new(Mutex::new(Vec::new()));
        let requests = Arc::new(RequestLog::default());
        let next_connection_id = Arc::new(AtomicUsize::new(0));
        let listener_workers = Arc::clone(&workers);
        let listener_requests = Arc::clone(&requests);
        let listener_next_connection_id = Arc::clone(&next_connection_id);
        let listener_thread = spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((client, _)) => {
                        let connection_id = listener_next_connection_id.fetch_add(1, Ordering::SeqCst) + 1;
                        let upstream = upstream.clone();
                        let gate = Arc::clone(&thread_gate);
                        let replacement = Arc::clone(&thread_replacement);
                        let drop_before_settlement = Arc::clone(&thread_drop_before_settlement);
                        let requests = Arc::clone(&listener_requests);
                        let worker = spawn(move || {
                            forward(
                                client,
                                connection_id,
                                &upstream,
                                &gate,
                                &replacement,
                                drop_before_settlement,
                                requests,
                            )
                        });
                        if let Ok(mut workers) = listener_workers.lock() {
                            workers.push(worker);
                        }
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(Self {
            address,
            stop,
            gate,
            replacement,
            drop_before_settlement,
            workers,
            requests,
            next_connection_id,
            listener_thread: Some(listener_thread),
        })
    }

    /// Returns a newly allocated Redis URL for this proxy's local TCP listener.
    pub fn url(&self) -> String {
        format!("redis://{}/", self.address)
    }

    /// Returns a shared handle to the reply gate controlled by this fixture.
    pub fn gate(&self) -> Arc<ReplyGate> {
        Arc::clone(&self.gate)
    }

    /// Copies raw RESP `reply` to replace the next applied reply for `command`.
    ///
    /// The command name is normalized to uppercase. State locking may block
    /// briefly and panics if the replacement mutex is poisoned; the proxy
    /// performs the IO.
    pub fn replace_next_reply(&self, command: &str, reply: &[u8]) {
        *self.replacement.lock().expect("reply replacement lock is healthy") =
            Some((command.as_bytes().to_ascii_uppercase(), reply.to_vec(), false));
    }

    /// Copies the raw reply to replace only the next settlement EVAL response.
    pub fn replace_next_settlement_reply(&self, reply: &[u8]) {
        *self.replacement.lock().expect("replacement mutex is healthy") =
            Some((b"EVAL".to_vec(), reply.to_vec(), true));
    }

    /// Drops the next settlement EVAL connection before forwarding it to Redis.
    pub fn drop_next_settlement_before_forward(&self) {
        self.drop_before_settlement.store(true, Ordering::SeqCst);
    }

    /// Arms the next applied reply for `command` and returns a shared gate
    /// handle.
    ///
    /// The static command name is copied; no network IO is performed here.
    /// State locking may block briefly and panics if the gate mutex is
    /// poisoned.
    pub fn pause_after_reply(&self, command: &'static str) -> Arc<ReplyGate> {
        self.gate.arm_for(command);
        Arc::clone(&self.gate)
    }

    /// Arms the next settlement EVAL reply, matched by command shape and script
    /// identity.
    pub fn pause_after_settlement_reply(&self) -> Arc<ReplyGate> {
        self.gate.arm_for_settlement();
        Arc::clone(&self.gate)
    }

    /// Returns commands forwarded upstream with their proxy connection IDs.
    pub fn request_observations(&self) -> Vec<RequestObservation> {
        self.requests
            .entries
            .lock()
            .expect("request log lock is healthy")
            .clone()
    }

    /// Waits for at least `count` forwarded `command` requests up to `timeout`.
    pub fn wait_for_requests(&self, command: &'static str, count: usize, timeout: Duration) -> Vec<RequestObservation> {
        let deadline = Instant::now() + timeout;
        let mut entries = self.requests.entries.lock().expect("request log lock is healthy");
        loop {
            if entries
                .iter()
                .filter(|entry| entry.command.eq_ignore_ascii_case(command.as_bytes()))
                .count()
                >= count
            {
                return entries.clone();
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return entries.clone();
            }
            let (next, wait) = self
                .requests
                .changed
                .wait_timeout(entries, remaining)
                .expect("request log lock is healthy");
            entries = next;
            if wait.timed_out() {
                return entries.clone();
            }
        }
    }

    /// Returns the number of downstream client connections accepted so far.
    pub fn connection_count(&self) -> usize {
        self.next_connection_id.load(Ordering::SeqCst)
    }
}

impl Drop for ControlledRedis {
    /// Unblocks the gate, stops the listener, and joins owned forwarding
    /// threads.
    ///
    /// Performs socket IO and blocks until the threads finish; a poisoned gate
    /// mutex may panic while releasing the gate. Join errors are
    /// deliberately ignored.
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.gate.release();
        let _ = TcpStream::connect(&self.address);
        if let Some(listener_thread) = self.listener_thread.take() {
            let _ = listener_thread.join();
        }
        if let Ok(mut workers) = self.workers.lock() {
            for worker in workers.drain(..) {
                let _ = worker.join();
            }
        }
    }
}

/// Relays the owned `client` connection to the TCP address `upstream`.
///
/// `gate` controls applied replies and `replacement` supplies one raw response.
/// Performs blocking socket IO and exits without a result on transport failure
/// or a discarded reply. Panics if the replacement mutex is poisoned.
fn forward(
    client: TcpStream,
    connection_id: usize,
    upstream: &str,
    gate: &ReplyGate,
    replacement: &Mutex<ReplyReplacement>,
    drop_before_settlement: Arc<AtomicBool>,
    requests: Arc<RequestLog>,
) {
    let Ok(server) = TcpStream::connect(upstream) else {
        return;
    };
    let _ = client.set_read_timeout(Some(Duration::from_secs(3)));
    let _ = server.set_read_timeout(Some(Duration::from_secs(3)));
    let Ok(client_reader_stream) = client.try_clone() else {
        return;
    };
    let Ok(client_writer) = client.try_clone() else {
        return;
    };
    let Ok(server_writer) = server.try_clone() else {
        return;
    };
    let Ok(server_reader_stream) = server.try_clone() else {
        return;
    };
    let mut client_reader = BufReader::new(client_reader_stream);
    let mut server_reader = BufReader::new(server_reader_stream);
    let mut client_writer = client_writer;
    let mut server_writer = server_writer;
    let (sent, received) = std::sync::mpsc::channel::<(Vec<u8>, Vec<u8>)>();
    let request_thread = spawn(move || {
        while let Ok(Some((command, request))) = internal::read_request(&mut client_reader) {
            if command.eq_ignore_ascii_case(b"EVAL")
                && super::gate::is_settlement_eval(&request)
                && drop_before_settlement.swap(false, Ordering::SeqCst)
            {
                break;
            }
            if server_writer.write_all(&request).is_err() || server_writer.flush().is_err() {
                break;
            }
            requests
                .entries
                .lock()
                .expect("request log lock is healthy")
                .push(RequestObservation {
                    connection_id,
                    command: command.to_ascii_uppercase(),
                });
            requests.changed.notify_all();
            if sent.send((command, request)).is_err() {
                break;
            }
        }
    });
    while let Ok(mut response) = internal::read_response(&mut server_reader) {
        let Ok((command, request)) = received.recv() else {
            break;
        };
        if gate.hold_if_armed(&command.to_ascii_uppercase(), &request) {
            break;
        }
        {
            let mut replacement = replacement.lock().expect("replacement mutex is healthy");
            if replacement.as_ref().is_some_and(|(name, _, settlement_only)| {
                name.eq_ignore_ascii_case(&command) && (!settlement_only || super::gate::is_settlement_eval(&request))
            }) {
                let (_, bytes, _) = replacement.take().expect("matching replacement exists");
                response = bytes;
            }
        }
        if client_writer.write_all(&response).is_err() || client_writer.flush().is_err() {
            break;
        }
    }
    let _ = client_writer.shutdown(Shutdown::Both);
    let _ = request_thread.join();
}
