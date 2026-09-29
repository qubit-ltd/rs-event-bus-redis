// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! TCP forwarding proxy and its lifecycle.

use std::io::BufReader;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

use super::gate::ReplyGate;
#[path = "proxy/internal.rs"]
mod internal;

/// Forwards RESP traffic and pauses after the selected command is applied.
pub struct ControlledRedis {
    address: String,
    stop: Arc<AtomicBool>,
    gate: Arc<ReplyGate>,
    workers: Arc<Mutex<Vec<thread::JoinHandle<()>>>>,
    listener_thread: Option<thread::JoinHandle<()>>,
}

impl ControlledRedis {
    /// Starts a local proxy in front of an existing Redis endpoint.
    pub fn start(upstream: &str) -> std::io::Result<Self> {
        let connection_info = redis::Client::open(upstream)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid Redis URL"))?
            .get_connection_info()
            .clone();
        let redis::ConnectionAddr::Tcp(host, port) = connection_info.addr else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "controlled Redis proxy supports TCP Redis endpoints",
            ));
        };
        let upstream = format!("{host}:{port}");
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?.to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(ReplyGate::default());
        let thread_stop = Arc::clone(&stop);
        let thread_gate = Arc::clone(&gate);
        let workers = Arc::new(Mutex::new(Vec::new()));
        let listener_workers = Arc::clone(&workers);
        let listener_thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((client, _)) => {
                        let upstream = upstream.clone();
                        let gate = Arc::clone(&thread_gate);
                        let worker = thread::spawn(move || forward(client, &upstream, &gate));
                        if let Ok(mut workers) = listener_workers.lock() {
                            workers.push(worker);
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(Self {
            address,
            stop,
            gate,
            workers,
            listener_thread: Some(listener_thread),
        })
    }

    /// Returns the proxy URL accepted by Redis clients.
    pub fn url(&self) -> String {
        format!("redis://{}/", self.address)
    }

    /// Returns the response gate controlled by tests.
    pub fn gate(&self) -> Arc<ReplyGate> {
        Arc::clone(&self.gate)
    }

    /// Pauses the next reply for `command` after the upstream Redis server has
    /// fully applied the command.
    pub fn pause_after_reply(&self, command: &'static str) -> Arc<ReplyGate> {
        self.gate.arm_for(command);
        Arc::clone(&self.gate)
    }
}

impl Drop for ControlledRedis {
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

fn forward(client: TcpStream, upstream: &str, gate: &ReplyGate) {
    let Ok(server) = TcpStream::connect(upstream) else {
        return;
    };
    let _ = client.set_read_timeout(Some(Duration::from_secs(3)));
    let _ = server.set_read_timeout(Some(Duration::from_secs(3)));
    let Ok(client_writer) = client.try_clone() else {
        return;
    };
    let Ok(server_writer) = server.try_clone() else {
        return;
    };
    let mut client_reader = BufReader::new(client);
    let mut server_reader = BufReader::new(server);
    let mut client_writer = client_writer;
    let mut server_writer = server_writer;
    while let Ok(Some((command, request))) = internal::read_request(&mut client_reader) {
        if server_writer.write_all(&request).is_err() || server_writer.flush().is_err() {
            break;
        }
        let Ok(response) = internal::read_response(&mut server_reader) else {
            break;
        };
        if gate.hold_if_armed(&command.to_ascii_uppercase(), &request) {
            break;
        }
        if client_writer.write_all(&response).is_err() || client_writer.flush().is_err() {
            break;
        }
    }
}
