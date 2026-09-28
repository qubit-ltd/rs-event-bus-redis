// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Redis proxy that can hold a reply after the upstream command has completed.

use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

#[derive(Default)]
struct GateState {
    armed: bool,
    reached: bool,
    released: bool,
}

/// One-shot response gate for an already applied Redis command.
#[derive(Default)]
pub struct ReplyGate {
    state: Mutex<GateState>,
    changed: Condvar,
}

impl ReplyGate {
    /// Arms the next matching response.
    pub fn arm(&self) {
        let mut state = self.state.lock().expect("reply gate lock is healthy");
        state.armed = true;
        state.reached = false;
        state.released = false;
    }

    /// Waits until Redis has returned the response for the selected command.
    pub fn wait_until_reached(&self, timeout: Duration) -> bool {
        let state = self.state.lock().expect("reply gate lock is healthy");
        let (state, _) = self
            .changed
            .wait_timeout_while(state, timeout, |state| !state.reached)
            .expect("reply gate lock is healthy");
        state.reached
    }

    /// Allows the held response to continue to the client.
    pub fn release(&self) {
        let mut state = self.state.lock().expect("reply gate lock is healthy");
        state.released = true;
        self.changed.notify_all();
    }

    fn hold_if_armed(&self) {
        let mut state = self.state.lock().expect("reply gate lock is healthy");
        if !state.armed {
            return;
        }
        state.armed = false;
        state.reached = true;
        self.changed.notify_all();
        let _state = self
            .changed
            .wait_while(state, |state| !state.released)
            .expect("reply gate lock is healthy");
    }
}

/// Forwards RESP traffic and pauses after the selected command is applied.
pub struct ControlledRedis {
    address: String,
    stop: Arc<AtomicBool>,
    gate: Arc<ReplyGate>,
    listener_thread: Option<thread::JoinHandle<()>>,
}

impl ControlledRedis {
    /// Starts a local proxy in front of an existing Redis endpoint.
    pub fn start(upstream: &str) -> std::io::Result<Self> {
        let upstream = upstream
            .strip_prefix("redis://")
            .and_then(|address| address.split('/').next())
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid Redis URL"))?
            .to_owned();
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?.to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(ReplyGate::default());
        let thread_stop = Arc::clone(&stop);
        let thread_gate = Arc::clone(&gate);
        let listener_thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((client, _)) => {
                        let upstream = upstream.clone();
                        let gate = Arc::clone(&thread_gate);
                        thread::spawn(move || forward(client, &upstream, &gate));
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
}

impl Drop for ControlledRedis {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.gate.release();
        let _ = TcpStream::connect(&self.address);
        if let Some(listener_thread) = self.listener_thread.take() {
            let _ = listener_thread.join();
        }
    }
}

fn forward(client: TcpStream, upstream: &str, gate: &ReplyGate) {
    let Ok(server) = TcpStream::connect(upstream) else {
        return;
    };
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
    while let Ok(Some((command, request))) = read_request(&mut client_reader) {
        if server_writer.write_all(&request).is_err() || server_writer.flush().is_err() {
            break;
        }
        let Ok(response) = read_response(&mut server_reader) else {
            break;
        };
        if command.eq_ignore_ascii_case(b"XREADGROUP") && request.contains(&b'>') {
            gate.hold_if_armed();
        }
        if client_writer.write_all(&response).is_err() || client_writer.flush().is_err() {
            break;
        }
    }
}

fn read_request(reader: &mut BufReader<TcpStream>) -> std::io::Result<Option<(Vec<u8>, Vec<u8>)>> {
    let Some((line, mut wire)) = read_line(reader)? else {
        return Ok(None);
    };
    if line.first() != Some(&b'*') {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "expected RESP array",
        ));
    }
    let count = parse_length(&line[1..])?;
    let mut command = Vec::new();
    for index in 0..count {
        let (header, mut header_wire) = read_line(reader)?.ok_or_else(unexpected_eof)?;
        if header.first() != Some(&b'$') {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "expected RESP bulk string",
            ));
        }
        let length = usize::try_from(parse_length(&header[1..])?)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid RESP bulk length"))?;
        let mut value = vec![0; length];
        reader.read_exact(&mut value)?;
        let mut ending = [0; 2];
        reader.read_exact(&mut ending)?;
        header_wire.extend_from_slice(&value);
        header_wire.extend_from_slice(&ending);
        wire.extend_from_slice(&header_wire);
        if index == 0 {
            command = value;
        }
    }
    Ok(Some((command, wire)))
}

fn read_response(reader: &mut BufReader<TcpStream>) -> std::io::Result<Vec<u8>> {
    let (line, mut wire) = read_line(reader)?.ok_or_else(unexpected_eof)?;
    match line.first() {
        Some(b'$') => {
            let length = parse_length(&line[1..])?;
            if length >= 0 {
                let mut body = vec![0; length as usize + 2];
                reader.read_exact(&mut body)?;
                wire.extend_from_slice(&body);
            }
        }
        Some(b'*') => {
            let count = parse_length(&line[1..])?;
            if count >= 0 {
                for _ in 0..count {
                    wire.extend_from_slice(&read_response(reader)?);
                }
            }
        }
        _ => {}
    }
    Ok(wire)
}

fn read_line(reader: &mut BufReader<TcpStream>) -> std::io::Result<Option<(Vec<u8>, Vec<u8>)>> {
    let mut wire = Vec::new();
    let mut byte = [0];
    loop {
        match reader.read_exact(&mut byte) {
            Ok(()) => wire.push(byte[0]),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof && wire.is_empty() => return Ok(None),
            Err(error) => return Err(error),
        }
        if wire.ends_with(b"\r\n") {
            return Ok(Some((wire[..wire.len() - 2].to_vec(), wire)));
        }
    }
}

fn parse_length(bytes: &[u8]) -> std::io::Result<isize> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid RESP length"))
}

fn unexpected_eof() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "truncated RESP response")
}
