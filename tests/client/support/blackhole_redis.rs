// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Local TCP endpoint that withholds setup or application replies.

use std::io::BufRead;
use std::io::BufReader;
use std::io::Error as IoError;
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

/// Owns bounded TCP workers; drop unblocks every socket and joins them.
pub struct BlackholeRedis {
    url: String,
    stop: Arc<AtomicBool>,
    sockets: Arc<Mutex<Vec<TcpStream>>>,
    #[cfg(feature = "async")]
    accepted: Arc<AtomicUsize>,
    commands: Arc<AtomicUsize>,
    worker: Option<JoinHandle<()>>,
    #[cfg(feature = "async")]
    setup_reply: Arc<AtomicBool>,
}
impl BlackholeRedis {
    /// Starts blocking TCP workers; `setup_reply` enables setup responses and
    /// `reply` supplies XADD's raw response, or None to withhold it. Returns
    /// the endpoint and panics on fixture bind, socket setup, or worker
    /// failures.
    pub fn start(setup_reply: bool, reply: Option<&'static [u8]>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind TCP fixture");
        let url = format!(
            "redis://{}/",
            listener.local_addr().expect("listener address")
        );
        listener.set_nonblocking(true).expect("nonblocking accept");
        let stop = Arc::new(AtomicBool::new(false));
        let sockets = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(AtomicUsize::new(0));
        let commands = Arc::new(AtomicUsize::new(0));
        let setup_reply = Arc::new(AtomicBool::new(setup_reply));
        let thread_setup_reply = Arc::clone(&setup_reply);
        let thread_stop = Arc::clone(&stop);
        let thread_sockets = Arc::clone(&sockets);
        let thread_accepted = Arc::clone(&accepted);
        let thread_commands = Arc::clone(&commands);
        let worker = spawn(move || {
            let mut workers = Vec::new();
            while !thread_stop.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        stream
                            .set_read_timeout(Some(Duration::from_secs(3)))
                            .expect("fixture read timeout");
                        thread_sockets
                            .lock()
                            .expect("socket registry")
                            .push(stream.try_clone().expect("clone socket"));
                        thread_accepted.fetch_add(1, Ordering::SeqCst);
                        let commands = Arc::clone(&thread_commands);
                        let setup_reply = Arc::clone(&thread_setup_reply);
                        workers.push(spawn(move || {
                            let mut reader = BufReader::new(stream);
                            while let Ok(Some(command)) = read_command(&mut reader) {
                                if command == "CLIENT" || command == "AUTH" || command == "SELECT" {
                                    if setup_reply.load(Ordering::SeqCst) {
                                        let _ = reader.get_mut().write_all(b"+OK\r\n");
                                    }
                                } else {
                                    commands.fetch_add(1, Ordering::SeqCst);
                                    if let Some(reply) = reply {
                                        let _ = reader.get_mut().write_all(reply);
                                    }
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
                worker.join().expect("fixture socket worker");
            }
        });
        Self {
            url,
            stop,
            sockets,
            #[cfg(feature = "async")]
            accepted,
            commands,
            worker: Some(worker),
            #[cfg(feature = "async")]
            setup_reply,
        }
    }
    /// Enables setup replies for later requests and subsequent connections.
    #[cfg(feature = "async")]
    pub fn enable_setup_replies(&self) {
        self.setup_reply.store(true, Ordering::SeqCst);
    }
    /// Returns the local endpoint URL.
    pub fn url(&self) -> &str {
        &self.url
    }
    /// Returns accepted socket count, excluding no observer connections.
    #[cfg(feature = "async")]
    pub fn connections(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }
    /// Returns application command count.
    pub fn commands(&self) -> usize {
        self.commands.load(Ordering::SeqCst)
    }
}
impl Drop for BlackholeRedis {
    /// Unblocks fixture workers before joining the listener thread.
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        for stream in self.sockets.lock().expect("socket registry").iter() {
            let _ = stream.shutdown(Shutdown::Both);
        }
        if let Some(worker) = self.worker.take() {
            worker.join().expect("fixture listener worker");
        }
    }
}
/// Reads one bounded RESP command from `reader` using blocking socket I/O.
/// Returns Some(first argument), None on initial EOF, or malformed RESP,
/// invalid UTF-8, excessive argument sizes/counts, and socket read errors.
fn read_command(reader: &mut BufReader<TcpStream>) -> IoResult<Option<String>> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(None);
    }
    let count: usize = line
        .trim()
        .strip_prefix('*')
        .ok_or_else(|| IoError::other("array expected"))?
        .parse()
        .map_err(IoError::other)?;
    if count == 0 || count > 64 {
        return Err(IoError::other("invalid argument count"));
    }
    let mut name = String::new();
    for index in 0..count {
        line.clear();
        reader.read_line(&mut line)?;
        let len: usize = line
            .trim()
            .strip_prefix('$')
            .ok_or_else(|| IoError::other("bulk expected"))?
            .parse()
            .map_err(IoError::other)?;
        if len > 1024 * 1024 {
            return Err(IoError::other("argument too large"));
        }
        let mut bytes = vec![0; len + 2];
        reader.read_exact(&mut bytes)?;
        if index == 0 {
            name = String::from_utf8(bytes[..len].to_vec()).map_err(IoError::other)?;
        }
    }
    Ok(Some(name))
}
