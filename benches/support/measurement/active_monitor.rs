// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Sampled active-client maxima outside event worker timing.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use redis::Client;

use super::snapshot::Snapshot;

/// Samples peak active clients independently of the timed event workers.
pub struct ActiveMonitor {
    stop: Arc<AtomicBool>,
    peak: Arc<AtomicU64>,
    worker: Option<JoinHandle<()>>,
}

impl ActiveMonitor {
    /// Connects the observer before the measured counter baseline.
    pub fn start(url: &str) -> Result<Self, Box<dyn Error>> {
        let mut connection = Client::open(url)?.get_connection()?;
        let stop = Arc::new(AtomicBool::new(false));
        let peak = Arc::new(AtomicU64::new(Snapshot::read(&mut connection)?.active));
        let worker_stop = Arc::clone(&stop);
        let worker_peak = Arc::clone(&peak);
        let worker = thread::spawn(move || {
            while !worker_stop.load(Ordering::Relaxed) {
                if let Ok(snapshot) = Snapshot::read(&mut connection) {
                    worker_peak.fetch_max(snapshot.active, Ordering::Relaxed);
                }
                thread::sleep(Duration::from_millis(5));
            }
        });
        Ok(Self {
            stop,
            peak,
            worker: Some(worker),
        })
    }

    /// Joins the observer and returns the sampled active-client maximum.
    pub fn finish(mut self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        self.peak.load(Ordering::Relaxed)
    }
}
