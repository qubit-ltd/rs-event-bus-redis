// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Prebuilt messages and barrier-controlled public SPI workers.

use std::error::Error;
use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use std::time::Instant;
use std::time::SystemTime;

use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;

use super::backend::Backend;
use super::measurement::Sample;
use super::workload_result::WorkloadResult;

/// Encodes exactly the requested raw byte length; wire metadata is additional.
pub fn message(topic: &str, id: &str, payload: Arc<[u8]>) -> Result<OutboundMessage, Box<dyn Error>> {
    Ok(OutboundMessage::new(
        TopicAddress::new(topic)?,
        EventId::new(id)?,
        SystemTime::now(),
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            payload,
            ContentType::new("application/octet-stream")?,
            None,
        )),
    ))
}

/// Prepares threads and messages before releasing the timed start barrier.
pub fn run_workers(
    bus: &Backend,
    prefix: &str,
    concurrency: usize,
    samples: usize,
    payload_bytes: usize,
    idle: bool,
) -> Result<WorkloadResult, Box<dyn Error>> {
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let payload = Arc::<[u8]>::from(vec![b'x'; payload_bytes]);
    let mut workers = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let topic = format!("{prefix}-worker-{worker}");
        let mut receiver = bus.subscribe(&topic, worker)?;
        let count = samples / concurrency + usize::from(worker < samples % concurrency);
        let mut messages = Vec::with_capacity(count);
        for sequence in 0..count {
            messages.push(if idle {
                None
            } else {
                Some(message(
                    &topic,
                    &format!("event-{worker}-{sequence}"),
                    Arc::clone(&payload),
                )?)
            });
        }
        let worker_bus = bus.clone();
        let worker_barrier = Arc::clone(&barrier);
        workers.push(thread::spawn(move || {
            let mut results = Vec::with_capacity(count);
            worker_barrier.wait();
            for message in messages {
                let started = Instant::now();
                let outcome = worker_bus.attempt(&mut receiver, message);
                results.push(Sample {
                    nanos: started.elapsed().as_nanos(),
                    outcome,
                });
            }
            (results, receiver)
        }));
    }
    let started = Instant::now();
    barrier.wait();
    let mut results = Vec::with_capacity(concurrency);
    let mut receivers = Vec::with_capacity(concurrency);
    for worker in workers {
        let (samples, receiver) = worker.join().map_err(|_| "benchmark worker panicked")?;
        results.push(samples);
        receivers.push(receiver);
    }
    Ok(WorkloadResult {
        elapsed: started.elapsed(),
        groups: results,
        receivers,
    })
}
