// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Historical manual connection-churn and end-to-end latency workload.
//!
//! This 50ms receive polling workload retains the original before/after
//! evidence. The sampled 1ms workloads in `benches/redis_workloads.rs` use
//! different traffic and sampling parameters, so their measurements are not
//! equivalent.

#![cfg(feature = "sync")]

mod support;

use std::any::TypeId;
use std::env::var;
use std::error::Error;
use std::process::id;
use std::sync::Arc;
use std::thread::spawn;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_event_bus_redis::sync::RedisEventBusProvider;
use qubit_id::Id;
use qubit_spi::ServiceProvider;
use redis::Client;
use redis::Connection;
use redis::cmd;
use support::redis_server::RedisServer;

const IDLE_CONSUMERS: usize = 10;
const MESSAGES: usize = 1_000;

/// Measures idle receive connection churn for 30 seconds, followed by 1,000
/// synchronous publish/receive/settle round trips.
#[test]
#[ignore = "manual before/after connection benchmark; run with --ignored --nocapture"]
fn test_redis_connection_reuse_benchmark() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let namespace = format!("connection-benchmark-{}", id());
    let options: ProviderOptions = [
        ("redis.url".into(), server.url().into()),
        ("redis.namespace".into(), namespace.as_str().into()),
        ("redis.max_unsettled_per_subscription".into(), "1".into()),
    ]
    .into();
    let bus = RedisEventBusProvider.create_configured(&EventBusConfig::default().with_provider_options(options))?;

    let mut idle_receivers = Vec::with_capacity(IDLE_CONSUMERS);
    for index in 0..IDLE_CONSUMERS {
        idle_receivers.push(bus.subscribe(request(
            "idle-events",
            &format!("idle-worker-{index}"),
            &format!("idle-group-{index}"),
        )?)?);
    }
    let mut observer = Client::open(server.url())?.get_connection()?;
    let idle_connections_before = total_connections(&mut observer)?;
    let idle_seconds = var("REDIS_BENCH_IDLE_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30);
    let stop_at = Instant::now() + Duration::from_secs(idle_seconds);
    let started = Instant::now();
    let workers = idle_receivers
        .into_iter()
        .map(|mut receiver| {
            spawn(move || -> Result<(), String> {
                while Instant::now() < stop_at {
                    match receiver.receive(Duration::from_millis(50)) {
                        Ok(ReceiveOutcome::TimedOut | ReceiveOutcome::Gap(_)) => {}
                        Ok(_) => {
                            return Err("idle subscriber unexpectedly received an event".into());
                        }
                        Err(error) => return Err(error.to_string()),
                    }
                }
                receiver.close().map_err(|error| error.to_string())
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().map_err(|_| "idle benchmark worker panicked")??;
    }
    let idle_elapsed = started.elapsed();
    let idle_connections_after = total_connections(&mut observer)?;
    let idle_connection_delta = idle_connections_after - idle_connections_before;
    println!(
        "idle,consumers={IDLE_CONSUMERS},seconds={:.3},connections={idle_connection_delta},connections_per_second={:.2}",
        idle_elapsed.as_secs_f64(),
        idle_connection_delta as f64 / idle_elapsed.as_secs_f64()
    );

    let mut receiver = bus.subscribe(request("message-events", "message-worker", "message-group")?)?;
    let mut latencies = Vec::with_capacity(MESSAGES);
    let started = Instant::now();
    for index in 0..MESSAGES {
        let message_id = format!("message-{index}");
        let sent = Instant::now();
        bus.publish(message(&message_id)?)?;
        let ReceiveOutcome::Message(delivery) = receiver.receive(Duration::from_secs(3))? else {
            return Err(format!("message {message_id} was not received").into());
        };
        let token = delivery.settlement().ok_or("delivery omitted settlement token")?;
        receiver.settle(token, DeliveryDisposition::Accept)?;
        latencies.push(sent.elapsed());
    }
    let elapsed = started.elapsed();
    latencies.sort_unstable();
    let p50 = percentile(&latencies, 50);
    let p95 = percentile(&latencies, 95);
    println!(
        "round_trip,messages={MESSAGES},seconds={:.3},messages_per_second={:.2},p50_us={},p95_us={}",
        elapsed.as_secs_f64(),
        MESSAGES as f64 / elapsed.as_secs_f64(),
        p50.as_micros(),
        p95.as_micros()
    );
    receiver.close()?;
    Ok(())
}

/// Builds a durable new-position receiver for one historical workload identity.
fn request(topic: &str, subscriber: &str, group: &str) -> Result<SpiSubscriptionRequest, Box<dyn Error>> {
    Ok(SpiSubscriptionRequest::new(
        Id::new(u64::from(subscriber.as_bytes()[0])),
        TopicAddress::new(topic)?,
        SubscriberId::new(subscriber)?,
        Some(ConsumerGroup::new(group)?),
        SubscriptionDurability::Durable,
        StartPosition::New,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    ))
}

/// Builds the small encoded payload used by the historical round-trip loop.
fn message(id: &str) -> Result<OutboundMessage, Box<dyn Error>> {
    Ok(OutboundMessage::new(
        TopicAddress::new("message-events")?,
        EventId::new(id)?,
        SystemTime::now(),
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::<[u8]>::from(&b"small-payload"[..]),
            ContentType::new("application/octet-stream")?,
            None,
        )),
    ))
}

/// Reads the cumulative connection counter from Redis INFO stats.
fn total_connections(connection: &mut Connection) -> Result<u64, Box<dyn Error>> {
    let info: String = cmd("INFO").arg("stats").query(connection)?;
    let value = info
        .lines()
        .find_map(|line| line.strip_prefix("total_connections_received:"))
        .ok_or("Redis INFO stats omitted total_connections_received")?;
    Ok(value.parse()?)
}

/// Selects the nearest-rank percentile from the nonempty sorted durations.
fn percentile(values: &[Duration], percent: usize) -> Duration {
    let rank = values.len().saturating_mul(percent).div_ceil(100).max(1);
    values[rank - 1]
}
