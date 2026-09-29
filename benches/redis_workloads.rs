// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Reproducible public-API Redis workload benchmark with raw CSV evidence.

mod support;

use std::env;
use std::error::Error;
use std::fs::write;
use std::path::PathBuf;
use std::process::id;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use qubit_event_bus::SpiError;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::ProviderOptions;
use redis::Client;
use redis::cmd;
use support::backend::Backend;
use support::measurement::ActiveMonitor;
use support::measurement::Evidence;
use support::measurement::Sample;
use support::measurement::Snapshot;
use support::redis_fixture::RedisNode;
use support::redis_fixture::SentinelFixture;
use support::workload::message;
use support::workload::run_workers;

/// Runs the complete default matrix; filters allow focused diagnostics.
fn main() -> Result<(), Box<dyn Error>> {
    let samples = number("REDIS_BENCH_SAMPLES", 1000);
    let rounds = number("REDIS_BENCH_ROUNDS", 3);
    let first_round = number("REDIS_BENCH_FIRST_ROUND", 1);
    let label = env::var("REDIS_BENCH_LABEL").unwrap_or_else(|_| "after".into());
    let output =
        PathBuf::from(env::var("REDIS_BENCH_OUTPUT").unwrap_or_else(|_| "/tmp/redis-workload-benchmark".into()));
    let mut evidence = Evidence::new(&output, &label)?;
    let external = env::var("REDIS_BENCH_URL").ok();
    let fixture = if external.is_none() {
        Some(RedisNode::start(None, None)?)
    } else {
        None
    };
    let url = external
        .clone()
        .unwrap_or_else(|| fixture.as_ref().expect("owned standalone fixture").url());
    let info: String = {
        let mut observer = Client::open(url.as_str())?.get_connection()?;
        cmd("INFO").arg("server").query(&mut observer)?
    };
    write(output.join(format!("{label}-redis-server.txt")), info)?;
    for mode in ["sync", "async"] {
        if !selected("REDIS_BENCH_MODES", mode) {
            continue;
        }
        if selected("REDIS_BENCH_SCENARIOS", "round_trip") {
            for limits in ["default", "limited"] {
                if !selected("REDIS_BENCH_LIMITS", limits) {
                    continue;
                }
                // The old provider rejects the new limit options; compare defaults only.
                if label == "before" && limits == "limited" {
                    continue;
                }
                for payload in [64, 4096, 262144] {
                    if !selected("REDIS_BENCH_PAYLOADS", &payload.to_string()) {
                        continue;
                    }
                    for concurrency in [1, 8, 32] {
                        if !selected("REDIS_BENCH_CONCURRENCY", &concurrency.to_string()) {
                            continue;
                        }
                        for round in first_round..first_round + rounds {
                            let namespace = format!("bench-{}-{mode}-{limits}-{payload}-{concurrency}-{round}", id());
                            let mut options = options(&url, &namespace);
                            if limits == "limited" {
                                options.insert("redis.max_concurrent_commands".into(), "32".into());
                                options.insert("redis.max_idle_connections".into(), "1".into());
                            }
                            let bus = Backend::new(mode, options)?;
                            measure(
                                &mut evidence,
                                &label,
                                "round_trip",
                                mode,
                                limits,
                                payload,
                                concurrency,
                                round,
                                samples,
                                &url,
                                &bus,
                            )?;
                            drop(bus);
                            if fixture.is_some() {
                                clear_owned(&url)?;
                            }
                        }
                    }
                }
            }
        }
        if selected("REDIS_BENCH_SCENARIOS", "idle") {
            for concurrency in [10, 100] {
                if !selected("REDIS_BENCH_CONCURRENCY", &concurrency.to_string()) {
                    continue;
                }
                for round in first_round..first_round + rounds {
                    let bus = Backend::new(
                        mode,
                        options(&url, &format!("bench-idle-{}-{mode}-{concurrency}-{round}", id())),
                    )?;
                    measure(
                        &mut evidence,
                        &label,
                        "idle",
                        mode,
                        "default",
                        0,
                        concurrency,
                        round,
                        samples,
                        &url,
                        &bus,
                    )?;
                    drop(bus);
                    if fixture.is_some() {
                        clear_owned(&url)?;
                    }
                }
            }
        }
        if label != "before" && selected("REDIS_BENCH_SCENARIOS", "command_limit") {
            for round in first_round..first_round + rounds {
                let mut settings = options(&url, &format!("bench-command-limit-{}-{mode}-{round}", id()));
                settings.insert("redis.max_concurrent_commands".into(), "1".into());
                settings.insert("redis.max_idle_connections".into(), "1".into());
                let bus = Backend::new(mode, settings)?;
                measure(
                    &mut evidence,
                    &label,
                    "command_limit",
                    mode,
                    "command_1",
                    64,
                    32,
                    round,
                    samples,
                    &url,
                    &bus,
                )?;
                drop(bus);
                if fixture.is_some() {
                    clear_owned(&url)?;
                }
            }
        }
        if label != "before" && selected("REDIS_BENCH_SCENARIOS", "receiver_limit") {
            for round in first_round..first_round + rounds {
                receiver_limit(&mut evidence, &label, mode, round, samples, &url)?;
            }
        }
        if selected("REDIS_BENCH_SCENARIOS", "sentinel") {
            for round in first_round..first_round + rounds {
                sentinel(&mut evidence, &label, mode, round, samples)?;
            }
        }
    }
    Ok(())
}

/// Records one measured round, keeping setup, snapshots, and teardown untimed.
#[allow(clippy::too_many_arguments)]
fn measure(
    evidence: &mut Evidence,
    label: &str,
    scenario: &str,
    mode: &str,
    limits: &str,
    payload: usize,
    concurrency: usize,
    round: usize,
    samples: usize,
    url: &str,
    bus: &Backend,
) -> Result<(), Box<dyn Error>> {
    let mut observer = Client::open(url)?.get_connection()?;
    let monitor = ActiveMonitor::start(url)?;
    let before = Snapshot::read(&mut observer)?;
    let mut result = run_workers(
        bus,
        &format!("{scenario}-{round}"),
        concurrency,
        samples,
        payload,
        scenario == "idle",
    )?;
    let after = Snapshot::read(&mut observer)?;
    let peak = monitor.finish();
    evidence.record(
        label,
        scenario,
        mode,
        limits,
        payload,
        concurrency,
        round,
        result.elapsed,
        &result.groups,
        before,
        after,
        peak,
    )?;
    for receiver in &mut result.receivers {
        receiver.close()?;
    }
    Ok(())
}

/// Measures one provider/client instance before/after a real master failure.
fn sentinel(
    evidence: &mut Evidence,
    label: &str,
    mode: &str,
    round: usize,
    samples: usize,
) -> Result<(), Box<dyn Error>> {
    let mut fixture = SentinelFixture::start()?;
    let mut settings = options(
        &fixture.master.url(),
        &format!("bench-sentinel-{}-{mode}-{round}", id()),
    );
    settings.insert("redis.sentinel.nodes".into(), fixture.endpoints());
    settings.insert("redis.sentinel.service_name".into(), "benchmaster".into());
    let bus = Backend::new(mode, settings)?;
    measure(
        evidence,
        label,
        "sentinel_publish",
        mode,
        "default",
        64,
        1,
        round,
        samples,
        &fixture.master.url(),
        &bus,
    )?;
    // Prepare both observer connections before stopping the master. Their WAIT
    // is deliberately absent: observer sockets do not fence provider writes.
    let mut replica_observer = Client::open(fixture.replica.url())?.get_connection()?;
    let monitor = ActiveMonitor::start(&fixture.replica.url())?;
    let before = Snapshot::read(&mut replica_observer)?;
    let mut receiver = bus.subscribe(&format!("recovery-{round}"), 0)?;
    let (target_offset, replica_offset) = fixture.wait_replicated(&mut replica_observer)?;
    println!(
        "sentinel_replicated,round={round},master_port={},replica_port={},target_offset={target_offset},replica_offset={replica_offset}",
        fixture.master.port, fixture.replica.port
    );
    let started = Instant::now();
    fixture.master.stop()?;
    fixture.wait_promoted(&mut replica_observer)?;
    // Promotion closes normal clients, including the peak observer. Preserve
    // its sampled maximum and resume sampling with a new post-promotion client.
    let pre_promotion_peak = monitor.finish();
    let monitor = ActiveMonitor::start(&fixture.replica.url())?;
    println!("fixture_observer_reconnect,observer=peak_sampler,after_promotion=true");
    let mut attempts = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    let event_id = EventId::new(format!("recovery-{round}"))?;
    let publication_started = Instant::now();
    let publication = bus.publish(message(
        &format!("recovery-{round}"),
        event_id.as_str(),
        Arc::from(&b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"[..]),
    )?);
    if publication != "ok" {
        attempts.push(Sample {
            nanos: publication_started.elapsed().as_nanos(),
            outcome: publication,
        });
    } else {
        loop {
            let attempt_started = Instant::now();
            let result = receiver.receive_accept(&event_id);
            let succeeded = result == "ok";
            let terminal = result.starts_with("settle:") || result == "event_id_mismatch" || result == "missing_token";
            attempts.push(Sample {
                nanos: attempt_started.elapsed().as_nanos(),
                outcome: result,
            });
            if succeeded || terminal || Instant::now() >= deadline {
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
    }
    let after = Snapshot::read(&mut replica_observer)?;
    let elapsed = started.elapsed();
    let peak = pre_promotion_peak.max(monitor.finish());
    evidence.record(
        label,
        "sentinel_recovery",
        mode,
        "default",
        64,
        1,
        round,
        elapsed,
        &[attempts],
        before,
        after,
        peak,
    )?;
    receiver.close()?;
    measure(
        evidence,
        label,
        "sentinel_post_recovery",
        mode,
        "default",
        64,
        1,
        round,
        samples,
        &fixture.replica.url(),
        &bus,
    )?;
    Ok(())
}

/// Builds identical public provider options across revisions.
fn options(url: &str, namespace: &str) -> ProviderOptions {
    [
        ("redis.url".into(), url.into()),
        ("redis.namespace".into(), namespace.into()),
    ]
    .into()
}

/// Reads an optional list filter; absent filters select the complete matrix.
fn selected(variable: &str, value: &str) -> bool {
    env::var(variable).map_or(true, |filter| filter.split(',').any(|item| item == value))
}

/// Reads a positive run size with a documented default.
fn number(variable: &str, default: usize) -> usize {
    env::var(variable)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

/// Measures deterministic receiver-admission fail-fast with one held lease.
fn receiver_limit(
    evidence: &mut Evidence,
    label: &str,
    mode: &str,
    round: usize,
    samples: usize,
    url: &str,
) -> Result<(), Box<dyn Error>> {
    let mut settings = options(url, &format!("bench-limit-{}-{mode}-{round}", id()));
    settings.insert("redis.max_active_receivers".into(), "1".into());
    let bus = Backend::new(mode, settings)?;
    let mut held = bus.subscribe("held", 0)?;
    let mut observer = Client::open(url)?.get_connection()?;
    let monitor = ActiveMonitor::start(url)?;
    let before = Snapshot::read(&mut observer)?;
    let mut results = Vec::with_capacity(samples);
    let started = Instant::now();
    for index in 0..samples {
        let sent = Instant::now();
        let outcome = match bus.subscribe("denied", index + 1) {
            Ok(mut receiver) => {
                receiver.close()?;
                "unexpected_admission".into()
            }
            Err(error) => error
                .downcast_ref::<SpiError>()
                .map_or_else(|| "unclassified_error".into(), |error| error.kind().into()),
        };
        results.push(Sample {
            nanos: sent.elapsed().as_nanos(),
            outcome,
        });
    }
    let elapsed = started.elapsed();
    let after = Snapshot::read(&mut observer)?;
    let peak = monitor.finish();
    evidence.record(
        label,
        "receiver_limit",
        mode,
        "receiver_1",
        0,
        1,
        round,
        elapsed,
        &[results],
        before,
        after,
        peak,
    )?;
    held.close()?;
    Ok(())
}

/// Clears only a fixture-owned database outside the measured region.
fn clear_owned(url: &str) -> Result<(), Box<dyn Error>> {
    let mut observer = Client::open(url)?.get_connection()?;
    cmd("FLUSHDB").query::<()>(&mut observer)?;
    Ok(())
}
