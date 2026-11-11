//! Sentinel cost matrix built on the existing public-SPI benchmark workers.

use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus_redis::diagnostics::RedisProviderDiagnostics;

use super::measurement::{Config, Sample, summarize};

static IDS: AtomicU64 = AtomicU64::new(1);

pub fn run_matrix() -> Result<(), Box<dyn Error>> {
    let config = Config::from_env()?;
    fs::create_dir_all(&config.output)?;
    let mut raw = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(config.output.join("sentinel-cost-samples.csv"))?;
    let mut summary = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(config.output.join("sentinel-cost-summary.csv"))?;
    writeln!(
        raw,
        "mode,transport,concurrency,scenario,round,sample,latency_ns,outcome"
    )?;
    writeln!(
        summary,
        "mode,transport,concurrency,scenario,round,attempts,successes,errors,unknown,p50_ns,p95_ns,p99_ns,aggregate_success_per_wall_second,provider_connection_attempts,provider_connection_failures,provider_role_probes_estimate,sentinel_connections,redis_server_role_calls_mixed,handshake_ns"
    )?;
    for mode in ["sync", "async"] {
        for transport in ["plaintext", "tls"] {
            for concurrency in [1, 8] {
                for scenario in ["publish_only", "settle_only", "round_trip"] {
                    for round in 1..=config.rounds {
                        run_round(
                            &config,
                            &mut raw,
                            &mut summary,
                            mode,
                            transport,
                            concurrency,
                            scenario,
                            round,
                        )?;
                    }
                }
            }
        }
    }
    raw.flush()?;
    summary.flush()?;
    let mut manifest = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(config.output.join("manifest.json"))?;
    let host = command("uname", &["-a"]);
    let rustc = command("rustc", &["--version"]);
    let cargo = command("cargo", &["--version"]);
    let redis = command("redis-server", &["--version"]);
    let head = command("git", &["rev-parse", "HEAD"]);
    let lock_hash = command("sha256sum", &["Cargo.lock"]);
    let dirty_hash = dirty_content_hash()?;
    let cpu = fs::read_to_string("/proc/cpuinfo")
        .unwrap_or_default()
        .lines()
        .find(|line| line.starts_with("model name"))
        .unwrap_or("unavailable")
        .replace('"', "'");
    let load = fs::read_to_string("/proc/loadavg")
        .unwrap_or_else(|_| "unavailable".into())
        .trim()
        .to_owned();
    writeln!(
        manifest,
        "{{\"samples_per_cell\":{},\"rounds\":{},\"payload_bytes\":64,\"warmups_per_cell\":100,\"modes\":[\"sync\",\"async\"],\"transports\":[\"plaintext\",\"tls\"],\"concurrency\":[1,8],\"scenario_coverage\":[\"publish_only\",\"settle_only\",\"round_trip\"],\"source_head\":{},\"dirty_content_sha256\":{},\"cargo_lock_sha256\":{},\"rustc\":{},\"cargo\":{},\"redis_server\":{},\"host\":{},\"cpu\":{},\"load_average_at_manifest\":{},\"tls_mode\":\"TLS Redis master with plaintext Sentinel; transparent proxy counts provider Sentinel sockets without decrypting\",\"handshake\":\"not separately measured\",\"provider_connection_metric\":\"namespace-scoped logical provider open attempts and failures; one attempt may query multiple Sentinel endpoints and does not mean one TCP open\",\"provider_role_metric\":\"attributable estimate emitted only for zero-failure cells, where successful provider open attempts imply one validated ROLE call each; failed cells leave it unavailable\",\"raw_server_role_metric\":\"master INFO commandstats ROLE delta is retained separately but mixes provider, Sentinel, and background calls; not used for provider attribution\",\"observer_policy\":\"Sentinel proxy counters exclude observer and background sockets\"}}",
        config.samples,
        config.rounds,
        json(&head),
        json(&dirty_hash),
        json(&lock_hash),
        json(&rustc),
        json(&cargo),
        json(&redis),
        json(&host),
        json(&cpu),
        json(&load)
    )?;
    Ok(())
}

fn command(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_else(|| "unavailable".into())
}

fn json(value: &str) -> String {
    format!(
        "\"{}\"",
        value
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n")
            .replace('\t', " ")
    )
}

fn dirty_content_hash() -> Result<String, Box<dyn Error>> {
    let status = Command::new("git")
        .args(["status", "--porcelain"])
        .output()?;
    let mut bytes = Vec::new();
    for line in String::from_utf8_lossy(&status.stdout).lines() {
        let path = line.get(3..).unwrap_or("").trim();
        if !path.is_empty() {
            append_path(Path::new(path), &mut bytes)?;
        }
    }
    let mut hash = Command::new("sha256sum")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    hash.stdin
        .take()
        .ok_or("sha256sum stdin missing")?
        .write_all(&bytes)?;
    Ok(String::from_utf8(hash.wait_with_output()?.stdout)?
        .split_whitespace()
        .next()
        .unwrap_or("unavailable")
        .to_owned())
}

fn append_path(path: &Path, bytes: &mut Vec<u8>) -> Result<(), Box<dyn Error>> {
    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            append_path(&entry?.path(), bytes)?;
        }
    } else if path.is_file() {
        bytes.extend_from_slice(path.to_string_lossy().as_bytes());
        bytes.push(0);
        bytes.extend(fs::read(path)?);
        bytes.push(0);
    }
    Ok(())
}

#[derive(Clone, Copy, Default)]
struct CountSnapshot {
    attempts: u64,
    failures: u64,
    sentinel: u64,
    role: u64,
}

impl CountSnapshot {
    fn delta(self, after: Self) -> Self {
        Self {
            attempts: after.attempts.saturating_sub(self.attempts),
            failures: after.failures.saturating_sub(self.failures),
            sentinel: after.sentinel.saturating_sub(self.sentinel),
            role: after.role.saturating_sub(self.role),
        }
    }
    fn add(&mut self, value: Self) {
        self.attempts += value.attempts;
        self.failures += value.failures;
        self.sentinel += value.sentinel;
        self.role += value.role;
    }
}

fn capture_counts(
    namespace: &str,
    plain: Option<&crate::bench_support::redis_fixture::SentinelFixture>,
    tls: Option<&crate::tls_support::TlsSentinel>,
) -> Result<CountSnapshot, Box<dyn Error>> {
    let snapshot = snapshot(namespace)?;
    let (sentinels, role) = if let Some(fixture) = plain {
        (
            fixture.sentinel_connection_counts()?,
            fixture.master.role_probe_calls()?,
        )
    } else {
        tls.ok_or("missing TLS fixture")?
            .cost_connection_snapshot()?
    };
    Ok(CountSnapshot {
        attempts: snapshot.0,
        failures: snapshot.1,
        sentinel: sentinels.iter().sum(),
        role,
    })
}

fn run_settle_workers(
    bus: &crate::bench_support::backend::Backend,
    prefix: &str,
    concurrency: usize,
    samples: usize,
    capture: &dyn Fn() -> Result<CountSnapshot, Box<dyn Error>>,
) -> Result<
    (
        crate::bench_support::workload_result::WorkloadResult,
        Option<CountSnapshot>,
    ),
    Box<dyn Error>,
> {
    use crate::bench_support::workload::message;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Instant;

    let batch_size = 32usize;
    let max_worker_samples = samples.div_ceil(concurrency);
    let batch_count = max_worker_samples.div_ceil(batch_size);
    let prepared = Arc::new(Barrier::new(concurrency + 1));
    let release = Arc::new(Barrier::new(concurrency + 1));
    let finished = Arc::new(Barrier::new(concurrency + 1));
    let payload: Arc<[u8]> = Arc::from(vec![b'x'; 64]);
    let mut workers = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let topic = format!("{prefix}-worker-{worker}");
        let worker_samples = samples / concurrency + usize::from(worker < samples % concurrency);
        let mut receiver = bus.subscribe(&topic, worker)?;
        let worker_bus = bus.clone();
        let worker_payload = Arc::clone(&payload);
        let (prepared, release, finished) = (
            Arc::clone(&prepared),
            Arc::clone(&release),
            Arc::clone(&finished),
        );
        workers.push(thread::spawn(move || {
            let mut records = Vec::with_capacity(worker_samples);
            for batch in 0..batch_count {
                let begin = batch * batch_size;
                let end = (begin + batch_size).min(worker_samples);
                let mut tokens = Vec::with_capacity(end.saturating_sub(begin));
                for sequence in begin..end {
                    let id = format!("event-{worker}-{sequence}");
                    let published = worker_bus.publish(
                        message(&topic, &id, Arc::clone(&worker_payload))
                            .expect("valid preparation message"),
                    );
                    if published != "ok" {
                        records.push(crate::bench_support::measurement::Sample {
                            nanos: 0,
                            outcome: format!("prepare:{published}"),
                        });
                        continue;
                    }
                    match receive_token(&mut receiver, &id) {
                        Ok(token) => tokens.push(token),
                        Err(error) => records.push(crate::bench_support::measurement::Sample {
                            nanos: 0,
                            outcome: format!("prepare:{error}"),
                        }),
                    }
                }
                prepared.wait();
                release.wait();
                for token in tokens {
                    let started = Instant::now();
                    let outcome = settle_token(&mut receiver, token);
                    records.push(crate::bench_support::measurement::Sample {
                        nanos: started.elapsed().as_nanos(),
                        outcome,
                    });
                }
                finished.wait();
            }
            (records, receiver)
        }));
    }
    let mut counts = CountSnapshot::default();
    let mut elapsed = std::time::Duration::ZERO;
    for _ in 0..batch_count {
        prepared.wait();
        let before = capture()?;
        let batch_started = Instant::now();
        release.wait();
        finished.wait();
        elapsed += batch_started.elapsed();
        let after = capture()?;
        counts.add(before.delta(after));
    }
    let mut groups = Vec::with_capacity(concurrency);
    let mut receivers = Vec::with_capacity(concurrency);
    for worker in workers {
        let (records, receiver) = worker.join().map_err(|_| "settlement worker panicked")?;
        groups.push(records);
        receivers.push(receiver);
    }
    Ok((
        crate::bench_support::workload_result::WorkloadResult {
            elapsed,
            groups,
            receivers,
        },
        Some(counts),
    ))
}

fn run_round(
    config: &Config,
    raw: &mut File,
    summary: &mut File,
    mode: &str,
    transport: &str,
    concurrency: usize,
    scenario: &str,
    round: usize,
) -> Result<(), Box<dyn Error>> {
    use crate::bench_support::backend::Backend;
    use crate::bench_support::redis_fixture::SentinelFixture;

    let namespace = format!(
        "rer_cost_{}_{}_{}_{}_{}",
        mode,
        transport,
        concurrency,
        round,
        IDS.fetch_add(1, Ordering::Relaxed)
    );
    let mut tls_fixture = None;
    let mut plain_fixture = None;
    let mut options: ProviderOptions = [("redis.namespace".into(), namespace.clone())].into();
    if transport == "plaintext" {
        let fixture = SentinelFixture::start()?;
        options.insert("redis.sentinel.nodes".into(), fixture.endpoints());
        options.insert("redis.sentinel.service_name".into(), "benchmaster".into());
        plain_fixture = Some(fixture);
    } else {
        let fixture = crate::tls_support::TlsSentinel::start(false)?;
        options.insert(
            "redis.url".into(),
            format!("rediss://localhost:{}/", fixture.original_master_port()),
        );
        options.insert("redis.sentinel.nodes".into(), fixture.endpoints());
        options.insert("redis.sentinel.service_name".into(), "qeventbus".into());
        options.insert("redis.tls_ca_cert_path".into(), fixture.ca_path().into());
        tls_fixture = Some(fixture);
    }
    let bus = Backend::new(mode, options)?;
    let capture = || capture_counts(&namespace, plain_fixture.as_ref(), tls_fixture.as_ref());
    let (warm, _) = scenario_run(
        &bus,
        &format!("{namespace}-warm"),
        concurrency,
        100,
        scenario,
        &capture,
    )?;
    for mut receiver in warm.receivers {
        receiver.close()?;
    }
    let (sentinel_before, role_before) = if let Some(fixture) = plain_fixture.as_ref() {
        (
            fixture.sentinel_connection_counts()?,
            fixture.master.role_probe_calls()?,
        )
    } else {
        tls_fixture
            .as_ref()
            .expect("TLS fixture")
            .cost_connection_snapshot()?
    };
    let before = snapshot(&namespace)?;
    let (workload, stage_counts) = scenario_run(
        &bus,
        &format!("{namespace}-measure"),
        concurrency,
        config.samples,
        scenario,
        &capture,
    )?;
    let after = snapshot(&namespace)?;
    let (sentinel_after, role_after) = if let Some(fixture) = plain_fixture.as_ref() {
        (
            fixture.sentinel_connection_counts()?,
            fixture.master.role_probe_calls()?,
        )
    } else {
        tls_fixture
            .as_ref()
            .expect("TLS fixture")
            .cost_connection_snapshot()?
    };
    let mut samples = Vec::new();
    for (worker, group) in workload.groups.iter().enumerate() {
        for (sequence, sample) in group.iter().enumerate() {
            let record = Sample {
                nanos: sample.nanos,
                outcome: sample.outcome.clone(),
            };
            writeln!(
                raw,
                "{mode},{transport},{concurrency},{scenario},{round},{worker}-{sequence},{},{}",
                record.nanos, record.outcome
            )?;
            samples.push(record);
        }
    }
    assert_measurement_pel_empty(
        &namespace,
        &format!("{namespace}-measure"),
        concurrency,
        plain_fixture.as_ref(),
        tls_fixture.as_ref(),
    )?;
    for mut receiver in workload.receivers {
        receiver.close()?;
    }
    let result = summarize(&samples);
    let sentinel_delta = stage_counts.as_ref().map_or_else(
        || {
            sentinel_before
                .iter()
                .zip(&sentinel_after)
                .map(|(a, b)| b.saturating_sub(*a))
                .sum()
        },
        |c| c.sentinel,
    );
    let measured_provider = stage_counts.as_ref().map_or(
        (
            after.0.saturating_sub(before.0),
            after.1.saturating_sub(before.1),
        ),
        |c| (c.attempts, c.failures),
    );
    let throughput = format!(
        "{:.6}",
        result.successes as f64 / workload.elapsed.as_secs_f64()
    );
    let role_delta = stage_counts
        .as_ref()
        .map_or_else(|| role_after.saturating_sub(role_before), |c| c.role);
    let provider_role_estimate = (measured_provider.1 == 0).then_some(measured_provider.0);
    writeln!(
        summary,
        "{mode},{transport},{concurrency},{scenario},{round},{},{},{},{},{},{},{},{},{},{},{},{},{},not_separately_measured",
        result.attempts,
        result.successes,
        result.errors,
        result.unknown,
        fmt(result.p50_ns),
        fmt(result.p95_ns),
        fmt(result.p99_ns),
        throughput,
        measured_provider.0,
        measured_provider.1,
        fmt_count(provider_role_estimate),
        sentinel_delta,
        role_delta
    )?;
    println!(
        "{mode}/{transport}/c{concurrency}/{scenario}/r{round}: {}/{} successes, {} unknown",
        result.successes, result.attempts, result.unknown
    );
    Ok(())
}

fn assert_measurement_pel_empty(
    namespace: &str,
    prefix: &str,
    concurrency: usize,
    plain: Option<&crate::bench_support::redis_fixture::SentinelFixture>,
    tls: Option<&crate::tls_support::TlsSentinel>,
) -> Result<(), Box<dyn Error>> {
    use qubit_event_bus_redis::naming::{group_name, stream_key};
    for worker in 0..concurrency {
        let topic = format!("{prefix}-worker-{worker}");
        let group = group_name(
            namespace,
            &topic,
            &format!("worker-{worker}"),
            Some(&format!("group-{worker}")),
        );
        let stream = stream_key(namespace, &topic);
        if let Some(fixture) = plain {
            let mut connection = redis::Client::open(fixture.master.url())?.get_connection()?;
            let pending: redis::streams::StreamPendingCountReply = redis::cmd("XPENDING")
                .arg(stream)
                .arg(group)
                .arg("-")
                .arg("+")
                .arg(10)
                .query(&mut connection)?;
            if !pending.ids.is_empty() {
                return Err(
                    format!("worker {worker} left {} pending entries", pending.ids.len()).into(),
                );
            }
        } else {
            tls.ok_or("missing TLS fixture")?.assert_pending_empty(
                tls.ok_or("missing TLS fixture")?.original_master_port(),
                &stream,
                &group,
            )?;
        }
    }
    Ok(())
}

fn scenario_run(
    bus: &crate::bench_support::backend::Backend,
    prefix: &str,
    concurrency: usize,
    samples: usize,
    scenario: &str,
    capture: &dyn Fn() -> Result<CountSnapshot, Box<dyn Error>>,
) -> Result<
    (
        crate::bench_support::workload_result::WorkloadResult,
        Option<CountSnapshot>,
    ),
    Box<dyn Error>,
> {
    use crate::bench_support::workload::message;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Instant;

    if scenario == "settle_only" {
        return run_settle_workers(bus, prefix, concurrency, samples, capture);
    }
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let publish_only = scenario == "publish_only";
    let payload: Arc<[u8]> = Arc::from(vec![b'x'; 64]);
    let mut workers = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let topic = format!("{prefix}-worker-{worker}");
        let count = samples / concurrency + usize::from(worker < samples % concurrency);
        let worker_bus = bus.clone();
        let worker_barrier = Arc::clone(&barrier);
        let worker_payload = Arc::clone(&payload);
        let receiver = bus.subscribe(&topic, worker)?;
        let messages = (0..count)
            .map(|sequence| {
                message(
                    &topic,
                    &format!("event-{worker}-{sequence}"),
                    Arc::clone(&worker_payload),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        workers.push(thread::spawn(move || {
            let mut receiver = receiver;
            worker_barrier.wait();
            let mut records = Vec::with_capacity(count);
            for message in messages {
                let started = Instant::now();
                let outcome = if publish_only {
                    worker_bus.publish(message)
                } else {
                    worker_bus.attempt(&mut receiver, Some(message))
                };
                records.push(crate::bench_support::measurement::Sample {
                    nanos: started.elapsed().as_nanos(),
                    outcome,
                });
            }
            (records, Some(receiver))
        }));
    }
    let before = capture()?;
    let started = Instant::now();
    barrier.wait();
    let mut groups = Vec::with_capacity(concurrency);
    let mut receivers = Vec::new();
    for worker in workers {
        let (records, receiver) = worker.join().map_err(|_| "cost worker panicked")?;
        groups.push(records);
        if let Some(receiver) = receiver {
            receivers.push(receiver);
        }
    }
    let after = capture()?;
    Ok((
        crate::bench_support::workload_result::WorkloadResult {
            elapsed: started.elapsed(),
            groups,
            receivers,
        },
        Some(before.delta(after)),
    ))
}

fn receive_token(
    receiver: &mut crate::bench_support::receiver::Receiver,
    expected: &str,
) -> Result<qubit_event_bus::spi::SettlementToken, String> {
    use crate::bench_support::receiver::Receiver;
    use qubit_event_bus::spi::ReceiveOutcome;
    use std::time::Duration;
    match receiver {
        Receiver::Sync(subscription) => match subscription.receive(Duration::from_secs(3)) {
            Ok(ReceiveOutcome::Message(mut message)) if message.id().as_str() == expected => {
                message
                    .take_settlement()
                    .ok_or_else(|| "missing_token".into())
            }
            Ok(ReceiveOutcome::Message(_)) => Err("event_id_mismatch".into()),
            Ok(_) => Err("receive_not_message".into()),
            Err(error) => Err(format!("{}:{}", error.operation(), error.kind())),
        },
        Receiver::Async(subscription) => futures_lite::future::block_on(async {
            match subscription.receive(Duration::from_secs(3)).await {
                Ok(ReceiveOutcome::Message(mut message)) if message.id().as_str() == expected => {
                    message
                        .take_settlement()
                        .ok_or_else(|| "missing_token".into())
                }
                Ok(ReceiveOutcome::Message(_)) => Err("event_id_mismatch".into()),
                Ok(_) => Err("receive_not_message".into()),
                Err(error) => Err(format!("{}:{}", error.operation(), error.kind())),
            }
        }),
    }
}

fn settle_token(
    receiver: &mut crate::bench_support::receiver::Receiver,
    token: qubit_event_bus::spi::SettlementToken,
) -> String {
    use crate::bench_support::receiver::Receiver;
    use qubit_event_bus::spi::DeliveryDisposition;
    match receiver {
        Receiver::Sync(subscription) => subscription
            .settle(&token, DeliveryDisposition::Accept)
            .map_or_else(
                |error| format!("{}:{}", error.operation(), error.kind()),
                |_| "ok".into(),
            ),
        Receiver::Async(subscription) => {
            futures_lite::future::block_on(subscription.settle(&token, DeliveryDisposition::Accept))
                .map_or_else(
                    |error| format!("{}:{}", error.operation(), error.kind()),
                    |_| "ok".into(),
                )
        }
    }
}

fn snapshot(namespace: &str) -> Result<(u64, u64), Box<dyn Error>> {
    RedisProviderDiagnostics::snapshots()
        .into_iter()
        .find(|s| s.namespace() == namespace)
        .map(|s| (s.connection_attempts(), s.connection_failures()))
        .ok_or_else(|| "provider diagnostics snapshot missing".into())
}

fn fmt(value: Option<u128>) -> String {
    value.map_or_else(|| "".into(), |n| n.to_string())
}
fn fmt_count(value: Option<u64>) -> String {
    value.map_or_else(|| "".into(), |n| n.to_string())
}
