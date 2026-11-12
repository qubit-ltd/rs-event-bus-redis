// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Correctness checks and an explicitly ignored Sentinel cost matrix.

#![cfg(all(feature = "sync", feature = "async"))]

mod support;

#[test]
fn test_cost_statistics_retain_failures_and_unknown_outcomes() {
    use support::sentinel_cost::measurement::Sample;
    use support::sentinel_cost::measurement::summarize;

    let report = summarize(&[
        Sample {
            nanos: 10,
            outcome: "ok".into(),
        },
        Sample {
            nanos: 20,
            outcome: "ok".into(),
        },
        Sample {
            nanos: 30,
            outcome: "publish:outcome_unknown".into(),
        },
    ]);
    assert_eq!(report.attempts, 3);
    assert_eq!(report.successes, 2);
    assert_eq!(report.errors, 1);
    assert_eq!(report.unknown, 1);
    assert_eq!(report.p50_ns, Some(10));
    assert_eq!(report.p95_ns, Some(20));
}

#[test]
fn test_cost_statistics_have_no_percentiles_without_success() {
    use support::sentinel_cost::measurement::Sample;
    use support::sentinel_cost::measurement::summarize;
    let report = summarize(&[Sample {
        nanos: 7,
        outcome: "receive:timeout".into(),
    }]);
    assert_eq!(report.successes, 0);
    assert_eq!(report.p50_ns, None);
    assert_eq!(report.p99_ns, None);
}

#[test]
fn test_cost_elapsed_is_frozen_before_observation() {
    use std::cell::Cell;
    use std::time::Duration;

    use support::sentinel_cost::measurement::freeze_before_observation;

    let clock = Cell::new(10u64);
    let (elapsed, observed) = freeze_before_observation(
        || Duration::from_secs(clock.get()),
        || {
            clock.set(100);
            clock.get()
        },
    );
    assert_eq!(elapsed, Duration::from_secs(10));
    assert_eq!(observed, 100);
}

#[test]
fn test_cost_summary_requires_exact_complete_counts() {
    use support::sentinel_cost::measurement::Sample;
    use support::sentinel_cost::measurement::assert_complete;
    use support::sentinel_cost::measurement::summarize;
    let complete = summarize(&vec![
        Sample {
            nanos: 1,
            outcome: "ok".into()
        };
        3
    ]);
    assert!(assert_complete(&complete, 3).is_ok());
    for samples in [
        vec![
            Sample {
                nanos: 1,
                outcome: "ok".into()
            };
            2
        ],
        vec![
            Sample {
                nanos: 1,
                outcome: "ok".into(),
            },
            Sample {
                nanos: 2,
                outcome: "failed".into(),
            },
            Sample {
                nanos: 3,
                outcome: "ok".into(),
            },
        ],
        vec![
            Sample {
                nanos: 1,
                outcome: "ok".into(),
            },
            Sample {
                nanos: 2,
                outcome: "outcome_unknown".into(),
            },
            Sample {
                nanos: 3,
                outcome: "ok".into(),
            },
        ],
    ] {
        assert!(assert_complete(&summarize(&samples), 3).is_err());
    }
}

#[test]
fn test_failed_warmup_flushes_dimensional_samples_and_counts_first() -> Result<(), Box<dyn std::error::Error>> {
    use std::fs::OpenOptions;
    use std::io::Read;
    use std::io::Write;

    use support::sentinel_cost::measurement::Sample;
    use support::sentinel_cost::measurement::WarmupDimension;
    use support::sentinel_cost::measurement::persist_and_validate_warmup;
    use support::sentinel_cost::measurement::summarize;

    let path = std::env::temp_dir().join(format!(
        "rer-warmup-evidence-{}-{}.csv",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    let samples = vec![
        Sample {
            nanos: 7,
            outcome: "ok".into(),
        },
        Sample {
            nanos: 9,
            outcome: "publish:failure".into(),
        },
    ];
    let summary = summarize(&samples);
    let mut evidence = OpenOptions::new().write(true).create_new(true).open(&path)?;
    writeln!(
        evidence,
        "mode,transport,concurrency,scenario,round,record_type,sample,attempts,successes,errors,unknown,latency_ns,outcome"
    )?;
    let result = persist_and_validate_warmup(
        &mut evidence,
        WarmupDimension {
            mode: "async",
            transport: "tls",
            concurrency: 8,
            scenario: "publish_only",
            round: 2,
        },
        &samples,
        &summary,
        2,
    );
    assert!(result.is_err());
    drop(evidence);

    let mut contents = String::new();
    std::fs::File::open(&path)?.read_to_string(&mut contents)?;
    std::fs::remove_file(path)?;
    assert!(contents.contains("async,tls,8,publish_only,2,sample,0,,,,,7,ok"));
    assert!(contents.contains("async,tls,8,publish_only,2,sample,1,,,,,9,publish:failure"));
    assert!(contents.contains("async,tls,8,publish_only,2,summary,,2,1,1,0,,"));
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("async/tls/c8/publish_only/r2/warm")
    );
    Ok(())
}

#[test]
fn test_publish_content_validation_rejects_invalid_rows() {
    use std::collections::HashSet;

    use redis::streams::StreamId;
    use support::sentinel_cost::validation::validate_rows;
    fn row(id: &str, payload: Vec<u8>) -> StreamId {
        let fields = qubit_event_bus_redis::wire::WireFields {
            version: 1,
            event_id: id.into(),
            timestamp_ms: 1,
            headers_json: "{}".into(),
            ordering_key: None,
            content_type: "application/octet-stream".into(),
            schema_id: None,
            payload,
        };
        let mut row = StreamId::default();
        row.id = "1-0".into();
        row.map.insert(
            "wire".into(),
            redis::Value::BulkString(serde_json::to_vec(&fields).unwrap()),
        );
        row
    }
    let expected: HashSet<_> = ["event-0-0".to_owned(), "event-0-1".to_owned()].into();
    let valid = vec![row("event-0-0", vec![b'x'; 64]), row("event-0-1", vec![b'x'; 64])];
    assert!(validate_rows(&valid, &expected, "topic").is_ok());
    assert!(validate_rows(&valid[..1], &expected, "topic").is_err());
    assert!(
        validate_rows(
            &[valid[0].clone(), valid[1].clone(), valid[1].clone()],
            &expected,
            "topic"
        )
        .is_err()
    );
    assert!(
        validate_rows(
            &[valid[0].clone(), row("event-0-0", vec![b'x'; 64])],
            &expected,
            "topic"
        )
        .is_err()
    );
    assert!(
        validate_rows(
            &[row("event-0-0", vec![b'x'; 64]), row("unexpected", vec![b'x'; 64])],
            &expected,
            "topic"
        )
        .is_err()
    );
    assert!(
        validate_rows(
            &[row("event-0-0", vec![b'x'; 64]), row("event-0-1", vec![b'y'; 64])],
            &expected,
            "topic"
        )
        .is_err()
    );
    let mut invalid = valid[0].clone();
    invalid
        .map
        .insert("wire".into(), redis::Value::BulkString(b"{bad".to_vec()));
    assert!(validate_rows(&[invalid, valid[1].clone()], &expected, "topic").is_err());
}

#[test]
fn test_publish_content_observer_paginates_plaintext_and_tls() -> Result<(), Box<dyn std::error::Error>> {
    use qubit_event_bus_redis::naming::stream_key;
    use qubit_event_bus_redis::wire::WireFields;
    const N: usize = 129;
    let wires: Vec<String> = (0..N)
        .map(|sequence| {
            serde_json::to_string(&WireFields {
                version: 1,
                event_id: format!("event-0-{sequence}"),
                timestamp_ms: 1,
                headers_json: "{}".into(),
                ordering_key: None,
                content_type: "application/octet-stream".into(),
                schema_id: None,
                payload: vec![b'x'; 64],
            })
            .unwrap()
        })
        .collect();

    let namespace = "rer_cost_observer_plain";
    let prefix = format!("{namespace}-measure");
    let topic = format!("{prefix}-worker-0");
    let stream = stream_key(namespace, &topic);
    let plain = support::bench_support::redis_fixture::SentinelFixture::start()?;
    {
        let mut connection = redis::Client::open(plain.master.url())?.get_connection()?;
        for wire in &wires {
            let _: String = redis::cmd("XADD")
                .arg(&stream)
                .arg("*")
                .arg("wire")
                .arg(wire)
                .query(&mut connection)?;
        }
    }
    support::sentinel_cost::workload::validate_publish_contents(namespace, &prefix, 1, N, Some(&plain), None)?;

    let namespace = "rer_cost_observer_tls";
    let prefix = format!("{namespace}-measure");
    let topic = format!("{prefix}-worker-0");
    let stream = stream_key(namespace, &topic);
    let tls = support::tls_sentinel::TlsSentinel::start(false)?;
    tls.cost_seed_wire_rows(tls.original_master_port(), &stream, &wires)?;
    let before = tls.sentinel_proxy_counts();
    support::sentinel_cost::workload::validate_publish_contents(namespace, &prefix, 1, N, None, Some(&tls))?;
    assert_eq!(
        before,
        tls.sentinel_proxy_counts(),
        "TLS observer reads must not affect Sentinel provider proxy counters"
    );
    Ok(())
}

#[test]
fn test_cost_connection_snapshot_delta_is_saturating() {
    use support::sentinel_cost::measurement::ConnectionSnapshot;
    assert_eq!(
        ConnectionSnapshot {
            master: 12,
            sentinel: 4
        }
        .delta(ConnectionSnapshot {
            master: 17,
            sentinel: 9
        }),
        ConnectionSnapshot { master: 5, sentinel: 5 }
    );
    assert_eq!(
        ConnectionSnapshot {
            master: 17,
            sentinel: 9
        }
        .delta(ConnectionSnapshot {
            master: 12,
            sentinel: 4
        }),
        ConnectionSnapshot { master: 0, sentinel: 0 }
    );
}

#[test]
#[ignore = "starts isolated Redis/Sentinel services and runs the full cost matrix"]
fn test_sentinel_cost_matrix() -> Result<(), Box<dyn std::error::Error>> {
    support::sentinel_cost::workload::run_matrix()
}
