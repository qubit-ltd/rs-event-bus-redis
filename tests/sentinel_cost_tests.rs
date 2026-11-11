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
