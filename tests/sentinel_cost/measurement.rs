// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Samples, configuration and summaries for the Sentinel cost matrix.

use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct Sample {
    pub nanos: u128,
    pub outcome: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectionSnapshot {
    pub master: u64,
    pub sentinel: u64,
}

impl ConnectionSnapshot {
    pub fn delta(self, after: Self) -> Self {
        Self {
            master: after.master.saturating_sub(self.master),
            sentinel: after.sentinel.saturating_sub(self.sentinel),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Summary {
    pub attempts: usize,
    pub successes: usize,
    pub errors: usize,
    pub unknown: usize,
    pub p50_ns: Option<u128>,
    pub p95_ns: Option<u128>,
    pub p99_ns: Option<u128>,
}

pub fn summarize(samples: &[Sample]) -> Summary {
    let mut successful: Vec<_> = samples.iter().filter(|s| s.outcome == "ok").map(|s| s.nanos).collect();
    successful.sort_unstable();
    let percentile = |pct: usize| {
        (!successful.is_empty()).then(|| successful[(successful.len() * pct).div_ceil(100).saturating_sub(1)])
    };
    let successes = successful.len();
    Summary {
        attempts: samples.len(),
        successes,
        errors: samples.len() - successes,
        unknown: samples.iter().filter(|s| s.outcome.contains("outcome_unknown")).count(),
        p50_ns: percentile(50),
        p95_ns: percentile(95),
        p99_ns: percentile(99),
    }
}

pub struct Config {
    pub samples: usize,
    pub rounds: usize,
    pub output: PathBuf,
}

impl Config {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        fn positive(name: &str, default: usize) -> Result<usize, Box<dyn std::error::Error>> {
            let value = std::env::var(name).map_or(Ok(default), |s| {
                s.parse::<usize>()
                    .map_err(|e| format!("{name} must be a positive integer: {e}"))
            })?;
            if value == 0 {
                return Err(format!("{name} must be positive").into());
            }
            Ok(value)
        }
        let output = std::env::var_os("RER_COST_OUTPUT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(".infra/benchmark/evidence"));
        Ok(Self {
            samples: positive("RER_COST_SAMPLES", 1000)?,
            rounds: positive("RER_COST_ROUNDS", 5)?,
            output,
        })
    }
}
