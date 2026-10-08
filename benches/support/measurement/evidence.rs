// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Durable per-attempt and per-round CSV evidence.

use std::error::Error;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs::create_dir_all;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

use super::sample::Sample;
use super::snapshot::Snapshot;

/// CSV writer owning both per-attempt and per-round evidence files.
pub struct Evidence {
    summary: File,
    samples: File,
}

impl Evidence {
    /// Creates raw files; the caller chooses a fresh before/after prefix.
    pub fn new(directory: &Path, label: &str) -> Result<Self, Box<dyn Error>> {
        create_dir_all(directory)?;
        let mut summary = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(directory.join(format!("{label}-summary.csv")))?;
        let mut samples = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(directory.join(format!("{label}-samples.csv")))?;
        writeln!(
            summary,
            "label,scenario,mode,limits,payload_bytes,concurrency,round,attempts,success,errors,unknown,elapsed_ns,events_per_second,p50_ns,p95_ns,p99_ns,new_connections,active_before,active_after,active_peak_sampled,xautoclaim,xreadgroup,error_p50_ns,error_p95_ns,error_p99_ns"
        )?;
        writeln!(
            samples,
            "label,scenario,mode,limits,payload_bytes,concurrency,round,worker,sequence,latency_ns,outcome"
        )?;
        Ok(Self { summary, samples })
    }

    /// Writes all samples and nearest-rank successful latency percentiles.
    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &mut self,
        label: &str,
        scenario: &str,
        mode: &str,
        limits: &str,
        payload: usize,
        concurrency: usize,
        round: usize,
        elapsed: Duration,
        groups: &[Vec<Sample>],
        before: Snapshot,
        after: Snapshot,
        peak: u64,
    ) -> Result<(), Box<dyn Error>> {
        let mut latencies = Vec::new();
        let mut errors = 0;
        let mut failed_latencies = Vec::new();
        let mut unknown = 0;
        for (worker, samples) in groups.iter().enumerate() {
            for (sequence, sample) in samples.iter().enumerate() {
                if sample.outcome == "ok" || sample.outcome == "timed_out" {
                    latencies.push(sample.nanos);
                } else {
                    failed_latencies.push(sample.nanos);
                    errors += 1;
                    if sample.outcome.contains("outcome_unknown") {
                        unknown += 1;
                    }
                }
                writeln!(
                    self.samples,
                    "{label},{scenario},{mode},{limits},{payload},{concurrency},{round},{worker},{sequence},{},{}",
                    sample.nanos, sample.outcome
                )?;
            }
        }
        latencies.sort_unstable();
        failed_latencies.sort_unstable();
        let failure_percentile = |percent: usize| {
            let rank = failed_latencies
                .len()
                .saturating_mul(percent)
                .div_ceil(100)
                .max(1);
            failed_latencies.get(rank - 1).copied().unwrap_or(0)
        };
        let percentile = |percent: usize| {
            let rank = latencies.len().saturating_mul(percent).div_ceil(100).max(1);
            latencies.get(rank - 1).copied().unwrap_or(0)
        };
        let attempts = groups.iter().map(Vec::len).sum::<usize>();
        writeln!(
            self.summary,
            "{label},{scenario},{mode},{limits},{payload},{concurrency},{round},{attempts},{},{errors},{unknown},{},{:.6},{},{},{},{},{},{},{peak},{},{},{},{},{}",
            latencies.len(),
            elapsed.as_nanos(),
            latencies.len() as f64 / elapsed.as_secs_f64(),
            percentile(50),
            percentile(95),
            percentile(99),
            after.created.saturating_sub(before.created),
            before.active,
            after.active,
            after.claim.saturating_sub(before.claim),
            after.read.saturating_sub(before.read),
            failure_percentile(50),
            failure_percentile(95),
            failure_percentile(99)
        )?;
        self.samples.flush()?;
        self.summary.flush()?;
        println!(
            "{label} {scenario}/{mode}/{limits} {payload}B c{concurrency} r{round}: success={} errors={errors} unknown={unknown} {:.2}/s",
            latencies.len(),
            latencies.len() as f64 / elapsed.as_secs_f64()
        );
        Ok(())
    }
}
