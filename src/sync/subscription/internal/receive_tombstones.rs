// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded cleanup for deleted Redis records left in a consumer group's PEL.

use std::time::Instant;

use qubit_event_bus::error::SpiError;
use redis::Value;
use redis::cmd;

use super::Subscription;
use super::lock_state;
use super::receive_command::ReceiveCommand;
use crate::client::PooledConnection;
use crate::diagnostics::RedisDiagnosticCounter;
use crate::error::RedisProviderError;
use crate::error::from_redis_error as classified_spi_error;
use crate::internal::PoisonOutcome;
use crate::internal::PoisonReason;
use crate::internal::ReceiveDriver;
use crate::poison::quarantine;
use crate::stream_protocol::parse_pending_entries;
use crate::stream_protocol::parse_range;
use crate::sync::redis_event_bus::spi_error;

/// Clears deleted pending records while preserving the scan cursor and budget.
///
/// The claim reply's deleted-ID count is included in the result. Additional
/// stale IDs are probed through bounded `XPENDING` and `XRANGE` reads, then
/// cleared through the existing atomic quarantine script. A partial scan
/// retains its cursor so a later receive can resume it.
///
/// # Parameters
///
/// - `subscription`: Receiver whose Redis keys, cursor state, and idle policy
///   govern the scan.
/// - `connection`: Dedicated receive socket used for Redis commands.
/// - `driver`: Per-receive command and maintenance budget.
/// - `has_missing_entries`: Whether Redis reported deleted IDs in XAUTOCLAIM.
/// - `deleted_count`: Number of deleted IDs already reported by XAUTOCLAIM.
///
/// # Returns
///
/// The combined number of deleted pending records found in this scan.
///
/// # Errors
///
/// Returns classified admission or socket errors, or an outcome-unknown error
/// when a Redis reply or quarantine result cannot be trusted.
pub(super) fn scan(
    subscription: &Subscription,
    connection: &mut PooledConnection,
    driver: &mut ReceiveDriver,
    has_missing_entries: bool,
    mut deleted_count: u64,
) -> Result<u64, SpiError> {
    if !has_missing_entries || !driver.budget_mut().take_tombstone_probe(Instant::now()) {
        return Ok(deleted_count);
    }

    let tombstone_cursor = lock_state(&subscription.recovery, &subscription.topic, "receive", "recovery lock")?
        .tombstone_cursor()
        .to_owned();
    let pending_start = if tombstone_cursor == "0-0" {
        "-".to_owned()
    } else {
        format!("({tombstone_cursor}")
    };
    let pending_reply: Value = cmd("XPENDING")
        .arg(&subscription.key)
        .arg(&subscription.group)
        .arg(pending_start)
        .arg("+")
        .arg(4)
        .query_receive(connection)
        .map_err(|error| spi_error("receive", Some(&subscription.topic), error))?;
    let pending_rows = parse_pending_entries(pending_reply).map_err(|_| {
        spi_error(
            "receive",
            Some(&subscription.topic),
            RedisProviderError::OutcomeUnknown { operation: "receive" },
        )
    })?;
    let pending_row_count = pending_rows.len();
    let mut scan_complete = true;
    for (id, owner, idle_ms) in pending_rows {
        if idle_ms < subscription.claim_min_idle_ms as u64 {
            lock_state(&subscription.recovery, &subscription.topic, "receive", "recovery lock")?
                .set_tombstone_cursor(id);
            continue;
        }
        if !driver.budget_mut().take_tombstone_range(Instant::now()) {
            scan_complete = false;
            break;
        }
        let raw_rows: Value = cmd("XRANGE")
            .arg(&subscription.key)
            .arg(&id)
            .arg(&id)
            .query_receive(connection)
            .map_err(|error| spi_error("receive", Some(&subscription.topic), error))?;
        let rows = parse_range(raw_rows).map_err(|_| {
            spi_error(
                "receive",
                Some(&subscription.topic),
                RedisProviderError::OutcomeUnknown { operation: "receive" },
            )
        })?;
        if rows.ids.is_empty() {
            if !driver.budget_mut().take_maintenance_evaluation(Instant::now()) {
                scan_complete = false;
                break;
            }
            let timeout = subscription.client.command_timeout();
            connection
                .set_read_timeout(Some(timeout))
                .map_err(|error| classified_spi_error("receive", Some(&subscription.topic), &error))?;
            connection
                .set_write_timeout(Some(timeout))
                .map_err(|error| classified_spi_error("receive", Some(&subscription.topic), &error))?;
            match quarantine(
                connection,
                &subscription.key,
                &subscription.quarantine,
                &subscription.group,
                &owner,
                &id,
                PoisonReason::MissingWire,
            )
            .map_err(|_error| {
                spi_error(
                    "receive",
                    Some(&subscription.topic),
                    RedisProviderError::OutcomeUnknown {
                        operation: "quarantine",
                    },
                )
            })? {
                PoisonOutcome::TombstoneCleared => deleted_count += 1,
                PoisonOutcome::SourceGone | PoisonOutcome::OwnershipChanged => {}
                PoisonOutcome::Quarantined => {
                    if let Some(diagnostics) = subscription.client.diagnostics() {
                        diagnostics.increment(RedisDiagnosticCounter::QuarantineSucceeded);
                    }
                }
            }
        }
        lock_state(&subscription.recovery, &subscription.topic, "receive", "recovery lock")?.set_tombstone_cursor(id);
    }
    if scan_complete && pending_row_count < 4 {
        lock_state(&subscription.recovery, &subscription.topic, "receive", "recovery lock")?.reset_tombstone_cursor();
    }
    Ok(deleted_count)
}
