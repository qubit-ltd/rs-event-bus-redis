// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded, read-only validation of publish-only stream contents.

use std::collections::HashSet;

use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus_redis::wire::WireFields;
use redis::streams::StreamId;

/// Checks that each stream row contains one expected, valid wire event.
pub fn validate_rows(rows: &[StreamId], expected: &HashSet<String>, topic: &str) -> Result<(), String> {
    if rows.len() != expected.len() {
        return Err(format!("row_count expected={} actual={}", expected.len(), rows.len()));
    }
    let topic = TopicAddress::new(topic).map_err(|_| "invalid_topic")?;
    let mut seen = HashSet::new();
    for row in rows {
        let Some(redis::Value::BulkString(wire)) = row.map.get("wire") else {
            return Err("missing_or_invalid_wire_field".into());
        };
        let wire = std::str::from_utf8(wire).map_err(|_| "invalid_wire_utf8")?;
        let fields: WireFields = serde_json::from_str(wire).map_err(|_| "invalid_wire")?;
        if fields.payload != vec![b'x'; 64] {
            return Err("invalid_payload".into());
        }
        let id = fields.event_id.clone();
        if !expected.contains(&id) {
            return Err("unexpected_event_id".into());
        }
        if !seen.insert(id) {
            return Err("duplicate_event_id".into());
        }
        let _decoded = fields.into_parts(topic.clone()).map_err(|_| "invalid_wire_fields")?;
    }
    if seen != *expected {
        return Err("missing_event_id".into());
    }
    Ok(())
}
