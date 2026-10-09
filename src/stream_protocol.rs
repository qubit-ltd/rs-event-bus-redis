// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Normalization of Redis Streams protocol replies across Redis versions.

use std::num::NonZeroU32;

use redis::ErrorKind;
use redis::RedisError;
use redis::Value;
use redis::from_owned_redis_value;
use redis::from_redis_value;
use redis::streams::StreamAutoClaimReply;
use redis::streams::StreamId;
use redis::streams::StreamKey;
use redis::streams::StreamRangeReply;
use redis::streams::StreamReadReply;

/// Parses an XAUTOCLAIM reply and records Redis 6.2's nil tombstone markers.
///
/// # Parameters
///
/// - `value`: Raw RESP value returned by XAUTOCLAIM.
///
/// # Returns
///
/// The normalized XAUTOCLAIM reply and whether the server returned a deleted
/// pending entry without its ID.
///
/// # Errors
///
/// Returns a response error if the reply does not match the supported Redis
/// 6.2 or Redis 7 shape.
pub(crate) fn parse_auto_claim(value: Value) -> Result<(StreamAutoClaimReply, bool), RedisError> {
    let Value::Array(parts) = value else {
        return Err(invalid_reply());
    };
    if !(2..=3).contains(&parts.len()) {
        return Err(invalid_reply());
    }
    let mut parts = parts.into_iter();
    let next_stream_id = from_owned_redis_value(parts.next().ok_or_else(invalid_reply)?)?;
    let Value::Array(rows) = parts.next().ok_or_else(invalid_reply)? else {
        return Err(invalid_reply());
    };
    let mut has_missing_entries = false;
    let mut claimed = Vec::with_capacity(rows.len());
    for row in rows {
        let is_missing = match &row {
            Value::Nil => true,
            Value::Array(values) => values.as_slice() == [Value::Nil],
            _ => false,
        };
        if is_missing {
            has_missing_entries = true;
        } else {
            claimed.push(match row {
                Value::BulkString(_) => StreamId {
                    id: from_owned_redis_value(row)?,
                    ..StreamId::default()
                },
                row => parse_stream_id(row)?,
            });
        }
    }
    let deleted_ids = parts
        .next()
        .map(from_owned_redis_value)
        .transpose()?
        .unwrap_or_default();
    let reply = StreamAutoClaimReply {
        next_stream_id,
        claimed,
        deleted_ids,
    };
    Ok((reply, has_missing_entries))
}

/// Normalizes an owned XREADGROUP response without copying wire bulk values.
///
/// # Parameters
///
/// - `value`: Complete RESP2 array or RESP3 map returned by Redis.
///
/// # Returns
///
/// `None` for Redis's nil timeout reply; `Some` contains each stream's owned
/// entries, including empty field maps for deleted pending entries.
///
/// # Errors
///
/// Returns a stable protocol error when a stream or entry row has an invalid
/// shape. Existing wire allocations move into the reply without external I/O.
pub(crate) fn parse_read_group(value: Value) -> Result<Option<StreamReadReply>, RedisError> {
    let pairs = match value {
        Value::Nil => return Ok(None),
        Value::Array(rows) => rows.into_iter().map(parse_pair).collect::<Result<Vec<_>, _>>()?,
        Value::Map(pairs) => pairs,
        _ => return Err(invalid_reply()),
    };
    let keys = pairs
        .into_iter()
        .map(|(key, rows)| {
            Ok(StreamKey {
                key: from_owned_redis_value(key)?,
                ids: parse_range(rows)?.ids,
            })
        })
        .collect::<Result<Vec<_>, RedisError>>()?;
    Ok(Some(StreamReadReply { keys }))
}

/// Normalizes an owned XRANGE reply while retaining each field value
/// allocation.
///
/// # Parameters
///
/// - `value`: RESP array of stream entry rows, or nil for an empty result.
///
/// # Returns
///
/// The owned stream entries; nil and an empty array yield no entries.
///
/// # Errors
///
/// Returns a stable protocol error for an invalid row shape or malformed ID or
/// field map. No external I/O or wire byte cloning is performed.
pub(crate) fn parse_range(value: Value) -> Result<StreamRangeReply, RedisError> {
    let rows = match value {
        Value::Nil => Vec::new(),
        Value::Array(rows) => rows,
        _ => return Err(invalid_reply()),
    };
    Ok(StreamRangeReply {
        ids: rows.into_iter().map(parse_stream_id).collect::<Result<Vec<_>, _>>()?,
    })
}

/// Moves one Redis entry's ID and field map out of its two-element row.
///
/// # Parameters
///
/// - `value`: Owned array containing the stream ID and field map.
///
/// # Returns
///
/// A typed entry retaining the original bulk field allocations.
///
/// # Errors
///
/// Returns a protocol error for invalid row shape, non-string ID, or an
/// unsupported field map. Nil fields represent a deleted pending entry.
fn parse_stream_id(value: Value) -> Result<StreamId, RedisError> {
    let (id, fields) = parse_pair(value)?;
    Ok(StreamId {
        id: from_owned_redis_value(id)?,
        map: from_owned_redis_value(fields)?,
    })
}

/// Consumes an owned protocol row containing exactly two fields.
///
/// # Parameters
///
/// - `value`: Array row whose components are transferred to the caller.
///
/// # Returns
///
/// The first and second values without copying either allocation.
///
/// # Errors
///
/// Returns a stable protocol error for any other RESP type or field count.
fn parse_pair(value: Value) -> Result<(Value, Value), RedisError> {
    let Value::Array(values) = value else {
        return Err(invalid_reply());
    };
    if values.len() != 2 {
        return Err(invalid_reply());
    }
    let mut values = values.into_iter();
    Ok((
        values.next().ok_or_else(invalid_reply)?,
        values.next().ok_or_else(invalid_reply)?,
    ))
}

/// One row returned by the detailed XPENDING command.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PendingEntry {
    /// Redis stream entry ID.
    pub(crate) id: String,
    /// Consumer that currently owns the pending delivery.
    pub(crate) owner: String,
    /// Idle duration reported by Redis, in milliseconds.
    pub(crate) idle_ms: u64,
    /// Number of times Redis delivered this entry.
    pub(crate) times_delivered: u64,
}

/// Parses rows returned by the detailed XPENDING command.
///
/// # Parameters
///
/// - `value`: Raw XPENDING detail reply.
///
/// # Returns
///
/// Each row's ID, current owner, idle time in milliseconds, and delivery count.
///
/// # Errors
///
/// Returns a type error if any row is missing one of those fields or contains
/// a negative integer.
pub(crate) fn parse_pending_entries(value: Value) -> Result<Vec<PendingEntry>, RedisError> {
    let Value::Array(rows) = value else {
        return Err(invalid_reply());
    };
    rows.iter()
        .map(|row| {
            let Value::Array(fields) = row else {
                return Err(invalid_reply());
            };
            if fields.len() != 4 {
                return Err(invalid_reply());
            }
            let id = from_redis_value(&fields[0])?;
            let consumer = from_redis_value(&fields[1])?;
            let idle_ms: i64 = from_redis_value(&fields[2])?;
            let idle_ms = u64::try_from(idle_ms).map_err(|_| invalid_reply())?;
            let times_delivered: i64 = from_redis_value(&fields[3])?;
            let times_delivered = u64::try_from(times_delivered).map_err(|_| invalid_reply())?;
            Ok(PendingEntry {
                id,
                owner: consumer,
                idle_ms,
                times_delivered,
            })
        })
        .collect()
}

/// Returns a delivery count only when the pending detail matches its expected
/// ID and owner.
pub(crate) fn trusted_attempt(entry: &PendingEntry, expected_id: &str, consumer: &str) -> Option<NonZeroU32> {
    if entry.id != expected_id || entry.owner != consumer || entry.times_delivered == 0 {
        return None;
    }
    NonZeroU32::new(entry.times_delivered.min(u64::from(u32::MAX)) as u32)
}

/// Creates a stable type-error category for an invalid internal stream
/// response.
///
/// # Returns
///
/// A static diagnostic without source keys, wire bytes, or raw server details.
#[must_use]
fn invalid_reply() -> RedisError {
    RedisError::from((ErrorKind::TypeError, "invalid Redis Streams response"))
}
