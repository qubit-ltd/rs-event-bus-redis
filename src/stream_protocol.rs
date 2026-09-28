// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Normalization of Redis Streams protocol replies across Redis versions.

use redis::ErrorKind;
use redis::RedisError;
use redis::Value;
use redis::streams::StreamAutoClaimReply;

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
    let Value::Array(mut parts) = value else {
        return Err(invalid_reply());
    };
    if !(2..=3).contains(&parts.len()) {
        return Err(invalid_reply());
    }
    let Value::Array(rows) = &parts[1] else {
        return Err(invalid_reply());
    };
    let mut has_missing_entries = false;
    let mut normalized_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let is_missing = match row {
            Value::Nil => true,
            Value::Array(values) => values.as_slice() == [Value::Nil],
            _ => false,
        };
        if is_missing {
            has_missing_entries = true;
        } else {
            normalized_rows.push(row.clone());
        }
    }
    parts[1] = Value::Array(normalized_rows);
    let reply = redis::from_redis_value(&Value::Array(parts))?;
    Ok((reply, has_missing_entries))
}

/// Parses rows returned by the detailed XPENDING command.
///
/// # Parameters
///
/// - `value`: Raw XPENDING detail reply.
///
/// # Returns
///
/// Each row's stream ID, current consumer name, and idle time in milliseconds.
///
/// # Errors
///
/// Returns a type error if any row is missing one of those fields.
pub(crate) fn parse_pending_entries(value: Value) -> Result<Vec<(String, String, u64)>, RedisError> {
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
            let id = redis::from_redis_value(&fields[0])?;
            let consumer = redis::from_redis_value(&fields[1])?;
            let idle_ms: i64 = redis::from_redis_value(&fields[2])?;
            let idle_ms = u64::try_from(idle_ms).map_err(|_| invalid_reply())?;
            Ok((id, consumer, idle_ms))
        })
        .collect()
}

/// Creates a stable error for an invalid internal stream response.
fn invalid_reply() -> RedisError {
    RedisError::from((ErrorKind::TypeError, "invalid Redis Streams response"))
}

#[cfg(test)]
mod tests {
    use redis::Value;

    use super::parse_auto_claim;
    use super::parse_pending_entries;

    #[test]
    fn test_parse_auto_claim_preserves_redis_6_2_nil_tombstones() {
        let value = Value::Array(vec![
            Value::BulkString(b"9-0".to_vec()),
            Value::Array(vec![Value::Array(vec![Value::Nil])]),
        ]);
        let (reply, has_missing_entries) = parse_auto_claim(value).unwrap();
        assert_eq!(reply.next_stream_id, "9-0");
        assert!(reply.claimed.is_empty());
        assert!(has_missing_entries);
    }

    #[test]
    fn test_parse_pending_entries_returns_owner_and_idle_time() {
        let value = Value::Array(vec![Value::Array(vec![
            Value::BulkString(b"1-0".to_vec()),
            Value::BulkString(b"worker-a".to_vec()),
            Value::Int(501),
            Value::Int(2),
        ])]);
        assert_eq!(
            parse_pending_entries(value).unwrap(),
            vec![("1-0".into(), "worker-a".into(), 501)]
        );
    }

    #[test]
    fn test_parse_auto_claim_and_pending_entries_reject_invalid_replies() {
        assert!(parse_auto_claim(Value::Nil).is_err());
        assert!(parse_pending_entries(Value::Array(vec![Value::Nil])).is_err());
        let reply = Value::Array(vec![Value::Array(vec![
            Value::BulkString(b"1-0".to_vec()),
            Value::BulkString(b"worker-a".to_vec()),
            Value::Int(-1),
            Value::Int(1),
        ])]);
        assert!(parse_pending_entries(reply).is_err());
    }

    #[test]
    fn test_parse_pending_entries_rejects_invalid_shapes() {
        assert!(parse_pending_entries(Value::Int(1)).is_err());
        assert!(parse_pending_entries(Value::Array(vec![Value::BulkString(b"bad".to_vec())])).is_err());
        assert!(parse_pending_entries(Value::Array(vec![Value::Array(vec![Value::Nil])])).is_err());
        assert!(
            parse_pending_entries(Value::Array(vec![Value::Array(vec![
                Value::BulkString(b"1-0".to_vec()),
                Value::BulkString(b"worker".to_vec()),
                Value::Int(-1),
                Value::Int(1),
            ])]))
            .is_err()
        );
    }
}
