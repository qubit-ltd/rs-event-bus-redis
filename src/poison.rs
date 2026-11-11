// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Serialized quarantine transfer; Redis Lua does not roll back partial writes.

#[cfg(feature = "sync")]
use redis::ConnectionLike;
use redis::RedisError;
use redis::Value;
#[cfg(feature = "async")]
use redis::aio::MultiplexedConnection;
use redis::cmd;

use crate::error::RedisProviderError;
pub(crate) use crate::internal::PoisonOutcome;
pub(crate) use crate::internal::PoisonReason;

/// Lua preflights types and ownership, copies the last wire field, then ACKs.
const QUARANTINE_SCRIPT: &str = r#"
local source_type = redis.call('TYPE', KEYS[1]).ok
if source_type == 'none' then return 0 end
if source_type ~= 'stream' then return redis.error_reply('WRONGTYPE source must be a stream') end
local quarantine_type = redis.call('TYPE', KEYS[2]).ok
if quarantine_type ~= 'none' and quarantine_type ~= 'stream' then
    return redis.error_reply('WRONGTYPE quarantine must be a stream')
end
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1)
if #pending == 0 then return 0 end
if pending[1][2] ~= ARGV[2] then return -1 end
local rows = redis.call('XRANGE', KEYS[1], ARGV[3], ARGV[3])
if #rows == 0 then
    local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], ARGV[3])
    if acknowledged == 1 then return 2 end
    return 0
end
local fields = rows[1][2]
local wire = ''
local missing = '1'
for index = 1, #fields, 2 do
    if fields[index] == 'wire' then
        wire = fields[index + 1]
        missing = '0'
    end
end
redis.call('XADD', KEYS[2], '*',
    'source_stream', KEYS[1],
    'source_id', ARGV[3],
    'group', ARGV[1],
    'reason', ARGV[4],
    'wire', wire,
    'wire_missing', missing)
local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], ARGV[3])
if acknowledged ~= 1 then return -2 end
return 1
"#;

/// Transfers a malformed pending entry without interleaving other commands.
///
/// # Type Parameters
///
/// - `C`: Controlled blocking transport implementing Redis command execution.
///
/// # Parameters
///
/// - `connection`: Redis connection used for the single Lua command.
/// - `source`: Stream key from which the malformed entry was read.
/// - `quarantine`: Per-group quarantine stream key.
/// - `group`: Consumer group whose pending entry is being settled.
/// - `consumer`: Consumer that currently owns the pending entry.
/// - `id`: Redis stream ID of the malformed entry.
/// - `reason`: Stable decode failure category.
///
/// # Returns
///
/// A fixed transfer result; quarantined bytes are never returned to Rust.
///
/// # Errors
///
/// Returns a Redis error if the script cannot execute or returns an unknown
/// status. Type and owner checks precede writes, but Redis Lua does not roll
/// back an `XADD` when a subsequent `XACK` fails. A lost reply can hide a
/// successful copy and acknowledgement; callers must preserve that uncertainty
/// and inspect the source PEL before any later recovery attempt.
#[cfg(feature = "sync")]
pub(crate) fn quarantine<C: ConnectionLike>(
    connection: &mut C,
    source: &str,
    quarantine: &str,
    group: &str,
    consumer: &str,
    id: &str,
    reason: PoisonReason,
) -> Result<PoisonOutcome, RedisProviderError> {
    let result: Result<Value, RedisError> = cmd("EVAL")
        .arg(QUARANTINE_SCRIPT)
        .arg(2)
        .arg(source)
        .arg(quarantine)
        .arg(group)
        .arg(consumer)
        .arg(id)
        .arg(reason.as_str())
        .query(connection);
    parse_quarantine_reply(result)
}

/// Executes the quarantine script on a dedicated async receiver connection.
///
/// # Parameters
///
/// - `connection`: Controlled receiver connection executing one Redis script.
/// - `source`: Source stream key whose pending record is being recovered.
/// - `quarantine`: Group-specific destination stream key.
/// - `group`: Consumer group whose PEL owner must match.
/// - `consumer`: Consumer currently responsible for the source record.
/// - `id`: Source stream ID used to associate any quarantine copy.
/// - `reason`: Stable decode failure category stored beside the raw wire.
///
/// # Returns
///
/// A normalized transfer result without bringing the quarantined wire back to
/// Rust. The script can copy a record and acknowledge its PEL entry.
///
/// # Errors
///
/// Returns a Redis error for a failed script or unknown status. Redis Lua does
/// not roll back prior writes, and cancellation or reply loss can conceal a
/// completed transfer. Callers must report an unknown outcome conservatively.
#[cfg(feature = "async")]
pub(crate) async fn quarantine_async(
    connection: &mut MultiplexedConnection,
    source: &str,
    quarantine: &str,
    group: &str,
    consumer: &str,
    id: &str,
    reason: PoisonReason,
) -> Result<PoisonOutcome, RedisProviderError> {
    let result: Result<Value, RedisError> = cmd("EVAL")
        .arg(QUARANTINE_SCRIPT)
        .arg(2)
        .arg(source)
        .arg(quarantine)
        .arg(group)
        .arg(consumer)
        .arg(id)
        .arg(reason.as_str())
        .query_async(connection)
        .await;
    parse_quarantine_reply(result)
}

/// Converts the quarantine EVAL result into a confirmed outcome or a sanitized
/// error. Only a server-level permission response proves that the script did
/// not begin executing; every other failure can hide partial writes.
///
/// # Parameters
///
/// - `result`: The owned EVAL response, or the Redis error returned while
///   reading it.
///
/// # Returns
///
/// The confirmed quarantine outcome when Redis returns a recognized integer
/// status.
///
/// # Errors
///
/// Returns a sanitized authentication error for a direct permission rejection,
/// or an unknown outcome when the script's side effects cannot be confirmed.
fn parse_quarantine_reply(result: Result<Value, RedisError>) -> Result<PoisonOutcome, RedisProviderError> {
    let status = match result {
        Ok(Value::Int(status)) => status,
        Ok(Value::ServerError(error)) => {
            if is_direct_permission_error(error.code(), error.details()) {
                return Err(RedisProviderError::Transport {
                    operation: "quarantine",
                    kind: "authentication",
                    retryable: Some(false),
                });
            }
            return Err(unknown_quarantine_outcome());
        }
        Ok(_) => return Err(unknown_quarantine_outcome()),
        Err(error) => {
            if is_direct_permission_error(error.code().unwrap_or_default(), error.detail()) {
                return Err(RedisProviderError::Transport {
                    operation: "quarantine",
                    kind: "authentication",
                    retryable: Some(false),
                });
            }
            return Err(unknown_quarantine_outcome());
        }
    };
    match status {
        1 => Ok(PoisonOutcome::Quarantined),
        0 => Ok(PoisonOutcome::SourceGone),
        2 => Ok(PoisonOutcome::TombstoneCleared),
        -1 => Ok(PoisonOutcome::OwnershipChanged),
        _ => Err(unknown_quarantine_outcome()),
    }
}

/// Determines whether a structured server error proves EVAL was rejected before
/// execution.
///
/// # Parameters
///
/// - `code`: The structured Redis error code.
/// - `detail`: The optional server detail used to exclude errors raised inside
///   a Lua script.
fn is_direct_permission_error(code: &str, detail: Option<&str>) -> bool {
    matches!(code, "NOPERM" | "NOAUTH" | "WRONGPASS")
        && !detail.is_some_and(|detail| detail.starts_with("user_script:"))
}

/// Creates a sanitized error for a quarantine result whose effects are
/// unconfirmed.
///
/// The error deliberately has no source so server diagnostics and record data
/// cannot leak.
fn unknown_quarantine_outcome() -> RedisProviderError {
    RedisProviderError::OutcomeUnknown {
        operation: "quarantine",
    }
}

#[cfg(test)]
mod poison_tests {
    use std::error::Error;

    use redis::Value;

    use super::parse_quarantine_reply;
    use crate::error::RedisProviderError;
    use crate::internal::PoisonOutcome;

    fn redis_error(reply: &[u8]) -> redis::RedisError {
        match redis::parse_redis_value(reply).expect("parse test server error") {
            Value::ServerError(error) => error.into(),
            other => panic!("expected server error, got {other:?}"),
        }
    }

    fn assert_unknown(result: Result<Value, redis::RedisError>) {
        let error = parse_quarantine_reply(result).expect_err("ambiguous quarantine result");
        assert!(matches!(
            error,
            RedisProviderError::OutcomeUnknown {
                operation: "quarantine"
            }
        ));
        assert!(!error.to_string().contains("fixture-secret"));
        assert!(Error::source(&error).is_none());
    }

    #[test]
    fn test_poison_quarantine_reply_classifies_only_direct_permission_codes() {
        for code in ["NOPERM", "NOAUTH", "WRONGPASS"] {
            let error = parse_quarantine_reply(Err(redis_error(format!("-{code} fixture-secret\r\n").as_bytes())))
                .expect_err("direct permission failure");
            assert!(matches!(
                error,
                RedisProviderError::Transport {
                    operation: "quarantine",
                    kind: "authentication",
                    retryable: Some(false)
                }
            ));
            assert!(!error.to_string().contains("fixture-secret"));
            assert!(Error::source(&error).is_none());
        }
    }

    #[test]
    fn test_poison_quarantine_reply_requires_exact_integer_status() {
        for (value, expected) in [
            (Value::Int(1), PoisonOutcome::Quarantined),
            (Value::Int(0), PoisonOutcome::SourceGone),
            (Value::Int(2), PoisonOutcome::TombstoneCleared),
            (Value::Int(-1), PoisonOutcome::OwnershipChanged),
        ] {
            assert!(matches!(parse_quarantine_reply(Ok(value)), Ok(outcome) if outcome == expected));
        }
        let nested_error = redis::ServerError::try_from(redis_error(b"-ERR fixture-secret\r\n")).expect("server error");
        for value in [
            Value::Int(9),
            Value::BulkString(b"1".to_vec()),
            Value::Array(vec![Value::ServerError(nested_error)]),
        ] {
            assert_unknown(Ok(value));
        }
        assert_unknown(Err(redis_error(
            b"-ERR ACL failure in script: NOPERM fixture-secret\r\n",
        )));
        assert_unknown(Err(redis_error(b"-ERR fixture-secret\r\n")));
        assert_unknown(Err(redis::RedisError::from(std::io::Error::other("fixture-secret"))));
    }
}
