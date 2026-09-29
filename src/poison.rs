// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Atomic transfer of malformed pending entries into a quarantine stream.

#[cfg(feature = "sync")]
use redis::ConnectionLike;
use redis::RedisError;
use redis::cmd;

pub(crate) use crate::internal::PoisonOutcome;
pub(crate) use crate::internal::PoisonReason;

/// Lua transfer checks ownership, copies the raw wire field, then acknowledges.
const QUARANTINE_SCRIPT: &str = r#"
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
local missing = '0'
for index = 1, #fields, 2 do
    if fields[index] == 'wire' then
        wire = fields[index + 1]
        break
    end
    if index == #fields - 1 then missing = '1' end
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

/// Transfers a malformed pending entry to its quarantine stream atomically.
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
/// status. The original pending entry remains available for retry when the
/// script fails before its atomic `XADD` and `XACK` sequence completes.
#[cfg(feature = "sync")]
pub(crate) fn quarantine<C: ConnectionLike>(
    connection: &mut C,
    source: &str,
    quarantine: &str,
    group: &str,
    consumer: &str,
    id: &str,
    reason: PoisonReason,
) -> Result<PoisonOutcome, RedisError> {
    let status: i64 = cmd("EVAL")
        .arg(QUARANTINE_SCRIPT)
        .arg(2)
        .arg(source)
        .arg(quarantine)
        .arg(group)
        .arg(consumer)
        .arg(id)
        .arg(reason.as_str())
        .query(connection)?;
    match status {
        1 => Ok(PoisonOutcome::Quarantined),
        0 => Ok(PoisonOutcome::SourceGone),
        2 => Ok(PoisonOutcome::TombstoneCleared),
        -1 => Ok(PoisonOutcome::OwnershipChanged),
        _ => Err(RedisError::from((
            redis::ErrorKind::ResponseError,
            "invalid quarantine result",
        ))),
    }
}

/// Executes the same quarantine script on Redis's multiplexed async connection.
#[cfg(feature = "async")]
pub(crate) async fn quarantine_async(
    connection: &mut redis::aio::MultiplexedConnection,
    source: &str,
    quarantine: &str,
    group: &str,
    consumer: &str,
    id: &str,
    reason: PoisonReason,
) -> Result<PoisonOutcome, RedisError> {
    let status: i64 = cmd("EVAL")
        .arg(QUARANTINE_SCRIPT)
        .arg(2)
        .arg(source)
        .arg(quarantine)
        .arg(group)
        .arg(consumer)
        .arg(id)
        .arg(reason.as_str())
        .query_async(connection)
        .await?;
    match status {
        1 => Ok(PoisonOutcome::Quarantined),
        0 => Ok(PoisonOutcome::SourceGone),
        2 => Ok(PoisonOutcome::TombstoneCleared),
        -1 => Ok(PoisonOutcome::OwnershipChanged),
        _ => Err(RedisError::from((
            redis::ErrorKind::ResponseError,
            "invalid quarantine result",
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::PoisonReason;

    #[test]
    fn test_poison_reasons_have_stable_secret_free_names() {
        assert_eq!(PoisonReason::MissingWire.as_str(), "missing_wire");
        assert_eq!(PoisonReason::InvalidWireField.as_str(), "invalid_wire_field");
        assert_eq!(PoisonReason::InvalidJson.as_str(), "invalid_json");
        assert_eq!(PoisonReason::InvalidEventMetadata.as_str(), "invalid_event_metadata");
    }
}
