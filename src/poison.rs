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
use redis::ErrorKind;
use redis::RedisError;
#[cfg(feature = "async")]
use redis::aio::MultiplexedConnection;
use redis::cmd;

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
            ErrorKind::ResponseError,
            "invalid quarantine result",
        ))),
    }
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
            ErrorKind::ResponseError,
            "invalid quarantine result",
        ))),
    }
}
