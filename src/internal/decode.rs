// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared Redis wire decoding and receiver-bound message construction.

use std::str::from_utf8;
use std::sync::Arc;
use std::sync::Mutex;

use qubit_event_bus::spi::InboundMessage;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::TopicAddress;
use qubit_id::Id;
use redis::Value;
use redis::streams::StreamId;

use super::decode_failure::DecodeFailure;
use super::poison_reason::PoisonReason;
use super::recovery_state::RecoveryState;
use super::settlement_progress::SettlementProgress;
use super::settlement_state::SettlementState;
use super::wire_limits::WireLimits;
use crate::error::RedisProviderError;
use crate::wire::WireFields;

/// Decodes wire fields and binds the resulting token to the receiving
/// subscription.
///
/// # Parameters
///
/// - `subscription_id`: Facade identity bound into the settlement token.
/// - `topic`: Topic supplied by the receiving group rather than the wire.
/// - `stream`: Redis source key recorded in the settlement state.
/// - `group`: Consumer group acknowledged by terminal settlement.
/// - `recovery`: Shared active-delivery and cursor state bound to the token.
/// - `entry`: Owned Redis record whose wire bytes are borrowed during decoding.
/// - `limits`: Inclusive wire and decoded payload byte budgets.
///
/// # Returns
///
/// A typed inbound message carrying an open receiver-bound settlement token.
/// No Redis commands are sent and recovery state is not changed here.
///
/// # Errors
///
/// Returns `UnsupportedVersion` for an unknown numeric version within the wire
/// budget. Other failures return a stable poison reason for missing fields,
/// unsupported RESP values, oversized bytes, invalid UTF-8/JSON, or malformed
/// event metadata. The original wire bytes are never cloned into a String.
pub(crate) fn decode_entry(
    subscription_id: Id,
    topic: &TopicAddress,
    stream: &str,
    group: &str,
    recovery: &Arc<Mutex<RecoveryState>>,
    entry: StreamId,
    limits: WireLimits,
) -> Result<InboundMessage, DecodeFailure> {
    let Some(value) = entry.map.get("wire") else {
        return Err(DecodeFailure::Poison(PoisonReason::MissingWire));
    };
    let bytes = match value {
        Value::BulkString(bytes) => bytes.as_slice(),
        Value::SimpleString(value) => value.as_bytes(),
        _ => return Err(DecodeFailure::Poison(PoisonReason::InvalidWireField)),
    };
    limits
        .check_wire(bytes.len())
        .map_err(|_| DecodeFailure::LimitExceeded)?;
    let encoded = from_utf8(bytes).map_err(|_| DecodeFailure::Poison(PoisonReason::InvalidWireField))?;
    let fields = WireFields::decode_wire(encoded, limits).map_err(|error| match error {
        RedisProviderError::UnsupportedWireVersion => DecodeFailure::UnsupportedVersion,
        RedisProviderError::LimitExceeded | RedisProviderError::WireTooLarge | RedisProviderError::PayloadTooLarge => {
            DecodeFailure::LimitExceeded
        }
        _ => DecodeFailure::Poison(PoisonReason::InvalidJson),
    })?;
    let (message_topic, event_id, timestamp, headers, ordering_key, payload) =
        fields.into_parts(topic.clone()).map_err(|error| {
            if matches!(error, RedisProviderError::UnsupportedWireVersion) {
                DecodeFailure::UnsupportedVersion
            } else {
                DecodeFailure::Poison(PoisonReason::InvalidEventMetadata)
            }
        })?;
    let settlement = SettlementToken::new(
        subscription_id,
        SettlementState {
            stream: stream.to_owned(),
            group: group.to_owned(),
            message_id: entry.id,
            progress: Arc::new(Mutex::new(SettlementProgress::Open)),
            recovery: Arc::clone(recovery),
        },
    );
    Ok(InboundMessage::new(
        message_topic,
        event_id,
        timestamp,
        headers,
        ordering_key,
        payload,
        Some(settlement),
        Default::default(),
    ))
}
