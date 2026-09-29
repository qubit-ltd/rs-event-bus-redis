// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared Redis wire decoding and receiver-bound message construction.

use std::sync::Arc;
use std::sync::Mutex;

use qubit_event_bus::spi::InboundMessage;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::TopicAddress;
use qubit_id::Id;
use redis::FromRedisValue;
use redis::streams::StreamId;

use super::decode_failure::DecodeFailure;
use super::poison_reason::PoisonReason;
use super::recovery_state::RecoveryState;
use super::settlement_state::SettlementState;
use crate::error::RedisProviderError;
use crate::wire::WireFields;

/// Decodes wire fields and binds the resulting token to the receiving
/// subscription.
pub(crate) fn decode_entry(
    subscription_id: Id,
    topic: &TopicAddress,
    stream: &str,
    group: &str,
    recovery: &Arc<Mutex<RecoveryState>>,
    entry: StreamId,
) -> Result<InboundMessage, DecodeFailure> {
    let Some(value) = entry.map.get("wire") else {
        return Err(DecodeFailure::Poison(PoisonReason::MissingWire));
    };
    let encoded: String =
        String::from_redis_value(value).map_err(|_| DecodeFailure::Poison(PoisonReason::InvalidWireField))?;
    let fields = WireFields::decode_wire(&encoded).map_err(|error| {
        if matches!(error, RedisProviderError::UnsupportedWireVersion) {
            DecodeFailure::UnsupportedVersion
        } else {
            DecodeFailure::Poison(PoisonReason::InvalidJson)
        }
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
            disposition: Arc::new(Mutex::new(None)),
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
