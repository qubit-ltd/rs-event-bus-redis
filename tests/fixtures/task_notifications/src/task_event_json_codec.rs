// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! JSON codec used by the application notification consumer.

use std::sync::Arc;

use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::CodecError;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::SchemaId;
use qubit_task::event::TaskEvent;
use serde_json::from_slice;
use serde_json::to_vec;

/// Encodes lifecycle snapshots without adding application transaction
/// guarantees.
pub struct TaskEventJsonCodec(pub ContentType);

impl EventCodec<TaskEvent> for TaskEventJsonCodec {
    fn content_type(&self) -> &ContentType {
        &self.0
    }
    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }
    fn encode(&self, value: &TaskEvent) -> Result<Arc<[u8]>, CodecError> {
        to_vec(value).map(Arc::from).map_err(|source| CodecError::Encode {
            source: Box::new(source),
        })
    }
    fn decode(&self, bytes: &[u8]) -> Result<TaskEvent, CodecError> {
        from_slice(bytes).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}
