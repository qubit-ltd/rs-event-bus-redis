// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Result of decoding a Redis event wire record.

use qubit_event_bus::error::SpiError;
use qubit_event_bus::spi::TopicAddress;

use super::poison_reason::PoisonReason;
/// Separates incompatible records from records safe to quarantine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub(crate) enum DecodeFailure {
    /// The record is malformed and can be transferred to quarantine.
    Poison(
        /// Stable deterministic decode category, safe to store without raw
        /// message data.
        PoisonReason,
    ),
    /// The record uses a valid wire version this provider cannot decode.
    UnsupportedVersion,
    /// The original record must remain pending for recovery with larger limits.
    LimitExceeded,
}

impl DecodeFailure {
    /// Converts a non-poison decode failure into its stable receive SPI error.
    ///
    /// # Parameters
    ///
    /// - `topic`: Topic associated with the received record.
    ///
    /// Returns `None` for poison records, which require adapter-owned
    /// maintenance and quarantine I/O. This method performs no I/O.
    #[must_use]
    pub(crate) fn receive_error(self, topic: &TopicAddress) -> Option<SpiError> {
        match self {
            Self::Poison(_) => None,
            Self::LimitExceeded => Some(crate::error::to_spi_error(
                "receive",
                Some(topic),
                crate::redis_provider_error::RedisProviderError::LimitExceeded,
            )),
            Self::UnsupportedVersion => Some(SpiError::Operation {
                provider_id: "redis-streams".into(),
                operation: "receive",
                resource: Some(topic.as_str().into()),
                kind: "unsupported_wire_version",
                retryable: Some(false),
                source: Box::new(crate::redis_provider_error::RedisProviderError::UnsupportedWireVersion),
            }),
        }
    }
}
