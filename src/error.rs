// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public Redis provider errors.

#[cfg(any(feature = "sync", feature = "async"))]
use qubit_event_bus::error::SpiError;
#[cfg(any(feature = "sync", feature = "async"))]
use qubit_event_bus::model::PublishEffect;
#[cfg(any(feature = "sync", feature = "async"))]
use qubit_event_bus::spi::TopicAddress;
#[cfg(any(feature = "sync", feature = "async"))]
use redis::ErrorKind;
#[cfg(any(feature = "sync", feature = "async"))]
use redis::RedisError;

pub use crate::redis_provider_error::RedisProviderError;
#[cfg(any(feature = "sync", feature = "async"))]
use crate::redis_provider_error::from_redis_error as classify_redis_error;

/// Converts a sanitized provider failure into the facade's stable SPI shape.
///
/// # Parameters
///
/// `operation` identifies the caller; `topic` is `Some` for topic-scoped
/// failures and `None` for client-wide failures; `source` is the secret-safe
/// cause.
///
/// # Returns
///
/// A stable error category and retryability hint. Unknown settlement and
/// receive results permit retry in their recovery context; unknown publish
/// and quarantine results do not imply safe replay.
#[cfg(any(feature = "sync", feature = "async"))]
pub(crate) fn to_spi_error(
    operation: &'static str,
    topic: Option<&TopicAddress>,
    source: RedisProviderError,
) -> SpiError {
    let (kind, retryable) = match &source {
        RedisProviderError::Configuration(_) => ("configuration", Some(false)),
        RedisProviderError::UnsupportedWireVersion => ("unsupported_wire_version", Some(false)),
        RedisProviderError::LimitExceeded => (
            if operation == "receive" {
                "receive_limit_exceeded"
            } else {
                "publish_limit_exceeded"
            },
            Some(false),
        ),
        RedisProviderError::Transport {
            kind, retryable, ..
        } => (*kind, *retryable),
        RedisProviderError::OutcomeUnknown { operation } => (
            "outcome_unknown",
            Some(matches!(*operation, "settle" | "receive")),
        ),
        RedisProviderError::ResourceLimit { .. } => ("resource_limit", Some(true)),
        RedisProviderError::PayloadTooLarge => ("payload_too_large", Some(false)),
        RedisProviderError::WireTooLarge => ("wire_too_large", Some(false)),
        RedisProviderError::Operation(_) => ("redis_error", None),
    };
    SpiError::Operation {
        provider_id: "redis-streams".into(),
        operation,
        resource: topic.map(|value| value.as_str().into()),
        kind,
        retryable,
        source: Box::new(source),
    }
}

/// Converts a failed publish with admission evidence from the call site.
///
/// # Parameters
///
/// - `topic`: Topic associated with the failed publish.
/// - `source`: Sanitized provider error describing the failure.
/// - `effect`: Evidence indicating whether Redis may have accepted the write.
///
/// # Returns
///
/// A stable SPI publish error without raw client diagnostics.
#[cfg(any(feature = "sync", feature = "async"))]
pub(crate) fn to_publish_error(
    topic: &TopicAddress,
    source: RedisProviderError,
    effect: PublishEffect,
) -> SpiError {
    let error = to_spi_error("publish", Some(topic), source);
    let SpiError::Operation {
        provider_id,
        resource,
        kind,
        retryable,
        source,
        ..
    } = error
    else {
        unreachable!("provider conversion always yields an operation")
    };
    SpiError::Publish {
        provider_id,
        resource,
        kind,
        retryable,
        effect,
        source,
    }
}

/// Classifies failures after XADD entered query. Only actual server error codes
/// prove rejection. Type conversion, protocol, timeout, and disconnect failures
/// remain uncertain.
///
/// # Parameters
///
/// - `topic`: Topic associated with the publish command.
/// - `error`: Redis failure returned after submitting `XADD`.
///
/// # Returns
///
/// A publish SPI error that distinguishes proven rejection from an uncertain
/// write outcome.
#[cfg(any(feature = "sync", feature = "async"))]
pub(crate) fn query_publish_error(topic: &TopicAddress, error: &RedisError) -> SpiError {
    use PublishEffect;
    let effect = if error.code().is_some() {
        PublishEffect::NotAccepted
    } else {
        PublishEffect::MayHaveBeenAccepted
    };
    let source = if error.kind() == ErrorKind::TypeError && error.code().is_none() {
        RedisProviderError::Transport {
            operation: "publish",
            kind: "protocol",
            retryable: Some(false),
        }
    } else if error.code().is_none() {
        RedisProviderError::OutcomeUnknown {
            operation: "publish",
        }
    } else {
        classify_redis_error("publish", error)
    };
    to_publish_error(topic, source, effect)
}

/// Reports a syntactically invalid XADD acknowledgement after the command was
/// submitted.
///
/// # Parameters
///
/// - `topic`: Topic associated with the submitted publish command.
///
/// # Returns
///
/// A publish SPI error with an uncertain acceptance outcome.
#[cfg(any(feature = "sync", feature = "async"))]
pub(crate) fn invalid_publish_reply(topic: &TopicAddress) -> SpiError {
    to_publish_error(
        topic,
        RedisProviderError::OutcomeUnknown {
            operation: "publish",
        },
        PublishEffect::MayHaveBeenAccepted,
    )
}

/// Converts a Redis client failure using stable kind and retryability rules.
///
/// # Parameters
///
/// - `operation`: Static operation category used by the facade.
/// - `topic`: `Some` identifies the topic; `None` describes a client-wide
///   failure.
/// - `source`: Borrowed Redis diagnostic classified without preserving raw
///   text.
///
/// # Returns
///
/// A sanitized SPI failure carrying the known category and retryability hint.
#[cfg(any(feature = "sync", feature = "async"))]
pub(crate) fn from_redis_error(
    operation: &'static str,
    topic: Option<&TopicAddress>,
    source: &RedisError,
) -> SpiError {
    to_spi_error(operation, topic, classify_redis_error(operation, source))
}
