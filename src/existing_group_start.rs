// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Subscription policy for an already existing Redis consumer group.

use qubit_event_bus::SpiError;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::spi::SpiSubscriptionRequest;

const OPTION: &str = "redis.existing_group_start";

/// How to handle an existing group's cursor when a start position was
/// requested.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExistingGroupStartPolicy {
    /// Reject explicit positions that Redis cannot apply to an existing group.
    Reject,
    /// Keep the existing group's cursor even if an explicit position was given.
    Resume,
}

/// Returns a non-retryable option error tied to the subscription topic.
fn option_error(request: &SpiSubscriptionRequest) -> SpiError {
    SpiError::Operation {
        provider_id: "redis-streams".into(),
        operation: "subscribe",
        resource: Some(request.topic().as_str().into()),
        kind: "invalid_subscription_option",
        retryable: Some(false),
        source: Box::new(std::io::Error::other("invalid Redis subscription option")),
    }
}

/// Parses the existing-group policy from one request before Redis I/O.
///
/// Returns an `invalid_subscription_option` error for an unsupported policy
/// value or any other `redis.*` subscription key.
pub(crate) fn parse_existing_group_policy(
    request: &SpiSubscriptionRequest,
) -> Result<ExistingGroupStartPolicy, SpiError> {
    if request
        .provider_options()
        .keys()
        .any(|key| key.starts_with("redis.") && key != OPTION)
    {
        return Err(option_error(request));
    }
    match request.provider_options().get(OPTION).map(String::as_str) {
        None | Some("reject") => Ok(ExistingGroupStartPolicy::Reject),
        Some("resume") => Ok(ExistingGroupStartPolicy::Resume),
        Some(_) => Err(option_error(request)),
    }
}

/// Checks whether an existing group may retain its cursor for this request.
///
/// `New` always resumes the existing cursor. Explicit positions need the
/// `Resume` policy; otherwise this returns a non-retryable error because Redis
/// cannot move an existing group's cursor with `XGROUP CREATE`.
pub(crate) fn ensure_existing_group_start(
    request: &SpiSubscriptionRequest,
    policy: ExistingGroupStartPolicy,
) -> Result<(), SpiError> {
    if matches!(request.start_position(), StartPosition::New)
        || policy == ExistingGroupStartPolicy::Resume
    {
        return Ok(());
    }
    Err(SpiError::Operation {
        provider_id: "redis-streams".into(),
        operation: "subscribe",
        resource: Some(request.topic().as_str().into()),
        kind: "existing_group_start_position_ignored",
        retryable: Some(false),
        source: Box::new(std::io::Error::other(
            "existing Redis group keeps its cursor; use redis.existing_group_start=resume",
        )),
    })
}

#[cfg(test)]
mod tests {
    use std::any::TypeId;

    use qubit_event_bus::model::ProviderOptions;
    use qubit_event_bus::model::StartPosition;
    use qubit_event_bus::model::SubscriberId;
    use qubit_event_bus::model::SubscriptionDurability;
    use qubit_event_bus::spi::SpiSubscriptionRequest;
    use qubit_event_bus::spi::TopicAddress;
    use qubit_id::Id;

    use super::ExistingGroupStartPolicy;
    use super::ensure_existing_group_start;
    use super::parse_existing_group_policy;

    /// Builds a valid durable request with the desired cursor and options.
    fn request(position: StartPosition, options: ProviderOptions) -> SpiSubscriptionRequest {
        SpiSubscriptionRequest::new(
            Id::new(1),
            TopicAddress::new("orders.created").expect("valid topic"),
            SubscriberId::new("audit").expect("valid subscriber"),
            None,
            SubscriptionDurability::Durable,
            position,
            options,
            TypeId::of::<String>(),
        )
    }

    #[test]
    fn test_existing_group_policy_defaults_to_reject() {
        let request = request(StartPosition::New, ProviderOptions::new());
        assert_eq!(
            parse_existing_group_policy(&request).expect("empty options are valid"),
            ExistingGroupStartPolicy::Reject
        );
    }

    #[test]
    fn test_existing_group_policy_accepts_reject_and_resume() {
        for (value, expected) in [
            ("reject", ExistingGroupStartPolicy::Reject),
            ("resume", ExistingGroupStartPolicy::Resume),
        ] {
            let value: &str = value;
            let options: ProviderOptions = ProviderOptions::from([(
                "redis.existing_group_start".to_owned(),
                value.to_owned(),
            )]);
            let request = request(StartPosition::New, options);
            assert_eq!(
                parse_existing_group_policy(&request).expect("supported policy is valid"),
                expected
            );
        }
    }

    #[test]
    fn test_existing_group_policy_rejects_invalid_value_and_unknown_redis_key() {
        for options in [
            ProviderOptions::from([("redis.existing_group_start".into(), "ignore".into())]),
            ProviderOptions::from([("redis.unknown".into(), "resume".into())]),
        ] {
            let request = request(StartPosition::New, options);
            let error = parse_existing_group_policy(&request).expect_err("option is invalid");
            assert_eq!(error.kind(), "invalid_subscription_option");
        }
    }

    #[test]
    fn test_existing_group_start_accepts_new_without_resume() {
        let request = request(StartPosition::New, ProviderOptions::new());
        ensure_existing_group_start(&request, ExistingGroupStartPolicy::Reject)
            .expect("new position can resume existing group cursor");
    }

    #[test]
    fn test_existing_group_start_rejects_explicit_position_by_default() {
        for position in [StartPosition::Earliest, StartPosition::At("42-0".into())] {
            let request = request(position, ProviderOptions::new());
            let error = ensure_existing_group_start(&request, ExistingGroupStartPolicy::Reject)
                .expect_err("existing cursor overrides the requested position");
            assert_eq!(error.kind(), "existing_group_start_position_ignored");
        }
    }

    #[test]
    fn test_existing_group_start_resumes_explicit_position_when_requested() {
        let options =
            ProviderOptions::from([("redis.existing_group_start".into(), "resume".into())]);
        for position in [StartPosition::Earliest, StartPosition::At("42-0".into())] {
            let request = request(position, options.clone());
            let policy = parse_existing_group_policy(&request).expect("resume option is valid");
            ensure_existing_group_start(&request, policy)
                .expect("explicit resume retains the existing cursor");
        }
    }
}
