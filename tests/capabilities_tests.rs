// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! The sync and async provider modes expose the same fixed capability contract.

#![cfg(any(feature = "sync", feature = "async"))]

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::OrderingCapability;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_event_bus::spi::PublishVisibility;
use qubit_event_bus::spi::ReplayCapability;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::SubscriptionModes;

fn expected() -> EventBusCapabilities {
    EventBusCapabilities::new(
        PayloadModes::Encoded,
        SettlementCapabilities::AcceptRetryReject,
        OrderingCapability::None,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Durable,
        SubscriptionModes::DURABLE,
        true,
        ReplayCapability::Position,
        PublishGuarantee::Accepted,
        PublishVisibility::Opaque,
    )
}

#[cfg(feature = "sync")]
#[test]
fn test_sync_provider_declares_capabilities_without_network_io() {
    use qubit_event_bus_redis::sync::RedisEventBusProvider;
    use qubit_spi::ServiceProvider;

    let bus = RedisEventBusProvider
        .create_configured(&EventBusConfig::default())
        .expect("default provider configuration");
    assert_eq!(bus.capabilities(), expected());
}

#[cfg(feature = "async")]
#[test]
fn test_async_provider_declares_capabilities_without_network_io() {
    use futures_lite::future::block_on;
    use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
    use qubit_spi::AsyncServiceProvider;

    block_on(async {
        let bus = AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default())
            .await
            .expect("default provider configuration");
        assert_eq!(bus.capabilities(), expected());
    });
}
