// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public lazy provider creation and inventory identity.

use futures_lite::future::block_on;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
use qubit_spi::AsyncServiceProvider;
use qubit_spi::ProviderMetadata;

#[test]
fn test_provider_metadata_has_the_expected_identifier() {
    assert_eq!(
        ProviderMetadata::descriptor(&AsyncRedisEventBusProvider).id().as_str(),
        "redis-streams"
    );
}

#[test]
fn test_configured_provider_validates_options_without_connecting() {
    block_on(async {
        let provider = AsyncRedisEventBusProvider;
        let bus = provider
            .create_configured(&EventBusConfig::default())
            .await
            .expect("default settings create a lazy Redis SPI");
        let _capabilities = bus.capabilities();

        let options: ProviderOptions = [("redis.max_unsettled_per_subscription".into(), "0".into())].into();
        let invalid = EventBusConfig::default().with_provider_options(options);
        assert!(provider.create_configured(&invalid).await.is_err());
    });
}

#[cfg(feature = "async")]
#[test]
fn test_async_provider_returns_invalid_configuration_without_connecting() {
    let options: ProviderOptions = [("redis.max_idle_connections".into(), "0".into())].into();
    let config = EventBusConfig::default().with_provider_options(options);
    assert!(block_on(AsyncRedisEventBusProvider.create_configured(&config)).is_err());

    let options: ProviderOptions = [
        ("redis.sentinel.nodes".into(), "invalid:port/not-a-db".into()),
        ("redis.sentinel.service_name".into(), "primary".into()),
    ]
    .into();
    let config = EventBusConfig::default().with_provider_options(options);
    assert!(block_on(AsyncRedisEventBusProvider.create_configured(&config)).is_err());
}
