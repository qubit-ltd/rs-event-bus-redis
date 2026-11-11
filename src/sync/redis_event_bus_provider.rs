// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Synchronous provider factory and SPI implementation.

use std::sync::Arc;

use qubit_event_bus::EventBusProviderError;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::registry::EventBusSpec;
use qubit_event_bus::spi::EventBusSpi;
use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderMetadata;
use qubit_spi::ServiceProvider;
use qubit_spi::error::ProviderFailure;
use qubit_spi::provider_descriptor;

use super::redis_event_bus::RedisEventBus;
use crate::client::Client;
use crate::config::RedisEventBusConfig;
use crate::diagnostics::RedisProviderMode;
use crate::error::RedisProviderError;

/// Factory registered for synchronous Redis Streams access.
///
/// It validates provider options and creates the SPI implementation without
/// opening a network connection. Short commands acquire admission-owning
/// leases and reuse the bounded standalone idle pool; each receiver owns a
/// dedicated connection until close or drop. Sentinel leases resolve the master
/// afresh.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::registry::EventBusConfig;
/// use qubit_event_bus::spi::EventBusSpi;
/// use qubit_event_bus_redis::sync::RedisEventBusProvider;
/// use qubit_spi::ServiceProvider;
///
/// let config = EventBusConfig::default();
/// let bus = RedisEventBusProvider
///     .create_configured(&config)
///     .expect("default Redis settings are valid");
/// let _capabilities = bus.capabilities();
/// ```
pub struct RedisEventBusProvider;

impl ProviderMetadata for RedisEventBusProvider {
    /// Returns the stable registration descriptor used by the sync inventory.
    ///
    /// # Returns
    ///
    /// The descriptor identifying this provider as `redis-streams`.
    fn descriptor(&self) -> ProviderDescriptor {
        provider_descriptor!("redis-streams")
    }
}

impl ServiceProvider<EventBusSpec> for RedisEventBusProvider {
    /// Validates provider options and creates the synchronous Redis SPI.
    ///
    /// Creating the SPI parses connection settings but does not connect to
    /// Redis. Network failures are returned by later SPI operations.
    ///
    /// # Parameters
    ///
    /// - `config`: Event-bus configuration containing Redis provider options.
    ///
    /// # Returns
    ///
    /// A shared event-bus SPI implementation.
    ///
    /// # Errors
    ///
    /// Returns an invalid-configuration failure when provider options or the
    /// Redis client configuration are invalid.
    fn create_configured(
        &self,
        config: &EventBusConfig,
    ) -> Result<Arc<dyn EventBusSpi>, ProviderFailure<EventBusProviderError>> {
        let settings = RedisEventBusConfig::from_event_bus_config(config).map_err(|error| {
            ProviderFailure::invalid_configuration(EventBusProviderError::provider(error))
        })?;
        let mut client = Client::new(&settings).map_err(|_| {
            ProviderFailure::invalid_configuration(EventBusProviderError::provider(
                RedisProviderError::Configuration("invalid Redis connection configuration"),
            ))
        })?;
        client
            .attach_diagnostics(RedisProviderMode::Sync, settings.namespace())
            .map_err(|error| {
                ProviderFailure::invalid_configuration(EventBusProviderError::provider(error))
            })?;
        Ok(Arc::new(RedisEventBus {
            client: Arc::new(client),
            settings,
        }))
    }
}
