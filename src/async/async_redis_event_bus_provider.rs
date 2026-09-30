// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous provider factory and SPI implementation.

use std::sync::Arc;

use qubit_event_bus::EventBusProviderError;
use qubit_event_bus::SpiError;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::registry::EventBusSpec;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::TopicAddress;
use qubit_spi::AsyncServiceProvider;
use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderFuture;
use qubit_spi::ProviderMetadata;
use qubit_spi::error::ProviderFailure;
use qubit_spi::provider_descriptor;

use super::async_redis_event_bus::AsyncRedisEventBus;
use crate::client::Client;
use crate::config::RedisEventBusConfig;
use crate::error::RedisProviderError;

/// Factory registered for runtime-neutral asynchronous Redis Streams access.
///
/// The provider creates Redis clients and async SPI objects without choosing or
/// starting an executor. The host application drives the returned futures.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::registry::EventBusConfig;
/// use qubit_event_bus::spi::AsyncEventBusSpi;
/// use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
/// use qubit_spi::AsyncServiceProvider;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = EventBusConfig::default();
/// let bus = AsyncRedisEventBusProvider
///     .create_configured(&config)
///     .await
///     .map_err(|failure| failure.into_error())?;
/// assert_eq!(bus.capabilities().payload_modes(), qubit_event_bus::spi::PayloadModes::Encoded);
/// # Ok(())
/// # }
/// # futures_lite::future::block_on(example()).expect("lazy provider creation succeeds");
/// ```
pub struct AsyncRedisEventBusProvider;

impl ProviderMetadata for AsyncRedisEventBusProvider {
    /// Returns the stable registration descriptor used by both SPI inventories.
    ///
    /// # Returns
    ///
    /// The descriptor identifying this provider as `redis-streams`.
    fn descriptor(&self) -> ProviderDescriptor {
        provider_descriptor!("redis-streams")
    }
}

impl AsyncServiceProvider<EventBusSpec> for AsyncRedisEventBusProvider {
    /// Validates provider options and creates an asynchronous Redis backend.
    ///
    /// The returned future is runtime-neutral. The host executor must poll it;
    /// this method does not start a runtime or connect until an SPI operation
    /// needs a Redis connection.
    ///
    /// # Parameters
    ///
    /// - `config`: Event-bus configuration containing Redis provider options.
    ///
    /// # Type Parameters
    ///
    /// - `'a`: Lifetime shared by the provider borrow, configuration borrow,
    ///   and returned future.
    ///
    /// # Returns
    ///
    /// A future that resolves to an async SPI implementation.
    ///
    /// # Errors
    ///
    /// Resolves to an invalid-configuration failure when settings or the Redis
    /// client configuration cannot be constructed.
    fn create_configured<'a>(
        &'a self,
        config: &'a EventBusConfig,
    ) -> ProviderFuture<'a, Result<Arc<dyn AsyncEventBusSpi>, ProviderFailure<EventBusProviderError>>> {
        Box::pin(async move {
            let settings = RedisEventBusConfig::from_event_bus_config(config)
                .map_err(|error| ProviderFailure::invalid_configuration(EventBusProviderError::provider(error)))?;
            let client = Client::new(&settings).map_err(|_| {
                ProviderFailure::invalid_configuration(EventBusProviderError::provider(
                    RedisProviderError::Configuration("invalid Redis connection configuration"),
                ))
            })?;
            Ok(Arc::new(AsyncRedisEventBus {
                client: Arc::new(client),
                settings,
            }) as Arc<dyn AsyncEventBusSpi>)
        })
    }
}

/// Classifies an internal failure into a secret-safe SPI operation error.
///
/// Retryability follows the failure category and operation context: unknown
/// publish outcomes forbid a claim of safe replay, while unknown settlement
/// and receive outcomes permit their constrained recovery paths.
///
/// # Parameters
///
/// - `operation`: Stable operation name used by the facade.
/// - `topic`: Optional topic identifying the affected Redis resource.
/// - `source`: Sanitized provider failure category.
///
/// # Returns
///
/// An SPI error that omits the raw Redis client diagnostic.
#[inline]
pub(super) fn spi_error(operation: &'static str, topic: Option<&TopicAddress>, source: RedisProviderError) -> SpiError {
    crate::error::to_spi_error(operation, topic, source)
}
