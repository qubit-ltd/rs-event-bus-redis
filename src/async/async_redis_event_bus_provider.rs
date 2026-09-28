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
/// let _capabilities = bus.capabilities();
/// # Ok(())
/// # }
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

/// Converts an internal failure into a retryable, secret-safe SPI operation
/// error.
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
pub(super) fn spi_error(operation: &'static str, topic: Option<&TopicAddress>, source: RedisProviderError) -> SpiError {
    crate::error::to_spi_error(operation, topic, source)
}

#[cfg(test)]
mod tests {
    use qubit_event_bus::EventBusConfig;
    use qubit_event_bus::model::ProviderOptions;
    use qubit_spi::AsyncServiceProvider;
    use qubit_spi::ProviderMetadata;

    use super::AsyncRedisEventBusProvider;

    #[test]
    fn provider_metadata_has_the_expected_identifier() {
        assert_eq!(
            ProviderMetadata::descriptor(&AsyncRedisEventBusProvider).id().as_str(),
            "redis-streams"
        );
    }

    #[test]
    fn configured_provider_validates_options_without_connecting() {
        futures_lite::future::block_on(async {
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
}
