use futures_lite::future::block_on;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::registry::EventBusConfig;
use qubit_spi::AsyncServiceProvider;
use qubit_spi::ProviderMetadata;
use qubit_spi::ServiceProvider;

use crate::r#async::AsyncRedisEventBusProvider;
use crate::sync::RedisEventBusProvider;

#[test]
fn test_sync_and_async_provider_descriptors_and_factories() {
    let sync_provider = RedisEventBusProvider;
    let async_provider = AsyncRedisEventBusProvider;
    let sync_descriptor = sync_provider.descriptor();
    let async_descriptor = async_provider.descriptor();
    assert_eq!(format!("{sync_descriptor:?}"), format!("{async_descriptor:?}"));

    sync_provider
        .create_configured(&EventBusConfig::default())
        .expect("create lazy sync provider");
    block_on(async_provider.create_configured(&EventBusConfig::default())).expect("create lazy async provider");

    let invalid_options: ProviderOptions = [("redis.unknown".into(), "invalid".into())].into();
    let invalid_config = EventBusConfig::default().with_provider_options(invalid_options);
    assert!(sync_provider.create_configured(&invalid_config).is_err());
    assert!(block_on(async_provider.create_configured(&invalid_config)).is_err());
}
