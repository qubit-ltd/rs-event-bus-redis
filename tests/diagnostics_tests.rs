// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public diagnostics and provider instance lifecycle.

use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::OnceLock;

#[cfg(feature = "async")]
use futures_lite::future::block_on;
#[cfg(any(feature = "sync", feature = "async"))]
use qubit_event_bus::EventBusConfig;
#[cfg(any(feature = "sync", feature = "async"))]
use qubit_event_bus::model::ProviderOptions;
#[cfg(feature = "async")]
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
use qubit_event_bus_redis::diagnostics::RedisProviderDiagnostics;
#[cfg(feature = "async")]
use qubit_event_bus_redis::diagnostics::RedisProviderMode;
#[cfg(feature = "sync")]
use qubit_event_bus_redis::sync::RedisEventBusProvider;
#[cfg(feature = "async")]
use qubit_spi::AsyncServiceProvider;
#[cfg(feature = "sync")]
use qubit_spi::ServiceProvider;

/// Serializes tests that inspect the process-wide live-instance directory.
fn lock_diagnostics_tests() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A sync and an async SPI each register their own correctly labeled instance.
#[cfg(all(feature = "sync", feature = "async"))]
#[test]
fn test_snapshots_distinguish_sync_and_async_instances() {
    let _guard = lock_diagnostics_tests();
    let sync_options: ProviderOptions = [("redis.namespace".into(), "diagnostics_sync".into())].into();
    let async_options: ProviderOptions = [("redis.namespace".into(), "diagnostics_async".into())].into();
    let sync = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(sync_options))
        .expect("create lazy sync provider");
    let asynchronous = block_on(
        AsyncRedisEventBusProvider.create_configured(&EventBusConfig::default().with_provider_options(async_options)),
    )
    .expect("create lazy async provider");

    let snapshots = RedisProviderDiagnostics::snapshots();
    let sync_snapshot = snapshots
        .iter()
        .find(|snapshot| snapshot.namespace() == "diagnostics_sync")
        .expect("sync snapshot");
    let async_snapshot = snapshots
        .iter()
        .find(|snapshot| snapshot.namespace() == "diagnostics_async")
        .expect("async snapshot");
    assert_ne!(sync_snapshot.instance_id(), async_snapshot.instance_id());
    assert_eq!(sync_snapshot.mode(), RedisProviderMode::Sync);
    assert_eq!(async_snapshot.mode(), RedisProviderMode::Async);
    assert!(
        snapshots
            .windows(2)
            .all(|pair| pair[0].instance_id() < pair[1].instance_id())
    );

    drop((sync, asynchronous));
}

/// Dropping an SPI removes its instance from the public process snapshot.
#[cfg(feature = "sync")]
#[test]
fn test_snapshots_remove_dropped_spi() {
    let _guard = lock_diagnostics_tests();
    let options: ProviderOptions = [("redis.namespace".into(), "diagnostics_lifecycle".into())].into();
    let id = {
        let bus = RedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(options))
            .expect("create lazy provider");
        let snapshots = RedisProviderDiagnostics::snapshots();
        let id = snapshots
            .iter()
            .find(|snapshot| snapshot.namespace() == "diagnostics_lifecycle")
            .expect("new provider is registered")
            .instance_id();
        assert!(snapshots.iter().any(|snapshot| snapshot.instance_id() == id));
        let _ = bus.capabilities();
        id
    };

    assert!(
        RedisProviderDiagnostics::snapshots()
            .iter()
            .all(|snapshot| snapshot.instance_id() != id)
    );
}

/// Snapshot Debug output includes only public scope and counters, not endpoint
/// secrets.
#[cfg(feature = "sync")]
#[test]
fn test_snapshot_debug_redacts_connection_details() {
    let _guard = lock_diagnostics_tests();
    let url = "redis://diagnostics-secret.example:6389/";
    let options: ProviderOptions = [
        ("redis.url".into(), url.into()),
        ("redis.namespace".into(), "billing".into()),
        ("redis.password_env".into(), "PATH".into()),
    ]
    .into();
    let config = EventBusConfig::default().with_provider_options(options);
    let bus = RedisEventBusProvider
        .create_configured(&config)
        .expect("create lazy provider with ACL settings");
    let snapshots = RedisProviderDiagnostics::snapshots();
    let snapshot = snapshots
        .iter()
        .find(|snapshot| snapshot.namespace() == "billing")
        .expect("new provider is registered");
    let debug = format!("{snapshot:?}");
    assert_eq!(snapshot.namespace(), "billing");
    assert!(debug.contains("billing"));
    assert!(!debug.contains(url));
    assert!(!debug.contains("diagnostics-secret.example"));
    assert!(!debug.contains(&std::env::var("PATH").expect("PATH is set")));
    assert!(!debug.contains("raw Redis error"));
    drop(bus);
}

/// An async-only build registers its SPI and removes the instance on drop.
#[cfg(feature = "async")]
#[test]
fn test_async_snapshot_lifecycle() {
    let _guard = lock_diagnostics_tests();
    let options: ProviderOptions = [("redis.namespace".into(), "diagnostics_async_lifecycle".into())].into();
    let config = EventBusConfig::default().with_provider_options(options);
    let bus = block_on(AsyncRedisEventBusProvider.create_configured(&config)).expect("create lazy async provider");
    let snapshots = RedisProviderDiagnostics::snapshots();
    let snapshot = snapshots
        .iter()
        .find(|snapshot| snapshot.namespace() == "diagnostics_async_lifecycle")
        .expect("async provider is registered");
    assert_eq!(snapshot.mode(), RedisProviderMode::Async);
    let id = snapshot.instance_id();

    drop(bus);
    assert!(
        RedisProviderDiagnostics::snapshots()
            .iter()
            .all(|snapshot| snapshot.instance_id() != id)
    );
}

/// Without either provider feature there are no registered instances.
#[cfg(not(any(feature = "sync", feature = "async")))]
#[test]
fn test_snapshots_are_empty_without_provider_features() {
    let _guard = lock_diagnostics_tests();
    assert!(RedisProviderDiagnostics::snapshots().is_empty());
}
