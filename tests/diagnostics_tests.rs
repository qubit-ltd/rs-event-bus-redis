// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public diagnostics and provider instance lifecycle.

use qubit_event_bus_redis::diagnostics::RedisProviderDiagnostics;

#[cfg(all(feature = "sync", feature = "async"))]
use futures_lite::future::block_on;
#[cfg(all(feature = "sync", feature = "async"))]
use qubit_event_bus::EventBusConfig;
#[cfg(all(feature = "sync", feature = "async"))]
use qubit_event_bus::model::ProviderOptions;
#[cfg(all(feature = "sync", feature = "async"))]
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
#[cfg(all(feature = "sync", feature = "async"))]
use qubit_event_bus_redis::diagnostics::RedisProviderMode;
#[cfg(all(feature = "sync", feature = "async"))]
use qubit_event_bus_redis::sync::RedisEventBusProvider;
#[cfg(all(feature = "sync", feature = "async"))]
use qubit_spi::AsyncServiceProvider;
#[cfg(all(feature = "sync", feature = "async"))]
use qubit_spi::ServiceProvider;

/// A sync and an async SPI each register their own correctly labeled instance.
#[cfg(all(feature = "sync", feature = "async"))]
#[test]
fn test_snapshots_distinguish_sync_and_async_instances() {
    let sync_options: ProviderOptions =
        [("redis.namespace".into(), "diagnostics_sync".into())].into();
    let async_options: ProviderOptions =
        [("redis.namespace".into(), "diagnostics_async".into())].into();
    let sync = RedisEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(sync_options))
        .expect("create lazy sync provider");
    let asynchronous = block_on(
        AsyncRedisEventBusProvider
            .create_configured(&EventBusConfig::default().with_provider_options(async_options)),
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
#[cfg(all(feature = "sync", feature = "async"))]
#[test]
fn test_snapshots_remove_dropped_spi() {
    let options: ProviderOptions =
        [("redis.namespace".into(), "diagnostics_lifecycle".into())].into();
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
        assert!(
            snapshots
                .iter()
                .any(|snapshot| snapshot.instance_id() == id)
        );
        let _ = bus.capabilities();
        id
    };

    assert!(
        RedisProviderDiagnostics::snapshots()
            .iter()
            .all(|snapshot| snapshot.instance_id() != id)
    );
}

/// Snapshot Debug output includes only public scope and counters, not endpoint secrets.
#[cfg(all(feature = "sync", feature = "async"))]
#[test]
fn test_snapshot_debug_redacts_connection_details() {
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

/// Without either provider feature there are no registered instances.
#[cfg(not(any(feature = "sync", feature = "async")))]
#[test]
fn test_snapshots_are_empty_without_provider_features() {
    assert!(RedisProviderDiagnostics::snapshots().is_empty());
}
