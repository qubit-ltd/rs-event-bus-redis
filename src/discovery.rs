// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Submits Redis providers to both event-bus SPI inventories.

use qubit_event_bus::registry::EventBusSpec;
use qubit_event_bus::registry::async_provider_inventory::Entry as AsyncProviderEntry;
use qubit_event_bus::registry::sync_provider_inventory::Entry as SyncProviderEntry;
use qubit_spi::submit_async_provider;
use qubit_spi::submit_sync_provider;

submit_sync_provider! {
    inventory_entry = SyncProviderEntry;
    spec = EventBusSpec;
    provider = crate::sync::RedisEventBusProvider;
}

submit_async_provider! {
    inventory_entry = AsyncProviderEntry;
    spec = EventBusSpec;
    provider = crate::r#async::AsyncRedisEventBusProvider;
}
