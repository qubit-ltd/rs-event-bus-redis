// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_event_bus::registry::AsyncEventBusRegistry;
use qubit_event_bus::registry::EventBusRegistry;
use qubit_event_bus_redis as _;

fn main() {
    #[cfg(all(feature = "sync", feature = "discovery"))]
    let _sync = EventBusRegistry::discover();

    #[cfg(all(feature = "async", feature = "discovery"))]
    let _async = AsyncEventBusRegistry::discover();
}
