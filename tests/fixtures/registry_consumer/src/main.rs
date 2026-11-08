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
    {
        let registry = EventBusRegistry::discover().expect("sync discovery failed");
        assert!(registry.provider_ids().iter().any(|id| id.as_str() == "redis-streams"));
    }

    #[cfg(all(feature = "async", feature = "discovery"))]
    {
        let registry = AsyncEventBusRegistry::discover().expect("async discovery failed");
        assert!(registry.provider_ids().iter().any(|id| id.as_str() == "redis-streams"));
    }
}
