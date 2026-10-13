use qubit_event_bus::registry::{AsyncEventBusRegistry, EventBusRegistry};
use qubit_event_bus_redis as _;

fn main() {
    #[cfg(all(feature = "sync", feature = "discovery"))]
    let _sync = EventBusRegistry::discover();

    #[cfg(all(feature = "async", feature = "discovery"))]
    let _async = AsyncEventBusRegistry::discover();
}
