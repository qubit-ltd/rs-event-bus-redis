// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![no_main]
//! Exercises the actual production decoder with independently selected limits.

use libfuzzer_sys::fuzz_target;

// Compile the production helper itself, without copying its parser
// implementation or expanding the provider's public API solely for the fuzz
// harness.
#[path = "../../src/internal/bounded_wire_decoder.rs"]
mod bounded_wire_decoder;
#[path = "../../src/internal/wire_limits.rs"]
mod wire_limits;
mod error {
    pub use qubit_event_bus_redis::error::RedisProviderError;
}
mod wire_fields {
    pub use qubit_event_bus_redis::wire::WireFields;
}

fuzz_target!(|input: &[u8]| {
    if input.len() < 4 {
        return;
    }
    let limits = wire_limits::WireLimits {
        wire: usize::from(u16::from_le_bytes([input[0], input[1]])) + 1,
        payload: usize::from(input[2]) + 1,
        headers: usize::from(input[3]) + 1,
    };
    if let Ok(fields) = bounded_wire_decoder::decode(&input[4..], limits) {
        assert_eq!(fields.version, 1);
        assert!(input[4..].len() <= limits.wire);
        assert!(fields.payload.len() <= limits.payload);
        assert!(fields.headers_json.len() <= limits.headers);
    }
});
