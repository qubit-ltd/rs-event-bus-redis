// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Stable secret-free categories used by poison quarantine.

use crate::internal::PoisonReason;

#[test]
fn test_poison_reasons_have_stable_secret_free_names() {
    assert_eq!(PoisonReason::MissingWire.as_str(), "missing_wire");
    assert_eq!(PoisonReason::InvalidWireField.as_str(), "invalid_wire_field");
    assert_eq!(PoisonReason::InvalidJson.as_str(), "invalid_json");
    assert_eq!(PoisonReason::InvalidEventMetadata.as_str(), "invalid_event_metadata");
}
