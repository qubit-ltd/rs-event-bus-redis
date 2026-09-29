// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Independent consumer identity generation.

use crate::consumer_identity::new_consumer_name;

#[test]
fn test_consumer_names_are_distinct_uuid_v4_values() {
    let first = new_consumer_name().expect("UUID v4 generation succeeds");
    let second = new_consumer_name().expect("UUID v4 generation succeeds");

    assert_ne!(first, second);
    assert!(first.starts_with("qubit:consumer:"));
    let uuid = &first["qubit:consumer:".len()..];
    assert_eq!(uuid.len(), 36);
    assert_eq!(uuid.as_bytes()[14], b'4');
}
