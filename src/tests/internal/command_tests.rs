// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded consumer-group command construction.

use crate::internal::read_group_command;

#[test]
fn test_read_group_command_omits_unbounded_block_and_caps_finite_block() {
    let immediate =
        read_group_command("group", "consumer", "stream", ">", None).get_packed_command();
    let immediate = String::from_utf8(immediate).expect("RESP command is ASCII");
    assert!(!immediate.contains("BLOCK"));

    let blocking =
        read_group_command("group", "consumer", "stream", ">", Some(5_000)).get_packed_command();
    let blocking = String::from_utf8(blocking).expect("RESP command is ASCII");
    assert!(blocking.contains("$5\r\nBLOCK\r\n$4\r\n1000\r\n"));
}
