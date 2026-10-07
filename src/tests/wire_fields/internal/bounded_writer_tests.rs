// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! In-memory flush preserves accepted bytes and the remaining wire budget.

use std::io::Write;

use super::BoundedWriter;

#[test]
fn test_flush_preserves_contents_and_consumed_budget_after_rejection() {
    let mut writer = BoundedWriter::new(4);
    writer.write_all(b"ab").expect("first half fits");
    writer.flush().expect("in-memory flush succeeds");
    assert_eq!(writer.bytes.as_slice(), b"ab");

    writer
        .write_all(b"cd")
        .expect("flush preserves the remaining two bytes");
    writer.flush().expect("flushing the exact limit succeeds");
    assert_eq!(writer.bytes.as_slice(), b"abcd");

    assert!(
        writer.write_all(b"e").is_err(),
        "flush must not reset the consumed budget"
    );
    assert_eq!(
        writer.bytes.as_slice(),
        b"abcd",
        "a rejected write preserves accepted contents"
    );
    writer
        .flush()
        .expect("flush after rejection still succeeds");
    assert!(
        writer.write_all(b"f").is_err(),
        "another flush cannot release consumed capacity"
    );
    assert_eq!(writer.bytes.as_slice(), b"abcd");
}
