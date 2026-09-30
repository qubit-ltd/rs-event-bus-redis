// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Redis command builders shared by synchronous and asynchronous adapters.

use redis::Cmd;

/// Builds one consumer-group read while bounding the requested Redis BLOCK.
///
/// # Parameters
///
/// - `group`: Existing consumer-group name.
/// - `consumer`: Receiver identity that will own the pending delivery.
/// - `stream`: Redis source stream key.
/// - `cursor`: Own-pending position or `>` for new group entries.
/// - `block_ms`: `Some` requests 1 through 1,000 milliseconds after clamping;
///   `None` omits BLOCK for a non-blocking read.
///
/// # Returns
///
/// An owned COUNT=1 command. Building it allocates command bytes but issues no
/// I/O.
#[must_use]
pub(crate) fn read_group_command(
    group: &str,
    consumer: &str,
    stream: &str,
    cursor: &str,
    block_ms: Option<usize>,
) -> Cmd {
    let mut command = Cmd::new();
    command
        .arg("XREADGROUP")
        .arg("GROUP")
        .arg(group)
        .arg(consumer)
        .arg("COUNT")
        .arg(1);
    if let Some(block_ms) = block_ms {
        command.arg("BLOCK").arg(block_ms.clamp(1, 1_000));
    }
    command.arg("STREAMS").arg(stream).arg(cursor);
    command
}
