// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Redis command builders shared by synchronous and asynchronous adapters.

use redis::Cmd;

/// Builds one bounded consumer-group read, adding `BLOCK` only when requested.
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

#[cfg(test)]
mod tests {
    use super::read_group_command;

    #[test]
    fn test_read_group_command_omits_unbounded_block_and_caps_finite_block() {
        let immediate = read_group_command("group", "consumer", "stream", ">", None).get_packed_command();
        let immediate = String::from_utf8(immediate).expect("RESP command is ASCII");
        assert!(!immediate.contains("BLOCK"));

        let blocking = read_group_command("group", "consumer", "stream", ">", Some(5_000)).get_packed_command();
        let blocking = String::from_utf8(blocking).expect("RESP command is ASCII");
        assert!(blocking.contains("$5\r\nBLOCK\r\n$4\r\n1000\r\n"));
    }
}
