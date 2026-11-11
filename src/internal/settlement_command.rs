// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Atomic Redis command construction and reply classification for settlement.

use redis::Cmd;
use redis::Value;
use redis::cmd;

use super::settlement_reply::SettlementReply;

const SCRIPT: &str = "local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1)\nif #pending == 0 then return 0 end\nif pending[1][2] ~= ARGV[2] then return -1 end\nreturn redis.call('XACK', KEYS[1], ARGV[1], ARGV[3])";

/// Builds the atomic owner check and acknowledgement command.
///
/// # Parameters
///
/// - `stream`: Source Redis stream key.
/// - `group`: Consumer group name.
/// - `consumer`: Expected current pending-entry owner.
/// - `message_id`: Redis stream entry identifier.
///
/// # Returns
///
/// An EVAL command using one key and the settlement arguments in protocol
/// order.
pub(crate) fn settlement_command(
    stream: &str,
    group: &str,
    consumer: &str,
    message_id: &str,
) -> Cmd {
    let mut command = cmd("EVAL");
    command
        .arg(SCRIPT)
        .arg(1)
        .arg(stream)
        .arg(group)
        .arg(consumer)
        .arg(message_id);
    command
}

/// Classifies the settlement script's integer reply without coercion.
///
/// # Parameters
///
/// - `value`: Redis reply returned by the settlement EVAL command.
///
/// # Returns
///
/// A protocol reply class for integer 0, 1, or -1; `None` for every other
/// value.
pub(crate) fn parse_settlement_reply(value: &Value) -> Option<SettlementReply> {
    match value {
        Value::Int(0 | 1) => Some(SettlementReply::Completed),
        Value::Int(-1) => Some(SettlementReply::OwnershipChanged),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use redis::Value;

    use super::SettlementReply;
    use super::parse_settlement_reply;
    use super::settlement_command;

    #[test]
    fn test_settlement_reply_classifies_only_protocol_integers() {
        assert_eq!(
            parse_settlement_reply(&Value::Int(0)),
            Some(SettlementReply::Completed)
        );
        assert_eq!(
            parse_settlement_reply(&Value::Int(1)),
            Some(SettlementReply::Completed)
        );
        assert_eq!(
            parse_settlement_reply(&Value::Int(-1)),
            Some(SettlementReply::OwnershipChanged)
        );
        assert_eq!(parse_settlement_reply(&Value::Int(-2)), None);
        assert_eq!(
            parse_settlement_reply(&Value::BulkString(b"1".to_vec())),
            None
        );
        assert_eq!(
            parse_settlement_reply(&Value::Array(vec![Value::Int(1)])),
            None
        );
        assert_eq!(
            parse_settlement_reply(&Value::Array(vec![Value::BulkString(b"ERR".to_vec())])),
            None
        );
    }

    #[test]
    fn test_settlement_command_uses_single_key_and_ordered_arguments() {
        let packed = settlement_command("stream", "group", "consumer", "42-0").get_packed_command();
        let text = String::from_utf8(packed).expect("command is UTF-8");
        assert!(text.starts_with("*7\r\n$4\r\nEVAL\r\n"));
        assert!(text.contains(
            "\r\n$1\r\n1\r\n$6\r\nstream\r\n$5\r\ngroup\r\n$8\r\nconsumer\r\n$4\r\n42-0\r\n"
        ));
        assert!(text.contains("XPENDING"));
        assert!(text.contains("XACK"));
    }
}
