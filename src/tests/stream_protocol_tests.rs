// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Owning protocol normalization must transfer large bulk buffers unchanged.

use redis::Value;

use crate::stream_protocol::parse_auto_claim;
use crate::stream_protocol::parse_pending_entries;
use crate::stream_protocol::parse_range;
use crate::stream_protocol::parse_read_group;

#[test]
fn test_parse_auto_claim_moves_wire_buffer_without_clone() {
    let mut wire = Vec::with_capacity(2 * 1024 * 1024);
    wire.resize(1024 * 1024, b'x');
    let allocation = wire.as_ptr();
    let capacity = wire.capacity();
    let value = Value::Array(vec![
        Value::BulkString(b"0-0".to_vec()),
        Value::Array(vec![Value::Array(vec![
            Value::BulkString(b"1-0".to_vec()),
            Value::Array(vec![Value::BulkString(b"wire".to_vec()), Value::BulkString(wire)]),
        ])]),
        Value::Array(Vec::new()),
    ]);
    let (reply, missing) = parse_auto_claim(value).expect("valid reply");
    assert!(!missing);
    let Value::BulkString(wire) = reply.claimed[0].map.get("wire").expect("wire") else {
        panic!("bulk wire");
    };
    assert_eq!(
        wire.as_ptr(),
        allocation,
        "the owned RESP wire allocation must be moved, not copied"
    );
    assert_eq!(
        wire.capacity(),
        capacity,
        "owned normalization must preserve the original bulk allocation capacity"
    );
}

#[test]
fn test_parse_read_group_moves_resp2_and_resp3_wire_buffers() {
    for resp3 in [false, true] {
        let mut wire = Vec::with_capacity(2048);
        wire.resize(1024, b'x');
        let allocation = wire.as_ptr();
        let capacity = wire.capacity();
        let fields = if resp3 {
            Value::Map(vec![(Value::BulkString(b"wire".to_vec()), Value::BulkString(wire))])
        } else {
            Value::Array(vec![Value::BulkString(b"wire".to_vec()), Value::BulkString(wire)])
        };
        let rows = Value::Array(vec![Value::Array(vec![Value::BulkString(b"1-0".to_vec()), fields])]);
        let key = Value::BulkString(b"events".to_vec());
        let reply = if resp3 {
            Value::Map(vec![(key, rows)])
        } else {
            Value::Array(vec![Value::Array(vec![key, rows])])
        };
        let parsed = parse_read_group(reply).expect("supported RESP reply").expect("entries");
        assert_eq!(parsed.keys[0].key, "events");
        let Value::BulkString(wire) = parsed.keys[0].ids[0].map.get("wire").expect("wire") else {
            panic!("bulk wire");
        };
        assert_eq!(wire.as_ptr(), allocation);
        assert_eq!(wire.capacity(), capacity);
    }
}

#[test]
fn test_parse_range_moves_wire_and_keeps_pending_tombstones() {
    let mut wire = Vec::with_capacity(2048);
    wire.resize(1024, b'x');
    let allocation = wire.as_ptr();
    let capacity = wire.capacity();
    let rows = Value::Array(vec![
        Value::Array(vec![
            Value::BulkString(b"1-0".to_vec()),
            Value::Array(vec![Value::BulkString(b"wire".to_vec()), Value::BulkString(wire)]),
        ]),
        Value::Array(vec![Value::BulkString(b"2-0".to_vec()), Value::Nil]),
    ]);
    let reply = parse_range(rows).expect("stream rows");
    assert_eq!(reply.ids[1].id, "2-0");
    assert!(reply.ids[1].map.is_empty());
    let Value::BulkString(wire) = reply.ids[0].map.get("wire").expect("wire") else {
        panic!("bulk wire");
    };
    assert_eq!(wire.as_ptr(), allocation);
    assert_eq!(wire.capacity(), capacity);
}

#[test]
fn test_owned_stream_parsers_preserve_empty_and_reject_malformed_shapes() {
    assert!(parse_read_group(Value::Nil).expect("timeout").is_none());
    assert!(
        parse_read_group(Value::Array(Vec::new()))
            .expect("empty")
            .expect("reply")
            .keys
            .is_empty()
    );
    assert!(parse_range(Value::Nil).expect("empty").ids.is_empty());
    for bad in [
        Value::Int(1),
        Value::Array(vec![Value::Nil]),
        Value::Array(vec![Value::Array(vec![Value::Nil])]),
    ] {
        assert!(parse_read_group(bad.clone()).is_err());
        assert!(parse_range(bad).is_err());
    }
}

#[test]
fn test_parse_auto_claim_preserves_redis_6_2_nil_tombstones() {
    let value = Value::Array(vec![
        Value::BulkString(b"9-0".to_vec()),
        Value::Array(vec![Value::Array(vec![Value::Nil])]),
    ]);
    let (reply, has_missing_entries) = parse_auto_claim(value).unwrap();
    assert_eq!(reply.next_stream_id, "9-0");
    assert!(reply.claimed.is_empty());
    assert!(has_missing_entries);
}

#[test]
fn test_parse_pending_entries_returns_owner_and_idle_time() {
    let value = Value::Array(vec![Value::Array(vec![
        Value::BulkString(b"1-0".to_vec()),
        Value::BulkString(b"worker-a".to_vec()),
        Value::Int(501),
        Value::Int(2),
    ])]);
    assert_eq!(
        parse_pending_entries(value).unwrap(),
        vec![("1-0".into(), "worker-a".into(), 501)]
    );
}

#[test]
fn test_parse_auto_claim_and_pending_entries_reject_invalid_replies() {
    assert!(parse_auto_claim(Value::Nil).is_err());
    assert!(parse_pending_entries(Value::Array(vec![Value::Nil])).is_err());
}

#[test]
fn test_parse_pending_entries_rejects_invalid_shapes() {
    assert!(parse_pending_entries(Value::Int(1)).is_err());
    assert!(parse_pending_entries(Value::Array(vec![Value::BulkString(b"bad".to_vec())])).is_err());
    assert!(parse_pending_entries(Value::Array(vec![Value::Array(vec![Value::Nil])])).is_err());
    assert!(
        parse_pending_entries(Value::Array(vec![Value::Array(vec![
            Value::BulkString(b"1-0".to_vec()),
            Value::BulkString(b"worker".to_vec()),
            Value::Int(-1),
            Value::Int(1),
        ])]))
        .is_err()
    );
}
