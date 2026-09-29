// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Constant-memory nesting check covering Serde's ignored JSON fields.

use crate::error::RedisProviderError;

/// Serde JSON starts with 128 slots and rejects entry into the 128th container.
const JSON_RECURSION_BUDGET: usize = 128;

/// Checks structural JSON depth while ignoring quoted and escaped characters.
///
/// # Parameters
///
/// - `encoded`: UTF-8 wire JSON already bounded by the provider's byte budget.
///
/// # Returns
///
/// Success when every object or array has fewer than 128 enclosing/current
/// containers. Only counters and quote/escape state are stored; no value tree
/// or additional wire copy is allocated, and no external I/O is performed.
///
/// # Errors
///
/// Returns a stable operation error for excessive nesting or an unmatched
/// closing container. The typed Serde decoder validates all remaining syntax.
pub(super) fn check_json_depth(encoded: &str) -> Result<(), RedisProviderError> {
    let mut depth = 0_usize;
    let mut quoted = false;
    let mut escaped = false;
    for byte in encoded.bytes() {
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
            continue;
        }
        match byte {
            b'"' => quoted = true,
            b'[' | b'{' => {
                depth += 1;
                if depth >= JSON_RECURSION_BUDGET {
                    return Err(RedisProviderError::Operation("decode wire depth"));
                }
            }
            b']' | b'}' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or(RedisProviderError::Operation("decode wire depth"))?;
            }
            _ => {}
        }
    }
    Ok(())
}
