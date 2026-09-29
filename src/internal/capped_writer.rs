// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! JSON serialization that stops before growing past a provider limit.

use std::io;
use std::io::Write;

use serde::Serialize;

use crate::error::RedisProviderError;

/// Accumulates at most `limit` output bytes.
struct CappedWriter {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl Write for CappedWriter {
    /// Appends a complete chunk or rejects it without allocating beyond the
    /// cap.
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::other("Redis serialization limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    /// This memory writer has no buffered external output.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Serializes `value` without constructing output larger than `limit`.
/// Returns a sanitized limit or encoding error and performs no Redis IO.
pub(crate) fn to_capped_json<T: Serialize>(value: &T, limit: usize) -> Result<String, RedisProviderError> {
    let mut writer = CappedWriter {
        bytes: Vec::new(),
        limit,
        exceeded: false,
    };
    if serde_json::to_writer(&mut writer, value).is_err() {
        return Err(if writer.exceeded {
            RedisProviderError::LimitExceeded
        } else {
            RedisProviderError::Operation("encode JSON")
        });
    }
    String::from_utf8(writer.bytes).map_err(|_| RedisProviderError::Operation("encode JSON UTF-8"))
}
