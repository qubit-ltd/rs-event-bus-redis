// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Serialization sink that rejects writes before extending beyond its budget.

use std::io::Error as IoError;
use std::io::Result as IoResult;
use std::io::Write;

use serde::Serialize;
use serde_json::to_writer;

use crate::error::RedisProviderError;

/// Accumulates at most `limit` JSON bytes without performing external I/O.
pub(super) struct BoundedWriter {
    /// Serialized bytes accepted so far.
    bytes: Vec<u8>,
    /// Inclusive serialized byte budget.
    limit: usize,
    /// Records size rejection separately from other serialization failures.
    exceeded: bool,
}

impl BoundedWriter {
    /// Creates an empty sink with the inclusive `limit` byte budget.
    ///
    /// # Parameters
    ///
    /// - `limit`: Maximum accepted output length.
    ///
    /// # Returns
    ///
    /// An empty in-memory sink that allocates no payload storage yet.
    #[must_use]
    #[inline]
    pub(super) fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
            exceeded: false,
        }
    }

    /// Serializes `value` while bounding every individual write.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Any Serde value, including unsized values behind a reference.
    ///
    /// # Parameters
    ///
    /// - `value`: Serde value whose JSON encoding must fit the sink.
    ///
    /// # Returns
    ///
    /// The complete UTF-8 JSON string on success.
    ///
    /// # Errors
    ///
    /// Returns `WireTooLarge` for a rejected write and a stable operation error
    /// for another serializer failure. The partial output is discarded.
    pub(super) fn serialize<T: Serialize + ?Sized>(
        mut self,
        value: &T,
    ) -> Result<String, RedisProviderError> {
        if to_writer(&mut self, value).is_err() {
            return Err(if self.exceeded {
                RedisProviderError::WireTooLarge
            } else {
                RedisProviderError::Operation("encode message")
            });
        }
        String::from_utf8(self.bytes).map_err(|_| RedisProviderError::Operation("encode message"))
    }
}

impl Write for BoundedWriter {
    /// Appends `buffer` only when its complete length fits the remaining
    /// budget.
    ///
    /// # Parameters
    ///
    /// - `buffer`: Bytes appended without performing external I/O.
    ///
    /// # Returns
    ///
    /// The complete buffer length after a successful in-memory append.
    ///
    /// # Errors
    ///
    /// Returns an I/O error without modifying bytes when checked addition
    /// overflows or the inclusive limit would be exceeded.
    fn write(&mut self, buffer: &[u8]) -> IoResult<usize> {
        let end = self.bytes.len().checked_add(buffer.len());
        if end.is_none_or(|end| end > self.limit) {
            self.exceeded = true;
            return Err(IoError::other("wire byte limit exceeded"));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    /// Completes the in-memory write immediately without external I/O.
    ///
    /// # Returns
    ///
    /// Success, because this sink has no buffered external destination.
    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/wire_fields/internal/bounded_writer_tests.rs"]
mod tests;
