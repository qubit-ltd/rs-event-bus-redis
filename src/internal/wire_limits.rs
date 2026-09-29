// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider limits applied before copying or decoding wire components.

/// Independent bounds for one Redis record and its decoded components.
#[derive(Clone, Copy, Debug)]
pub(crate) struct WireLimits {
    /// UTF-8 JSON bytes in one stored wire field.
    pub(crate) wire: usize,
    /// Decoded byte-vector length.
    pub(crate) payload: usize,
    /// Decoded headers string length before its inner JSON parse.
    pub(crate) headers: usize,
}

impl Default for WireLimits {
    /// Returns the finite default wire, payload, and header limits.
    fn default() -> Self {
        Self {
            wire: 8_388_608,
            payload: 1_048_576,
            headers: 65_536,
        }
    }
}
