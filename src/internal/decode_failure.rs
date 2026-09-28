// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Result of decoding a Redis event wire record.

use super::poison_reason::PoisonReason;
/// Separates incompatible records from records safe to quarantine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DecodeFailure {
    /// The record is malformed and can be transferred to quarantine.
    Poison(PoisonReason),
    /// The record uses a valid wire version this provider cannot decode.
    UnsupportedVersion,
}
