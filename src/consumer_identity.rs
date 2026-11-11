// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Generates process-independent Redis consumer names.

use qubit_id::IdGenerationError;
use qubit_id::UuidV4Generator;

/// Generates a Redis consumer name unique across independent bus instances.
///
/// # Returns
///
/// A `qubit:consumer:` name with a random UUID v4 suffix.
///
/// # Errors
///
/// Returns an error if the operating system random source is unavailable.
pub(crate) fn new_consumer_name() -> Result<String, IdGenerationError> {
    Ok(format!(
        "qubit:consumer:{}",
        UuidV4Generator::new().generate()?
    ))
}
