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
    Ok(format!("qubit:consumer:{}", UuidV4Generator::new().generate()?))
}

#[cfg(test)]
mod tests {
    use super::new_consumer_name;

    #[test]
    fn consumer_names_are_distinct_uuid_v4_values() {
        let first = new_consumer_name().expect("UUID v4 generation succeeds");
        let second = new_consumer_name().expect("UUID v4 generation succeeds");

        assert_ne!(first, second);
        assert!(first.starts_with("qubit:consumer:"));
        let uuid = &first["qubit:consumer:".len()..];
        assert_eq!(uuid.len(), 36);
        assert_eq!(uuid.as_bytes()[14], b'4');
    }
}
