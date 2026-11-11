// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Validation for Redis Stream entry IDs.

/// Validates the two unsigned decimal components of a Redis stream ID.
///
/// # Parameters
///
/// - `value`: Milliseconds and sequence separated by exactly one dash.
///
/// # Returns
///
/// `true` only when both non-empty decimal components fit u64. No Redis I/O is
/// issued.
#[must_use]
pub(crate) fn valid_stream_id(value: &str) -> bool {
    let Some((milliseconds, sequence)) = value.split_once('-') else {
        return false;
    };
    !milliseconds.is_empty()
        && !sequence.is_empty()
        && milliseconds.bytes().all(|byte| byte.is_ascii_digit())
        && sequence.bytes().all(|byte| byte.is_ascii_digit())
        && milliseconds.parse::<u64>().is_ok()
        && sequence.parse::<u64>().is_ok()
}

#[cfg(test)]
mod tests {
    use super::valid_stream_id;

    #[test]
    fn test_valid_stream_id_accepts_decimal_components_within_u64() {
        for value in [
            "0-0",
            "123-0",
            "01-002",
            "18446744073709551615-18446744073709551615",
        ] {
            assert!(valid_stream_id(value), "rejected valid ID {value:?}");
        }
    }

    #[test]
    fn test_valid_stream_id_rejects_malformed_or_overflowing_components() {
        for value in [
            "",
            "1",
            "-0",
            "1-",
            "1-2-3",
            "+1-0",
            " 1-0",
            "１-0",
            "18446744073709551616-0",
            "0-18446744073709551616",
        ] {
            assert!(!valid_stream_id(value), "accepted malformed ID {value:?}");
        }
    }
}
