// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
// =============================================================================
//! Shared bounded delay policy for full local delivery capacity.

use std::time::Duration;

/// Caps a full-capacity receive delay to one short scheduling interval.
///
/// # Parameters
/// - `timeout`: caller-requested receive wait.
///
/// # Returns
/// The smaller of `timeout` and ten milliseconds. A zero timeout remains
/// non-blocking, and `Duration::MAX` cannot overflow.
pub(crate) fn full_capacity_backoff(timeout: Duration) -> Duration {
    timeout.min(Duration::from_millis(10))
}

#[cfg(test)]
mod tests {
    use super::full_capacity_backoff;
    use std::time::Duration;

    #[test]
    fn test_full_capacity_backoff_is_bounded_by_timeout_and_ten_milliseconds() {
        assert_eq!(Duration::ZERO, full_capacity_backoff(Duration::ZERO));
        assert_eq!(
            Duration::from_millis(1),
            full_capacity_backoff(Duration::from_millis(1))
        );
        assert_eq!(
            Duration::from_millis(10),
            full_capacity_backoff(Duration::from_millis(10))
        );
        assert_eq!(
            Duration::from_millis(10),
            full_capacity_backoff(Duration::from_secs(1))
        );
        assert_eq!(
            Duration::from_millis(10),
            full_capacity_backoff(Duration::MAX)
        );
    }
}
