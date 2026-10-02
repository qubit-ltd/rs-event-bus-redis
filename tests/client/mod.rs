// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Client behavior observed through the public SPI.
#[cfg(feature = "async")]
mod limit_tests;
mod receive_admission_tests;
mod receiver_stress_tests;
mod resource_lifecycle_tests;
mod transport_tests;

mod support;

pub(super) use self::support::helpers::assert_error;
pub(super) use self::support::helpers::message;
pub(super) use self::support::helpers::options;
