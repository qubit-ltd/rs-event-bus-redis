// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Benchmark helpers reused by the Sentinel cost diagnostic.

#[path = "../../../benches/support/backend.rs"]
pub mod backend;
#[path = "../../../benches/support/receiver.rs"]
pub mod receiver;
#[path = "../../../benches/support/workload.rs"]
pub mod workload;
#[path = "../../../benches/support/workload_result.rs"]
pub mod workload_result;

pub mod measurement;
pub mod redis_fixture;
