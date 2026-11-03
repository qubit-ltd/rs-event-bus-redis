// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private diagnostics state shared by provider implementations.

mod redis_diagnostic_counter;
mod redis_diagnostics_directory;
mod redis_diagnostics_state;

pub(crate) use redis_diagnostic_counter::RedisDiagnosticCounter;
pub(in crate::diagnostics) use redis_diagnostics_directory::directory;
pub(crate) use redis_diagnostics_state::RedisDiagnosticsState;
