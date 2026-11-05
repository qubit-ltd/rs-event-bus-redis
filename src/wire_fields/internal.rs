// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private serialization helpers for bounded wire fields.

#[cfg(any(feature = "sync", feature = "async"))]
mod borrowed_wire_fields;
#[cfg(any(feature = "sync", feature = "async"))]
mod bounded_writer;

#[cfg(any(feature = "sync", feature = "async"))]
pub(super) use borrowed_wire_fields::BorrowedWireFields;
#[cfg(any(feature = "sync", feature = "async"))]
pub(super) use bounded_writer::BoundedWriter;
