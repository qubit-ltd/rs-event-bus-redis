// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private Redis client resource types.

#[path = "internal/pooled_connection.rs"]
mod pooled_connection;

#[cfg(feature = "sync")]
pub(crate) use pooled_connection::PooledConnection;
#[cfg(feature = "sync")]
pub(crate) use pooled_connection::SyncConnectionPool;
