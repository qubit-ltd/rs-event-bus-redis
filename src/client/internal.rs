// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Redis connection leases, admission budgets, and discovery.
#[cfg(feature = "async")]
mod async_command_connection;
#[cfg(feature = "async")]
mod async_connection_cache;
#[cfg(feature = "async")]
mod cache_state;
mod command_permit;
#[cfg(feature = "sync")]
mod pooled_connection;
mod receiver_permit;
mod resource_budget;
pub(super) mod sentinel_resolver;
#[cfg(feature = "sync")]
mod sync_connection_pool;

#[cfg(feature = "async")]
pub(crate) use async_command_connection::AsyncCommandConnection;
#[cfg(feature = "async")]
pub(crate) use async_connection_cache::AsyncConnectionCache;
#[cfg(test)]
pub(crate) use command_permit::CommandPermit;
#[cfg(feature = "sync")]
pub(crate) use pooled_connection::PooledConnection;
pub(crate) use receiver_permit::ReceiverPermit;
pub(crate) use resource_budget::CommandClass;
pub(crate) use resource_budget::ResourceBudget;
pub(crate) use sentinel_resolver::SentinelResolver;
#[cfg(feature = "sync")]
pub(crate) use sync_connection_pool::SyncConnectionPool;
