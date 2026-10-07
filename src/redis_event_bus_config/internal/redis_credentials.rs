// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private storage and loading for Redis ACL credentials.

use std::env;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Result as FmtResult;

use qubit_event_bus::model::ProviderOptions;

use crate::error::RedisProviderError;

/// Holds resolved ACL credentials while keeping their values out of `Debug`.
#[derive(Clone, Default)]
pub(super) struct RedisCredentials {
    /// Optional Redis ACL username used during connection setup.
    pub(super) username: Option<String>,
    /// Optional Redis ACL password used during connection setup.
    pub(super) password: Option<String>,
}

impl Debug for RedisCredentials {
    /// Formats credential presence without disclosing usernames or passwords.
    ///
    /// # Parameters
    ///
    /// - `formatter`: Destination for the redacted debug representation.
    ///
    /// # Returns
    ///
    /// The formatter result produced while writing credential-presence markers.
    ///
    /// # Errors
    ///
    /// Returns a formatting error if the destination rejects a write.
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter
            .debug_struct("RedisCredentials")
            .field("username", &self.username.as_ref().map(|_| "<redacted>"))
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl RedisCredentials {
    /// Resolves username and password references from provider options.
    ///
    /// Each configured option names an environment variable. Missing variables
    /// are reported without returning the secret or variable contents.
    ///
    /// # Parameters
    ///
    /// - `options`: Provider settings containing optional `*_env` names.
    /// - `prefix`: Option namespace, either `redis` or `redis.sentinel`.
    ///
    /// # Returns
    ///
    /// Credential values read from the named environment variables, or `None`
    /// for credentials that were not configured.
    ///
    /// # Errors
    ///
    /// Returns a generic configuration error if a referenced environment
    /// variable is unavailable.
    pub(super) fn from_env_references(
        options: &ProviderOptions,
        prefix: &str,
    ) -> Result<Self, RedisProviderError> {
        let read = |key: &str| -> Result<Option<String>, RedisProviderError> {
            let option = format!("{prefix}.{key}_env");
            let Some(name) = options.get(&option) else {
                return Ok(None);
            };
            env::var(name).map(Some).map_err(|_| {
                RedisProviderError::Configuration("credential environment variable is unavailable")
            })
        };
        Ok(Self {
            username: read("username")?,
            password: read("password")?,
        })
    }
}
