// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Controlled dedicated receiver command execution.

use redis::Cmd;
use redis::ConnectionLike;
use redis::FromRedisValue;
use redis::RedisError;
use redis::Value;

use crate::client::Client;
use crate::client::PooledConnection;
use crate::error::RedisProviderError;
use crate::redis_provider_error::from_redis_error;

/// Dedicated receiver commands reserve only short-command admission; BLOCK is
/// exempt.
pub(super) trait ReceiveCommand {
    /// Sends a receiver command with controlled socket waits and optional
    /// admission.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Owned reply type converted only after classifying a top-level
    ///   rejection.
    ///
    /// # Parameters
    ///
    /// - `connection`: Dedicated receiver socket, discarded after uncertain
    ///   failures.
    /// - `client`: Shared command admission and waiting-budget policy.
    /// - `short`: Reserves short-command admission when true; BLOCK reads are
    ///   exempt.
    ///
    /// # Returns
    ///
    /// A typed owned response after bounded blocking I/O.
    ///
    /// # Errors
    ///
    /// Returns exhaustion before sending, sanitized setup/top-level rejection,
    /// or an unknown receive outcome for I/O or malformed conversion
    /// replies.
    fn query_receive<T: FromRedisValue>(
        &self,
        connection: &mut PooledConnection,
        client: &Client,
        short: bool,
    ) -> Result<T, RedisProviderError>;
}
impl ReceiveCommand for Cmd {
    /// Applies the receiver-command contract to Redis command bytes.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Owned RESP conversion target.
    ///
    /// # Parameters
    ///
    /// - `connection`: Dedicated socket with finite waits.
    /// - `client`: Admission and timeout policy shared by this receiver.
    /// - `short`: Whether the request consumes one short-command permit.
    ///
    /// # Returns
    ///
    /// The converted owned reply; the permit is released before returning.
    ///
    /// # Errors
    ///
    /// Returns pre-send exhaustion/setup errors, classified top-level
    /// rejections, or outcome-unknown after uncertain I/O or
    /// nested/protocol failures.
    fn query_receive<T: FromRedisValue>(
        &self,
        connection: &mut PooledConnection,
        client: &Client,
        short: bool,
    ) -> Result<T, RedisProviderError> {
        let _permit = if short { Some(client.try_command()?) } else { None };
        if short {
            let timeout = client.command_timeout();
            connection
                .set_read_timeout(Some(timeout))
                .map_err(|error| from_redis_error("receive", &error))?;
            connection
                .set_write_timeout(Some(timeout))
                .map_err(|error| from_redis_error("receive", &error))?;
        }
        let raw = connection
            .req_command(self)
            .map_err(|_| RedisProviderError::OutcomeUnknown { operation: "receive" })?;
        if let Value::ServerError(error) = raw {
            let error: RedisError = error.into();
            return Err(from_redis_error("receive", &error));
        }
        T::from_owned_redis_value(raw).map_err(|_| {
            connection.discard();
            RedisProviderError::OutcomeUnknown { operation: "receive" }
        })
    }
}

#[cfg(test)]
mod tests {
    use qubit_event_bus::model::ProviderOptions;
    use redis::ConnectionLike;
    use redis::cmd;

    use super::ReceiveCommand;
    use crate::client::Client;
    use crate::config::RedisEventBusConfig;
    use crate::error::RedisProviderError;
    use crate::tests::support::redis_support::scripted_redis::ScriptedRedis;
    use crate::tests::support::redis_support::scripted_redis::Step;

    #[test]
    fn test_sync_typed_receive_conversion_failure_releases_admission() {
        // The public receiver currently requests Value. This deliberately tests
        // the private helper's generic conversion contract with u64 instead.
        let server = ScriptedRedis::start(vec![
            Step::reply("SELECT", b"+OK\r\n"),
            Step::reply("ECHO", b"$3\r\nbad\r\n"),
            Step::reply("SELECT", b"+OK\r\n"),
            Step::reply("SELECT", b"+OK\r\n"),
            Step::reply("ECHO", b"$3\r\nbad\r\n"),
        ])
        .expect("scripted Redis");
        let options: ProviderOptions = [
            ("redis.url".into(), format!("{}3", server.url())),
            ("redis.max_concurrent_commands".into(), "1".into()),
            ("redis.max_idle_connections".into(), "1".into()),
            ("redis.connect_timeout_ms".into(), "100".into()),
            ("redis.command_timeout_ms".into(), "100".into()),
        ]
        .into();
        let client =
            Client::new(&RedisEventBusConfig::from_provider_options(&options).expect("settings")).expect("client");
        let mut command = cmd("ECHO");
        command.arg("bad");
        // A pre-admitted pooled lease uses the helper's admission-exempt
        // branch. A complete malformed reply must discard even an open socket.
        let mut pooled = client.get_connection().expect("pooled lease");
        let error = command
            .query_receive::<u64>(&mut pooled, &client, false)
            .expect_err("invalid integer reply");
        assert!(matches!(
            error,
            RedisProviderError::OutcomeUnknown { operation: "receive" }
        ));
        assert!(pooled.is_open(), "full malformed response leaves the socket open");
        drop(pooled);
        drop(
            client
                .get_connection()
                .expect("conversion failure discards idle lease and releases cap-one permit"),
        );
        let mut dedicated = client.get_dedicated_connection().expect("dedicated receiver socket");
        let held = client.try_command().expect("occupy the single short-command slot");
        assert!(matches!(
            command.query_receive::<u64>(&mut dedicated, &client, true),
            Err(RedisProviderError::ResourceLimit { .. })
        ));
        drop(held);
        let error = command
            .query_receive::<u64>(&mut dedicated, &client, true)
            .expect_err("typed conversion fails");
        assert!(matches!(
            error,
            RedisProviderError::OutcomeUnknown { operation: "receive" }
        ));
        drop(
            client
                .try_command()
                .expect("conversion failure releases short-command admission"),
        );
        let observed = server.finish();
        assert_eq!(observed.iter().filter(|command| command[0] == "SELECT").count(), 3);
        assert_eq!(
            observed.iter().filter(|command| command[0] == "ECHO").count(),
            2,
            "contention must fail before sending"
        );
    }
}
