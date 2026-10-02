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

use crate::client::PooledConnection;
use crate::error::RedisProviderError;
use crate::redis_provider_error::from_redis_error;

/// Dedicated receiver commands are covered by the receiver permit.
pub(super) trait ReceiveCommand {
    /// Sends a receiver command on a dedicated socket with bounded socket waits.
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
    ///
    /// # Returns
    ///
    /// A typed owned response after bounded blocking I/O.
    ///
    /// # Errors
    ///
    /// Returns sanitized setup/top-level rejection or an unknown receive
    /// outcome for I/O or malformed conversion replies.
    fn query_receive<T: FromRedisValue>(
        &self,
        connection: &mut PooledConnection,
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
    ///
    /// # Returns
    ///
    /// The converted owned reply.
    ///
    /// # Errors
    ///
    /// Returns setup errors, classified top-level rejections, or
    /// outcome-unknown after uncertain I/O or nested/protocol failures.
    fn query_receive<T: FromRedisValue>(
        &self,
        connection: &mut PooledConnection,
    ) -> Result<T, RedisProviderError> {
        let raw = connection
            .req_command(self)
            .map_err(|_| RedisProviderError::OutcomeUnknown {
                operation: "receive",
            })?;
        if let Value::ServerError(error) = raw {
            let error: RedisError = error.into();
            return Err(from_redis_error("receive", &error));
        }
        T::from_owned_redis_value(raw).map_err(|_| {
            connection.discard();
            RedisProviderError::OutcomeUnknown {
                operation: "receive",
            }
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
    use crate::client::CommandClass;
    use crate::config::RedisEventBusConfig;
    use crate::error::RedisProviderError;
    use crate::tests::support::redis_support::scripted_redis::ScriptedRedis;
    use crate::tests::support::redis_support::scripted_redis::Step;

    #[test]
    fn test_sync_typed_receive_conversion_ignores_general_saturation() {
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
            ("redis.max_concurrent_commands".into(), "2".into()),
            ("redis.max_idle_connections".into(), "1".into()),
            ("redis.connect_timeout_ms".into(), "100".into()),
            ("redis.command_timeout_ms".into(), "100".into()),
        ]
        .into();
        let client =
            Client::new(&RedisEventBusConfig::from_provider_options(&options).expect("settings"))
                .expect("client");
        let mut command = cmd("ECHO");
        command.arg("bad");
        // A complete malformed reply must discard even an open socket.
        let mut pooled = client
            .get_connection(CommandClass::General)
            .expect("pooled lease");
        let error = command
            .query_receive::<u64>(&mut pooled)
            .expect_err("invalid integer reply");
        assert!(matches!(
            error,
            RedisProviderError::OutcomeUnknown {
                operation: "receive"
            }
        ));
        assert!(
            pooled.is_open(),
            "full malformed response leaves the socket open"
        );
        drop(pooled);
        drop(
            client
                .get_connection(CommandClass::General)
                .expect("conversion failure discards idle lease and releases general permit"),
        );
        let mut dedicated = client
            .get_dedicated_connection()
            .expect("dedicated receiver socket");
        let held = client
            .try_command(CommandClass::General)
            .expect("occupy general slot");
        let error = command
            .query_receive::<u64>(&mut dedicated)
            .expect_err("typed conversion fails");
        assert!(matches!(
            error,
            RedisProviderError::OutcomeUnknown {
                operation: "receive"
            }
        ));
        assert!(matches!(
            client.try_command(CommandClass::General),
            Err(RedisProviderError::ResourceLimit { .. })
        ));
        drop(held);
        let observed = server.finish();
        assert_eq!(
            observed
                .iter()
                .filter(|command| command[0] == "SELECT")
                .count(),
            3
        );
        assert_eq!(
            observed
                .iter()
                .filter(|command| command[0] == "ECHO")
                .count(),
            2,
            "dedicated receiver sends despite general saturation"
        );
    }
}
