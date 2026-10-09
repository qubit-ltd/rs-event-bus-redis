// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Controlled dedicated receiver command execution.

use qubit_event_bus::spi::SpiFuture;
use redis::Cmd;
use redis::FromRedisValue;
use redis::RedisError;
use redis::Value;
use redis::aio::MultiplexedConnection;

use crate::error::RedisProviderError;
use crate::redis_provider_error::from_redis_error;

/// Dedicated receiver commands are covered by the receiver permit.
pub(super) trait ReceiveCommand {
    /// Sends one receiver command on the host executor.
    ///
    /// # Type Parameters
    ///
    /// - `'a`: Shared command, client, connection, and returned-future
    ///   lifetime.
    /// - `T`: Send owned response converted after top-level rejection
    ///   classification.
    ///
    /// # Parameters
    ///
    /// - `connection`: Dedicated multiplexed receiver transport.
    ///
    /// # Returns
    ///
    /// A host-polled future yielding the typed response; no executor is
    /// started.
    ///
    /// # Errors
    ///
    /// Returns sanitized top-level rejection or unknown outcome after I/O or
    /// malformed conversion. Cancellation may
    /// leave execution unknown.
    fn query_receive<'a, T: FromRedisValue + Send + 'a>(
        &'a self,
        connection: &'a mut MultiplexedConnection,
    ) -> SpiFuture<'a, Result<T, RedisProviderError>>;
}
impl ReceiveCommand for Cmd {
    /// Applies host-polled receiver I/O while retaining application admission.
    ///
    /// # Type Parameters
    ///
    /// - `'a`: Shared request, client, transport, and future lifetime.
    /// - `T`: Send owned RESP conversion target.
    ///
    /// # Parameters
    ///
    /// - `connection`: Dedicated transport whose response wait is set for short
    ///   commands.
    ///
    /// # Returns
    ///
    /// A future yielding a typed owned reply. Cancellation may leave the Redis
    /// request executing in the transport driver.
    ///
    /// # Errors
    ///
    /// Returns classified top-level rejection or outcome-unknown for I/O,
    /// malformed replies, and nested server errors.
    fn query_receive<'a, T: FromRedisValue + Send + 'a>(
        &'a self,
        connection: &'a mut MultiplexedConnection,
    ) -> SpiFuture<'a, Result<T, RedisProviderError>> {
        Box::pin(async move {
            let raw = connection
                .send_packed_command(self)
                .await
                .map_err(|_| RedisProviderError::OutcomeUnknown { operation: "receive" })?;
            if let Value::ServerError(error) = raw {
                let error: RedisError = error.into();
                return Err(from_redis_error("receive", &error));
            }
            T::from_owned_redis_value(raw).map_err(|_| RedisProviderError::OutcomeUnknown { operation: "receive" })
        })
    }
}

#[cfg(test)]
mod tests {
    use futures_lite::future::block_on;
    use qubit_event_bus::model::ProviderOptions;
    use redis::cmd;

    use super::ReceiveCommand;
    use crate::client::Client;
    use crate::client::CommandClass;
    use crate::config::RedisEventBusConfig;
    use crate::error::RedisProviderError;
    use crate::tests::support::redis_support::scripted_redis::ScriptedRedis;
    use crate::tests::support::redis_support::scripted_redis::Step;

    #[test]
    fn test_async_typed_receive_conversion_ignores_general_saturation() {
        // The public receiver currently requests Value. This deliberately tests
        // the private helper's generic conversion contract with u64 instead.
        let server = ScriptedRedis::start(vec![
            Step::reply("SELECT", b"+OK\r\n"),
            Step::reply("ECHO", b"$3\r\nbad\r\n"),
        ])
        .expect("scripted Redis");
        let options: ProviderOptions = [
            ("redis.url".into(), format!("{}3", server.url())),
            ("redis.max_concurrent_commands".into(), "2".into()),
            ("redis.max_idle_connections".into(), "1".into()),
            ("redis.connect_timeout_ms".into(), "1000".into()),
            ("redis.command_timeout_ms".into(), "100".into()),
        ]
        .into();
        let client =
            Client::new(&RedisEventBusConfig::from_provider_options(&options).expect("settings")).expect("client");
        let mut command = cmd("ECHO");
        command.arg("bad");
        block_on(async {
            let mut connection = client
                .get_async_dedicated_connection()
                .await
                .expect("dedicated receiver socket");
            let held = client.try_command(CommandClass::General).expect("occupy general slot");
            let error = command
                .query_receive::<u64>(&mut connection)
                .await
                .expect_err("invalid integer reply");
            assert!(matches!(
                error,
                RedisProviderError::OutcomeUnknown { operation: "receive" }
            ));
            assert!(matches!(
                client.try_command(CommandClass::General),
                Err(RedisProviderError::ResourceLimit { .. })
            ));
            drop(held);
        });
        let observed = server.finish();
        assert_eq!(
            observed,
            [
                vec!["SELECT".to_owned(), "3".to_owned()],
                vec!["ECHO".to_owned(), "bad".to_owned()]
            ],
            "dedicated receiver sends despite general saturation"
        );
    }
}
