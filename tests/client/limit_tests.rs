// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Fail-fast command admission observed while another XADD is pending.
use std::sync::Arc;
use std::sync::mpsc::channel;
use std::thread::sleep;
use std::thread::spawn;
use std::time::Duration;
use std::time::Instant;

use futures_lite::future::block_on;
use futures_lite::future::poll_once;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
use qubit_spi::AsyncServiceProvider;

use super::assert_error;
use super::message;
use super::options;
use super::support::blackhole_redis::BlackholeRedis;

#[cfg(feature = "async")]
#[test]
fn test_async_command_cap_fails_before_second_xadd_and_cancel_releases() {
    let server = BlackholeRedis::start(true, None);
    let mut settings = options(server.url(), 1);
    settings.insert("redis.command_timeout_ms".into(), "2000".into());
    let bus = block_on(
        AsyncRedisEventBusProvider.create_configured(&EventBusConfig::default().with_provider_options(settings)),
    )
    .expect("provider");
    block_on(async {
        let mut first = bus.publish(message());
        assert!(poll_once(&mut first).await.is_none());
        let deadline = Instant::now() + Duration::from_secs(1);
        while server.commands() == 0 && Instant::now() < deadline {
            assert!(poll_once(&mut first).await.is_none());
            sleep(Duration::from_millis(1));
        }
        assert_eq!(server.commands(), 1);
        let (sender, receiver) = channel();
        let concurrent = Arc::clone(&bus);
        spawn(move || {
            sender.send(block_on(concurrent.publish(message()))).expect("watchdog");
        });
        let result = receiver
            .recv_timeout(Duration::from_millis(250))
            .expect("cap must fail fast");
        assert_error(result.expect_err("second command rejected"), "resource_limit", true);
        assert_eq!(server.commands(), 1);
        drop(first);
        let mut next = bus.publish(message());
        assert!(
            poll_once(&mut next).await.is_none(),
            "cancelled operation releases application permit"
        );
    });
}
