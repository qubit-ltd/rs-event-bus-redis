// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public configuration bounds for finite transport and resource budgets.

use std::env::var;
use std::time::Duration;

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus_redis::config::RedisEventBusConfig;
use qubit_event_bus_redis::error::RedisProviderError;

#[test]
fn test_transport_limits_defaults() {
    let config = RedisEventBusConfig::default();
    assert_eq!(config.connect_timeout(), Duration::from_millis(2_000));
    assert_eq!(config.command_timeout(), Duration::from_millis(2_000));
    assert_eq!(config.max_concurrent_commands(), 64);
    assert_eq!(config.reserved_settlement_commands(), 8);
    assert_eq!(config.max_active_receivers(), 256);
    assert_eq!(config.max_payload_bytes(), 1_048_576);
    assert_eq!(config.max_wire_bytes(), 8_388_608);
}

#[test]
fn test_transport_limits_bounds_and_relationships() {
    for (key, maximum) in [
        ("redis.connect_timeout_ms", 60_000),
        ("redis.command_timeout_ms", 60_000),
        ("redis.max_concurrent_commands", 4_096),
        ("redis.max_active_receivers", 4_096),
        ("redis.max_payload_bytes", 67_108_864),
        ("redis.max_wire_bytes", 268_435_456),
    ] {
        for value in [
            0.to_string(),
            (maximum + 1).to_string(),
            "184467440737095516160".into(),
            "+1".into(),
            "x".into(),
        ] {
            let options: ProviderOptions = [(key.into(), value.clone())].into();
            assert!(
                matches!(
                    RedisEventBusConfig::from_provider_options(&options),
                    Err(RedisProviderError::Configuration(_))
                ),
                "{key}={value}"
            );
        }
        for value in [
            if key == "redis.max_concurrent_commands" {
                2
            } else {
                1
            },
            maximum,
        ] {
            let options: ProviderOptions = [
                (key.into(), value.to_string()),
                ("redis.max_idle_connections".into(), "1".into()),
                ("redis.max_payload_bytes".into(), "1".into()),
                ("redis.max_wire_bytes".into(), "268435456".into()),
            ]
            .into_iter()
            .chain([(key.into(), value.to_string())])
            .collect();
            assert!(
                RedisEventBusConfig::from_provider_options(&options).is_ok(),
                "{key}={value}"
            );
        }
    }
    for options in [
        [
            ("redis.max_payload_bytes".into(), "9".into()),
            ("redis.max_wire_bytes".into(), "8".into()),
        ]
        .into(),
        [
            ("redis.max_concurrent_commands".into(), "7".into()),
            ("redis.max_idle_connections".into(), "8".into()),
        ]
        .into(),
    ] {
        assert!(RedisEventBusConfig::from_provider_options(&options).is_err());
    }
}

#[test]
fn test_reserved_settlement_limits_reject_invalid_relationships() {
    for options in [
        [("redis.max_concurrent_commands".into(), "1".into())].into(),
        [("redis.reserved_settlement_commands".into(), "0".into())].into(),
        [
            ("redis.max_concurrent_commands".into(), "4".into()),
            ("redis.reserved_settlement_commands".into(), "4".into()),
            ("redis.max_idle_connections".into(), "1".into()),
        ]
        .into(),
    ] {
        assert!(matches!(
            RedisEventBusConfig::from_provider_options(&options),
            Err(RedisProviderError::Configuration(_))
        ));
    }
    let options: ProviderOptions = [
        ("redis.max_concurrent_commands".into(), "4".into()),
        ("redis.reserved_settlement_commands".into(), "1".into()),
        ("redis.max_idle_connections".into(), "1".into()),
    ]
    .into();
    let config =
        RedisEventBusConfig::from_provider_options(&options).expect("valid short-command lanes");
    assert_eq!(config.reserved_settlement_commands(), 1);
}

#[test]
fn test_transport_limits_sentinel_endpoints() {
    for nodes in [
        "localhost:0".into(),
        "localhost:65536".into(),
        ",localhost:26379".into(),
        "localhost:26379,".into(),
        "localhost".into(),
        vec!["localhost:26379"; 17].join(","),
    ] {
        let options: ProviderOptions = [
            ("redis.sentinel.nodes".into(), nodes),
            ("redis.sentinel.service_name".into(), "master".into()),
        ]
        .into();
        assert!(RedisEventBusConfig::from_provider_options(&options).is_err());
    }
}

#[test]
fn test_transport_limits_sentinel_valid_boundary() {
    for nodes in [vec!["localhost:65535"; 16].join(","), "[::1]:26379".into()] {
        let options: ProviderOptions = [
            ("redis.sentinel.nodes".into(), nodes),
            ("redis.sentinel.service_name".into(), "master".into()),
        ]
        .into();
        let config = RedisEventBusConfig::from_provider_options(&options)
            .expect("valid bounded Sentinel endpoints");
        assert!(config.sentinel_nodes().expect("Sentinel endpoints").len() <= 16);
    }
}

#[test]
fn test_config_defaults_and_debug_redact_connection_details() {
    let config = RedisEventBusConfig::default();
    assert_eq!(config.connection_url(), "redis://127.0.0.1/");
    assert_eq!(config.namespace(), "qubit");
    assert_eq!(config.max_idle_connections(), 8);
    assert_eq!(config.recovery_interval_ms(), 1_000);
    assert!(config.sentinel_nodes().is_none());
    assert!(config.sentinel_service().is_none());
    assert!(config.stream_maxlen_approx().is_none());
    assert!(!format!("{config:?}").contains("127.0.0.1"));
}

#[test]
fn test_new_config_exposes_supplied_values_and_defaults() {
    let config = RedisEventBusConfig::new("redis://localhost/", "orders").unwrap();
    assert_eq!(config.connection_url(), "redis://localhost/");
    assert_eq!(config.namespace(), "orders");
    assert_eq!(config.recovery_interval_ms(), 1_000);
    assert_eq!(config.max_idle_connections(), 8);
}

#[test]
fn test_new_rejects_invalid_url_credentials_and_namespace() {
    assert!(RedisEventBusConfig::new("not a URL", "orders").is_err());
    assert!(RedisEventBusConfig::new("redis://user:secret@localhost/", "orders").is_err());
    assert!(RedisEventBusConfig::new("redis://localhost/", "").is_err());
    assert!(RedisEventBusConfig::new("redis://localhost/", &"x".repeat(129)).is_err());
}

#[test]
fn test_provider_options_accept_recovery_interval_boundaries_and_limits() {
    for interval in [50, 60_000] {
        let options: ProviderOptions =
            [("redis.recovery_interval_ms".into(), interval.to_string())].into();
        assert_eq!(
            RedisEventBusConfig::from_provider_options(&options)
                .unwrap()
                .recovery_interval_ms(),
            interval
        );
    }
    for interval in ["49", "60001", "0", "overflow"] {
        let options: ProviderOptions =
            [("redis.recovery_interval_ms".into(), interval.into())].into();
        assert!(RedisEventBusConfig::from_provider_options(&options).is_err());
    }
}

#[test]
fn test_event_bus_config_parsing_reads_provider_options() {
    let options: ProviderOptions = [("redis.namespace".into(), "billing".into())].into();
    let event_bus_config = EventBusConfig::default().with_provider_options(options);
    assert_eq!(
        RedisEventBusConfig::from_event_bus_config(&event_bus_config)
            .unwrap()
            .namespace(),
        "billing"
    );
}

#[test]
fn test_provider_options_reject_inline_credentials_invalid_namespaces_and_partial_sentinel() {
    for options in [
        [("redis.url".into(), "redis://user:secret@localhost/".into())].into(),
        [("redis.namespace".into(), "bad\nnamespace".into())].into(),
        [("redis.sentinel.nodes".into(), "127.0.0.1:26379".into())].into(),
    ] {
        assert!(RedisEventBusConfig::from_provider_options(&options).is_err());
    }
    let error = RedisEventBusConfig::from_provider_options(
        &[("redis.url".into(), "not a redis url".into())].into(),
    )
    .unwrap_err();
    assert!(!error.to_string().contains("secret"));
}

#[test]
fn test_provider_options_reject_unknown_redis_keys_without_echoing_values() {
    let options: ProviderOptions = [("redis.passwrod".into(), "secret-value".into())].into();
    let error = RedisEventBusConfig::from_provider_options(&options).unwrap_err();
    assert_eq!(
        error.to_string(),
        "invalid Redis provider configuration: unknown Redis provider option"
    );
    assert!(!error.to_string().contains("secret-value"));
}

#[test]
fn test_sentinel_endpoint_rejects_uri_delimiters() {
    let mut accepted = Vec::new();
    for endpoint in [
        "127.0.0.1?ignored:26379",
        "127.0.0.1#ignored:26379",
        r"127.0.0.1\ignored:26379",
        "localhost]:26379",
        "local[host:26379",
        "[::1:26379",
    ] {
        let options: ProviderOptions = [
            ("redis.sentinel.nodes".into(), endpoint.into()),
            ("redis.sentinel.service_name".into(), "master".into()),
        ]
        .into();
        match RedisEventBusConfig::from_provider_options(&options) {
            Err(RedisProviderError::Configuration("invalid redis.sentinel.nodes")) => {}
            result => accepted.push((endpoint, format!("{result:?}"))),
        }
    }
    assert!(
        accepted.is_empty(),
        "invalid endpoints not rejected: {accepted:?}"
    );
}

#[test]
fn test_sentinel_endpoint_preserves_valid_hosts_and_ports() {
    for endpoint in [
        "sentinel.example.org:26379",
        "localhost:26379",
        "127.0.0.1:26379",
        "[::1]:26379",
        "[2001:db8::1]:26379",
    ] {
        let options: ProviderOptions = [
            ("redis.sentinel.nodes".into(), endpoint.into()),
            ("redis.sentinel.service_name".into(), "master".into()),
        ]
        .into();
        let config = RedisEventBusConfig::from_provider_options(&options)
            .expect("domain, IPv4, and bracketed IPv6 endpoints are valid");
        assert_eq!(
            config.sentinel_nodes().expect("configured endpoints"),
            &[endpoint.to_owned()]
        );
    }
}

#[test]
fn test_config_debug_redacts_password_in_url() {
    let error =
        RedisEventBusConfig::new("redis://user:test-secret@127.0.0.1/", "test").unwrap_err();
    assert!(!error.to_string().contains("test-secret"));
}

#[test]
fn test_provider_options_validate_boundaries_and_sentinel_pairing() {
    for options in [
        [("redis.url".into(), "not a redis URL".into())].into(),
        [("redis.namespace".into(), "".into())].into(),
        [("redis.namespace".into(), "bad\nnamespace".into())].into(),
        [("redis.max_unsettled_per_subscription".into(), "0".into())].into(),
        [(
            "redis.max_unsettled_per_subscription".into(),
            "10001".into(),
        )]
        .into(),
        [("redis.max_unsettled_per_subscription".into(), "NaN".into())].into(),
        [("redis.max_idle_connections".into(), "0".into())].into(),
        [("redis.max_idle_connections".into(), "65".into())].into(),
        [("redis.recovery_interval_ms".into(), "49".into())].into(),
        [("redis.recovery_interval_ms".into(), "60001".into())].into(),
        [("redis.stream_maxlen_approx".into(), "0".into())].into(),
        [("redis.stream_maxlen_approx".into(), "nope".into())].into(),
        [("redis.claim_min_idle_ms".into(), "forever".into())].into(),
        [("redis.sentinel.nodes".into(), " , ".into())].into(),
        [("redis.sentinel.service_name".into(), "master".into())].into(),
        [("redis.sentinel.nodes".into(), "127.0.0.1:26379".into())].into(),
        [("redis.url".into(), "redis://user:secret@localhost/".into())].into(),
    ] {
        assert!(RedisEventBusConfig::from_provider_options(&options).is_err());
    }
}

#[test]
fn test_provider_options_defaults_and_sentinel_credentials_are_redacted() {
    let defaults = RedisEventBusConfig::from_provider_options(&ProviderOptions::new()).unwrap();
    assert_eq!(defaults.connection_url(), "redis://127.0.0.1/");
    assert_eq!(defaults.namespace(), "qubit");
    assert_eq!(defaults.max_idle_connections(), 8);
    assert_eq!(defaults.recovery_interval_ms(), 1_000);
    assert_eq!(defaults.stream_maxlen_approx(), None);
    let options: ProviderOptions = [
        (
            "redis.sentinel.nodes".into(),
            "127.0.0.1:26379, 127.0.0.1:26380".into(),
        ),
        ("redis.sentinel.service_name".into(), "primary".into()),
        (
            "redis.username_env".into(),
            "REDIS_TEST_MISSING_USER".into(),
        ),
    ]
    .into();
    assert!(RedisEventBusConfig::from_provider_options(&options).is_err());
    let options: ProviderOptions = [
        (
            "redis.sentinel.nodes".into(),
            "127.0.0.1:26379, 127.0.0.1:26380".into(),
        ),
        ("redis.sentinel.service_name".into(), "primary".into()),
    ]
    .into();
    let config = RedisEventBusConfig::from_provider_options(&options).unwrap();
    assert_eq!(config.sentinel_nodes().unwrap().len(), 2);
    assert_eq!(config.sentinel_service(), Some("primary"));
}

#[test]
fn test_stream_maxlen_approx_is_an_optional_positive_limit() {
    let options: ProviderOptions = [
        ("redis.stream_maxlen_approx".into(), "4096".into()),
        ("redis.allow_lossy_retention".into(), "true".into()),
    ]
    .into();
    let config = RedisEventBusConfig::from_provider_options(&options).unwrap();
    assert_eq!(
        config.stream_maxlen_approx().map(|value| value.get()),
        Some(4096)
    );
}

#[test]
fn test_redis_and_sentinel_environment_credentials_stay_redacted() {
    let options: ProviderOptions = [
        ("redis.username_env".into(), "PATH".into()),
        ("redis.password_env".into(), "HOME".into()),
        ("redis.sentinel.nodes".into(), "127.0.0.1:26379".into()),
        ("redis.sentinel.service_name".into(), "primary".into()),
        ("redis.sentinel.username_env".into(), "PATH".into()),
        ("redis.sentinel.password_env".into(), "HOME".into()),
        ("redis.claim_min_idle_ms".into(), "125".into()),
        ("redis.max_unsettled_per_subscription".into(), "8".into()),
    ]
    .into();
    let config = RedisEventBusConfig::from_provider_options(&options).unwrap();
    let debug = format!("{config:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains(&var("PATH").unwrap()));
    assert!(!debug.contains(&var("HOME").unwrap()));
}
