// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared Testcontainers lifecycle and startup errors for Redis test fixtures.

use std::error::Error;
use std::fmt;

use testcontainers::Container;
use testcontainers::ContainerRequest;
use testcontainers::GenericImage;
use testcontainers::ImageExt;
use testcontainers::TestcontainersError;
use testcontainers::core::IntoContainerPort;
use testcontainers::core::Mount;
use testcontainers::core::WaitFor;
use testcontainers::runners::SyncRunner;

pub(crate) type RedisContainer = Container<GenericImage>;

/// Distinguishes an unavailable container backend from a broken Redis fixture.
#[derive(Debug)]
pub(crate) enum ContainerStartError {
    Unavailable {
        image_tag: String,
        source: TestcontainersError,
    },
    Fixture {
        image_tag: String,
        source: TestcontainersError,
    },
}

impl fmt::Display for ContainerStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable { image_tag, source } => {
                write!(
                    formatter,
                    "Redis container backend unavailable for redis:{image_tag}: {source}"
                )
            }
            Self::Fixture { image_tag, source } => {
                write!(
                    formatter,
                    "Redis container fixture failed for redis:{image_tag}: {source}"
                )
            }
        }
    }
}

impl Error for ContainerStartError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Unavailable { source, .. } | Self::Fixture { source, .. } => Some(source),
        }
    }
}

/// Builds a Redis request with a dynamic port and Redis log readiness check.
pub(crate) fn redis_container_request(
    image_tag: &str,
    command: impl IntoIterator<Item = impl Into<String>>,
) -> ContainerRequest<GenericImage> {
    GenericImage::new("redis", image_tag)
        .with_exposed_port(6379.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .with_cmd(command)
}

/// Starts Redis with the requested mounts and optional Docker network.
pub(crate) fn start_redis_container(
    image_tag: &str,
    command: impl IntoIterator<Item = impl Into<String>>,
    mounts: &[Mount],
    network: Option<&str>,
) -> Result<RedisContainer, ContainerStartError> {
    let mut request = redis_container_request(image_tag, command);
    for mount in mounts {
        request = request.with_mount(mount.clone());
    }
    if let Some(network) = network {
        request = request.with_network(network);
    }
    request.start().map_err(|source| match source {
        TestcontainersError::Client(_) => ContainerStartError::Unavailable {
            image_tag: image_tag.to_owned(),
            source,
        },
        _ => ContainerStartError::Fixture {
            image_tag: image_tag.to_owned(),
            source,
        },
    })
}
