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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use testcontainers::Container;
use testcontainers::ContainerRequest;
use testcontainers::GenericImage;
use testcontainers::ImageExt;
use testcontainers::TestcontainersError;
use testcontainers::bollard::errors::Error as DockerError;
use testcontainers::bollard::query_parameters::RemoveContainerOptionsBuilder;
use testcontainers::core::IntoContainerPort;
use testcontainers::core::Mount;
use testcontainers::core::WaitFor;
use testcontainers::core::client::docker_client_instance;
use testcontainers::core::error::ClientError;
use testcontainers::runners::SyncRunner;

pub(crate) type RedisContainer = Container<GenericImage>;

/// Distinguishes an unavailable container backend from a broken Redis fixture.
#[derive(Debug)]
pub(crate) enum ContainerStartError {
    Unavailable {
        image_tag: String,
        source: TestcontainersError,
        container_name: Option<String>,
        cleanup_error: Option<String>,
    },
    Fixture {
        image_tag: String,
        source: TestcontainersError,
        container_name: Option<String>,
        cleanup_error: Option<String>,
    },
}

impl fmt::Display for ContainerStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable { image_tag, source, .. } => {
                write!(
                    formatter,
                    "Redis container backend unavailable for redis:{image_tag}: {source}"
                )?;
            }
            Self::Fixture { image_tag, source, .. } => {
                write!(
                    formatter,
                    "Redis container fixture failed for redis:{image_tag}: {source}"
                )?;
            }
        }
        let (container_name, cleanup_error) = match self {
            Self::Unavailable { container_name, cleanup_error, .. }
            | Self::Fixture { container_name, cleanup_error, .. } => (container_name, cleanup_error),
        };
        if let Some(name) = container_name {
            write!(formatter, "; attempted container={name}")?;
        }
        if let Some(error) = cleanup_error {
            write!(formatter, "; failed container cleanup: {error}")?;
        }
        Ok(())
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
    container_request(image_tag, command, false)
}

/// Builds Redis or Sentinel readiness, exposing a port only outside host networking.
fn container_request(
    image_tag: &str,
    command: impl IntoIterator<Item = impl Into<String>>,
    host_network: bool,
) -> ContainerRequest<GenericImage> {
    let command: Vec<String> = command.into_iter().map(Into::into).collect();
    let ready_message = if command.iter().any(|argument| argument == "--sentinel") {
        "Sentinel ID is"
    } else {
        "Ready to accept connections"
    };
    let mut image = GenericImage::new("redis", image_tag)
        .with_wait_for(WaitFor::message_on_stdout(ready_message));
    if !host_network {
        image = image.with_exposed_port(6379.tcp());
    }
    image.with_cmd(command)
}

/// Starts Redis with the requested mounts and optional Docker network.
pub(crate) fn start_redis_container(
    image_tag: &str,
    command: impl IntoIterator<Item = impl Into<String>>,
    mounts: &[Mount],
    network: Option<&str>,
) -> Result<RedisContainer, ContainerStartError> {
    start_container(image_tag, command, mounts, network, None)
}

/// Starts Redis with a stable host port that survives a container restart.
pub(crate) fn start_redis_container_with_host_port(
    image_tag: &str,
    command: impl IntoIterator<Item = impl Into<String>>,
    mounts: &[Mount],
    network: Option<&str>,
    host_port: u16,
) -> Result<RedisContainer, ContainerStartError> {
    start_container(image_tag, command, mounts, network, Some(host_port))
}

fn start_container(
    image_tag: &str,
    command: impl IntoIterator<Item = impl Into<String>>,
    mounts: &[Mount],
    network: Option<&str>,
    host_port: Option<u16>,
) -> Result<RedisContainer, ContainerStartError> {
    let host_network = network == Some("host");
    let mut request = if host_network {
        container_request(image_tag, command, true)
    } else {
        redis_container_request(image_tag, command)
    };
    for mount in mounts {
        request = request.with_mount(mount.clone());
    }
    if let Some(network) = network {
        request = request.with_network(network);
    }
    if host_network {
        // The runner publishes image ports by default even for host networking.
        request = request.with_host_config_modifier(|host_config| {
            host_config.publish_all_ports = Some(false);
            host_config.port_bindings = None;
        });
    } else if let Some(host_port) = host_port {
        request = request.with_mapped_port(host_port, 6379.tcp());
    }
    let container_name = unique_container_name();
    request.with_container_name(&container_name).start().map_err(|source| {
        // StartContainer can fail after Docker creates a container but before
        // Testcontainers returns its RAII handle. Remove that owned attempt by name.
        let cleanup = cleanup_failed_container(&container_name).err();
        let mut error = classify_start_error(image_tag, source);
        match &mut error {
            ContainerStartError::Unavailable { container_name: name, cleanup_error, .. }
            | ContainerStartError::Fixture { container_name: name, cleanup_error, .. } => {
                *name = Some(container_name);
                *cleanup_error = cleanup;
            }
        }
        error
    })
}

fn unique_container_name() -> String {
    static ATTEMPT: AtomicU64 = AtomicU64::new(0);
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    format!("rer-redis-{}-{timestamp}-{}", std::process::id(), ATTEMPT.fetch_add(1, Ordering::Relaxed))
}

fn cleanup_failed_container(container_name: &str) -> Result<(), String> {
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("redis-container-cleanup".into())
            .spawn_scoped(scope, || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| error.to_string())?;
                runtime.block_on(async {
                    let docker = docker_client_instance().await.map_err(|error| error.to_string())?;
                    let options = RemoveContainerOptionsBuilder::new().force(true).v(true).build();
                    match docker.remove_container(container_name, Some(options)).await {
                        Ok(()) | Err(DockerError::DockerResponseServerError { status_code: 404, .. }) => Ok(()),
                        Err(error) => Err(error.to_string()),
                    }
                })
            })
            .map_err(|error| error.to_string())?
            .join()
            .map_err(|_| "container cleanup thread panicked".to_owned())?
    })
}

fn classify_start_error(image_tag: &str, source: TestcontainersError) -> ContainerStartError {
    match source {
        TestcontainersError::Client(
            ClientError::Init(_)
            | ClientError::Configuration(_)
            | ClientError::InvalidDockerHost(_)
            | ClientError::PullImage { .. }
            | ClientError::BuildImage { .. }
            | ClientError::CreateContainer(_),
        ) => ContainerStartError::Unavailable {
            image_tag: image_tag.to_owned(),
            source,
            container_name: None,
            cleanup_error: None,
        },
        _ => ContainerStartError::Fixture {
            image_tag: image_tag.to_owned(),
            source,
            container_name: None,
            cleanup_error: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use testcontainers::core::error::ConfigurationError;
    use testcontainers::core::error::WaitContainerError;
    use testcontainers::core::ports::PortMappingError;

    use super::*;

    #[test]
    fn test_pre_creation_errors_allow_local_fallback() {
        let errors = [
            ClientError::Init(std::io::Error::other("init").into()),
            ClientError::Configuration(ConfigurationError::InvalidDockerHost("bad host".into())),
            ClientError::InvalidDockerHost("bad host".into()),
            ClientError::PullImage {
                descriptor: "redis:7-alpine".into(),
                err: std::io::Error::other("pull").into(),
            },
            ClientError::BuildImage {
                descriptor: "redis:7-alpine".into(),
                err: std::io::Error::other("build").into(),
            },
            ClientError::CreateContainer(std::io::Error::other("create").into()),
        ];
        for source in errors {
            let error = classify_start_error("7-alpine", source.into());
            assert!(
                matches!(error, ContainerStartError::Unavailable { .. }),
                "{error}"
            );
            assert!(error.to_string().contains("redis:7-alpine"));
            assert!(error.source().is_some());
        }
    }

    #[test]
    fn test_post_creation_errors_reject_local_fallback() {
        let errors = [
            ClientError::StartContainer(std::io::Error::other("start").into()).into(),
            ClientError::InspectContainer(std::io::Error::other("inspect").into()).into(),
            ClientError::PortMapping(PortMappingError::FailedToParseHostPort(
                "invalid".parse::<u16>().unwrap_err(),
            ))
            .into(),
            TestcontainersError::WaitContainer(WaitContainerError::StartupTimeout),
        ];
        for source in errors {
            let error = classify_start_error("7-alpine", source);
            assert!(
                matches!(error, ContainerStartError::Fixture { .. }),
                "{error}"
            );
            assert!(error.to_string().contains("redis:7-alpine"));
            assert!(error.source().is_some());
        }
    }

    #[test]
    fn test_cleanup_failure_preserves_original_start_error() {
        let error = ContainerStartError::Fixture {
            image_tag: "7-alpine".into(),
            source: ClientError::StartContainer(std::io::Error::other("original start failure").into()).into(),
            container_name: Some("rer-redis-attempt".into()),
            cleanup_error: Some("cleanup denied".into()),
        };
        let display = error.to_string();
        assert!(display.contains("original start failure"));
        assert!(display.contains("rer-redis-attempt"));
        assert!(display.contains("cleanup denied"));
        assert!(matches!(
            error.source().and_then(|source| source.downcast_ref::<TestcontainersError>()),
            Some(TestcontainersError::Client(ClientError::StartContainer(_)))
        ));
    }
}
