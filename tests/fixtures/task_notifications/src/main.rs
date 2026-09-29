// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Application example: monotonic task projection and fallible notifications.

mod task_event_json_codec;

use std::env::args;
use std::error::Error;
use std::io::Error as IoError;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc::channel;
use std::time::Duration;

use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus_redis as _;
use qubit_spi::ProviderSelection;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::event::TaskEvent;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskState;
use qubit_task::service::LocalTaskOutcome;
use tokio::main as tokio_main;
use tokio::runtime::Handle;

use crate::task_event_json_codec::TaskEventJsonCodec;

/// Creates the discovered sync facade for Redis `url` and scope `namespace`.
///
/// If `codec` is true, registers the application TaskEvent JSON codec. Returns
/// the facade, or codec/configuration/discovery/provider errors. Construction
/// performs no Redis IO; intentionally omitting the codec exercises publication
/// failure.
fn create_bus(url: &str, namespace: &str, codec: bool) -> Result<EventBus, Box<dyn Error>> {
    let mut codecs = CodecRegistry::new();
    if codec {
        codecs.register::<TaskEvent>(Arc::new(TaskEventJsonCodec(ContentType::new("application/json")?)));
    }
    let options: ProviderOptions = [
        ("redis.url".into(), url.into()),
        ("redis.namespace".into(), namespace.into()),
    ]
    .into();
    let config = EventBusConfig::default()
        .with_selection(ProviderSelection::named("redis-streams")?)
        .with_provider_options(options)
        .with_facade_config(EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs)));
    Ok(EventBusRegistry::discover()?.create(&config)?)
}

/// Runs real task notifications against the Redis URL in the first CLI
/// argument.
///
/// Uses a Tokio multithread runtime and performs task/Redis/channel IO. Returns
/// argument/configuration/provider/task/channel errors; assertions panic for
/// lifecycle, projection-regression, or notification-failure contract
/// violations.
#[tokio_main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = args().nth(1).ok_or("expected Redis URL")?;
    let bus = create_bus(&url, "task-notification-fixture", true)?;
    let topic = Topic::<TaskEvent>::new("task.lifecycle")?;
    let projection = Arc::new(Mutex::new(None::<TaskEvent>));
    let consumer_projection = projection.clone();
    let (sender, receiver) = channel();
    let subscription = bus.subscribe(
        SubscribeRequest::builder()
            .subscriber_id(SubscriberId::new("task-projection")?)
            .topic(topic.clone())
            .start_position(StartPosition::Earliest)
            .durability(SubscriptionDurability::Durable)
            .build()?,
        move |delivery| {
            let event = delivery.payload().clone();
            let mut current = consumer_projection.lock().expect("projection lock must remain usable");
            let applied = current.as_ref().is_none_or(|previous| {
                previous.task_id == event.task_id && event.state_version > previous.state_version
            });
            if applied {
                *current = Some(event.clone());
            }
            let _ = sender.send((event, applied));
            Ok::<(), DeliveryError>(())
        },
    )?;
    let service = TaskExecutionServiceBuilder::in_memory()
        .runtime_handle(Handle::current())
        .event_bus(bus.clone())
        .build()
        .await?;
    let id = service
        .submit_local(|_| LocalTaskOutcome::<(), IoError>::Succeeded {
            value: (),
            summary: TaskOutput::default(),
        })
        .await?
        .task_id();
    let summary = service.wait(id).await?;
    assert_eq!(summary.state, TaskState::Succeeded);
    service.shutdown().await?;
    let mut lifecycle = Vec::new();
    for _ in 0..3 {
        let (event, applied) = receiver.recv_timeout(Duration::from_secs(5))?;
        assert_eq!(event.task_id, id);
        assert!(applied, "initial increasing lifecycle revision must apply");
        lifecycle.push(event);
    }
    assert_eq!(
        lifecycle.iter().map(|event| event.state_version).collect::<Vec<_>>(),
        [0, 1, 2]
    );
    assert_eq!(
        lifecycle.iter().map(|event| &event.state).collect::<Vec<_>>(),
        [&TaskState::Queued, &TaskState::Running, &TaskState::Succeeded]
    );
    let completed = TaskEvent::from(&summary);
    for event in [
        completed.clone(),
        completed.clone(),
        lifecycle[1].clone(),
        lifecycle[0].clone(),
    ] {
        bus.publish(PublishRequest::new(topic.clone(), event)?)?;
        let (_, applied) = receiver.recv_timeout(Duration::from_secs(5))?;
        assert!(
            !applied,
            "duplicate and stale notifications must not change the projection"
        );
        let snapshot = projection.lock().expect("projection lock must remain usable");
        let snapshot = snapshot.as_ref().ok_or("projection missing")?;
        assert_eq!(snapshot.task_id, id);
        assert_eq!(snapshot.state_version, summary.state_version);
        assert_eq!(snapshot.state, TaskState::Succeeded);
    }
    assert_eq!(
        service
            .notification_stats()
            .ok_or("missing notification stats")?
            .publish_error,
        0
    );
    subscription.cancel()?;
    bus.shutdown(ShutdownMode::Immediate)?;

    // Missing application codec makes every notification fail. The task's
    // business state still commits; this example does not implement an outbox.
    let failing_bus = create_bus(&url, "task-failed-notification-fixture", false)?;
    let service = TaskExecutionServiceBuilder::in_memory()
        .runtime_handle(Handle::current())
        .event_bus(failing_bus.clone())
        .build()
        .await?;
    let id = service
        .submit_local(|_| LocalTaskOutcome::<(), IoError>::Succeeded {
            value: (),
            summary: TaskOutput::default(),
        })
        .await?
        .task_id();
    assert_eq!(service.wait(id).await?.state, TaskState::Succeeded);
    service.shutdown().await?;
    assert_eq!(
        service
            .notification_stats()
            .ok_or("missing failure stats")?
            .publish_error,
        3
    );
    assert_eq!(
        service.wait(id).await?.state,
        TaskState::Succeeded,
        "notification failure cannot roll back state"
    );
    failing_bus.shutdown(ShutdownMode::Immediate)?;
    println!("task notifications: lifecycle, duplicate/stale projection, failure preserves business state passed");
    Ok(())
}
