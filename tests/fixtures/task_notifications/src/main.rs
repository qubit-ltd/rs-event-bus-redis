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
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc::channel;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_codec::ValueBytesCodecDescriptor;
use qubit_codec::ValueBytesCodecRegistration;
use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_codec::ValueCodecRegistration;
use qubit_codec::ValueCodecRegistrationSource;
use qubit_event_bus::DeliveryError;
use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::AsyncEventBusRegistry;
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
use qubit_model_metadata::metadata::ModelId;
use qubit_model_metadata::metadata::ModelIdBuf;
use qubit_spi::ProviderSelection;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::CancellationMode;
use qubit_task::TaskContext;
use qubit_task::TaskHandler;
use qubit_task::TaskHandlerDescriptor;
use qubit_task::model::ResourceCapacity;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::event::TaskEvent;
use qubit_task::TaskRequest;
use qubit_task::TaskSummary;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskState;
use qubit_task::TaskExecutionService;
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::TaskFuture;

use crate::task_event_json_codec::TaskEventJsonCodec;

#[derive(Default)]
struct U32Codec;

impl qubit_codec::ValueEncoder<u32> for U32Codec {
    type Output = Vec<u8>;
    type Error = std::convert::Infallible;

    fn encode(&mut self, value: &u32) -> Result<Vec<u8>, Self::Error> {
        Ok(value.to_le_bytes().to_vec())
    }
}

impl qubit_codec::ValueDecoder<[u8]> for U32Codec {
    type Output = u32;
    type Error = std::array::TryFromSliceError;

    fn decode(&mut self, bytes: &[u8]) -> Result<u32, Self::Error> {
        let bytes: [u8; 4] = bytes.try_into()?;
        Ok(u32::from_le_bytes(bytes))
    }
}

static TASK_CODEC_DESCRIPTOR: ValueBytesCodecDescriptor = ValueBytesCodecDescriptor::of::<U32Codec, u32>();
static TASK_CODEC: ValueBytesCodecRegistration = ValueCodecRegistration::new(
    ValueCodecId::new("example.task_notifications.u32"),
    &TASK_CODEC_DESCRIPTOR,
    ValueCodecRegistrationSource::new("rs-event-bus-redis", "task_notifications", "main.rs", 1),
);

struct TaskIds(AtomicU64);

impl qubit_id::IdGenerator for TaskIds {
    fn generate(&self) -> Result<qubit_id::Id, qubit_id::IdGenerationError> {
        Ok(qubit_id::Id::new(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

struct TypedHandler;

impl TaskHandler<u32> for TypedHandler {
    fn run<'a>(&'a self, value: u32, _context: TaskContext) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
        Box::pin(async move {
            assert_eq!(value, 7);
            Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
        })
    }
}

async fn create_task_service(
    store: Arc<SqliteTaskStore>,
    bus: Arc<AsyncEventBus>,
) -> Result<TaskExecutionService, Box<dyn Error>> {
    let codecs = Arc::new(ValueBytesCodecRegistry::from_registrations([&TASK_CODEC])?);
    let mut builder = TaskExecutionServiceBuilder::new(
        store,
        codecs,
        Arc::new(TaskIds(AtomicU64::new(1))),
    )
    .capacity(ResourceCapacity {
        cpu_slots: 1,
        ..ResourceCapacity::default()
    })
    .event_bus(bus)
    .notification_shutdown_timeout(Duration::from_secs(3));
    builder.handlers_mut().register::<u32, _>(
        TaskHandlerDescriptor {
            kind_id: "example.task_notifications".into(),
            payload_type_id: ModelIdBuf::try_from("example.TaskNotificationPayload")?,
            accepted_schema_versions: vec![1],
            cancellation_mode: CancellationMode::Unsupported,
        },
        Arc::new(TypedHandler),
    )?;
    Ok(builder.build().await?)
}

/// Creates the discovered sync facade for Redis `url` and scope `namespace`.
///
/// If `codec` is true, registers the application TaskEvent JSON codec. Returns
/// the facade, or codec/configuration/discovery/provider errors. Construction
/// performs no Redis IO; intentionally omitting the codec exercises publication
/// failure.
fn create_bus(url: &str, namespace: &str, codec: bool) -> Result<EventBus, Box<dyn Error>> {
    let mut codecs = CodecRegistry::new();
    if codec {
        codecs.register::<TaskEvent>(Arc::new(TaskEventJsonCodec(ContentType::new("application/json")?)))?;
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

async fn create_async_bus(url: &str, namespace: &str) -> Result<AsyncEventBus, Box<dyn Error>> {
    let mut codecs = CodecRegistry::new();
    codecs.register::<TaskEvent>(Arc::new(TaskEventJsonCodec(ContentType::new("application/json")?)))?;
    let options: ProviderOptions = [
        ("redis.url".into(), url.into()),
        ("redis.namespace".into(), namespace.into()),
    ].into();
    let config = EventBusConfig::default()
        .with_selection(ProviderSelection::named("redis-streams")?)
        .with_provider_options(options)
        .with_facade_config(EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs)));
    Ok(AsyncEventBusRegistry::discover()?.create(&config).await?)
}

/// Runs real task notifications against the Redis URL in the first CLI
/// argument.
///
/// Uses a Tokio multithread runtime and performs task/Redis/channel IO. Returns
/// argument/configuration/provider/task/channel errors; assertions panic for
/// lifecycle, projection-regression, or notification-failure contract
/// violations.
#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = args().nth(1).ok_or("expected Redis URL")?;
    let database_dir = tempfile::tempdir()?;
    let store = Arc::new(SqliteTaskStore::open_next(database_dir.path().join("tasks.sqlite"))?);
    let bus = create_bus(&url, "task-notification-fixture", true)?;
    let async_bus = Arc::new(create_async_bus(&url, "task-notification-fixture").await?);
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
            if applied { *current = Some(event.clone()); }
            let _ = sender.send((event, applied));
            Ok::<(), DeliveryError>(())
        },
    )?;
    let task_service = create_task_service(Arc::clone(&store), Arc::clone(&async_bus)).await?;
    let request = TaskRequest::new(
        "example.task_notifications",
        ModelId::new("example.TaskNotificationPayload"),
        1,
        ValueCodecId::new("example.task_notifications.u32"),
        7_u32,
    );
    let accepted = task_service.submit(request).await?;
    let summary = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(task) = task_service.get(accepted.id).await? {
                if task.state == TaskState::Succeeded {
                    return Ok::<TaskSummary, Box<dyn Error>>(task);
                }
            }
            tokio::task::yield_now().await;
        }
    }).await??;
    let id = summary.id;
    let event_task_id = id;
    let mut lifecycle = Vec::new();
    for _ in 0..3 {
        let (event, applied) = receiver.recv_timeout(Duration::from_secs(5))?;
        assert_eq!(event.task_id, event_task_id);
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
        let _ = bus.publish(PublishRequest::new(topic.clone(), event)?)?;
        let (_, applied) = receiver.recv_timeout(Duration::from_secs(5))?;
        assert!(
            !applied,
            "duplicate and stale notifications must not change the projection"
        );
        let snapshot = projection.lock().expect("projection lock must remain usable");
        let snapshot = snapshot.as_ref().ok_or("projection missing")?;
        assert_eq!(snapshot.task_id, event_task_id);
        assert_eq!(snapshot.state_version, summary.state_version);
        assert_eq!(snapshot.state, TaskState::Succeeded);
    }
    task_service.shutdown().await?;
    assert_eq!(task_service.notification_stats().published, 3);
    let _ = async_bus.shutdown(ShutdownMode::Immediate).await?;
    subscription.cancel()?;
    let _ = bus.shutdown(ShutdownMode::Immediate)?;

    assert_eq!(task_service.get(id).await?.ok_or("task disappeared")?.state, TaskState::Succeeded,
        "task lifecycle publication cannot roll back committed task state");
    println!("task notifications: durable lifecycle, duplicate/stale projection, business state passed");
    Ok(())
}
