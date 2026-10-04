# Migration to Redis provider 0.7

[简体中文](migration.zh_CN.md) · [User guide](user_guide.md)

## Upgrade from provider 0.6 to 0.7

Upgrade `qubit-event-bus-redis` to 0.7 together with `qubit-event-bus` 0.20. Replace repeated `durability(Durable)`, `start_position(...)`, and optional `consumer_group(...)` setup with `RedisSubscriptionProfile::new(start_position).consumer_group(group).options()` (omit `consumer_group` when not used). The profile requires an explicit `StartPosition` and always starts with durable options; a later `.durability(Ephemeral)` is rejected by the Redis capability check.

New entries returned from `XREADGROUP >` expose `provider_attempt() == Some(1)`. Pending and `XAUTOCLAIM` recovery entries remain `None` because the historical delivery count is not propagated yet. Existing wire v1 records, Redis groups, and settlement behavior remain compatible. Core 0.19 also changes `CodecRegistry::register` to return an error on duplicate payload registration; see the [core migration guide](https://github.com/qubit-ltd/rs-event-bus/blob/main/doc/migration.md).

Upgrade `qubit-event-bus-redis` from 0.5 to 0.6 together with
`qubit-event-bus` 0.18. Update direct dependencies, downstream fixtures, and
lockfiles together; do not mix SPI minor generations. The
[core migration guide](https://github.com/qubit-ltd/rs-event-bus/blob/main/doc/migration.md)
explains the `decode(&EncodedPayload)`, `PublishFailure`, and `PayloadLimits`
API changes. Register migrated codecs with exact metadata validation, or an
explicit documented historical schema allowlist.

## Migrate Redis command admission settings

The short-command budget now defaults to 64 total slots, of which 8 are reserved
for settlement. Dedicated receiver connections use a separate default cap of
256 and do not consume this short-command budget. Configurations that set
`redis.max_concurrent_commands=1` are rejected; there is no legacy behavior
switch. Set the total to at least 2. The omitted reservation defaults to
`min(8, total - 1)`; set `redis.reserved_settlement_commands` explicitly only
when a custom reserve is needed. Command limits remain
local to each created provider instance, so account for multiple instances and
other Redis clients separately.

## Replace scheduling configuration and choose a settlement policy

The old `SyncDeliverySchedulerConfig` / `DeliveryAdmissionConfig` types and their facade setters/getters are removed. Their old execution and queue budgets are not interchangeable with the new ownership budget. Choose all four positive limits deliberately:

```rust,ignore
// Before: qubit-event-bus 0.17 only.
use qubit_event_bus::facade::DeliveryAdmissionConfig;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::facade::SyncDeliverySchedulerConfig;

let facade = EventBusFacadeConfig::new()
    .with_sync_delivery_scheduler(SyncDeliverySchedulerConfig::new(4, 256)?)
    .with_delivery_admission(DeliveryAdmissionConfig::new(256)?);
```

After, with core 0.18 (call this function during bus construction and retain the existing codec registry):

```rust
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::time::Duration;

use qubit_event_bus::error::ConfigurationError;
use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::facade::SettlementRetryConfig;

fn facade_config() -> Result<EventBusFacadeConfig, ConfigurationError> {
    let scheduling = DeliverySchedulingConfig::new(
        NonZeroUsize::new(4).expect("positive running limit"),
        NonZeroUsize::new(256).expect("positive owned limit"),
        NonZeroUsize::new(32).expect("positive per-subscription limit"),
        NonZeroUsize::new(256).expect("positive subscription limit"),
    )?;
    let settlement = SettlementRetryConfig::new(
        NonZeroU32::new(5).expect("positive attempt limit"),
        Duration::from_secs(5),
        Duration::from_millis(10),
        Duration::from_secs(1),
    )?;
    Ok(EventBusFacadeConfig::new()
        .with_delivery_scheduling(scheduling)
        .with_settlement_retry(settlement))
}
```

The order is running handlers, globally owned deliveries, owned per subscription, and registered subscriptions. Defaults are 4/256/32/256. Running and per-subscription ownership must not exceed global ownership. Owned deliveries include receive reservations, queued work, handlers, and settlement; async paused sessions remain registered. An old queue capacity of zero no longer means direct handoff: choose owned=running if that fits the intended backpressure, and select positive per-subscription/subscription limits. This is a new capacity model, not an exact queue conversion.

`SettlementRetryConfig` defaults to five attempts including the first, five seconds elapsed, 10 ms initial backoff, and one second maximum backoff. Set one attempt to disable retries. Only `retryable() == Some(true)` retries: `Some(false)` and `None` stop, with `None` classified as `RetryabilityUnknown`. Panic, invalid token, and infrastructure failures also stop. The elapsed budget governs attempts and waits, not forced cancellation of an in-flight call; success after the deadline is still success.

## Migrate diagnostics and recovery

After rollout, collect `RedisProviderDiagnostics::snapshots()` from each running process and label samples with process identity plus `instance_id`, `mode`, and `namespace`. The ID is unique only within one process and a released SPI disappears; counters reset on a new instance or process. General short-command occupancy includes settlements using the general lane, while `settlement_in_flight` is the reserved lane; add them for current short-command use. Snapshot fields are read separately, and provider counters do not replace facade delivery metrics or Redis `XPENDING`. Compare pre-upgrade admission limits with the new rejection counters, then use the [operations guide](user_guide.md#11-operate-groups-and-downstream-notifications) for Redis-side collection and alerts.

`Diagnostic::SettlementFailed.error` is now `Arc<SpiError>` rather than `Box<str>`, and `attempt` is the one-based SPI attempt. A `Diagnostic::SettlementStopped` adds the final `attempts` and `termination`. Update pattern matches to read structured error fields and preserve the source chain rather than classifying formatted strings; include `_` for the non-exhaustive diagnostic enum. `SubscriptionStopReason::Settlement` preserves the same context in `terminal_failure()`. Cleanup cannot replace the first terminal cause.

Use `bus.delivery_metrics()` and `subscription.delivery_metrics().metrics` to distinguish queued work, running handlers, settlement retries, and terminal failures. Close the failed subscription after recording these snapshots, repair the cause, then create a new durable subscription with the same namespace/topic/group. Pending history can be claimed only while retained and eligible under `redis.claim_min_idle_ms`; changing `StartPosition` does not rewind an existing group. If Redis already applied `XACK` but its reply was lost, there may be nothing left to recover.

Graceful shutdown can return `Err(ShutdownError::TimedOut)` as well as a report. A two-call bounded policy must handle the first timeout before trying again and pass unfinished work to an external supervisor after the second; see the [user guide](user_guide.md). It must not use `Immediate` as timeout rescue. Bounded caller waiting does not guarantee non-cooperative work or the process exits.


Wire version 1 remains supported. Test retained stream data before rollout;
a Rust API upgrade does not require deleting streams or consumer groups.
Malformed in-limit version 1 records keep the existing quarantine-and-ack path;
a valid unsupported version remains pending for a compatible consumer.

New positive provider options default to `redis.max_wire_bytes=8388608`,
`redis.max_payload_bytes=1048576`, and `redis.max_headers_bytes=65536`.
The facade separately defaults to 1 MiB publish and receive encoded limits.
Choose both sets deliberately. Payload is checked before publication copying,
headers/wire serialization is capped before `XADD`, and received wire bytes are
checked before string copying and bounded field decoding. Limits do not cap
the Redis client's initial RESP allocation or total process memory.

A received wire, payload, or headers overflow returns `receive_limit_exceeded`
and stops the facade subscription. It preserves the source PEL entry without
`XACK`, `XDEL`, or quarantine. Inspect `terminal_failure()`, correct the limit
or codec, and create a new durable subscription in the same group. Confirm
recovery with `XPENDING`; do not clear pending data to silence errors.

SPI publication failures now declare `PublishEffect`. Connection opening
before submission and explicit server refusal are `NotAccepted`; disconnect,
timeout, or response conversion failure after query starts is
`MayHaveBeenAccepted`. Default `DuplicateRiskPolicy::Forbid` prevents automatic
resubmission of uncertain admission, even through a custom retry rule.
`AllowDuplicates` only enables the existing policy to consider retry. Earlier
uncertainty remains in a final failure and in a later successful receipt's
`duplicate_possible()`. RetryPolicy budgets are soft, not universal in-flight
command deadlines. Cancellation of a started publish may leave a record in
Redis; preserve its EventId and reconcile business effects.

Redis does not deduplicate by EventId, and accepted `XADD` does not prove fsync
or handler completion. Facade dead-letter forwarding and source `XACK` are
separate operations, so consumers must handle duplicate logical dead-letters.
Run provider feature-matrix, conformance, bounded-decoder, lost-reply, and durable
recovery tests against the configured Redis version before deployment.


## Lossy stream retention

Redis streams are not trimmed by default. Before changing retention, inventory every group with `XINFO GROUPS <stream-key>`, check each PEL with `XPENDING <stream-key> <group>`, confirm unread positions, future replay needs and audit deadlines, and inspect source/quarantine `XLEN` separately. Derive the exact keys with `naming::stream_key`, `naming::group_name` and `naming::poison_key`. Keep unlimited retention while any item is unconfirmed. Only if the application explicitly accepts historical loss, configure both `redis.stream_maxlen_approx=<positive integer>` and `redis.allow_lossy_retention=true`. Approximate `XADD MAXLEN ~` may remove unread or pending payloads; a missed unread record need not produce a Gap. Recheck the groups and quarantine after rollout; archive or remove quarantined evidence only under the business and audit policy. An accepted `XADD` is not an fsync or replication guarantee. See the [manual checklist](user_guide.md#decide-retention-manually).
