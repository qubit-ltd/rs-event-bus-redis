# Migration to Redis provider 0.5

[简体中文](migration.zh_CN.md) · [User guide](user_guide.md)

Upgrade `qubit-event-bus-redis` from 0.4 to 0.5 together with
`qubit-event-bus` 0.17. Update direct dependencies, downstream fixtures, and
lockfiles together; do not mix SPI minor generations. The
[core migration guide](https://github.com/qubit-ltd/rs-event-bus/blob/main/doc/migration.md)
explains the `decode(&EncodedPayload)`, `PublishFailure`, and `PayloadLimits`
API changes. Register migrated codecs with exact metadata validation, or an
explicit documented historical schema allowlist.

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
