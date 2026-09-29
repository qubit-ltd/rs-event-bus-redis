# Changelog

## Unreleased

This migration is not released. The package version remains `0.4.0`; no release
tag has been created. Publishing these incompatible changes would require a new
release, proposed as `0.5.0`.

### Migration

- Connections and commands now have finite default waiting budgets. Set
  `redis.connect_timeout_ms` and `redis.command_timeout_ms` explicitly if the
  default 2,000 ms does not fit your environment. Each accepts 1–60,000 ms.
  A blocking read allows its actual BLOCK duration plus the command budget.
  Receive deadlines govern scheduling; synchronous DNS, multiple address
  attempts and continuing socket traffic can exceed an overall wall-clock budget.
- Review workloads against the new per-client admission and size limits:

  | Option | Default | Accepted range |
  | --- | --- | --- |
  | `redis.max_concurrent_commands` | 64 | 1–4,096 |
  | `redis.max_active_receivers` | 256 | 1–4,096 |
  | `redis.max_payload_bytes` | 1,048,576 | 1–67,108,864 |
  | `redis.max_wire_bytes` | 8,388,608 | 1–268,435,456 |
  | `redis.max_idle_connections` | 8 | 1–64 |

  Idle connections must not exceed command admission, and wire capacity must be
  at least payload capacity. If command admission is below eight, lower the idle
  limit too. Payload plus encoded metadata must also fit the wire limit.
  Sentinel configuration accepts at most 16 endpoints. Admission exhaustion
  fails immediately with `resource_limit`; applications may back off. Historical
  oversized records are quarantined and acknowledged, producing a Gap.
- Handle `outcome_unknown` explicitly. A publication can have reached Redis even
  when its reply is lost. It is marked non-retryable and is not transparently
  replayed. Do not blindly retry it; use application identifiers and an explicit
  policy for possible duplicates. An outbox remains an application concern.
- An unknown XACK fixes the original Accept or Reject intent for that live
  receiver/token. Retry only the same intent. Changing to Retry or the opposite
  terminal intent is rejected, including after a later explicit Redis error.
  A matching retry with an XACK result of zero completes idempotently. A first
  attempt explicitly rejected before applying XACK can leave the decision open.
- Public provider errors include outcome, resource and size variants. Update
  exhaustive matches and inspect SPI `kind()` and `retryable()` in context:
  unknown receive/settlement permits recovery, while unknown publication or
  quarantine does not imply safe replay. Cancellation releases local admission
  but does not guarantee that Redis stopped an in-flight command.
- Public contract tests now mirror production module paths; crate-internal tests
  live under `src/tests`. Update scripts that select old test locations, and run
  the sync/async conformance and all-features configurations.

### Retained data and lifecycle contracts

Redis key naming, the `wire` field, wire version 1 and byte-array payload encoding
remain unchanged. Existing data needs no format migration. Unknown wire versions
within the wire-size limit remain pending. Close and Drop do not automatically
acknowledge messages or delete consumers, groups or streams. Quarantine scripts
exclude interleaving but do not provide rollback or exactly-once isolation.

See the [user guide](doc/user_guide.md) for configuration, recovery and consumer
operations, and the [design](doc/design.md) for the state and resource contracts.
