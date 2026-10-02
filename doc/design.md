# Redis Streams Provider Design

[简体中文](design.zh_CN.md) · [User guide](user_guide.md) · [README](../README.md)

This document describes the unreleased working tree whose Cargo version is `0.6.0`. It records the implemented contracts and their limitations; test and benchmark reports carry the separate evidence for acceptance. No new release or measured performance result is implied.

## Boundaries and responsibilities

The provider implements synchronous and runtime-neutral asynchronous event-bus SPIs. The facade owns typed codecs, handler execution and application policies. Redis Streams owns retained records, group cursors and pending entries. Business idempotence, durable notification transactions, Redis persistence/replication and retention remain application or deployment responsibilities.

The implementation shares pure command construction, protocol normalization, receive scheduling, wire decoding and settlement decisions. Sync and async adapters keep their distinct I/O, timeout and cancellation behavior; there is no hidden executor driving the sync API. Supported transport is Redis 6.2+ standalone or Sentinel with encoded payloads. Cluster, TLS configuration, delayed delivery, native payloads, business DLQ routing and exactly-once processing are outside the contract.

| Implementation owner | Responsibility |
| --- | --- |
| `src/sync/subscription.rs`, `src/async/subscription.rs` | Adapter I/O, cancellation boundaries and invoking shared decisions |
| `src/internal/settlement_progress.rs`, `settlement_state.rs` | Permitted intents and atomic local bookkeeping commit |
| `src/internal/receive_driver.rs`, `recovery_scan_budget.rs` | One action per dispatch, deadlines and scan/maintenance quotas |
| `src/client.rs`, `src/client/internal/` | Controlled transport, admission permits, standalone pool/cache and Sentinel probes |
| `src/internal/transport_policy.rs` | Connect/command waits and actual BLOCK margin |
| `src/wire_fields.rs`, `src/wire_fields/internal/`, `src/internal/decode.rs` | Bounded provider encoding, version/depth policy and receiver-bound token construction |
| `src/poison.rs` | Redis-local owner-checked quarantine copy and ACK |

## Settlement intent and local commit

Tokens carry Redis coordinates, receiver identity and shared progress. The progression is Open → AckPending(original terminal intent) → Applied(original intent), or Open → Applied(Retry) without Redis ACK. An identical Applied request is a local success; conflicting requests are rejected without I/O.

Connection acquisition and command admission precede fixing a new terminal intent. The short critical section marking AckPending occurs immediately before XACK can be sent, without an await or network operation under a standard mutex. Unknown results or cancellation retain intent; only the original Accept or Reject may retry. XACK integers 0 and 1 both permit identical-intent completion. Local commit acquires progress before recovery and changes progress plus active-slot bookkeeping without an await; lock failure does not report success.

A complete top-level Redis rejection on the first XACK attempt can restore Open. A later rejection after an earlier unknown result cannot prove that earlier ACK failed and cannot reopen the token. Nested RESP errors and malformed replies are not explicit non-application evidence. Close/drop releases receiver resources without inventing Retry or reconstructing Redis PEL. Intent is local to token/receiver lifetime, not persisted across restart, and does not fence another consumer.

## Receive scheduling and recovery

Each dispatch obtains one action exactly once and reports a normalized reply only after a real response. Claim and own-pending scan quotas are independent: eight commands each per recovery round. Tombstone maintenance permits one XPENDING probe, four XRANGE checks and four total quarantine EVAL commands per round. Shared cursors and deferred records survive receive calls; the clock budget belongs to one call.

Recovery scheduling is subscription state shared across receive calls. A new
subscription's first receive scans pending work immediately. After a complete
round the next round is due after `redis.recovery_interval_ms` (default 1,000
ms); calls before that deadline skip claim and own-pending scans and read new
entries. Retry, a failed receive, or cancellation after an async receive has
started marks recovery due for the next call. Dropping a never-polled future
does not change the schedule. An incomplete round does not advance the deadline.

Zero timeout permits at most one claim, one own-pending read and one new-message read, all without BLOCK, with early delivery/Gap allowed; it skips tombstone scans and may quarantine one malformed record. `Duration::MAX` uses finite BLOCK intervals of at most one second. Finite deadlines stop additional recovery work and bound the chosen BLOCK; they do not force an already-started command to stop. Recovery preserves active deduplication, live records deferred behind gaps and Redis 6.2 tombstone repair.

## Connection policy and resource lifecycle

| Provider option | Default | Inclusive range / relation |
| --- | ---: | --- |
| `redis.connect_timeout_ms` | 2000 | 1–60,000 |
| `redis.command_timeout_ms` | 2000 | 1–60,000 |
| `redis.max_concurrent_commands` | 64 | 2–4,096 |
| `redis.reserved_settlement_commands` | 8 | 1–(total commands − 1) |
| `redis.max_active_receivers` | 256 | 1–4,096 |
| `redis.max_idle_connections` | 8 | 1–64; at most concurrent commands |
| `redis.max_payload_bytes` | 1,048,576 | 1–67,108,864 |
| `redis.max_wire_bytes` | 8,388,608 | 1–268,435,456; at least payload limit |
| `redis.sentinel.nodes` | unset | At most 16 valid host/port endpoints |

The [guide](user_guide.md) lists the other existing options. New finite limits reject zero, signs, overflow and invalid decimal text. Total command concurrency must be at least 2; the default settlement reservation is 8 when the total is 64, and for smaller configured totals defaults to `min(8, total − 1)`. Set the reservation explicitly only when a custom reserve is needed. Lowering command concurrency below eight requires lowering idle retention as well. Existing `redis.max_concurrent_commands=1` configurations are rejected and require migration; there is no compatibility mode.

Standalone sync short operations take fail-fast RAII command permits and reuse idle connections outside network I/O locks. Every checkout restores read/write command waits; I/O/protocol/timeout failures discard the connection. Async standalone short operations share a multiplexed connection. An async mutex spans cold initialization for single-flight publication; cancellation leaves an empty cache. Monotonic generations ensure a failing old lease invalidates only its own generation, with overflow rejected rather than reused. Receiver reads use dedicated connections and response budgets of actual BLOCK plus command timeout.

Sentinel discovery tries each configured node once, prioritizing the last successful node, queries `SENTINEL get-master-addr-by-name`, validates host/port and verifies candidate ROLE=master. Sentinel and master ACLs remain independent. Setup, probe and target command waits all use the transport policy. These master sockets are not pooled/cached as standalone command sockets. ROLE cannot prevent a later promotion; XADD is not transparently replayed after a write error.

Command admission is split into general and settlement lanes under one total budget; reserved settlement slots keep XACK eligible while general capacity is full. Receiver connections do not consume short-command permits. Command and receiver admission is shared by one created SPI instance and its Arc clones; a new `create_configured` call receives an independent budget. Registries do not merge these budgets and Redis has no global provider admission cap. There is no unbounded provider wait queue. Receivers reserve admission before setup and release it on close/drop even while tokens survive. Failure/cancellation releases local command permits, but a multiplexed driver or Redis may finish an in-flight request later. Caps do not count every server task/socket or bound all clients in a process. Closing resources does not require new receiver admission.

Sync timeouts are soft per-stage/per-I/O waits. DNS, multiple address attempts, setup and sustained small packets can exceed the overall caller budget. The receive deadline is a scheduling constraint, not an absolute wall-clock deadline. This design does not add uncancellable helper threads to promise a sync hard deadline.

## Result certainty and error contract

Adapters classify failure at the actual operation phase; there is no separate two-state outcome enum. Secret-safe `RedisProviderError` variants become stable `SpiError::Operation` categories.

| Result | Kind | Retry hint / meaning |
| --- | --- | --- |
| Unknown publish | `outcome_unknown` | `Some(false)`; explicit application replay can duplicate |
| Unknown receive | `outcome_unknown` | `Some(true)`; recover through PEL/claim |
| Unknown settlement | `outcome_unknown` | `Some(true)`; same token and original terminal intent |
| Unknown/possibly partial quarantine | `outcome_unknown` | `Some(false)`; inspect source/PEL/owner first |
| Rejected admission | `resource_limit` | `Some(true)`; no command from that operation was sent |
| Publish byte limit | `payload_too_large` / `wire_too_large` | `Some(false)`; no XADD |
| Unknown version within wire limit | `unsupported_wire_version` | `Some(false)`; retain PEL |

Known configuration/Redis rejection categories remain sanitized. Retryability is a contextual hint, not automatic retry or duplicate-free delivery. Failures before business command send do not fix settlement intent. Error formatting does not include URL secrets, raw Redis text or payloads.

## Wire v1 and quarantine

Wire remains JSON version 1 in the `wire` stream field, with a byte-array encoded payload and metadata. Publish checks payload length before serialization, bounds outer wire and intermediate headers JSON using a Write sink, and does not allocate an unlimited final String first. Provider encoding borrows caller-owned event ID, content type, schema ID, ordering key and payload instead of cloning arbitrary-sized metadata; only bounded headers JSON and complete wire strings are allocated. The public `WireFields::from_outbound` convenience conversion is separate and does not apply configured provider limits. Receive borrows raw Redis wire bytes, checks their size before UTF-8/JSON, reads a small version structure, then typed v1 fields and decoded payload length. Version 1 additionally scans structural depth with constant-memory counters, accepting at most 127 nested containers and rejecting the 128th, including ignored fields; Serde's recursion limit remains enabled. Unknown versions return before v1 field-shape/depth checks. The version probe's ignored-field scratch may grow with nesting, within the already enforced wire byte bound; the entire decoder is not constant-space.

The wire limit takes priority: oversized history is poison even if its version would otherwise be unknown. Within that limit, unknown unsigned 64-bit integer versions remain pending with an error. Other malformed records and oversized v1 payloads are quarantined and acknowledged, producing Gap. Limits bound additional provider parsing/copying, not the Redis RESP library's original bulk receive allocation or all application memory.

The quarantine Lua script validates source/destination types and current PEL owner, reads the source wire inside Redis, copies to quarantine, then XACKs. Duplicate `wire` fields use the same last-field-wins interpretation as the owned Redis parser. Execution excludes interleaving, but Lua does not roll back an earlier copy if a later operation fails. Reply loss or partial failure can leave duplicate copies; correlate `source_stream`, `group` and `source_id`. OwnershipChanged does not ACK another owner's record; missing source does not invent payload; tombstone clearing and successful quarantine report Gap. Quarantine retention is an operator responsibility.

## Durability, downstream use and migration

Accept/Reject ACK Redis PEL; Retry is local and leaves PEL pending. A newly returned message from `XREADGROUP >` carries `provider_attempt = Some(1)`; pending and `XAUTOCLAIM` recovery paths leave it unknown (`None`) because the provider does not currently propagate the historical count. Closing does not ACK, delete consumer groups or delete streams. Optional `XADD MAXLEN ~` can lose unread/pending history; unread trimming need not produce Gap. At-least-once handlers require business idempotence and a claim idle threshold suited to their latency.

Accepted XADD does not prove fsync, replica durability or business completion. WAIT on a new observer connection does not fence a provider connection's writes. Sentinel tests must observe replicated group cursor, PEL IDs and owners rather than infer durability from that WAIT. Obsolete consumer cleanup requires stopping it and confirming empty PEL plus business retention requirements; no automatic DELCONSUMER is introduced.

Task notification consumers deduplicate by TaskId and highest `state_version`, reject older versions and consult task service state. Failed notifications do not roll back committed task/business state. The Redis provider does not supply transactional outbox semantics; the typed SQLite task service has an optional outbox integration for its own task lifecycle transitions.

This unreleased change adds finite timeout/resource/byte defaults, new public error variants and unknown-outcome rules. Review limits before deployment, adjust idle/concurrency together, stop blindly retrying unknown publish, and preserve unknown ACK intent. Wire v1 is retained. If a future release chooses a breaking version, publication is a separate operation; this tree does not claim a published `0.5.0`.

## Acceptance evidence

Public tests cover settlement reply loss/cancellation, receive command sequence, timeouts, limits, byte boundaries, quarantine and Redis/Sentinel behavior. Isolated downstream fixtures check runtime features and task notifications. Documentation acceptance reads marked Rust blocks from both actual guides, composes documented module fragments with their main, and builds/runs independent feature graphs against real Redis, checking delivery and empty PEL. Example binaries are a separate check.

[Coverage review](coverage-review.md) distinguishes historical measurements from the final refactor gate. [Workload benchmark](connection-reuse-benchmark.md) owns measured workloads/results. Neither successful documentation builds nor historical percentages prove current complete acceptance.
