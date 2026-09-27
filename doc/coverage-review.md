# Redis coverage review

## Change in the CI coverage report

| Metric | Before this follow-up | After this follow-up |
| --- | ---: | ---: |
| Functions | 69.59% (135/194) | 77.18% (159/206) |
| Lines | 86.17% (1,427/1,656) | 89.24% (1,576/1,766) |
| Regions | 84.54% (1,859/2,199) | 88.09% (2,085/2,367) |

The final report passes the configured thresholds: functions at least 70%, lines
above 89%, and regions above 85%.

## Uncovered paths inspected and tests added

- Provider construction error maps had not been exercised. Tests now cover
  invalid pool settings and malformed Sentinel endpoints for both provider
  modes.
- Subscription setup's invalid stream-position branch had not been exercised.
  Sync and async tests now verify rejection occurs before Redis is contacted.
- Native payload rejection was tested at the wire codec, but its SPI error
  mapping was not. The sync and async command-failure tests now publish a native
  payload through the provider and assert an error.
- Malformed-record tests previously covered only missing wire data. They now
  cover invalid UTF-8, invalid JSON, unsupported wire versions, and invalid
  event metadata through quarantine and gap reporting.
- Receive connection reuse and discard behavior lacked regression coverage.
  Tests verify reuse across consecutive timeouts, disposal after Redis command
  errors, and connection acquisition errors after Redis stops.
- Internal defensive settlement and recovery-lock errors were uncovered. Unit
  tests now cover unrecognized settlement-token state and poisoned recovery
  locks in both sync and async receivers.

The lowest remaining source coverage is in the receive state machines:
`sync/subscription.rs` is at 77.06% lines and `async/subscription.rs` at 87.50%.
The remaining gaps are mostly defensive branches for lock poisoning after the
initial receive check, active-delivery races, Redis errors between sequential
commands, and quarantine ownership changes. They require internal fault
injection or a race between Redis operations; the integration tests cover the
reachable command and recovery paths without adding timing-dependent tests.
