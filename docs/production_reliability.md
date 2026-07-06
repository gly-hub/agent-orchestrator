# Production Reliability Model

This package keeps workflow execution framework-independent and store-driven.
The default consistency model is intentionally explicit so applications can
choose the right persistence provider for their risk level.

## Source of Truth

Live execution state is stored in the checkpoint store. The event store is an
append-only audit and replay log. The artifact store holds large node outputs
when configured.

For a running service, treat these stores as separate durability boundaries:

- checkpoint store: resume state, pending actions, run leases, idempotency
- event store: stream/audit history and replay materialization
- artifact store: large output payloads referenced from node state

The built-in stores do not make checkpoint and event writes one distributed
transaction. If an application requires a single durable commit for state and
events, provide a custom store pair that shares one database transaction, or
write an outbox record from the checkpoint transaction and drain it into the
event log.

## Failure Semantics

Event append failure is terminal for the current stream. The engine returns an
unpersisted `run.failed` event so clients receive a clear terminal state even
when the event log is unavailable. The configured observer also receives an
`event.append_failed` observation.

Checkpoint save failure while creating a human or tool-confirmation pause is
also terminal. A `human.required` event may already have been emitted before the
save attempt fails, but the final stream event is `run.failed`. Clients should
only persist a `pending_action_id` as actionable when the stream ends with
`run.waiting`.

Resume is idempotency-protected by the checkpoint store. File, SQLite, and Redis
stores reject duplicate resume attempts for the same pending action. SQLite uses
a transaction and an `executed_actions` table; Redis uses `SET NX`/Lua where
available.

Run leases prevent two workers from advancing the same run at the same time.
Set `run_lease_ttl_ms=None` only for tests or single-process demos.

## Recommended Client Rules

Streaming clients should follow these rules:

- process events in order
- treat `run.finished` and `run.failed` as terminal
- treat `run.waiting` as the only durable signal that a `pending_action_id` can
  be submitted later
- ignore or mark stale any previously seen `human.required` event if the same
  stream ends with `run.failed`
- retry resume only on transport failures where the server outcome is unknown;
  duplicate submissions are safe but may return an "already resolved" error

## Store Guidance

Use memory stores for tests and demos only. Use file stores for local
integration tests or single-host services. Use SQLite for embedded or small
single-file deployments. Use Redis or a custom database-backed provider for
multi-worker services.

For stricter production guarantees, prefer a custom Postgres/MySQL/etc.
checkpoint implementation that combines these actions in one transaction:

- resolve pending action
- update run state
- mark pending action executed
- write an outbox/event row

Then publish event rows from the outbox to any secondary streaming or replay
system.

## Observability

`WorkflowEngine(..., observer=...)` receives best-effort `WorkflowObservation`
records outside the workflow event log. Observer failures are swallowed so
monitoring cannot break execution.

Useful observation types include:

- `event.appended`
- `event.append_failed`
- `node.started`
- `node.finished`
- `node.failed`
- `run.waiting`
- `run.failed`

Use these observations for metrics such as node duration, retry/error counts,
event-store failures, and run-lease contention.
