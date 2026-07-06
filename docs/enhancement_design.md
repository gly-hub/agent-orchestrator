# Agent Orchestrator Enhancement Design

## Score Gaps

The first version is a solid workflow kernel, but it loses points in these areas:

- Graph expressiveness: no explicit decision node, no parallel nodes, no subflows.
- Human interaction: human nodes can pause, but had no structured form contract.
- State binding: missing paths failed hard, with no ergonomic defaults.
- Reliability: transient node failures had no retry policy.
- Observability: node events did not expose timing or attempt metadata.
- Production persistence: file and memory checkpoint stores are useful, but services need pluggable DB/Redis-backed stores.

## Design Direction

Keep the core small and embeddable. Prefer composable primitives over a large DSL:

- A `condition` node writes a visible decision to state, and normal edges branch from that value.
- A `human` node remains a generic checkpoint, but can attach `fields` and `response_schema` for structured user input.
- Template defaults use `{{path | default('value')}}`, preserving the existing path-based mental model.
- Retry is node-local and opt-in through `retry.max_attempts` and `retry.delay_ms`.
- Observability metadata is written into each node record and emitted with `node.finished`.
- Persistence is interface-first: users can pass store instances directly or register provider factories in a plugin registry.

## Implemented In This Pass

- Added `condition` node type.
- Added human `fields` and `response_schema` support.
- Added resume decision validation for required fields, primitive types, and enum constraints.
- Added template defaults via `| default(...)`.
- Added `retry` support and `node.retrying` events.
- Added `started_at_ms`, `finished_at_ms`, `duration_ms`, and `attempt` to node records.
- Added `EventStore`, `InMemoryEventStore`, `FileEventStore`, and `NoopEventStore`.
- Added `PersistencePluginRegistry`, `create_checkpoint_store`, and `create_event_store`.
- Added default built-in providers for memory/file checkpoint stores and noop/memory/file event stores.
- Integrated optional event persistence into `WorkflowEngine`.
- Documented usage in `README.md`.

## Remaining Roadmap

- Add official DB/Redis checkpoint-store packages using the plugin interfaces.
- Add structured tool argument schema validation.
- Add retry filters, backoff, and timeout policies.
- Add parallel nodes after event ordering and state merge semantics are explicit.
- Add subflow support for reusable workflow fragments.
- Add input/output size limits and artifact references for large payloads.
