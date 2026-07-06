# Agent Orchestrator Plan

This file is the execution checklist for continuing development. Keep it updated
after each implementation pass.

## Done

- [x] Core workflow engine with `agent`, `tool`, `transform`, `human` nodes.
- [x] `condition` node with visible routing output.
- [x] Shared state data binding with `{{path.to.value}}`.
- [x] Template defaults with `| default(...)`.
- [x] Human checkpoint, pending action, resume flow.
- [x] Human `fields` and `response_schema`.
- [x] Tool confirmation, permission gate, risk policy.
- [x] Tool input/output schema validation.
- [x] Agent output schema validation.
- [x] Node retry with `max_attempts` and `delay_ms`.
- [x] Node timing metadata and `node.retrying` event.
- [x] `on_timeout` default decision for expired pending actions.
- [x] Pluggable checkpoint, event, and artifact stores.
- [x] In-memory and file stores for checkpoints, events, and artifacts.
- [x] Event replay helpers.
- [x] Artifact output offloading.
- [x] Claude Agent SDK runner adapter.
- [x] SSE/message event adapter.

## Next

- [x] Enhanced retry policy.
  - [x] `backoff_multiplier`
  - [x] `max_delay_ms`
  - [x] `retry_on`
  - [x] `timeout_ms`
  - [x] failure output written to node state
  - [x] fallback edges after terminal failure
- [x] Stronger condition expressions.
  - [x] numeric comparisons: `>`, `<`, `>=`, `<=`
  - [x] membership: `in`, `not in`
  - [x] boolean composition: `and`, `or`
  - [x] no `eval`
- [x] Artifact reference resolution helpers.
  - [x] `resolve_artifacts(value, artifact_store)`
  - [x] optional node input artifact resolution
- [x] Pending action timeout scanner.
  - [x] `CheckpointStore.list_expired_actions(...)`
  - [x] scanner applies `on_timeout`
  - [x] scanner emits timeout/resume events where possible
- [x] Official no-dependency SQL example.
  - [x] SQLite checkpoint store example
  - [x] SQLite event store example
- [x] Policy enhancements.
  - [x] per-workflow tool allowlist
  - [x] per-node permission overrides
  - [x] policy decision event
- [x] Parallel node execution.
  - [x] state merge semantics
  - [x] event ordering semantics
  - [x] partial failure policy
- [x] Subflow/reusable workflow node.
  - [x] inline child workflow config
  - [x] child input and output selection
  - [x] namespaced child node state/events
  - [x] no-wait first version
- [x] Run/event compaction.
  - [x] replayable `run.compacted` snapshot event
  - [x] compact in-memory event lists
  - [x] replace persisted runs in memory/file/SQLite event stores
- [x] More complete schema subset.
  - [x] `minLength`, `maxLength`
  - [x] `minimum`, `maximum`
  - [x] `additionalProperties`

## Later

- [x] Packaging split for optional stores, such as Redis/Postgres.
  - [x] core registry without optional providers
  - [x] optional SQLite provider registration function
  - [x] documented plugin-style registration pattern

## Current Focus

Engine decomposition.

- [x] Extract event-buffer runtime helper.
- [x] Extract workflow router.
- [x] Extract retry helpers.
- [x] Extract built-in basic node executors.
- [x] Extract parallel node executor.
- [x] Extract subflow node executor.
- [x] Switch test command to unittest discovery.
- [x] Add PEP 561 `py.typed` marker.
- [x] Split routing/retry tests.
- [x] Split event replay/compaction tests.
- [x] Split persistence/checkpoint tests.
- [x] Split parallel/subflow tests.
- [x] Split artifacts/schema tests.
- [x] Split registry/runner tests.
- [x] Split tool policy tests.
- [x] Split workflow config validation tests.
- [x] Split retry/timeout integration tests.
- [x] Split persistence plugin tests.
- [x] Split checkpoint store tests.
- [x] Split pending action timeout tests.
- [ ] Continue splitting large workflow integration test module by feature area.

## Acceptance Rules

- Preserve existing public APIs unless a change is intentionally additive.
- Keep core dependency-free.
- Add tests for each new behavior.
- Update `README.md` for user-facing features.
- Update this file after each completed pass.
- Run `rtk python3 -m unittest tests.test_agent_orchestrator` before finishing.
