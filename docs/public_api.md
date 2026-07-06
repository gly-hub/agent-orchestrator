# Public API Policy

The package root, `agent_orchestrator`, is the stable import surface for
applications.

## Stable Surface

These groups are intended for application use and follow semantic-versioned
compatibility once the package reaches `1.0`:

- workflow runtime: `WorkflowEngine`, `WorkflowConfig`, `StartRunRequest`,
  `ResumeRunRequest`, `WorkflowEvent`
- registries and definitions: `AgentRegistry`, `ToolRegistry`,
  `AgentDefinition`, `ToolDefinition`
- persistence interfaces and built-ins: `CheckpointStore`, `EventStore`,
  `ArtifactStore`, memory/file/SQLite/Redis store classes, and
  `create_*_store(...)`
- policy extension points: `ToolPolicyGate`, `ToolPolicyDecision`,
  `DefaultToolPolicyGate`
- observability hooks: `WorkflowObservation`, `WorkflowObserver`, and
  `WorkflowEngine(..., observer=...)`
- replay/SSE helpers: `replay_run`, `replay_events`, `to_message_event`,
  `encode_sse`, `stream_sse`
- schema/workflow validation helpers: `validate_schema_value`,
  `validate_workflow_config`

## Internal Modules

Modules such as `agent_orchestrator.engine`, `parallel`, `subflow`,
`execution`, `runtime`, and `validation` are importable for advanced
integrators, but their internals are not the preferred application API. Use
objects exported from `agent_orchestrator` when possible.

Names prefixed with `_` are private implementation details.

## Runtime Error Handling

`WorkflowEngine.start(...)` and `WorkflowEngine.resume(...)` are streaming APIs.
By default, execution errors are reported as a terminal `run.failed` event
instead of being raised to the caller. Applications should always consume the
stream to a terminal event and check for `run.failed`, or construct the engine
with `raise_on_error=True` when exceptions must also propagate through the
async iterator.

## Compatibility

Before `1.0`, minor releases may adjust workflow language details, event
payloads, or store behavior when needed to stabilize the design. Breaking
changes should be documented in release notes and accompanied by migration
guidance.
