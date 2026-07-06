# Agent Orchestrator

Composable Agent Workflow Engine with resumable human checkpoints.

## Features

- Workflow nodes: `agent`, `tool`, `transform`, `human`, `condition`, `parallel`, `subflow`
- Shared state with `{{path.to.value}}` template resolution and `| default(...)`
- Workflow config validation
- Human checkpoint and resume flow
- Human response schema validation for structured user input
- Tool confirmation before side effects
- Tool input/output schema validation
- Agent output schema validation
- Workflow and node-level tool policy controls
- Tool permission, risk, confirmation, and audit decision events
- Node retry policy and node duration metadata
- Pluggable checkpoint and event stores
- Pluggable artifact stores for large node outputs
- Replayable run/event compaction
- In-memory, file-based, and SQLite built-in stores
- Message/SSE event adapter for stream continuity
- Optional Claude Agent SDK runner
- Framework-independent core

## Run Tests

```bash
make check
```

Or run the unittest suite directly:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -p "test*.py"
```

## Run Examples

```bash
PYTHONPATH=src python3 examples/orchestrator_demo.py
PYTHONPATH=src python3 examples/sqlite_persistence_demo.py
PYTHONPATH=src python3 examples/service_stream_demo.py
PYTHONPATH=src python3 examples/aiohttp_orchestrator_server.py
```

## Workflow Language Boundaries

The workflow language is intentionally small: templates resolve dotted state
paths, `when` supports simple comparisons plus `and`/`or`, and schemas use a
JSON-Schema-like subset. See [docs/workflow_language.md](docs/workflow_language.md)
for the exact supported surface.

## Public API

Applications should import from the package root, `agent_orchestrator`, whenever
possible. See [docs/public_api.md](docs/public_api.md) for the stable API
surface and internal-module policy.

## Production Reliability

The checkpoint store is the source of truth for live execution. Event stores are
append-only audit/replay logs unless an application supplies a transactional
store pair. See [docs/production_reliability.md](docs/production_reliability.md)
for failure semantics, resume idempotency, and client rules around
`human.required`, `run.waiting`, and `run.failed`.

## Service Flow

Use `WorkflowEngine.start(...)` for the first stream. When a `human.required`
event is emitted, persist `pending_action_id` in the client. After user
confirmation, call `WorkflowEngine.resume(...)` and continue streaming.

## Persistent Checkpoints

Use `FileCheckpointStore` for local services or integration tests that need to
resume across engine instances:

```python
from agent_orchestrator import FileCheckpointStore, WorkflowEngine

engine = WorkflowEngine(
    workflow,
    agents=agents,
    tools=tools,
    checkpoints=FileCheckpointStore("/tmp/agent-orchestrator-checkpoints"),
    pending_action_ttl_ms=600_000,
)
```

`FileCheckpointStore` persists waiting run state and pending actions as JSON
files, and uses an execution lock file to reject duplicate resume attempts.

### Persistence Production Guidance

The built-in stores are intentionally small and dependency-light, but they have
different operational envelopes:

- `memory`: tests, examples, and single-process demos only. Data is lost on
  process restart and cannot coordinate multiple workers.
- `file`: local development, integration tests, or simple single-host services.
  It uses atomic file replacement for JSON payloads and lock files for duplicate
  resume protection, but it is not a transactional multi-worker store.
- `sqlite`: small deployments, local services, and embedded use cases. It
  provides transactional resume handling and ordered event logs without external
  infrastructure, but it is still a single-file database.
- `redis` or a custom database-backed plugin: recommended for multi-instance
  services, distributed workers, stronger operational controls, and production
  workloads that need shared checkpoint state.

For a no-dependency SQL option, use SQLite stores. They use Python's standard
library `sqlite3` module, so the core package still has no external persistence
dependency:

```python
from agent_orchestrator import SQLiteCheckpointStore, SQLiteEventStore, WorkflowEngine

engine = WorkflowEngine(
    workflow,
    agents=agents,
    tools=tools,
    checkpoints=SQLiteCheckpointStore("/tmp/agent-orchestrator.sqlite"),
    event_store=SQLiteEventStore("/tmp/agent-orchestrator.sqlite"),
)
```

`SQLiteCheckpointStore` persists waiting run state, pending actions, action
expiry timestamps, and an idempotency table used to reject duplicate resume
attempts. `SQLiteEventStore` stores workflow events in append order for audit
and replay.

### Execution Leases and Event Consistency

`WorkflowEngine.start(...)` and `WorkflowEngine.resume(...)` hold a run-level
execution lease while advancing a run. Built-in checkpoint stores implement the
lease with process memory, local files, SQLite transactions, or Redis `SET NX`
locks depending on the provider. The lease prevents two workers from advancing
the same run at the same time; set `run_lease_ttl_ms=None` only for tests or
single-process demos that explicitly do not need this guard.

Run state remains the source of truth for live execution. Event stores are an
append-only audit and replay log. If appending an event fails, the engine marks
the run failed and still returns an unpersisted `run.failed` event to the caller
so streaming clients receive a terminal state. Applications that require
stronger guarantees should use a transactional checkpoint/event store pair or
replay from the event log as their own source of truth.

## Persistence Plugins

Persistence is intentionally interface-based. Applications can pass store
instances directly:

```python
engine = WorkflowEngine(
    workflow,
    agents=agents,
    tools=tools,
    checkpoints=my_checkpoint_store,
    event_store=my_event_store,
)
```

To make persistence configurable, register provider factories:

```python
from agent_orchestrator import PersistencePluginRegistry, create_checkpoint_store

plugins = PersistencePluginRegistry()
plugins.checkpoints.register(
    "postgres",
    lambda config: PostgresCheckpointStore(config["dsn"]),
)

checkpoints = create_checkpoint_store(
    {"provider": "postgres", "dsn": "postgresql://..."},
    registry=plugins,
)
```

Custom checkpoint stores implement:

```python
class CheckpointStore:
    async def save_waiting(self, run_state, action) -> None: ...
    async def load_run(self, run_id): ...
    async def load_action(self, pending_action_id): ...
    async def resolve_action(self, pending_action_id, decision): ...
```

If your store inherits `BaseCheckpointStore`, you only need to implement the
storage primitives and can reuse TTL, decision validation, and duplicate-resume
logic:

```python
class MyCheckpointStore(BaseCheckpointStore):
    async def save_waiting(self, run_state, action) -> None: ...
    async def load_run(self, run_id): ...
    async def load_action(self, pending_action_id): ...
    async def _save_action(self, action) -> None: ...
    async def _mark_executed_once(self, pending_action_id) -> bool: ...
```

Event stores are optional and default to `NoopEventStore`. They can be used for
audit and replay:

```python
class EventStore:
    async def append(self, event) -> None: ...
    async def list_by_run(self, run_id): ...
```

Built-in providers:

- checkpoint stores: `memory`, `file`, `sqlite`, `redis`
- event stores: `noop`, `memory`, `file`, `sqlite`, `redis`
- artifact stores: `memory`, `file`

SQLite providers accept `path`:

```python
from agent_orchestrator import create_checkpoint_store, create_event_store

checkpoints = create_checkpoint_store(
    {"provider": "sqlite", "path": "/tmp/agent-orchestrator.sqlite"}
)
events = create_event_store(
    {"provider": "sqlite", "path": "/tmp/agent-orchestrator.sqlite"}
)
```

For plugin-style packaging, build a core registry and register optional stores
explicitly:

```python
from agent_orchestrator import (
    core_persistence_plugins,
    create_checkpoint_store,
    create_event_store,
    register_sqlite_stores,
)

plugins = register_sqlite_stores(core_persistence_plugins())

checkpoints = create_checkpoint_store(
    {"provider": "sqlite", "path": "/tmp/agent-orchestrator.sqlite"},
    registry=plugins,
)
events = create_event_store(
    {"provider": "sqlite", "path": "/tmp/agent-orchestrator.sqlite"},
    registry=plugins,
)
```

The default registry keeps `sqlite` registered for backward compatibility. New
optional stores such as Postgres should follow the same shape:
`register_<provider>_stores(registry)`.

Redis providers accept `url`, `prefix`, and optional retention knobs. Install
the optional dependency before using them:

```bash
pip install "dandelion-orchestrator[redis]"
```

```python
from agent_orchestrator import create_checkpoint_store, create_event_store

checkpoints = create_checkpoint_store(
    {"provider": "redis", "url": "redis://localhost:6379/0", "prefix": "prod"}
)
events = create_event_store(
    {
        "provider": "redis",
        "url": "redis://localhost:6379/0",
        "prefix": "prod",
        "max_events_per_run": 10_000,
    }
)
```

## Event Replay

Persisted events can be replayed into a compact run view:

```python
from agent_orchestrator import replay_run

replay = await replay_run(event_store, run_id)

print(replay.status)
print(replay.nodes["deploy"]["output"])
print(replay.message_events)
```

`replay.workflow_events` preserves internal events, while
`replay.message_events` contains the chat/SSE-friendly envelopes produced by
`to_message_event(...)`.

Workflow events include a `schema_version` field. Built-in event stores read
older persisted events that do not have this field as version `1`, so existing
logs remain replayable after upgrading.

## Event Compaction

Long event logs can be compacted into a replayable `run.compacted` snapshot plus
an optional tail of recent events:

```python
from agent_orchestrator import compact_run, replay_run

result = await compact_run(event_store, run_id, retain_last=20)
replay = await replay_run(event_store, run_id)

print(result.original_event_count)
print(result.compacted_event_count)
print(replay.status)
```

`compact_events(...)` is the pure in-memory helper for event lists.
`compact_run(...)` loads a run from an event store, writes back the compacted
event list with `replace_run(...)`, and remains replay-compatible.

Built-in event stores support replacement:

- `InMemoryEventStore`
- `FileEventStore`
- `SQLiteEventStore`

Custom stores that support compaction should implement `CompactableEventStore`.
Stores that only append and list events can implement the smaller `EventStore`
interface.

The compaction snapshot stores the materialized run view used by replay:
status, node records, message ids, waiting action id, and run error. Retained
tail events are applied after the snapshot, so clients can keep recent detailed
events while shrinking older history.

## Config Validation

`WorkflowConfig.from_dict(...)` validates workflow structure by default:

- node ids must be unique
- node types must be supported
- `agent` nodes must declare `agent`
- `tool` nodes must declare `tool`
- `condition` node cases must declare `when` and `value`
- `parallel` nodes must declare non-empty `branches`
- `parallel` branches cannot contain `human` nodes
- `subflow` nodes must declare an inline `workflow`
- `subflow` workflows cannot contain `human` nodes
- edge endpoints must reference existing nodes
- obvious graph cycles are rejected

Manual configs are also validated by `WorkflowEngine(...)` during
initialization.

## Data Binding

Nodes read from shared run state with template expressions:

```python
{
    "id": "deploy",
    "type": "tool",
    "tool": "deploy",
    "args": {
        "env": "{{nodes.collect.output.env}}",
        "version": "{{nodes.collect.output.version}}",
        "region": "{{context.region | default('us-east-1')}}",
    },
}
```

When the whole string is a template, the original value type is preserved. Inline
templates are converted to strings.

## Condition Nodes

Use a `condition` node when a routing decision should be visible in run state:

```python
{
    "id": "route",
    "type": "condition",
    "input": {"level": "{{context.level | default('normal')}}"},
    "cases": [
        {"when": "{{input.level}} == 'vip'", "value": "vip"},
        {"when": "{{input.level}} == 'normal'", "value": "normal"},
    ],
    "default": "fallback",
}
```

The selected value is stored at `{{nodes.route.output.value}}`, so normal edge
conditions can branch from it:

```python
{"from": "route", "to": "vip_handler", "when": "{{nodes.route.output.value}} == 'vip'"}
```

Condition expressions support a safe subset without `eval`:

```text
{{context.score}} >= 90
{{context.level}} in ['vip', 'svip']
'prod' in {{context.tags}}
{{context.level}} not in ['guest', 'normal']
{{context.score}} >= 90 and {{context.level}} == 'vip'
{{context.score}} < 60 or {{context.level}} == 'guest'
```

Supported operators: `==`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `not in`,
`and`, and `or`.

## Parallel Nodes

Use a `parallel` node to fan out independent work and merge the branch outputs
before the workflow continues:

```python
{
    "id": "collect",
    "type": "parallel",
    "failure_policy": "continue",
    "branches": [
        {
            "id": "profile",
            "type": "tool",
            "tool": "query_profile",
            "args": {"user_id": "{{context.user_id}}"},
        },
        {
            "id": "orders",
            "type": "tool",
            "tool": "query_orders",
            "args": {"user_id": "{{context.user_id}}"},
        },
    ],
}
```

Branches run concurrently. Their node records are merged back into shared state
under their branch ids, and the parallel node output contains a compact summary:

```python
{
    "branches": {
        "profile": {"name": "Ada"},
        "orders": {"count": 3},
    },
    "failed_branches": [],
}
```

Branch events are streamed as they happen. Single-node branches keep their node
ids unchanged for compatibility.

For multi-step branches, use a workflow branch:

```python
{
    "id": "collect",
    "type": "parallel",
    "branches": [
        {
            "id": "profile",
            "input": {"user_id": "{{context.user_id}}"},
            "output": {"level": "{{nodes.decorate.output.level}}"},
            "workflow": {
                "nodes": [
                    {
                        "id": "lookup",
                        "type": "tool",
                        "tool": "query_profile",
                        "args": {"user_id": "{{input.user_id}}"},
                    },
                    {
                        "id": "decorate",
                        "type": "transform",
                        "input": {"level": "{{nodes.lookup.output.profile.level}}"},
                    },
                ],
            },
        },
    ],
}
```

Workflow-branch child node records and events are namespaced as
`profile.lookup`, `profile.decorate`, and so on.

Supported failure policies:

- `fail`: default; any failed branch fails the parallel node
- `continue`: preserve failed branch output and continue the workflow

`human` nodes are intentionally rejected inside parallel branches. A parallel
branch is a nested execution scope; checkpointing user waits there would require
a multi-action or nested-continuation resume contract. Use `human` before or
after the `parallel` node, or route through ordinary condition/edge branches
when user confirmation is required.

## Subflow Nodes

Use a `subflow` node to package a reusable workflow fragment behind one parent
node:

```python
{
    "id": "profile_flow",
    "type": "subflow",
    "input": {"user_id": "{{context.user_id}}"},
    "output": {"level": "{{nodes.decorate.output.level}}"},
    "workflow": {
        "id": "profile-lookup",
        "version": 1,
        "nodes": [
            {
                "id": "lookup",
                "type": "tool",
                "tool": "query_profile",
                "args": {"user_id": "{{input.user_id}}"},
            },
            {
                "id": "decorate",
                "type": "transform",
                "input": {"level": "{{nodes.lookup.output.profile.level}}"},
            },
        ],
    },
}
```

The child workflow runs inside the parent run and shares the same agent/tool
registries, policy gate, artifact store, and event store. Child node state is
merged back with namespaced ids such as `profile_flow.lookup`.

By default, the subflow node exposes the last child node output. Declare
`output` on the subflow node to select a stable contract from the child state.
The parent node output shape is:

```python
{
    "workflow_id": "profile-lookup",
    "status": "completed",
    "nodes": {"profile_flow.lookup": {...}},
    "output": {"level": "vip"},
}
```

Child events are emitted as namespaced subflow events, for example
`subflow.node.started`, with `subflow_node_id` and `subflow_event_type` in the
event data.

`human` nodes are intentionally rejected inside subflows. A subflow is a nested
execution scope; waiting inside it would require persisting and resuming the
child run state separately from the parent node. Use `human` in the parent
workflow before or after the `subflow` node.

## Human Forms

`human` nodes can describe fields and validate the resume payload with a small
JSON-schema-like `response_schema`:

```python
{
    "id": "collect_deploy_params",
    "type": "human",
    "title": "补充部署参数",
    "fields": [
        {"id": "env", "type": "select", "options": ["staging", "prod"]},
        {"id": "version", "type": "text"},
    ],
    "response_schema": {
        "required": ["decision", "env", "version"],
        "properties": {
            "decision": {"type": "string", "enum": ["submit", "cancel"]},
            "env": {"type": "string", "enum": ["staging", "prod"]},
            "version": {"type": "string"},
        },
    },
}
```

Resume with structured user input:

```python
async for event in engine.resume(
    pending_action_id=pending_action_id,
    decision={"decision": "submit", "env": "staging", "version": "1.2.3"},
):
    ...
```

Supported field types in `response_schema.properties.*.type` are `string`,
`number`, `integer`, `boolean`, `object`, and `array`.

The same schema subset supports `enum`, `required`, `items`, `minLength`,
`maxLength`, `minimum`, `maximum`, and `additionalProperties`.

Human and tool-confirmation pending actions can provide a default timeout
decision:

```python
{
    "id": "confirm",
    "type": "human",
    "title": "确认执行",
    "on_timeout": {"decision": "reject", "reason": "expired"},
}
```

If the pending action has expired when `resume(...)` is called, this decision is
used instead of the submitted payload. Without `on_timeout`, expired actions are
rejected with an error.

Services can proactively scan and resume expired pending actions:

```python
events = await engine.resume_expired_actions()
```

Actions with `on_timeout` resume through the normal workflow path using the
timeout decision. Actions without `on_timeout` are marked expired and emit a
`human.expired` event.

## Node Retry and Observability

Any node can opt into retry:

```python
{
    "id": "query_profile",
    "type": "tool",
    "tool": "query_profile",
    "args": {"user_id": "{{context.user_id}}"},
    "timeout_ms": 5_000,
    "retry": {
        "max_attempts": 3,
        "delay_ms": 200,
        "backoff_multiplier": 2,
        "max_delay_ms": 2_000,
        "retry_on": ["TimeoutError", "ConnectionError"],
    },
}
```

The engine emits `node.retrying` before another attempt. `node.finished` includes
`started_at_ms`, `finished_at_ms`, `duration_ms`, and `attempt`.

Terminal node failures can branch through `on_error` edges:

```python
"edges": [
    {"from": "query_profile", "to": "fallback_agent", "on_error": True}
]
```

When a node uses an error edge, the run continues and the failed node output is:

```json
{
  "failed": true,
  "error": "...",
  "error_type": "TimeoutError"
}
```

## Tool Policy

Tools can declare permissions, risk level, and confirmation policy:

```python
tools.register(
    "deploy",
    deploy_handler,
    permissions=["deploy:write"],
    risk_level="high",
    confirmation_policy="risk_based",
)
```

The default policy gate reads granted permissions from run context:

```python
StartRunRequest(
    message="deploy api",
    context={"permissions": ["deploy:write"]},
)
```

Supported confirmation policies:

- `never`: execute directly when permissions pass
- `always`: always create a pending action before execution
- `risk_based`: require confirmation when `risk_level == "high"`

`requires_confirmation=True` remains supported and maps to `always`.

Workflows can restrict which tools are callable:

```python
workflow = WorkflowConfig.from_dict(
    {
        "id": "deploy-flow",
        "version": 1,
        "policy": {"tool_allowlist": ["deploy", "query_profile"]},
        "nodes": [...],
    }
)
```

Tool nodes can override permissions and confirmation policy for that specific
call site:

```python
{
    "id": "deploy_staging",
    "type": "tool",
    "tool": "deploy",
    "permissions": ["deploy:staging"],
    "confirmation_policy": "always",
}
```

Every tool policy evaluation emits a `policy.decision` event before execution,
confirmation, or denial. The event includes the decision, reason, required
permissions, risk level, and confirmation policy. In SSE/message adapters it is
mapped to `POLICY_DECISION`.

## Tool Schemas

Tools can validate rendered arguments and returned output:

```python
tools.register(
    "deploy",
    deploy_handler,
    input_schema={
        "type": "object",
        "required": ["env", "version"],
        "properties": {
            "env": {"type": "string", "enum": ["staging", "prod"]},
            "version": {"type": "string", "minLength": 5},
            "replicas": {"type": "integer", "minimum": 1, "maximum": 5},
        },
        "additionalProperties": False,
    },
    output_schema={
        "type": "object",
        "required": ["deployment_id"],
        "properties": {
            "deployment_id": {"type": "string", "maxLength": 64},
            "ok": {"type": "boolean"},
        },
        "additionalProperties": False,
    },
)
```

Nodes can override the registered tool schemas with `input_schema` or
`output_schema` when a specific workflow needs a narrower contract.

Supported schema keywords:

- `type`: `string`, `number`, `integer`, `boolean`, `object`, `array`, `null`
- `enum`
- `required`
- `properties`
- `items`
- `minLength`, `maxLength`
- `minimum`, `maximum`
- `additionalProperties`: `false` rejects unknown object fields; a schema value
  validates each unknown field

Agent nodes can also validate their final output:

```python
{
    "id": "planner",
    "type": "agent",
    "agent": "planner",
    "output_schema": {
        "type": "object",
        "required": ["requires_confirmation"],
        "properties": {
            "requires_confirmation": {"type": "boolean"},
            "tools": {"type": "array", "items": {"type": "string"}},
        },
    },
}
```

## Artifact Outputs

Large node outputs can be moved out of run state and stored as artifacts. This is
opt-in per node:

```python
{
    "id": "summarize_documents",
    "type": "tool",
    "tool": "summarize_documents",
    "output_artifact": True,
}
```

Or threshold-based:

```python
engine = WorkflowEngine(
    workflow,
    agents=agents,
    tools=tools,
    artifact_store=FileArtifactStore("/tmp/agent-orchestrator-artifacts"),
    artifact_threshold_bytes=64_000,
)
```

When a node output is stored as an artifact, `nodes.<id>.output` becomes:

```json
{
  "artifact_ref": {
    "artifact_id": "art_...",
    "run_id": "run_...",
    "node_id": "summarize_documents",
    "name": "output",
    "store": "file",
    "uri": "/tmp/agent-orchestrator-artifacts/art_....json"
  }
}
```

Use `resolve_artifacts(...)` to recursively replace artifact refs with their
stored values:

```python
from agent_orchestrator import resolve_artifacts

value = await resolve_artifacts({"document": artifact_output}, artifact_store)
```

Nodes can opt into automatic input artifact resolution before calling their
agent/tool/transform logic:

```python
{
    "id": "consumer",
    "type": "tool",
    "tool": "consume_document",
    "resolve_input_artifacts": True,
    "args": {
        "document": "{{nodes.producer.output}}"
    },
}
```

## Claude Agent SDK Runner

Install the optional dependency when using the Claude runner:

```bash
pip install 'dandelion-orchestrator[claude]'
```

Register the runner like any other agent handler:

```python
from agent_orchestrator import AgentRegistry
from agent_orchestrator.runners import ClaudeAgentRunner, ClaudeAgentRunnerConfig

agents = AgentRegistry()
agents.register(
    "claude",
    ClaudeAgentRunner(
        ClaudeAgentRunnerConfig(
            options={
                "cwd": "/path/to/project",
                "model": "claude-sonnet-4-5",
                "system_prompt": "You are a senior engineer.",
                "allowed_tools": ["Read", "Edit"],
                "include_partial_messages": True,
            },
            prompt_template="{message}",
        )
    ),
)
```

The runner maps SDK messages to workflow events:

- `TextBlock` -> `agent.delta`
- `ToolUseBlock` -> `agent.tool_use`
- `ToolResultBlock` -> `agent.tool_result`
- final collected text -> `agent.output`
