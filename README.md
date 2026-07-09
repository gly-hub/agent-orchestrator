# Agent Orchestrator

Composable Agent Workflow Engine with resumable human checkpoints.

```
pip install dandelion-orchestrator
```

## Quick Start

```python
import asyncio
from agent_orchestrator import (
    AgentRegistry, StartRunRequest, ToolRegistry,
    WorkflowConfig, WorkflowEngine, WorkflowEvent,
)

async def main():
    agents = AgentRegistry()
    tools = ToolRegistry()

    async def greet_agent(agent_input, run_state):
        yield WorkflowEvent(
            type="agent.output",
            run_id=run_state.run_id,
            node_id="greet",
            data={"greeting": f"Hello, {agent_input['name']}!"},
        )

    agents.register("greeter", greet_agent)

    workflow = WorkflowConfig.from_dict({
        "id": "hello-world", "version": 1,
        "nodes": [
            {"id": "greet", "type": "agent", "agent": "greeter",
             "input": {"name": "{{context.user_name}}"}},
        ],
    })

    engine = WorkflowEngine(workflow, agents=agents, tools=tools)
    async for event in engine.start(
        StartRunRequest(message="hi", context={"user_name": "Ada"})
    ):
        print(event.type, event.data)

asyncio.run(main())
```

## Architecture

```
                        ┌──────────────────────┐
                        │    WorkflowEngine     │
                        │  (start / resume)     │
                        └──────────┬───────────┘
                                   │
               ┌───────────────────┼───────────────────┐
               │                   │                   │
        ┌──────▼──────┐    ┌──────▼──────┐    ┌───────▼──────┐
        │ DAG Scheduler│    │  Executors   │    │   Stores     │
        │ ready queue  │    │  agent/tool  │    │  checkpoint  │
        │ edge state   │    │  transform   │    │  event       │
        │ joins/when   │    │  human/cond  │    │  artifact    │
        │              │    │  parallel    │    │              │
        │              │    │  subflow     │    │              │
        │              │    │  loop        │    │              │
        └──────────────┘    └─────────────┘    └──────────────┘
```

The engine drives a run through its workflow graph. The **DAG scheduler** finds
all ready nodes from explicit edges, join state, and `when` conditions, then runs
ready nodes concurrently. **Executors** run each node type.
**Stores** persist checkpoint state, events, and artifacts.

## Core Concepts

**WorkflowConfig** — a validated DAG of nodes and edges, created from a dict.
Nodes have types (`agent`, `tool`, `transform`, `human`, `condition`, `parallel`,
`subflow`, `loop`) and connect via edges with optional `when` conditions.
Node declaration order is only a stable listing order; execution dependencies
must be expressed with explicit edges.

**RunState** — the mutable state of a single workflow execution. Holds node
outputs, context, status, scheduler state, edge activation state, and pending
human actions.

**Templates** — `{{path.to.value}}` expressions resolve dotted paths from run
state. Supports `| default(fallback)` filters. When a whole string is a single
template, the original value type is preserved.

**Human Checkpoints** — `human` nodes pause the run and emit `human.required`.
The client stores the `pending_action_id`, collects user input, and calls
`engine.resume(...)` to continue. Human nodes pause only their own DAG path;
other ready nodes continue running. A run may contain multiple simultaneous
pending human actions.

**Events** — every lifecycle step emits a `WorkflowEvent`. Events power
streaming, replay, and compaction.

## Features

- 8 node types: `agent`, `tool`, `transform`, `human`, `condition`, `parallel`, `subflow`, `loop`
- Shared state with `{{path.to.value}}` template resolution and `| default(...)`
- Condition expressions: `==`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `not in`, `and`, `or`, parentheses
- DAG scheduler with concurrent ready nodes, edge activation, and multi-input joins
- Human checkpoint and resume flow with response schema validation and multiple pending actions
- Human nodes in concurrent DAG paths and subflow nodes
- Loop nodes with condition-based exit and max iteration caps
- Tool confirmation, permissions, risk levels, and policy decisions
- Tool and agent input/output schema validation
- Node retry with exponential backoff and error edges
- Pluggable checkpoint, event, and artifact stores
- Built-in stores: in-memory, file, SQLite, Redis
- Replayable event compaction
- Event schema versioning with migration registry
- Message/SSE event adapter for stream continuity
- Optional Claude Agent SDK runner
- Framework-independent core, zero required dependencies

## Run Tests

```bash
make check
```

Or run tests directly:

```bash
PYTHONPATH=src python3 -m pytest tests/
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
paths, `when` supports simple comparisons plus `and`/`or` with parentheses for
grouping, and schemas use a JSON-Schema-like subset. See
[docs/workflow_language.md](docs/workflow_language.md) for the exact supported
surface.

## Public API

Applications should import from the package root, `agent_orchestrator`, whenever
possible. See [docs/public_api.md](docs/public_api.md) for the stable API
surface and internal-module policy.

## Production Reliability

The checkpoint store is the source of truth for live execution. Event stores are
append-only audit/replay logs unless an application supplies a transactional store
pair. Streaming clients should treat `run.finished`, `run.failed`, and
`run.waiting` as the durable terminal states for a stream.

## Service Flow

Use `WorkflowEngine.start(...)` for the first stream. When a `human.required`
event is emitted, persist `pending_action_id` in the client. After user
confirmation, call `WorkflowEngine.resume(...)` and continue streaming.

## Node Types

### Data Binding

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

### Condition Nodes

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
({{context.a}} == 1 or {{context.b}} == 2) and {{context.c}} == 3
```

Supported operators: `==`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `not in`,
`and`, `or`. Parentheses are supported for grouping.

### Parallel Nodes

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

For new visual workflows, prefer plain DAG edges for fan-out/fan-in. A `parallel`
node remains available as a compact compatibility primitive for branch output
contracts and failure policy, but general joins, conditions, and human waits are
handled by the DAG scheduler.

### Subflow Nodes

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

Child events are emitted as namespaced subflow events, for example
`subflow.node.started`, with `subflow_node_id` and `subflow_event_type` in the
event data.

`human` nodes inside subflows pause the parent run. On resume, the child
workflow continues from where it left off.

### Loop Nodes

Use a `loop` node to repeat a body workflow while a condition holds:

```python
{
    "id": "retry_until_done",
    "type": "loop",
    "condition": "{{nodes.retry_until_done.output.last_output.status}} != 'done'",
    "max_iterations": 10,
    "body": {
        "nodes": [
            {"id": "check", "type": "tool", "tool": "check_status"},
        ],
    },
}
```

The condition is evaluated before each iteration (after the first). If omitted,
the loop runs for exactly `max_iterations` (default: 100). The loop node output
is:

```python
{
    "iterations": 3,
    "outputs": [{"status": "pending"}, {"status": "pending"}, {"status": "done"}],
    "last_output": {"status": "done"},
}
```

Body node events are namespaced as `loop_id.iteration_N.child_node`.

### Human Forms

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
used instead of the submitted payload. Services can proactively scan and resume
expired pending actions:

```python
events = await engine.resume_expired_actions()
```

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

Every tool policy evaluation emits a `policy.decision` event before execution,
confirmation, or denial.

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
        },
    },
    output_schema={
        "type": "object",
        "required": ["deployment_id"],
        "properties": {
            "deployment_id": {"type": "string", "maxLength": 64},
        },
    },
)
```

Supported schema keywords: `type`, `enum`, `required`, `properties`, `items`,
`minLength`, `maxLength`, `minimum`, `maximum`, `additionalProperties`.

## Artifact Outputs

Large node outputs can be stored as artifacts. Opt-in per node:

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
    workflow, agents=agents, tools=tools,
    artifact_store=FileArtifactStore("/tmp/artifacts"),
    artifact_threshold_bytes=64_000,
)
```

Use `resolve_artifacts(...)` to recursively replace artifact refs with their
stored values:

```python
from agent_orchestrator import resolve_artifacts

value = await resolve_artifacts({"document": artifact_output}, artifact_store)
```

## Persistent Checkpoints

Use `FileCheckpointStore` for local services or integration tests that need to
resume across engine instances:

```python
from agent_orchestrator import FileCheckpointStore, WorkflowEngine

engine = WorkflowEngine(
    workflow, agents=agents, tools=tools,
    checkpoints=FileCheckpointStore("/tmp/checkpoints"),
    pending_action_ttl_ms=600_000,
)
```

### Store Options

| Store | Use Case | Dependencies |
|-------|----------|-------------|
| `InMemoryCheckpointStore` | Tests, single-process demos | None |
| `FileCheckpointStore` | Local dev, integration tests | None |
| `SQLiteCheckpointStore` | Small deployments, embedded | None (stdlib) |
| `RedisCheckpointStore` | Multi-instance production | `redis>=5.0.0` |

Event and artifact stores follow the same pattern. Install optional dependencies:

```bash
pip install "dandelion-orchestrator[redis]"   # Redis stores
pip install "dandelion-orchestrator[all]"     # All optional deps
```

### Execution Leases

`start(...)` and `resume(...)` hold a run-level execution lease. The lease
prevents two workers from advancing the same run at the same time. Built-in
stores implement leases with process memory, lock files, SQLite transactions,
or Redis `SET NX`.

## Persistence Plugins

Persistence is interface-based. Register custom provider factories:

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

## Event Replay and Compaction

Persisted events can be replayed into a compact run view:

```python
from agent_orchestrator import replay_run

replay = await replay_run(event_store, run_id)
print(replay.status, replay.nodes["deploy"]["output"])
```

Long event logs can be compacted:

```python
from agent_orchestrator import compact_run

result = await compact_run(event_store, run_id, retain_last=20)
```

Workflow events include a `schema_version` field. Use `EventMigrationRegistry`
to register migrations for upgrading persisted events across versions.

## Claude Agent SDK Runner

Install the optional dependency:

```bash
pip install 'dandelion-orchestrator[claude]'
```

```python
from agent_orchestrator import AgentRegistry
from agent_orchestrator.runners import ClaudeAgentRunner, ClaudeAgentRunnerConfig

agents = AgentRegistry()
agents.register(
    "claude",
    ClaudeAgentRunner(
        ClaudeAgentRunnerConfig(
            options={
                "model": "claude-sonnet-4-5",
                "system_prompt": "You are a senior engineer.",
                "allowed_tools": ["Read", "Edit"],
            },
            prompt_template="$message",
        )
    ),
)
```

The runner maps SDK messages to workflow events: `TextBlock` -> `agent.delta`,
`ToolUseBlock` -> `agent.tool_use`, `ToolResultBlock` -> `agent.tool_result`,
final collected text -> `agent.output`.

## Config Validation

`WorkflowConfig.from_dict(...)` validates workflow structure:

- Node ids must be unique
- Node types must be supported
- `agent` nodes must declare `agent`, `tool` nodes must declare `tool`
- `condition` cases must declare `when` and `value`
- `parallel` branches must be non-empty
- `subflow` and `loop` nodes must declare valid body/workflow
- `loop` nodes must have `max_iterations >= 1`
- Edge endpoints must reference existing nodes
- Graph cycles are rejected
- `human` nodes are rejected inside `loop` bodies
