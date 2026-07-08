# Agent Orchestrator

Composable Agent Workflow Engine with resumable human checkpoints.
PyPI package: `dandelion-orchestrator` — import as `agent_orchestrator`.

## Build & Test

```bash
make check          # compile + lint + typecheck + test (with coverage)
```

Or run steps individually:

```bash
PYTHONPATH=src python3 -m pytest tests/
python3 -m ruff check src tests
python3 -m pyright src
```

## Architecture

The engine (`WorkflowEngine`) composes behavior via mixins:

- `EngineRuntimeMixin` — event emission, run state helpers
- `BasicNodeExecutorMixin` — agent, tool, transform, human, condition nodes
- `ParallelNodeExecutorMixin` — concurrent branch execution
- `SubflowNodeExecutorMixin` — nested workflow execution
- `LoopNodeExecutorMixin` — iteration with condition-based exit

Node types: `agent`, `tool`, `transform`, `human`, `condition`, `parallel`, `subflow`, `loop`.

Store hierarchy: InMemory → File → SQLite → Redis (checkpoint, event, artifact).

## Conventions

- Zero required dependencies — stdlib only for core
- Async generators for streaming events
- Dataclass models with `slots=True`
- Template expressions `{{path.to.value}}` for state binding
- Condition expressions use safe parsing (no `eval`)
- `py.typed` marker for downstream type checking
