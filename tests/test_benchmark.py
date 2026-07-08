import tempfile
import time
from pathlib import Path

from agent_orchestrator import (
    AgentRegistry,
    InMemoryCheckpointStore,
    PendingAction,
    RunState,
    SQLiteCheckpointStore,
    StartRunRequest,
    ToolRegistry,
    WorkflowConfig,
    WorkflowEngine,
    WorkflowEvent,
    compact_events,
    replay_events,
)
from agent_orchestrator.state import render_template


async def test_benchmark_linear_workflow_throughput():
    tools = ToolRegistry()

    async def noop(args, run_state):
        return {"ok": True}

    tools.register("noop", noop)
    workflow = WorkflowConfig.from_dict({
        "id": "bench-linear",
        "version": 1,
        "nodes": [{"id": f"step_{i}", "type": "tool", "tool": "noop"} for i in range(10)],
    })
    engine = WorkflowEngine(workflow, agents=AgentRegistry(), tools=tools)

    iterations = 100
    t0 = time.perf_counter_ns()
    for _ in range(iterations):
        events = [e async for e in engine.start(StartRunRequest(message="go"))]
        assert events[-1].type == "run.finished"
    elapsed_ms = (time.perf_counter_ns() - t0) / 1_000_000

    avg_ms = elapsed_ms / iterations
    print(f"\nLinear 10-node workflow: {avg_ms:.2f}ms avg over {iterations} runs")
    assert avg_ms < 50


async def test_benchmark_parallel_branches():
    tools = ToolRegistry()

    async def noop(args, run_state):
        return {"ok": True}

    tools.register("noop", noop)
    workflow = WorkflowConfig.from_dict({
        "id": "bench-parallel",
        "version": 1,
        "nodes": [{
            "id": "fanout",
            "type": "parallel",
            "branches": [
                {"id": f"b_{i}", "type": "tool", "tool": "noop"} for i in range(10)
            ],
        }],
    })
    engine = WorkflowEngine(workflow, agents=AgentRegistry(), tools=tools)

    iterations = 50
    t0 = time.perf_counter_ns()
    for _ in range(iterations):
        events = [e async for e in engine.start(StartRunRequest(message="go"))]
        assert events[-1].type == "run.finished"
    elapsed_ms = (time.perf_counter_ns() - t0) / 1_000_000

    avg_ms = elapsed_ms / iterations
    print(f"\nParallel 10-branch workflow: {avg_ms:.2f}ms avg over {iterations} runs")
    assert avg_ms < 100


async def test_benchmark_template_rendering():
    state = {
        "context": {"user_id": "u_123", "org": {"name": "Acme", "tier": "enterprise"}},
        "nodes": {
            "step_0": {"output": {"profile": {"level": "vip", "tags": ["a", "b"]}}},
            "step_1": {"output": {"result": "done"}},
        },
    }
    templates = [
        "Hello {{context.user_id}} from {{context.org.name}}",
        "Level: {{nodes.step_0.output.profile.level}}",
        "Result: {{nodes.step_1.output.result}}",
        "Tier: {{context.org.tier}}",
    ]

    iterations = 1000
    t0 = time.perf_counter_ns()
    for _ in range(iterations):
        for tmpl in templates:
            render_template(tmpl, state)
    elapsed_ms = (time.perf_counter_ns() - t0) / 1_000_000

    ops = iterations * len(templates)
    us_per_op = (elapsed_ms * 1000) / ops
    print(f"\nTemplate rendering: {us_per_op:.2f}µs/op over {ops} ops")
    assert us_per_op < 500


async def test_benchmark_checkpoint_roundtrip_in_memory():
    store = InMemoryCheckpointStore()

    iterations = 200
    t0 = time.perf_counter_ns()
    for i in range(iterations):
        run_state = RunState(
            run_id=f"run_{i}",
            workflow_id="wf",
            workflow_version=1,
            status="waiting_for_user",
            waiting_action_id=f"pa_{i}",
            current_node_id="confirm",
            state={"nodes": {"confirm": {"status": "waiting"}}},
        )
        action = PendingAction(
            id=f"pa_{i}",
            run_id=f"run_{i}",
            node_id="confirm",
            action_type="human",
            request={"response_schema": {"type": "object"}},
        )
        await store.save_waiting(run_state, action)
        await store.resolve_action(f"pa_{i}", {"decision": "approve"})
    elapsed_ms = (time.perf_counter_ns() - t0) / 1_000_000

    avg_ms = elapsed_ms / iterations
    print(f"\nInMemory checkpoint save+resolve: {avg_ms:.3f}ms avg over {iterations} cycles")
    assert avg_ms < 5


async def test_benchmark_checkpoint_roundtrip_sqlite():
    with tempfile.TemporaryDirectory() as tmp:
        store = SQLiteCheckpointStore(Path(tmp) / "bench.sqlite")

        iterations = 100
        t0 = time.perf_counter_ns()
        for i in range(iterations):
            run_state = RunState(
                run_id=f"run_{i}",
                workflow_id="wf",
                workflow_version=1,
                status="waiting_for_user",
                waiting_action_id=f"pa_{i}",
                current_node_id="confirm",
                state={"nodes": {"confirm": {"status": "waiting"}}},
            )
            action = PendingAction(
                id=f"pa_{i}",
                run_id=f"run_{i}",
                node_id="confirm",
                action_type="human",
                request={"response_schema": {"type": "object"}},
            )
            await store.save_waiting(run_state, action)
            await store.resolve_action(f"pa_{i}", {"decision": "approve"})
        elapsed_ms = (time.perf_counter_ns() - t0) / 1_000_000

    avg_ms = elapsed_ms / iterations
    print(f"\nSQLite checkpoint save+resolve: {avg_ms:.3f}ms avg over {iterations} cycles")
    assert avg_ms < 20


async def test_benchmark_event_replay():
    events = [
        WorkflowEvent(
            type="node.started" if i % 2 == 0 else "node.finished",
            run_id="run_replay",
            node_id=f"node_{i // 2}",
            data={"iteration": i, "payload": "x" * 100},
        )
        for i in range(1000)
    ]
    events.insert(0, WorkflowEvent(type="run.started", run_id="run_replay", data={"status": "running"}))
    events.append(WorkflowEvent(type="run.finished", run_id="run_replay", data={"status": "completed"}))

    iterations = 50
    t0 = time.perf_counter_ns()
    for _ in range(iterations):
        replay_events(events)
    elapsed_ms = (time.perf_counter_ns() - t0) / 1_000_000

    avg_ms = elapsed_ms / iterations
    print(f"\nReplay 1002 events: {avg_ms:.2f}ms avg over {iterations} runs")
    assert avg_ms < 50


async def test_benchmark_event_compaction():
    events = [
        WorkflowEvent(
            type="node.started" if i % 2 == 0 else "node.finished",
            run_id="run_compact",
            node_id=f"node_{i // 2}",
            data={"iteration": i},
        )
        for i in range(1000)
    ]
    events.insert(0, WorkflowEvent(type="run.started", run_id="run_compact", data={"status": "running"}))
    events.append(WorkflowEvent(type="run.finished", run_id="run_compact", data={"status": "completed"}))

    iterations = 50
    t0 = time.perf_counter_ns()
    for _ in range(iterations):
        result = compact_events(events, retain_last=10)
        assert result.compacted_event_count > 0
    elapsed_ms = (time.perf_counter_ns() - t0) / 1_000_000

    avg_ms = elapsed_ms / iterations
    print(f"\nCompact 1002 events (retain 10): {avg_ms:.2f}ms avg over {iterations} runs")
    assert avg_ms < 50
