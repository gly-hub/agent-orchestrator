import asyncio

import pytest

from agent_orchestrator import (
    AgentRegistry,
    StartRunRequest,
    ToolRegistry,
    WorkflowConfig,
    WorkflowEngine,
    WorkflowEvent,
)


async def test_node_retry_retries_transient_tool_failure_and_records_duration():
    agents = AgentRegistry()
    tools = ToolRegistry()
    calls = []

    async def flaky_tool(args, run_state):
        calls.append(args)
        if len(calls) == 1:
            raise RuntimeError("temporary failure")
        return {"ok": True}

    tools.register("flaky", flaky_tool)
    workflow = WorkflowConfig.from_dict(
        {
            "id": "retry-demo",
            "version": 1,
            "nodes": [
                {
                    "id": "flaky",
                    "type": "tool",
                    "tool": "flaky",
                    "args": {"value": "{{input.message}}"},
                    "retry": {"max_attempts": 2},
                }
            ],
        }
    )
    engine = WorkflowEngine(workflow, agents=agents, tools=tools)

    events = [event async for event in engine.start(StartRunRequest(message="x"))]

    retrying = [event for event in events if event.type == "node.retrying"]
    finished = [event for event in events if event.type == "node.finished"][0]
    assert len(calls) == 2
    assert retrying[0].data["next_attempt"] == 2
    assert finished.data["attempt"] == 2
    assert "duration_ms" in finished.data

async def test_retry_policy_uses_backoff_and_retry_on_filter():
    agents = AgentRegistry()
    tools = ToolRegistry()
    calls = []

    async def flaky_tool(args, run_state):
        calls.append(args)
        if len(calls) < 3:
            raise RuntimeError("temporary")
        return {"ok": True}

    tools.register("flaky", flaky_tool)
    workflow = WorkflowConfig.from_dict(
        {
            "id": "retry-backoff",
            "version": 1,
            "nodes": [
                {
                    "id": "flaky",
                    "type": "tool",
                    "tool": "flaky",
                    "retry": {
                        "max_attempts": 3,
                        "delay_ms": 1,
                        "backoff_multiplier": 2,
                        "max_delay_ms": 10,
                        "retry_on": ["RuntimeError"],
                    },
                }
            ],
        }
    )
    engine = WorkflowEngine(workflow, agents=agents, tools=tools)

    events = [event async for event in engine.start(StartRunRequest(message="run"))]

    retry_events = [event for event in events if event.type == "node.retrying"]
    finished = [event for event in events if event.type == "node.finished"][0]
    assert len(calls) == 3
    assert [event.data["delay_ms"] for event in retry_events] == [1, 2]
    assert finished.data["attempt"] == 3

async def test_retry_on_filter_skips_unmatched_errors():
    agents = AgentRegistry()
    tools = ToolRegistry()
    calls = []

    async def bad_tool(args, run_state):
        calls.append(args)
        raise ValueError("bad input")

    tools.register("bad", bad_tool)
    workflow = WorkflowConfig.from_dict(
        {
            "id": "retry-filter",
            "version": 1,
            "nodes": [
                {
                    "id": "bad",
                    "type": "tool",
                    "tool": "bad",
                    "retry": {"max_attempts": 3, "retry_on": ["RuntimeError"]},
                }
            ],
        }
    )
    engine = WorkflowEngine(workflow, agents=agents, tools=tools)

    events = [event async for event in engine.start(StartRunRequest(message="run"))]

    assert len(calls) == 1
    assert events[-1].type == "run.failed"
    assert events[-1].data["error_type"] == "ValueError"

async def test_engine_can_raise_after_emitting_failed_event_and_observe_error():
    agents = AgentRegistry()
    tools = ToolRegistry()
    observed = []

    async def bad_tool(args, run_state):
        raise ValueError("bad input")

    async def observer(exc, run_state):
        observed.append((type(exc).__name__, run_state.status, run_state.current_node_id))

    tools.register("bad", bad_tool)
    workflow = WorkflowConfig.from_dict(
        {
            "id": "raise-on-error",
            "version": 1,
            "nodes": [{"id": "bad", "type": "tool", "tool": "bad"}],
        }
    )
    engine = WorkflowEngine(
        workflow,
        agents=agents,
        tools=tools,
        raise_on_error=True,
        error_observer=observer,
    )

    events = []
    with pytest.raises(ValueError, match="bad input"):
        async for event in engine.start(StartRunRequest(message="run")):
            events.append(event)

    assert events[-1].type == "run.failed"
    assert observed == [("ValueError", "failed", "bad")]

async def test_timeout_can_fallback_to_error_edge():
    agents = AgentRegistry()
    tools = ToolRegistry()

    async def slow_tool(args, run_state):
        await asyncio.sleep(0.05)
        return {"ok": True}

    async def fallback_agent(agent_input, run_state):
        yield WorkflowEvent(
            type="agent.output",
            run_id=run_state.run_id,
            node_id="fallback",
            data={"handled": agent_input["failed"], "error_type": agent_input["error_type"]},
        )

    tools.register("slow", slow_tool)
    agents.register("fallback", fallback_agent)
    workflow = WorkflowConfig.from_dict(
        {
            "id": "timeout-fallback",
            "version": 1,
            "nodes": [
                {
                    "id": "slow",
                    "type": "tool",
                    "tool": "slow",
                    "timeout_ms": 1,
                },
                {
                    "id": "fallback",
                    "type": "agent",
                    "agent": "fallback",
                    "input": {
                        "failed": "{{nodes.slow.output.failed}}",
                        "error_type": "{{nodes.slow.output.error_type}}",
                    },
                },
            ],
            "edges": [
                {
                    "from": "slow",
                    "to": "fallback",
                    "on_error": True,
                }
            ],
        }
    )
    engine = WorkflowEngine(workflow, agents=agents, tools=tools)

    events = [event async for event in engine.start(StartRunRequest(message="run"))]

    failed = [event for event in events if event.type == "node.failed"][0]
    fallback = [
        event for event in events if event.node_id == "fallback" and event.type == "node.finished"
    ][0]
    assert failed.data["error_type"] == "TimeoutError"
    assert fallback.data["output"] == {"handled": True, "error_type": "TimeoutError"}

async def test_timeout_preserves_streaming_node_events():
    agents = AgentRegistry()
    tools = ToolRegistry()
    release_agent = asyncio.Event()

    async def streaming_agent(agent_input, run_state):
        yield WorkflowEvent(
            type="agent.delta",
            run_id=run_state.run_id,
            node_id="writer",
            data={"text": "first"},
        )
        await release_agent.wait()
        yield WorkflowEvent(
            type="agent.output",
            run_id=run_state.run_id,
            node_id="writer",
            data={"text": "done"},
        )

    agents.register("writer", streaming_agent)
    workflow = WorkflowConfig.from_dict(
        {
            "id": "streaming-timeout",
            "version": 1,
            "nodes": [
                {
                    "id": "writer",
                    "type": "agent",
                    "agent": "writer",
                    "timeout_ms": 1_000,
                }
            ],
        }
    )
    engine = WorkflowEngine(workflow, agents=agents, tools=tools)
    stream = engine.start(StartRunRequest(message="run")).__aiter__()

    try:
        assert (await anext(stream)).type == "run.started"
        assert (await anext(stream)).type == "node.started"
        delta = await asyncio.wait_for(anext(stream), timeout=0.05)
        assert delta.type == "agent.delta"
        assert delta.data["text"] == "first"
    finally:
        release_agent.set()

    remaining = [event async for event in stream]
    assert remaining[-1].type == "run.finished"
