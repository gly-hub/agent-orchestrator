from agent_orchestrator import (
    AgentRegistry,
    StartRunRequest,
    ToolRegistry,
    WorkflowConfig,
    WorkflowEngine,
    WorkflowEvent,
)


class FailingEventStore:
    async def append(self, event):
        raise RuntimeError("event store unavailable")

    async def list_by_run(self, run_id):
        return []


async def test_observer_receives_node_lifecycle_and_event_append_observations():
    agents = AgentRegistry()
    tools = ToolRegistry()
    observations = []

    async def ok_tool(args, run_state):
        return {"ok": True}

    tools.register("ok", ok_tool)
    workflow = WorkflowConfig.from_dict(
        {
            "id": "observability",
            "version": 1,
            "nodes": [{"id": "ok", "type": "tool", "tool": "ok"}],
        }
    )
    engine = WorkflowEngine(
        workflow,
        agents=agents,
        tools=tools,
        observer=observations.append,
    )

    events = [event async for event in engine.start(StartRunRequest(message="hello"))]

    assert events[-1].type == "run.finished"
    types = [observation.type for observation in observations]
    assert "node.started" in types
    assert "node.finished" in types
    assert "event.appended" in types
    finished = [item for item in observations if item.type == "node.finished"][0]
    assert finished.node_id == "ok"
    assert finished.data["status"] == "success"
    assert "duration_ms" in finished.data

async def test_observer_failure_does_not_fail_workflow():
    agents = AgentRegistry()
    tools = ToolRegistry()

    async def ok_tool(args, run_state):
        return {"ok": True}

    def broken_observer(observation):
        raise RuntimeError("observer down")

    tools.register("ok", ok_tool)
    workflow = WorkflowConfig.from_dict(
        {
            "id": "observer-failure",
            "version": 1,
            "nodes": [{"id": "ok", "type": "tool", "tool": "ok"}],
        }
    )
    engine = WorkflowEngine(
        workflow,
        agents=agents,
        tools=tools,
        observer=broken_observer,
    )

    events = [event async for event in engine.start(StartRunRequest(message="hello"))]

    assert events[-1].type == "run.finished"

async def test_event_store_failure_emits_append_failed_observation():
    agents = AgentRegistry()
    tools = ToolRegistry()
    observations = []

    workflow = WorkflowConfig.from_dict(
        {
            "id": "event-store-failure-observed",
            "version": 1,
            "nodes": [{"id": "noop", "type": "transform"}],
        }
    )
    engine = WorkflowEngine(
        workflow,
        agents=agents,
        tools=tools,
        event_store=FailingEventStore(),
        observer=observations.append,
    )

    events = [event async for event in engine.start(StartRunRequest(message="hello"))]

    assert events == [
        WorkflowEvent(
            type="run.failed",
            run_id=events[0].run_id,
            node_id=None,
            data=events[0].data,
        )
    ]
    failed = [item for item in observations if item.type == "event.append_failed"][0]
    assert failed.data["event_type"] == "run.started"
    assert failed.data["error_type"] == "RuntimeError"
