import asyncio
import re
import tempfile
from pathlib import Path

import pytest

from agent_orchestrator import (
    AgentRegistry,
    FileCheckpointStore,
    InMemoryCheckpointStore,
    SQLiteCheckpointStore,
    StartRunRequest,
    ToolRegistry,
    WorkflowConfig,
    WorkflowEngine,
    WorkflowEvent,
)
from agent_orchestrator.exceptions import WorkflowError


class FailingSaveCheckpointStore(InMemoryCheckpointStore):
    async def save_waiting(self, run_state, action):
        raise RuntimeError("checkpoint unavailable")


async def planner_agent(agent_input, run_state):
    yield WorkflowEvent(
        type="agent.output",
        run_id=run_state.run_id,
        node_id="planner",
        data={"ok": True},
    )


async def executor_agent(agent_input, run_state):
    yield WorkflowEvent(
        type="agent.output",
        run_id=run_state.run_id,
        node_id="executor",
        data={"answer": f"done for {agent_input['profile']['level']}"},
    )


async def query_profile(args, run_state):
    return {"profile": {"user_id": args["user_id"], "level": "vip"}}


async def _collect_events(stream):
    return [event async for event in stream]


async def test_checkpoint_save_failure_returns_failed_terminal_event():
    agents = AgentRegistry()
    tools = ToolRegistry()
    checkpoints = FailingSaveCheckpointStore()
    workflow = WorkflowConfig.from_dict(
        {
            "id": "checkpoint-save-failure",
            "version": 1,
            "nodes": [{"id": "confirm", "type": "human", "title": "确认"}],
        }
    )
    engine = WorkflowEngine(
        workflow,
        agents=agents,
        tools=tools,
        checkpoints=checkpoints,
    )

    events = [event async for event in engine.start(StartRunRequest(message="hello"))]

    assert [event.type for event in events] == [
        "run.started",
        "node.started",
        "human.required",
        "run.failed",
    ]
    assert events[-1].data["error_type"] == "RuntimeError"
    assert "checkpoint unavailable" in events[-1].data["error"]
    pending_action_id = events[2].data["pending_action_id"]
    with pytest.raises(WorkflowError, match="pending action not found"):
        await checkpoints.load_action(pending_action_id)

async def test_in_memory_checkpoint_store_run_lease_is_exclusive():
    checkpoints = InMemoryCheckpointStore()

    async with checkpoints.lease_run("run_lease"):
        with pytest.raises(WorkflowError, match="already being executed"):
            async with checkpoints.lease_run("run_lease"):
                pass

    async with checkpoints.lease_run("run_lease"):
        pass

async def test_file_checkpoint_store_run_lease_is_exclusive():
    with tempfile.TemporaryDirectory() as tmp:
        first = FileCheckpointStore(tmp)
        second = FileCheckpointStore(tmp)

        async with first.lease_run("run_lease"):
            with pytest.raises(WorkflowError, match="already being executed"):
                async with second.lease_run("run_lease"):
                    pass

        async with second.lease_run("run_lease"):
            pass

async def test_sqlite_checkpoint_store_run_lease_is_exclusive():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "workflow.sqlite"
        first = SQLiteCheckpointStore(path)
        second = SQLiteCheckpointStore(path)

        async with first.lease_run("run_lease"):
            with pytest.raises(WorkflowError, match="already being executed"):
                async with second.lease_run("run_lease"):
                    pass

        async with second.lease_run("run_lease"):
            pass

async def test_engine_run_lease_rejects_concurrent_start_for_same_run():
    agents = AgentRegistry()
    tools = ToolRegistry()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_agent(agent_input, run_state):
        entered.set()
        await asyncio.wait_for(release.wait(), timeout=1)
        yield WorkflowEvent(
            type="agent.output",
            run_id=run_state.run_id,
            node_id="slow",
            data={"ok": True},
        )

    agents.register("slow", slow_agent)
    workflow = WorkflowConfig.from_dict(
        {
            "id": "lease-engine",
            "version": 1,
            "nodes": [{"id": "slow", "type": "agent", "agent": "slow"}],
        }
    )
    engine = WorkflowEngine(
        workflow,
        agents=agents,
        tools=tools,
        checkpoints=InMemoryCheckpointStore(),
    )

    first_task = asyncio.create_task(
        _collect_events(engine.start(StartRunRequest(message="hello", run_id="run_same")))
    )
    await asyncio.wait_for(entered.wait(), timeout=1)
    second_events = await _collect_events(
        engine.start(StartRunRequest(message="hello", run_id="run_same"))
    )
    release.set()
    first_events = await first_task

    assert second_events[-1].type == "run.failed"
    assert "already being executed" in second_events[-1].data["error"]
    assert first_events[-1].type == "run.finished"

async def test_file_checkpoint_store_resumes_across_engine_instances():
    agents = AgentRegistry()
    agents.register("planner", planner_agent)
    agents.register("executor", executor_agent)

    tools = ToolRegistry()
    tools.register("query_profile", query_profile)

    workflow = WorkflowConfig.from_dict(
        {
            "id": "file-store-demo",
            "version": 1,
            "nodes": [
                {"id": "planner", "type": "agent", "agent": "planner"},
                {
                    "id": "query_profile",
                    "type": "tool",
                    "tool": "query_profile",
                    "args": {"user_id": "{{context.user_id}}"},
                },
                {
                    "id": "confirm",
                    "type": "human",
                    "title": "确认",
                    "options": [{"id": "approve", "label": "确认"}],
                },
                {
                    "id": "executor",
                    "type": "agent",
                    "agent": "executor",
                    "input": {"profile": "{{nodes.query_profile.output.profile}}"},
                },
            ],
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        first_engine = WorkflowEngine(
            workflow,
            agents=agents,
            tools=tools,
            checkpoints=FileCheckpointStore(tmp),
        )
        first_events = [
            event
            async for event in first_engine.start(
                StartRunRequest(message="hello", context={"user_id": "u_1"})
            )
        ]
        pending_action_id = first_events[-1].data["pending_action_id"]
        run_id = first_events[-1].run_id

        second_engine = WorkflowEngine(
            workflow,
            agents=agents,
            tools=tools,
            checkpoints=FileCheckpointStore(tmp),
        )
        second_events = [
            event
            async for event in second_engine.resume(
                pending_action_id=pending_action_id,
                decision={"decision": "approve"},
            )
        ]

    assert second_events[0].type == "run.resumed"
    assert second_events[-1].type == "run.finished"
    assert all(event.run_id == run_id for event in second_events)

async def test_sqlite_checkpoint_store_resumes_across_engine_instances():
    agents = AgentRegistry()
    agents.register("planner", planner_agent)
    agents.register("executor", executor_agent)

    tools = ToolRegistry()
    tools.register("query_profile", query_profile)

    workflow = WorkflowConfig.from_dict(
        {
            "id": "sqlite-store-demo",
            "version": 1,
            "nodes": [
                {"id": "planner", "type": "agent", "agent": "planner"},
                {
                    "id": "query_profile",
                    "type": "tool",
                    "tool": "query_profile",
                    "args": {"user_id": "{{context.user_id}}"},
                },
                {
                    "id": "confirm",
                    "type": "human",
                    "title": "确认",
                    "options": [{"id": "approve", "label": "确认"}],
                },
                {
                    "id": "executor",
                    "type": "agent",
                    "agent": "executor",
                    "input": {"profile": "{{nodes.query_profile.output.profile}}"},
                },
            ],
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "workflow.sqlite"
        first_engine = WorkflowEngine(
            workflow,
            agents=agents,
            tools=tools,
            checkpoints=SQLiteCheckpointStore(path),
        )
        first_events = [
            event
            async for event in first_engine.start(
                StartRunRequest(message="hello", context={"user_id": "u_1"})
            )
        ]
        pending_action_id = first_events[-1].data["pending_action_id"]
        run_id = first_events[-1].run_id

        second_engine = WorkflowEngine(
            workflow,
            agents=agents,
            tools=tools,
            checkpoints=SQLiteCheckpointStore(path),
        )
        second_events = [
            event
            async for event in second_engine.resume(
                pending_action_id=pending_action_id,
                decision={"decision": "approve"},
            )
        ]

    assert second_events[0].type == "run.resumed"
    assert second_events[-1].type == "run.finished"
    assert all(event.run_id == run_id for event in second_events)

async def test_sqlite_checkpoint_store_persists_resolved_run_state_atomically():
    agents = AgentRegistry()
    tools = ToolRegistry()
    workflow = WorkflowConfig.from_dict(
        {
            "id": "sqlite-atomic-resume",
            "version": 1,
            "nodes": [{"id": "confirm", "type": "human", "title": "确认"}],
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "workflow.sqlite"
        checkpoints = SQLiteCheckpointStore(path)
        engine = WorkflowEngine(
            workflow,
            agents=agents,
            tools=tools,
            checkpoints=checkpoints,
        )
        first_events = [event async for event in engine.start(StartRunRequest(message="hello"))]
        pending_action_id = first_events[-1].data["pending_action_id"]
        run_id = first_events[-1].run_id

        run_state = await SQLiteCheckpointStore(path).resolve_action(
            pending_action_id,
            {"decision": "approve"},
        )
        persisted_run = await SQLiteCheckpointStore(path).load_run(run_id)
        action = await SQLiteCheckpointStore(path).load_action(pending_action_id)

    assert run_state.state["nodes"]["confirm"]["output"] == {"decision": "approve"}
    assert persisted_run.state["nodes"]["confirm"]["output"] == {"decision": "approve"}
    assert action.status == "approved"

async def test_file_checkpoint_store_rejects_expired_pending_action():
    agents = AgentRegistry()
    tools = ToolRegistry()
    workflow = WorkflowConfig.from_dict(
        {
            "id": "expired-demo",
            "version": 1,
            "nodes": [
                {
                    "id": "confirm",
                    "type": "human",
                    "title": "确认",
                    "options": [{"id": "approve", "label": "确认"}],
                }
            ],
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        engine = WorkflowEngine(
            workflow,
            agents=agents,
            tools=tools,
            checkpoints=FileCheckpointStore(tmp),
            pending_action_ttl_ms=-1,
        )
        first_events = [
            event async for event in engine.start(StartRunRequest(message="hello"))
        ]
        pending_action_id = first_events[-1].data["pending_action_id"]

        with pytest.raises(WorkflowError, match="expired"):
            [
                event
                async for event in engine.resume(
                    pending_action_id=pending_action_id,
                    decision={"decision": "approve"},
                )
            ]

async def test_sqlite_checkpoint_store_marks_expired_action_when_resume_fails():
    agents = AgentRegistry()
    tools = ToolRegistry()
    workflow = WorkflowConfig.from_dict(
        {
            "id": "sqlite-expired-mark",
            "version": 1,
            "nodes": [{"id": "confirm", "type": "human", "title": "确认"}],
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "workflow.sqlite"
        engine = WorkflowEngine(
            workflow,
            agents=agents,
            tools=tools,
            checkpoints=SQLiteCheckpointStore(path),
            pending_action_ttl_ms=-1,
        )
        first_events = [event async for event in engine.start(StartRunRequest(message="hello"))]
        pending_action_id = first_events[-1].data["pending_action_id"]

        with pytest.raises(WorkflowError, match="expired"):
            await SQLiteCheckpointStore(path).resolve_action(
                pending_action_id,
                {"decision": "approve"},
            )
        action = await SQLiteCheckpointStore(path).load_action(pending_action_id)

    assert action.status == "expired"

async def test_file_checkpoint_store_rejects_duplicate_resume():
    agents = AgentRegistry()
    tools = ToolRegistry()
    workflow = WorkflowConfig.from_dict(
        {
            "id": "duplicate-demo",
            "version": 1,
            "nodes": [
                {
                    "id": "confirm",
                    "type": "human",
                    "title": "确认",
                    "options": [{"id": "approve", "label": "确认"}],
                }
            ],
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        engine = WorkflowEngine(
            workflow,
            agents=agents,
            tools=tools,
            checkpoints=FileCheckpointStore(tmp),
        )
        first_events = [
            event async for event in engine.start(StartRunRequest(message="hello"))
        ]
        pending_action_id = first_events[-1].data["pending_action_id"]

        [
            event
            async for event in engine.resume(
                pending_action_id=pending_action_id,
                decision={"decision": "approve"},
            )
        ]

        with pytest.raises(WorkflowError, match="already resolved|already resumed"):
            [
                event
                async for event in engine.resume(
                    pending_action_id=pending_action_id,
                    decision={"decision": "approve"},
                )
            ]

async def test_sqlite_checkpoint_store_rejects_duplicate_resume():
    agents = AgentRegistry()
    tools = ToolRegistry()
    workflow = WorkflowConfig.from_dict(
        {
            "id": "sqlite-duplicate-demo",
            "version": 1,
            "nodes": [
                {
                    "id": "confirm",
                    "type": "human",
                    "title": "确认",
                    "options": [{"id": "approve", "label": "确认"}],
                }
            ],
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "workflow.sqlite"
        engine = WorkflowEngine(
            workflow,
            agents=agents,
            tools=tools,
            checkpoints=SQLiteCheckpointStore(path),
        )
        first_events = [
            event async for event in engine.start(StartRunRequest(message="hello"))
        ]
        pending_action_id = first_events[-1].data["pending_action_id"]

        [
            event
            async for event in engine.resume(
                pending_action_id=pending_action_id,
                decision={"decision": "approve"},
            )
        ]

        duplicate_engine = WorkflowEngine(
            workflow,
            agents=agents,
            tools=tools,
            checkpoints=SQLiteCheckpointStore(path),
        )
        with pytest.raises(WorkflowError, match="already resolved|already resumed"):
            [
                event
                async for event in duplicate_engine.resume(
                    pending_action_id=pending_action_id,
                    decision={"decision": "approve"},
                )
            ]

async def test_sqlite_checkpoint_store_allows_only_one_concurrent_resolve():
    agents = AgentRegistry()
    tools = ToolRegistry()
    workflow = WorkflowConfig.from_dict(
        {
            "id": "sqlite-concurrent-resolve",
            "version": 1,
            "nodes": [{"id": "confirm", "type": "human", "title": "确认"}],
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "workflow.sqlite"
        engine = WorkflowEngine(
            workflow,
            agents=agents,
            tools=tools,
            checkpoints=SQLiteCheckpointStore(path),
        )
        first_events = [event async for event in engine.start(StartRunRequest(message="hello"))]
        pending_action_id = first_events[-1].data["pending_action_id"]
        first_store = SQLiteCheckpointStore(path)
        second_store = SQLiteCheckpointStore(path)

        results = await asyncio.gather(
            first_store.resolve_action(pending_action_id, {"decision": "approve"}),
            second_store.resolve_action(pending_action_id, {"decision": "approve"}),
            return_exceptions=True,
        )

    successes = [result for result in results if not isinstance(result, Exception)]
    failures = [result for result in results if isinstance(result, WorkflowError)]
    assert len(successes) == 1
    assert len(failures) == 1
    assert re.search("already resolved|already resumed", str(failures[0]))

async def test_file_checkpoint_store_lists_expired_actions():
    agents = AgentRegistry()
    tools = ToolRegistry()
    workflow = WorkflowConfig.from_dict(
        {
            "id": "file-expired-list",
            "version": 1,
            "nodes": [{"id": "confirm", "type": "human", "title": "确认"}],
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        checkpoints = FileCheckpointStore(tmp)
        engine = WorkflowEngine(
            workflow,
            agents=agents,
            tools=tools,
            checkpoints=checkpoints,
            pending_action_ttl_ms=-1,
        )
        first_events = [event async for event in engine.start(StartRunRequest(message="hello"))]
        pending_action_id = first_events[-1].data["pending_action_id"]
        expired = await checkpoints.list_expired_actions(10**15)

    assert [action.id for action in expired] == [pending_action_id]

async def test_sqlite_checkpoint_store_lists_expired_actions():
    agents = AgentRegistry()
    tools = ToolRegistry()
    workflow = WorkflowConfig.from_dict(
        {
            "id": "sqlite-expired-list",
            "version": 1,
            "nodes": [{"id": "confirm", "type": "human", "title": "确认"}],
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        checkpoints = SQLiteCheckpointStore(Path(tmp) / "workflow.sqlite")
        engine = WorkflowEngine(
            workflow,
            agents=agents,
            tools=tools,
            checkpoints=checkpoints,
            pending_action_ttl_ms=-1,
        )
        first_events = [event async for event in engine.start(StartRunRequest(message="hello"))]
        pending_action_id = first_events[-1].data["pending_action_id"]
        expired = await checkpoints.list_expired_actions(10**15)

    assert [action.id for action in expired] == [pending_action_id]
