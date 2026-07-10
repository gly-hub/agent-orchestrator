"""Regression tests for recently fixed bugs."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from unittest.mock import AsyncMock

import pytest

from agent_orchestrator.models import PendingAction, RunState
from agent_orchestrator.parallel import _ParallelResultMerger
from agent_orchestrator.stores.redis import RedisCheckpointStore
from agent_orchestrator.stores.sqlite import SQLiteCheckpointStore

# ---------------------------------------------------------------------------
# Test 1: Redis stale expiry handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_expired_actions_skips_stale_keys():
    """When load_action raises WorkflowError (TTL-deleted key), list_expired_actions
    should clean up the sorted-set entry with zrem and continue to the next action."""
    mock_client = AsyncMock()
    store = RedisCheckpointStore(client=mock_client)

    mock_client.zrangebyscore.return_value = ["action_1", "action_2"]

    action_2 = PendingAction(
        id="action_2",
        run_id="run_1",
        node_id="node_1",
        action_type="human",
        request={},
        status="pending",
        expires_at_ms=100,
    )

    def mock_get(key):
        if "action_1" in key:
            return None  # TTL-deleted → load_action will raise WorkflowError
        return json.dumps(asdict(action_2))

    mock_client.get = AsyncMock(side_effect=mock_get)
    mock_client.zrem = AsyncMock()

    result = await store.list_expired_actions(now_ms=200)

    assert len(result) == 1
    assert result[0].id == "action_2"
    mock_client.zrem.assert_called_once()
    # Verify zrem was called for the stale action_1, not for action_2
    zrem_args = mock_client.zrem.call_args
    assert "action_1" in zrem_args[0]


# ---------------------------------------------------------------------------
# Test 2: SQLite connection close on exception
# ---------------------------------------------------------------------------


def test_sqlite_connection_closed_on_exception(tmp_path):
    """_open() context manager must close the connection even when an exception
    occurs inside the with block."""
    store = SQLiteCheckpointStore(tmp_path / "test.db")
    connections: list[sqlite3.Connection] = []
    original_connect = sqlite3.connect

    def tracking_connect(*args, **kwargs):
        conn = original_connect(*args, **kwargs)
        connections.append(conn)
        return conn

    from unittest.mock import patch

    with (
        pytest.raises(RuntimeError, match="test error"),
        patch("agent_orchestrator.stores.sqlite.sqlite3.connect", tracking_connect),
        store._open() as _conn,
    ):
        raise RuntimeError("test error")

    # Connection should be closed despite the exception
    assert len(connections) == 1
    # Attempting to use a closed connection raises ProgrammingError
    with pytest.raises(sqlite3.ProgrammingError):
        connections[0].execute("SELECT 1")


# ---------------------------------------------------------------------------
# Test 3: Parallel node_id merge conflict
# ---------------------------------------------------------------------------


def test_parallel_merge_detects_node_id_conflict():
    """When two parallel branches produce records with the same non-branch
    node_id, the second one should be prefixed with its branch_id."""
    run_state = RunState(
        run_id="r1",
        workflow_id="w1",
        workflow_version=1,
        status="running",
        state={"nodes": {}},
    )
    branches = [{"id": "branch_a"}, {"id": "branch_b"}]
    results_by_branch = {
        "branch_a": {
            "branch_id": "branch_a",
            "records": {
                "branch_a": {"status": "success", "output": "a"},
                "shared_node": {"status": "success", "output": "from_a"},
            },
            "error": None,
            "error_type": None,
        },
        "branch_b": {
            "branch_id": "branch_b",
            "records": {
                "branch_b": {"status": "success", "output": "b"},
                "shared_node": {"status": "success", "output": "from_b"},
            },
            "error": None,
            "error_type": None,
        },
    }
    merger = _ParallelResultMerger(branches, results_by_branch, run_state)
    output, failed = merger.merge()

    nodes = run_state.state["nodes"]
    # branch records should exist
    assert "branch_a" in nodes
    assert "branch_b" in nodes
    # shared_node from branch_a keeps the original name
    assert "shared_node" in nodes
    assert nodes["shared_node"]["output"] == "from_a"
    # shared_node from branch_b gets prefixed
    assert "branch_b.shared_node" in nodes
    assert nodes["branch_b.shared_node"]["output"] == "from_b"
    assert failed == []


# ---------------------------------------------------------------------------
# Test 4: Parallel branch failure does NOT emit node.finished
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_parallel_branch_failure_no_finished_event():
    """When a parallel branch raises an exception, a node.failed event should
    be emitted but node.finished must NOT be emitted for that branch."""
    from agent_orchestrator import (
        AgentRegistry,
        StartRunRequest,
        ToolRegistry,
        WorkflowConfig,
        WorkflowEngine,
    )

    agents = AgentRegistry()
    tools = ToolRegistry()

    async def ok_tool(args, run_state):
        return {"ok": True}

    async def fail_tool(args, run_state):
        raise RuntimeError("branch-boom")

    tools.register("ok", ok_tool)
    tools.register("fail", fail_tool)

    workflow = WorkflowConfig.from_dict(
        {
            "id": "parallel-no-finished-on-fail",
            "version": 1,
            "nodes": [
                {
                    "id": "fanout",
                    "type": "parallel",
                    "failure_policy": "continue",
                    "branches": [
                        {"id": "good", "type": "tool", "tool": "ok"},
                        {"id": "bad", "type": "tool", "tool": "fail"},
                    ],
                }
            ],
        }
    )
    engine = WorkflowEngine(workflow, agents=agents, tools=tools)

    events = [event async for event in engine.start(StartRunRequest(message="go"))]

    # The failed branch should have a node.failed event
    bad_failed = [
        e for e in events if e.type == "node.failed" and e.node_id == "bad"
    ]
    assert len(bad_failed) == 1

    # The failed branch must NOT have a node.finished event
    bad_finished = [
        e for e in events if e.type == "node.finished" and e.node_id == "bad"
    ]
    assert bad_finished == [], (
        "node.finished should not be emitted for a failed branch"
    )

    # The good branch should have node.finished
    good_finished = [
        e for e in events if e.type == "node.finished" and e.node_id == "good"
    ]
    assert len(good_finished) == 1

    # The overall run should finish successfully (failure_policy=continue)
    assert events[-1].type == "run.finished"


# ---------------------------------------------------------------------------
# Test 5: FileArtifactStore path traversal prevention
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_file_artifact_store_rejects_path_traversal(tmp_path):
    """FileArtifactStore.get must reject URIs that resolve outside the store root."""
    from agent_orchestrator.artifacts import FileArtifactStore
    from agent_orchestrator.exceptions import WorkflowError

    store = FileArtifactStore(tmp_path / "artifacts")
    outside = tmp_path / "secret.json"
    outside.write_text('{"leaked": true}', encoding="utf-8")

    crafted_ref = {"uri": str(outside)}
    with pytest.raises(WorkflowError, match="artifact path outside root"):
        await store.get(crafted_ref)


# ---------------------------------------------------------------------------
# Test 6: FileEventStore migration_registry on read
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_file_event_store_applies_migration_on_read(tmp_path):
    """FileEventStore.list_by_run must pass migration_registry to
    workflow_event_from_dict so stored events are migrated on read."""
    from agent_orchestrator.events import FileEventStore
    from agent_orchestrator.models import WorkflowEvent

    class MockMigrationRegistry:
        def __init__(self):
            self.called = False

        def migrate(self, payload):
            self.called = True
            payload["data"]["migrated"] = True
            return payload

    registry = MockMigrationRegistry()
    store = FileEventStore(tmp_path / "events", migration_registry=registry)

    event = WorkflowEvent(type="node.started", run_id="r1", node_id="n1", data={})
    await store.append(event)

    events = await store.list_by_run("r1")
    assert registry.called, "migration_registry.migrate was never called"
    assert events[0].data.get("migrated") is True


# ---------------------------------------------------------------------------
# Test 7: InMemoryEventStore accepts migration_registry
# ---------------------------------------------------------------------------


def test_in_memory_event_store_accepts_migration_registry():
    """InMemoryEventStore.__init__ must accept migration_registry kwarg."""
    from agent_orchestrator.events import InMemoryEventStore

    registry = object()
    store = InMemoryEventStore(migration_registry=registry)
    assert store.migration_registry is registry


# ---------------------------------------------------------------------------
# Test 8: Scheduler on_error edge does NOT emit node.finished
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scheduler_error_edge_no_finished_event():
    """When a DAG node fails and has an on_error edge, the scheduler must emit
    node.failed but NOT node.finished for that node."""
    from agent_orchestrator import (
        AgentRegistry,
        StartRunRequest,
        ToolRegistry,
        WorkflowConfig,
        WorkflowEngine,
    )

    agents = AgentRegistry()
    tools = ToolRegistry()

    async def boom_tool(args, run_state):
        raise RuntimeError("kaboom")

    async def recover_tool(args, run_state):
        return {"recovered": True}

    tools.register("boom", boom_tool)
    tools.register("recover", recover_tool)

    workflow = WorkflowConfig.from_dict(
        {
            "id": "dag-error-edge",
            "version": 1,
            "nodes": [
                {"id": "will_fail", "type": "tool", "tool": "boom"},
                {"id": "recovery", "type": "tool", "tool": "recover"},
            ],
            "edges": [
                {"from": "will_fail", "to": "recovery", "on_error": True},
            ],
        }
    )
    engine = WorkflowEngine(workflow, agents=agents, tools=tools)
    events = [e async for e in engine.start(StartRunRequest(message="go"))]

    failed_events = [
        e for e in events if e.type == "node.failed" and e.node_id == "will_fail"
    ]
    assert len(failed_events) == 1

    finished_events = [
        e for e in events if e.type == "node.finished" and e.node_id == "will_fail"
    ]
    assert finished_events == [], (
        "node.finished must not be emitted for a node that failed with an on_error edge"
    )

    recovery_finished = [
        e for e in events if e.type == "node.finished" and e.node_id == "recovery"
    ]
    assert len(recovery_finished) == 1

    assert events[-1].type == "run.finished"
