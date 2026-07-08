import json
import sqlite3
import tempfile
from pathlib import Path

from agent_orchestrator import (
    AgentRegistry,
    FileEventStore,
    InMemoryEventStore,
    SQLiteEventStore,
    StartRunRequest,
    ToolRegistry,
    WorkflowConfig,
    WorkflowEngine,
    WorkflowEvent,
    compact_events,
    compact_run,
    replay_events,
    workflow_event_from_dict,
    workflow_event_to_dict,
)


class FailingEventStore:
    async def append(self, event):
        raise RuntimeError("event store unavailable")

    async def list_by_run(self, run_id):
        return []


async def test_event_store_records_workflow_events():
    agents = AgentRegistry()
    tools = ToolRegistry()
    events_store = InMemoryEventStore()

    async def ok_tool(args, run_state):
        return {"ok": True}

    tools.register("ok", ok_tool)
    workflow = WorkflowConfig.from_dict(
        {
            "id": "event-store",
            "version": 1,
            "nodes": [{"id": "ok", "type": "tool", "tool": "ok"}],
        }
    )
    engine = WorkflowEngine(workflow, agents=agents, tools=tools, event_store=events_store)

    events = [event async for event in engine.start(StartRunRequest(message="hello"))]
    stored = await events_store.list_by_run(events[0].run_id)

    assert [event.type for event in stored] == [event.type for event in events]
    assert stored[-1].type == "run.finished"

async def test_event_store_append_failure_returns_unpersisted_failed_event():
    agents = AgentRegistry()
    tools = ToolRegistry()

    workflow = WorkflowConfig.from_dict(
        {
            "id": "event-store-failure",
            "version": 1,
            "nodes": [{"id": "ok", "type": "transform"}],
        }
    )
    engine = WorkflowEngine(
        workflow,
        agents=agents,
        tools=tools,
        event_store=FailingEventStore(),
    )

    events = [event async for event in engine.start(StartRunRequest(message="hello"))]

    assert [event.type for event in events] == ["run.failed"]
    assert events[0].data["error_type"] == "RuntimeError"
    assert "event store unavailable" in events[0].data["error"]

async def test_workflow_event_serialization_is_versioned_and_backward_compatible():
    event = WorkflowEvent(type="node.started", run_id="run_ser", node_id="node_a")
    serialized = workflow_event_to_dict(event)
    legacy = {
        "type": "node.finished",
        "run_id": "run_legacy",
        "node_id": "node_b",
        "data": {"status": "success"},
    }

    assert serialized["schema_version"] == 1
    assert workflow_event_from_dict(serialized) == event
    assert workflow_event_from_dict(legacy).schema_version == 1

async def test_file_event_store_replays_events_by_run():
    event = WorkflowEvent(
        type="run.started",
        run_id="run_file",
        node_id=None,
        data={"status": "running"},
    )

    with tempfile.TemporaryDirectory() as tmp:
        store = FileEventStore(tmp)
        await store.append(event)
        replayed = await store.list_by_run("run_file")

    assert len(replayed) == 1
    assert replayed[0].type == "run.started"
    assert replayed[0].data["status"] == "running"
    assert replayed[0].schema_version == 1

async def test_file_event_store_reads_legacy_events_without_schema_version():
    legacy_event = {
        "type": "run.started",
        "run_id": "run_file_legacy",
        "node_id": None,
        "data": {"status": "running"},
    }

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "run_file_legacy.jsonl"
        path.write_text(json.dumps(legacy_event), encoding="utf-8")
        store = FileEventStore(tmp)
        replayed = await store.list_by_run("run_file_legacy")

    assert len(replayed) == 1
    assert replayed[0].schema_version == 1
    assert replayed[0].type == "run.started"

async def test_sqlite_event_store_replays_events_by_run():
    event = WorkflowEvent(
        type="run.started",
        run_id="run_sqlite",
        node_id=None,
        data={"status": "running"},
    )

    with tempfile.TemporaryDirectory() as tmp:
        store = SQLiteEventStore(Path(tmp) / "workflow.sqlite")
        await store.append(event)
        replayed = await store.list_by_run("run_sqlite")

    assert len(replayed) == 1
    assert replayed[0].type == "run.started"
    assert replayed[0].data["status"] == "running"
    assert replayed[0].schema_version == 1

async def test_sqlite_event_store_reads_legacy_events_without_schema_version():
    legacy_event = {
        "type": "run.started",
        "run_id": "run_sqlite_legacy",
        "node_id": None,
        "data": {"status": "running"},
    }

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "workflow.sqlite"
        store = SQLiteEventStore(db_path)
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                INSERT INTO events(run_id, event_type, node_id, payload, created_at_ms)
                VALUES(?, ?, ?, ?, ?)
                """,
                (
                    "run_sqlite_legacy",
                    "run.started",
                    None,
                    json.dumps(legacy_event, separators=(",", ":")),
                    123,
                ),
            )
        replayed = await store.list_by_run("run_sqlite_legacy")

    assert len(replayed) == 1
    assert replayed[0].schema_version == 1
    assert replayed[0].type == "run.started"

async def test_replay_events_reconstructs_run_view():
    agents = AgentRegistry()
    tools = ToolRegistry()
    event_store = InMemoryEventStore()

    async def ok_tool(args, run_state):
        return {"ok": True}

    tools.register("ok", ok_tool)
    workflow = WorkflowConfig.from_dict(
        {
            "id": "replay-demo",
            "version": 1,
            "nodes": [{"id": "ok", "type": "tool", "tool": "ok"}],
        }
    )
    engine = WorkflowEngine(workflow, agents=agents, tools=tools, event_store=event_store)

    events = [event async for event in engine.start(StartRunRequest(message="hello"))]
    replay = replay_events(await event_store.list_by_run(events[0].run_id))

    assert replay.run_id == events[0].run_id
    assert replay.status == "completed"
    assert replay.nodes["ok"]["output"] == {"ok": True}
    assert replay.message_events[-1]["event"] == "FINISH"

async def test_compact_events_preserves_replay_view_with_retained_tail():
    agents = AgentRegistry()
    tools = ToolRegistry()

    async def ok_tool(args, run_state):
        return {"ok": True}

    tools.register("ok", ok_tool)
    workflow = WorkflowConfig.from_dict(
        {
            "id": "compact-events",
            "version": 1,
            "nodes": [{"id": "ok", "type": "tool", "tool": "ok"}],
        }
    )
    engine = WorkflowEngine(workflow, agents=agents, tools=tools)

    events = [event async for event in engine.start(StartRunRequest(message="hello"))]
    result = compact_events(events, retain_last=1, compacted_at_ms=123)
    replay = replay_events(result.events)

    assert result.original_event_count == len(events)
    assert result.compacted_event_count == len(events) - 1
    assert result.events[0].type == "run.compacted"
    assert result.events[0].data["compacted_at_ms"] == 123
    assert len(result.events) == 2
    assert replay.status == "completed"
    assert replay.nodes["ok"]["output"] == {"ok": True}
    assert replay.message_events[0]["event"] == "RUN_COMPACTED"

async def test_compact_run_replaces_in_memory_event_log():
    agents = AgentRegistry()
    tools = ToolRegistry()
    event_store = InMemoryEventStore()

    async def ok_tool(args, run_state):
        return {"ok": True}

    tools.register("ok", ok_tool)
    workflow = WorkflowConfig.from_dict(
        {
            "id": "compact-memory",
            "version": 1,
            "nodes": [{"id": "ok", "type": "tool", "tool": "ok"}],
        }
    )
    engine = WorkflowEngine(workflow, agents=agents, tools=tools, event_store=event_store)

    events = [event async for event in engine.start(StartRunRequest(message="hello"))]
    result = await compact_run(event_store, events[0].run_id, retain_last=1)
    stored = await event_store.list_by_run(events[0].run_id)
    replay = replay_events(stored)

    assert len(stored) < len(events)
    assert stored == result.events
    assert stored[0].type == "run.compacted"
    assert replay.status == "completed"
    assert replay.nodes["ok"]["output"] == {"ok": True}

async def test_file_event_store_replace_run_supports_compaction():
    first = WorkflowEvent(type="run.started", run_id="run_file_replace", data={"status": "running"})
    compacted = WorkflowEvent(
        type="run.compacted",
        run_id="run_file_replace",
        data={
            "status": "running",
            "snapshot": {
                "status": "running",
                "nodes": {},
                "messages": {},
                "waiting_action_id": None,
                "error": None,
            },
        },
    )

    with tempfile.TemporaryDirectory() as tmp:
        store = FileEventStore(tmp)
        await store.append(first)
        await store.replace_run("run_file_replace", [compacted])
        replayed = await store.list_by_run("run_file_replace")

    assert replayed == [compacted]

async def test_sqlite_event_store_replace_run_supports_compaction():
    first = WorkflowEvent(type="run.started", run_id="run_sqlite_replace", data={"status": "running"})
    compacted = WorkflowEvent(
        type="run.compacted",
        run_id="run_sqlite_replace",
        data={
            "status": "running",
            "snapshot": {
                "status": "running",
                "nodes": {},
                "messages": {},
                "waiting_action_id": None,
                "error": None,
            },
        },
    )

    with tempfile.TemporaryDirectory() as tmp:
        store = SQLiteEventStore(Path(tmp) / "workflow.sqlite")
        await store.append(first)
        await store.replace_run("run_sqlite_replace", [compacted])
        replayed = await store.list_by_run("run_sqlite_replace")

    assert replayed == [compacted]
