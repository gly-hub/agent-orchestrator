import os

import pytest

from agent_orchestrator import (
    PendingAction,
    RedisCheckpointStore,
    RedisEventStore,
    RunState,
    WorkflowEvent,
)

pytestmark = pytest.mark.skipif(
    not os.environ.get("REDIS_URL"),
    reason="REDIS_URL is not set",
)


@pytest.fixture
def redis_prefix():
    return f"agent-orchestrator-test:{os.getpid()}"


@pytest.fixture
def redis_url():
    return os.environ.get("REDIS_URL", "")


async def test_redis_checkpoint_store_round_trips_resume_state(redis_url, redis_prefix):
    store = RedisCheckpointStore(url=redis_url, prefix=redis_prefix)
    run_state = RunState(
        run_id="run_redis_1",
        workflow_id="wf",
        workflow_version=1,
        status="waiting_for_user",
        current_node_id="confirm",
        waiting_action_id="pa_redis_1",
        state={"nodes": {"confirm": {"status": "waiting"}}},
    )
    action = PendingAction(
        id="pa_redis_1",
        run_id="run_redis_1",
        node_id="confirm",
        action_type="human",
        request={"response_schema": {"type": "object", "required": ["decision"]}},
    )

    await store.save_waiting(run_state, action)
    resumed = await store.resolve_action("pa_redis_1", {"decision": "approve"})

    assert resumed.status == "running"
    assert resumed.state["nodes"]["confirm"]["output"] == {"decision": "approve"}
    assert (await store.load_action("pa_redis_1")).status == "approved"


async def test_redis_event_store_round_trips_and_replaces_events(redis_url, redis_prefix):
    store = RedisEventStore(url=redis_url, prefix=redis_prefix)
    first = WorkflowEvent(type="run.started", run_id="run_redis_2")
    second = WorkflowEvent(type="run.finished", run_id="run_redis_2")

    await store.append(first)
    assert [event.type for event in await store.list_by_run("run_redis_2")] == ["run.started"]

    await store.replace_run("run_redis_2", [second])
    assert [event.type for event in await store.list_by_run("run_redis_2")] == ["run.finished"]
