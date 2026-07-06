import os
import sys
import unittest
from pathlib import Path

PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from agent_orchestrator import (
    PendingAction,
    RedisCheckpointStore,
    RedisEventStore,
    RunState,
    WorkflowEvent,
)


@unittest.skipUnless(os.environ.get("REDIS_URL"), "REDIS_URL is not set")
class RedisIntegrationTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.redis_url = os.environ["REDIS_URL"]
        self.prefix = f"agent-orchestrator-test:{os.getpid()}:{self.id()}"

    async def test_redis_checkpoint_store_round_trips_resume_state(self):
        store = RedisCheckpointStore(url=self.redis_url, prefix=self.prefix)
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

        self.assertEqual(resumed.status, "running")
        self.assertEqual(resumed.state["nodes"]["confirm"]["output"], {"decision": "approve"})
        self.assertEqual((await store.load_action("pa_redis_1")).status, "approved")

    async def test_redis_event_store_round_trips_and_replaces_events(self):
        store = RedisEventStore(url=self.redis_url, prefix=self.prefix)
        first = WorkflowEvent(type="run.started", run_id="run_redis_2")
        second = WorkflowEvent(type="run.finished", run_id="run_redis_2")

        await store.append(first)
        self.assertEqual([event.type for event in await store.list_by_run("run_redis_2")], ["run.started"])

        await store.replace_run("run_redis_2", [second])
        self.assertEqual([event.type for event in await store.list_by_run("run_redis_2")], ["run.finished"])
