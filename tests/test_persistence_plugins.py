import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from agent_orchestrator import (
    InMemoryCheckpointStore,
    PendingAction,
    PersistencePluginRegistry,
    RedisCheckpointStore,
    RedisEventStore,
    RunState,
    SQLiteCheckpointStore,
    SQLiteEventStore,
    WorkflowEvent,
    core_persistence_plugins,
    create_checkpoint_store,
    create_event_store,
    register_redis_stores,
    register_sqlite_stores,
)
from agent_orchestrator.exceptions import WorkflowError


class PersistencePluginsTest(unittest.IsolatedAsyncioTestCase):
    async def test_custom_checkpoint_store_provider_can_be_registered(self):
        registry = PersistencePluginRegistry()
        created = []

        def create_custom(config):
            created.append(config["dsn"])
            return InMemoryCheckpointStore()

        registry.checkpoints.register("custom-db", create_custom)
        store = create_checkpoint_store(
            {"provider": "custom-db", "dsn": "db://workflow"},
            registry=registry,
        )

        self.assertIsInstance(store, InMemoryCheckpointStore)
        self.assertEqual(created, ["db://workflow"])

    async def test_sqlite_store_providers_are_registered(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "workflow.sqlite"
            checkpoint_store = create_checkpoint_store(
                {"provider": "sqlite", "path": str(path)}
            )
            event_store = create_event_store(
                {"provider": "sqlite", "path": str(path)}
            )

        self.assertIsInstance(checkpoint_store, SQLiteCheckpointStore)
        self.assertIsInstance(event_store, SQLiteEventStore)

    async def test_core_persistence_plugins_exclude_optional_sqlite_provider(self):
        registry = core_persistence_plugins()

        with self.assertRaisesRegex(WorkflowError, "store provider not registered: sqlite"):
            create_checkpoint_store({"provider": "sqlite", "path": "/tmp/workflow.sqlite"}, registry=registry)

    async def test_sqlite_provider_can_be_registered_as_optional_store_plugin(self):
        registry = register_sqlite_stores(core_persistence_plugins())

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "workflow.sqlite"
            checkpoint_store = create_checkpoint_store(
                {"provider": "sqlite", "path": str(path)},
                registry=registry,
            )
            event_store = create_event_store(
                {"provider": "sqlite", "path": str(path)},
                registry=registry,
            )

        self.assertIsInstance(checkpoint_store, SQLiteCheckpointStore)
        self.assertIsInstance(event_store, SQLiteEventStore)

    async def test_redis_checkpoint_store_resolves_action_with_fake_client(self):
        client = FakeRedis()
        store = RedisCheckpointStore(client=client, prefix="test")
        run_state = RunState(
            run_id="run_1",
            workflow_id="wf",
            workflow_version=1,
            status="waiting_for_user",
            waiting_action_id="pa_1",
            current_node_id="confirm",
            state={"nodes": {"confirm": {"status": "waiting"}}},
        )
        action = PendingAction(
            id="pa_1",
            run_id="run_1",
            node_id="confirm",
            action_type="human",
            request={"response_schema": {"type": "object", "required": ["decision"]}},
            expires_at_ms=None,
        )

        await store.save_waiting(run_state, action)
        resumed = await store.resolve_action("pa_1", {"decision": "approve"})

        self.assertEqual(resumed.status, "running")
        self.assertEqual(
            resumed.state["nodes"]["confirm"]["output"],
            {"decision": "approve"},
        )
        self.assertEqual((await store.load_action("pa_1")).status, "approved")

    async def test_redis_event_store_lists_and_replaces_events_with_fake_client(self):
        client = FakeRedis()
        store = RedisEventStore(client=client, prefix="test")
        first = WorkflowEvent(type="run.started", run_id="run_1")
        second = WorkflowEvent(type="run.finished", run_id="run_1")

        await store.append(first)
        self.assertEqual([event.type for event in await store.list_by_run("run_1")], ["run.started"])

        await store.replace_run("run_1", [second])
        self.assertEqual([event.type for event in await store.list_by_run("run_1")], ["run.finished"])

    async def test_redis_store_providers_can_be_registered(self):
        registry = register_redis_stores(PersistencePluginRegistry())
        client = FakeRedis()

        checkpoint_store = registry.checkpoints.create(
            {"provider": "redis", "prefix": "test", "client": client}
        )
        event_store = registry.events.create(
            {"provider": "redis", "prefix": "test", "client": client}
        )

        self.assertIsInstance(checkpoint_store, RedisCheckpointStore)
        self.assertIsInstance(event_store, RedisEventStore)


class FakePipeline:
    def __init__(self, client):
        self.client = client
        self.commands = []

    def __getattr__(self, name):
        def queue(*args, **kwargs):
            self.commands.append((name, args, kwargs))
            return self

        return queue

    async def execute(self):
        results = []
        for name, args, kwargs in self.commands:
            results.append(await getattr(self.client, name)(*args, **kwargs))
        return results


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.sorted_sets = {}
        self.lists = {}

    def pipeline(self, transaction=True):
        return FakePipeline(self)

    async def set(self, key, value, nx=False):
        if nx and key in self.values:
            return False
        self.values[key] = str(value)
        return True

    async def get(self, key):
        return self.values.get(key)

    async def expire(self, key, seconds):
        return True

    async def zadd(self, key, mapping):
        zset = self.sorted_sets.setdefault(key, {})
        zset.update(mapping)
        return len(mapping)

    async def zrem(self, key, member):
        return int(self.sorted_sets.get(key, {}).pop(member, None) is not None)

    async def zrangebyscore(self, key, min, max):
        return [
            member
            for member, score in sorted(self.sorted_sets.get(key, {}).items(), key=lambda item: item[1])
            if float(min) <= score <= float(max)
        ]

    async def rpush(self, key, *values):
        self.lists.setdefault(key, []).extend(str(value) for value in values)
        return len(self.lists[key])

    async def lrange(self, key, start, end):
        values = self.lists.get(key, [])
        if end == -1:
            end = len(values) - 1
        return values[start:end + 1]

    async def ltrim(self, key, start, end):
        values = self.lists.get(key, [])
        if end == -1:
            end = len(values) - 1
        self.lists[key] = values[start:end + 1]
        return True

    async def delete(self, key):
        existed = key in self.values or key in self.lists or key in self.sorted_sets
        self.values.pop(key, None)
        self.lists.pop(key, None)
        self.sorted_sets.pop(key, None)
        return int(existed)
