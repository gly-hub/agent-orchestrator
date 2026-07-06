"""Optional built-in persistence stores."""

from agent_orchestrator.stores.plugins import register_redis_stores, register_sqlite_stores
from agent_orchestrator.stores.redis import RedisCheckpointStore, RedisEventStore
from agent_orchestrator.stores.sqlite import SQLiteCheckpointStore, SQLiteEventStore

__all__ = [
    "RedisCheckpointStore",
    "RedisEventStore",
    "SQLiteCheckpointStore",
    "SQLiteEventStore",
    "register_redis_stores",
    "register_sqlite_stores",
]
