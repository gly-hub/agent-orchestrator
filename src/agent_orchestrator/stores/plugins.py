"""Persistence provider registration helpers for optional stores."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agent_orchestrator.exceptions import WorkflowError
from agent_orchestrator.stores.redis import RedisCheckpointStore, RedisEventStore
from agent_orchestrator.stores.sqlite import (
    SQLiteArtifactStore,
    SQLiteCheckpointStore,
    SQLiteEventStore,
)

if TYPE_CHECKING:
    from agent_orchestrator.persistence import PersistencePluginRegistry


def register_sqlite_stores(registry: PersistencePluginRegistry) -> PersistencePluginRegistry:
    """Register standard-library SQLite checkpoint and event stores."""

    registry.checkpoints.register("sqlite", lambda config: SQLiteCheckpointStore(_require_path(config)))
    registry.events.register("sqlite", lambda config: SQLiteEventStore(_require_path(config), migration_registry=config.get("migration_registry")))
    registry.artifacts.register("sqlite", lambda config: SQLiteArtifactStore(_require_path(config)))
    return registry


def register_redis_stores(registry: PersistencePluginRegistry) -> PersistencePluginRegistry:
    """Register optional Redis checkpoint and event stores."""

    registry.checkpoints.register(
        "redis",
        lambda config: RedisCheckpointStore(
            url=config.get("url"),
            client=config.get("client"),
            prefix=str(config.get("prefix", "agent-orchestrator")),
            action_ttl_seconds=config.get("action_ttl_seconds"),
            run_ttl_seconds=config.get("run_ttl_seconds"),
        ),
    )
    registry.events.register(
        "redis",
        lambda config: RedisEventStore(
            url=config.get("url"),
            client=config.get("client"),
            prefix=str(config.get("prefix", "agent-orchestrator")),
            max_events_per_run=config.get("max_events_per_run"),
            migration_registry=config.get("migration_registry"),
        ),
    )
    return registry


def _require_path(config: dict[str, Any]) -> str:
    path = config.get("path") or config.get("root")
    if not path:
        raise WorkflowError("sqlite store requires path")
    return str(path)
