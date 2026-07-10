"""Pluggable persistence registry for workflow services."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Generic, TypeVar

from agent_orchestrator.artifacts import ArtifactStore, FileArtifactStore, InMemoryArtifactStore
from agent_orchestrator.checkpoint import (
    CheckpointStore,
    FileCheckpointStore,
    InMemoryCheckpointStore,
)
from agent_orchestrator.events import EventStore, FileEventStore, InMemoryEventStore, NoopEventStore
from agent_orchestrator.exceptions import WorkflowError
from agent_orchestrator.stores import register_redis_stores, register_sqlite_stores

StoreT = TypeVar("StoreT")
StoreFactory = Callable[[dict[str, Any]], StoreT]


class StoreRegistry(Generic[StoreT]):
    """Registry that maps a persistence provider name to a store factory."""

    def __init__(self) -> None:
        self._factories: dict[str, StoreFactory[StoreT]] = {}

    def register(self, name: str, factory: StoreFactory[StoreT]) -> None:
        if not name:
            raise WorkflowError("store provider name is required")
        self._factories[name] = factory

    def create(self, config: dict[str, Any] | str | None) -> StoreT:
        if isinstance(config, str):
            config = {"provider": config}
        config = dict(config or {})
        provider = config.pop("provider", "memory")
        try:
            factory = self._factories[provider]
        except KeyError as exc:
            raise WorkflowError(f"store provider not registered: {provider}") from exc
        return factory(config)


class PersistencePluginRegistry:
    """Registry for checkpoint and event-store persistence plugins."""

    def __init__(self) -> None:
        self.checkpoints: StoreRegistry[CheckpointStore] = StoreRegistry()
        self.events: StoreRegistry[EventStore] = StoreRegistry()
        self.artifacts: StoreRegistry[ArtifactStore] = StoreRegistry()


def core_persistence_plugins() -> PersistencePluginRegistry:
    """Create a registry with only dependency-free core memory/file providers."""

    registry = PersistencePluginRegistry()
    registry.checkpoints.register("memory", lambda config: InMemoryCheckpointStore())
    registry.checkpoints.register("file", lambda config: FileCheckpointStore(_require_root(config)))
    registry.events.register("noop", lambda config: NoopEventStore())
    registry.events.register("memory", lambda config: InMemoryEventStore(migration_registry=config.get("migration_registry")))
    registry.events.register("file", lambda config: FileEventStore(_require_root(config), migration_registry=config.get("migration_registry")))
    registry.artifacts.register("memory", lambda config: InMemoryArtifactStore())
    registry.artifacts.register("file", lambda config: FileArtifactStore(_require_root(config)))
    return registry


def default_persistence_plugins() -> PersistencePluginRegistry:
    """Create the default registry, including built-in optional providers."""

    return register_redis_stores(register_sqlite_stores(core_persistence_plugins()))


DEFAULT_PERSISTENCE_PLUGINS = default_persistence_plugins()


def create_checkpoint_store(
    config: dict[str, Any] | str | None,
    *,
    registry: PersistencePluginRegistry | None = None,
) -> CheckpointStore:
    return (registry or DEFAULT_PERSISTENCE_PLUGINS).checkpoints.create(config)


def create_event_store(
    config: dict[str, Any] | str | None,
    *,
    registry: PersistencePluginRegistry | None = None,
) -> EventStore:
    return (registry or DEFAULT_PERSISTENCE_PLUGINS).events.create(config)


def create_artifact_store(
    config: dict[str, Any] | str | None,
    *,
    registry: PersistencePluginRegistry | None = None,
) -> ArtifactStore:
    return (registry or DEFAULT_PERSISTENCE_PLUGINS).artifacts.create(config)


def _require_root(config: dict[str, Any]) -> str:
    root = config.get("root") or config.get("path")
    if not root:
        raise WorkflowError("file store requires root")
    return str(root)
