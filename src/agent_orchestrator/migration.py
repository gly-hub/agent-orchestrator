"""Event schema version migration registry."""

from __future__ import annotations

import logging
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Protocol

logger = logging.getLogger(__name__)

CURRENT_SCHEMA_VERSION = 2


class EventMigration(Protocol):
    """A single version-to-version event migration."""

    from_version: int
    to_version: int

    def migrate(self, event_data: dict[str, Any]) -> dict[str, Any]: ...


@dataclass(slots=True)
class BaseEventMigration:
    from_version: int
    to_version: int

    def migrate(self, event_data: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError


class EventMigrationRegistry:
    """Registry of event migrations that can upgrade events to the current schema version."""

    def __init__(self) -> None:
        self._migrations: dict[int, EventMigration] = {}

    def register(self, migration: EventMigration) -> None:
        if migration.from_version in self._migrations:
            raise ValueError(f"migration from version {migration.from_version} already registered")
        self._migrations[migration.from_version] = migration

    def migrate(self, event_data: dict[str, Any], target_version: int | None = None) -> dict[str, Any]:
        target = target_version if target_version is not None else CURRENT_SCHEMA_VERSION
        current_version = event_data.get("schema_version", 1)
        if current_version >= target:
            return event_data
        result = deepcopy(event_data)
        while current_version < target:
            migration = self._migrations.get(current_version)
            if migration is None:
                logger.debug("no migration from version %d, stopping at current", current_version)
                break
            result = migration.migrate(result)
            result["schema_version"] = migration.to_version
            logger.debug("migrated event from v%d to v%d", current_version, migration.to_version)
            current_version = migration.to_version
        return result


class V1ToV2Migration(BaseEventMigration):
    """Migrate events from schema version 1 to version 2.

    v2 normalizes the event data structure by ensuring all events have
    explicit ``status`` and ``messages`` fields in their data payload.
    """

    from_version: int = 1
    to_version: int = 2

    def migrate(self, event_data: dict[str, Any]) -> dict[str, Any]:
        data = dict(event_data)
        payload = dict(data.get("data", {}))
        payload.setdefault("status", None)
        payload.setdefault("messages", {})
        data["data"] = payload
        data["schema_version"] = 2
        return data


def default_migration_registry() -> EventMigrationRegistry:
    """Create a registry pre-loaded with built-in migrations."""

    registry = EventMigrationRegistry()
    registry.register(V1ToV2Migration(from_version=1, to_version=2))
    return registry
