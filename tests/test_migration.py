"""Tests for migration.py — event schema version migration registry."""

from __future__ import annotations

from typing import Any

import pytest

from agent_orchestrator.migration import (
    CURRENT_SCHEMA_VERSION,
    BaseEventMigration,
    EventMigrationRegistry,
    V1ToV2Migration,
    default_migration_registry,
)


class TestEventMigrationRegistry:
    def test_register_and_migrate(self) -> None:
        registry = EventMigrationRegistry()
        migration = V1ToV2Migration(from_version=1, to_version=2)
        registry.register(migration)
        event = {"type": "node.started", "data": {}}
        result = registry.migrate(event)
        assert result["schema_version"] == 2

    def test_duplicate_registration_raises(self) -> None:
        registry = EventMigrationRegistry()
        registry.register(V1ToV2Migration(from_version=1, to_version=2))
        with pytest.raises(ValueError, match="already registered"):
            registry.register(V1ToV2Migration(from_version=1, to_version=2))

    def test_no_op_when_already_at_target(self) -> None:
        registry = EventMigrationRegistry()
        event = {"type": "test", "schema_version": 5}
        result = registry.migrate(event, target_version=5)
        assert result is event

    def test_no_op_when_above_target(self) -> None:
        registry = EventMigrationRegistry()
        event = {"type": "test", "schema_version": 3}
        result = registry.migrate(event, target_version=2)
        assert result is event

    def test_stops_when_no_migration_available(self) -> None:
        registry = EventMigrationRegistry()
        registry.register(V1ToV2Migration(from_version=1, to_version=2))
        event = {"type": "test", "data": {}}
        result = registry.migrate(event, target_version=5)
        assert result["schema_version"] == 2

    def test_chain_migration(self) -> None:
        class V2ToV3(BaseEventMigration):
            from_version: int = 2
            to_version: int = 3

            def migrate(self, event_data: dict[str, Any]) -> dict[str, Any]:
                data = dict(event_data)
                payload = dict(data.get("data", {}))
                payload["v3_field"] = True
                data["data"] = payload
                return data

        registry = EventMigrationRegistry()
        registry.register(V1ToV2Migration(from_version=1, to_version=2))
        registry.register(V2ToV3(from_version=2, to_version=3))

        event = {"type": "test", "data": {"existing": "value"}}
        result = registry.migrate(event, target_version=3)
        assert result["schema_version"] == 3
        assert result["data"]["status"] is None
        assert result["data"]["messages"] == {}
        assert result["data"]["v3_field"] is True
        assert result["data"]["existing"] == "value"

    def test_migrate_does_not_mutate_original(self) -> None:
        registry = EventMigrationRegistry()
        registry.register(V1ToV2Migration(from_version=1, to_version=2))
        event = {"type": "test", "data": {"x": 1}}
        registry.migrate(event)
        assert "schema_version" not in event

    def test_default_version_is_1(self) -> None:
        registry = EventMigrationRegistry()
        registry.register(V1ToV2Migration(from_version=1, to_version=2))
        event = {"type": "test"}
        result = registry.migrate(event)
        assert result["schema_version"] == 2

    def test_explicit_target_version(self) -> None:
        registry = EventMigrationRegistry()
        registry.register(V1ToV2Migration(from_version=1, to_version=2))
        event = {"type": "test", "data": {}}
        result = registry.migrate(event, target_version=2)
        assert result["schema_version"] == 2


class TestV1ToV2Migration:
    def test_adds_status_and_messages(self) -> None:
        migration = V1ToV2Migration(from_version=1, to_version=2)
        event = {"type": "node.started", "data": {"output": "hello"}}
        result = migration.migrate(event)
        assert result["data"]["status"] is None
        assert result["data"]["messages"] == {}
        assert result["data"]["output"] == "hello"
        assert result["schema_version"] == 2

    def test_preserves_existing_status(self) -> None:
        migration = V1ToV2Migration(from_version=1, to_version=2)
        event = {"type": "test", "data": {"status": "running"}}
        result = migration.migrate(event)
        assert result["data"]["status"] == "running"

    def test_empty_data(self) -> None:
        migration = V1ToV2Migration(from_version=1, to_version=2)
        event = {"type": "test"}
        result = migration.migrate(event)
        assert result["data"]["status"] is None
        assert result["data"]["messages"] == {}

    def test_does_not_mutate_input(self) -> None:
        migration = V1ToV2Migration(from_version=1, to_version=2)
        original_data = {"output": "hi"}
        event = {"type": "test", "data": original_data}
        migration.migrate(event)
        assert "status" not in original_data


class TestDefaultMigrationRegistry:
    def test_returns_registry(self) -> None:
        registry = default_migration_registry()
        assert isinstance(registry, EventMigrationRegistry)

    def test_can_migrate_v1(self) -> None:
        registry = default_migration_registry()
        event = {"type": "test", "data": {}}
        result = registry.migrate(event)
        assert result["schema_version"] == CURRENT_SCHEMA_VERSION

    def test_current_schema_version(self) -> None:
        assert CURRENT_SCHEMA_VERSION == 2


class TestBaseEventMigration:
    def test_not_implemented(self) -> None:
        migration = BaseEventMigration(from_version=1, to_version=2)
        with pytest.raises(NotImplementedError):
            migration.migrate({"type": "test"})
