"""Event persistence interfaces and built-in stores."""

from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from pathlib import Path
from typing import Protocol

from agent_orchestrator.models import (
    WorkflowEvent,
    workflow_event_from_dict,
    workflow_event_to_dict,
)


class EventStore(Protocol):
    """Persistence API for workflow event logs."""

    async def append(self, event: WorkflowEvent) -> None: ...

    async def list_by_run(self, run_id: str) -> list[WorkflowEvent]: ...


class CompactableEventStore(EventStore, Protocol):
    """Event stores that can atomically replace a run log after compaction."""

    async def replace_run(self, run_id: str, events: list[WorkflowEvent]) -> None: ...


class NoopEventStore:
    """Event store that discards events."""

    async def append(self, event: WorkflowEvent) -> None:
        return None

    async def list_by_run(self, run_id: str) -> list[WorkflowEvent]:
        return []

    async def replace_run(self, run_id: str, events: list[WorkflowEvent]) -> None:
        return None


class InMemoryEventStore:
    """In-memory event log suitable for tests and demos."""

    def __init__(self) -> None:
        self._events: dict[str, list[WorkflowEvent]] = {}

    async def append(self, event: WorkflowEvent) -> None:
        self._events.setdefault(event.run_id, []).append(deepcopy(event))

    async def list_by_run(self, run_id: str) -> list[WorkflowEvent]:
        return deepcopy(self._events.get(run_id, []))

    async def replace_run(self, run_id: str, events: list[WorkflowEvent]) -> None:
        self._events[run_id] = deepcopy(events)


class FileEventStore:
    """JSONL event store suitable for local services and integration tests."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    async def append(self, event: WorkflowEvent) -> None:
        await asyncio.to_thread(self._append_sync, event)

    def _append_sync(self, event: WorkflowEvent) -> None:
        path = self._run_path(event.run_id)
        with path.open("a", encoding="utf-8") as fd:
            fd.write(json.dumps(workflow_event_to_dict(event), ensure_ascii=False, separators=(",", ":")))
            fd.write("\n")

    async def list_by_run(self, run_id: str) -> list[WorkflowEvent]:
        return await asyncio.to_thread(self._list_by_run_sync, run_id)

    def _list_by_run_sync(self, run_id: str) -> list[WorkflowEvent]:
        path = self._run_path(run_id)
        if not path.exists():
            return []
        events = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if line:
                events.append(workflow_event_from_dict(json.loads(line)))
        return events

    async def replace_run(self, run_id: str, events: list[WorkflowEvent]) -> None:
        await asyncio.to_thread(self._replace_run_sync, run_id, events)

    def _replace_run_sync(self, run_id: str, events: list[WorkflowEvent]) -> None:
        path = self._run_path(run_id)
        temp = path.with_suffix(".tmp")
        with temp.open("w", encoding="utf-8") as fd:
            for event in events:
                fd.write(json.dumps(workflow_event_to_dict(event), ensure_ascii=False, separators=(",", ":")))
                fd.write("\n")
        temp.replace(path)

    def _run_path(self, run_id: str) -> Path:
        return self.root / f"{run_id}.jsonl"
