"""Helpers for compacting long workflow event logs."""

from __future__ import annotations

import time
from dataclasses import dataclass

from agent_orchestrator.events import CompactableEventStore
from agent_orchestrator.models import WorkflowEvent
from agent_orchestrator.replay import replay_events


@dataclass(slots=True)
class EventCompactionResult:
    run_id: str
    original_event_count: int
    compacted_event_count: int
    compacted_event: WorkflowEvent | None
    events: list[WorkflowEvent]


def compact_events(
    events: list[WorkflowEvent],
    *,
    retain_last: int = 0,
    compacted_at_ms: int | None = None,
) -> EventCompactionResult:
    """Replace older events with one replayable snapshot event."""

    if not events:
        return EventCompactionResult(
            run_id="",
            original_event_count=0,
            compacted_event_count=0,
            compacted_event=None,
            events=[],
        )
    if retain_last < 0:
        raise ValueError("retain_last must be >= 0")

    run_id = events[0].run_id
    split_at = max(0, len(events) - retain_last)
    compacted_source = events[:split_at]
    retained = events[split_at:]
    if not compacted_source:
        return EventCompactionResult(
            run_id=run_id,
            original_event_count=len(events),
            compacted_event_count=0,
            compacted_event=None,
            events=list(events),
        )

    compacted_event = create_compaction_event(
        compacted_source,
        compacted_at_ms=compacted_at_ms,
        retained_event_count=len(retained),
    )
    return EventCompactionResult(
        run_id=run_id,
        original_event_count=len(events),
        compacted_event_count=len(compacted_source),
        compacted_event=compacted_event,
        events=[compacted_event, *retained],
    )


async def compact_run(
    event_store: CompactableEventStore,
    run_id: str,
    *,
    retain_last: int = 0,
    compacted_at_ms: int | None = None,
) -> EventCompactionResult:
    """Compact a persisted run in an event store that supports replacement."""

    result = compact_events(
        await event_store.list_by_run(run_id),
        retain_last=retain_last,
        compacted_at_ms=compacted_at_ms,
    )
    await event_store.replace_run(run_id, result.events)
    return result


def create_compaction_event(
    events: list[WorkflowEvent],
    *,
    compacted_at_ms: int | None = None,
    retained_event_count: int = 0,
) -> WorkflowEvent:
    if not events:
        raise ValueError("events are required")

    replay = replay_events(events)
    compacted_at_ms = compacted_at_ms if compacted_at_ms is not None else int(time.time() * 1000)
    return WorkflowEvent(
        type="run.compacted",
        run_id=replay.run_id,
        node_id=None,
        data={
            "status": replay.status,
            "messages": replay.messages,
            "snapshot": {
                "status": replay.status,
                "nodes": replay.nodes,
                "messages": replay.messages,
                "waiting_action_id": replay.waiting_action_id,
                "error": replay.error,
            },
            "compacted_event_count": len(events),
            "retained_event_count": retained_event_count,
            "compacted_at_ms": compacted_at_ms,
        },
    )
