"""Runtime helpers shared by workflow execution internals."""

from __future__ import annotations

import contextvars
from typing import Protocol

from agent_orchestrator.events import EventStore
from agent_orchestrator.models import WorkflowEvent


class EventSink(Protocol):
    async def append(self, event: WorkflowEvent) -> None: ...


EventBuffer = list[WorkflowEvent] | EventSink | None


EVENT_BUFFER: contextvars.ContextVar[EventBuffer] = contextvars.ContextVar(
    "agent_orchestrator_event_buffer",
    default=None,
)


async def record_event(event_store: EventStore, event: WorkflowEvent) -> WorkflowEvent:
    """Append an event to the active branch buffer or the configured event store."""

    buffer = EVENT_BUFFER.get()
    if buffer is None:
        await event_store.append(event)
    elif isinstance(buffer, list):
        buffer.append(event)
    else:
        await buffer.append(event)
    return event
