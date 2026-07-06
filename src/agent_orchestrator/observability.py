"""Lightweight workflow observability hooks."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class WorkflowObservation:
    """Structured runtime observation emitted outside the workflow event log."""

    type: str
    run_id: str
    node_id: str | None = None
    data: dict[str, Any] = field(default_factory=dict)


WorkflowObserver = Callable[[WorkflowObservation], Awaitable[None] | None]


async def notify_observer(
    observer: WorkflowObserver | None,
    observation: WorkflowObservation,
) -> None:
    """Notify an observer without letting monitoring failures break execution."""

    if observer is None:
        return
    try:
        result = observer(observation)
        if result is not None:
            await result
    except Exception:
        return
