"""Replay helpers for persisted workflow events."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from agent_orchestrator.models import WorkflowEvent
from agent_orchestrator.sse import to_message_event


@dataclass(slots=True)
class RunReplay:
    """Materialized view reconstructed from workflow events."""

    run_id: str
    status: str | None = None
    workflow_events: list[WorkflowEvent] = field(default_factory=list)
    message_events: list[dict[str, Any]] = field(default_factory=list)
    nodes: dict[str, dict[str, Any]] = field(default_factory=dict)
    messages: dict[str, Any] = field(default_factory=dict)
    waiting_action_id: str | None = None
    error: dict[str, Any] | None = None


def replay_events(events: list[WorkflowEvent]) -> RunReplay:
    """Rebuild a compact run view from an ordered event list."""

    run_id = events[0].run_id if events else ""
    replay = RunReplay(run_id=run_id)

    for event in events:
        replay.workflow_events.append(event)
        replay.message_events.append(to_message_event(event))

        if event.type == "run.compacted":
            _apply_compacted_event(replay, event)
            continue

        status = event.data.get("status")
        if status:
            replay.status = status
        messages = event.data.get("messages")
        if messages:
            replay.messages = dict(messages)

        if event.node_id:
            node = replay.nodes.setdefault(event.node_id, {})
            _apply_node_event(node, event)

        if event.type == "human.required" or event.type == "run.waiting":
            replay.waiting_action_id = event.data.get("pending_action_id")
        elif event.type == "run.resumed":
            replay.waiting_action_id = None
        elif event.type == "run.failed":
            replay.error = {
                "node_id": event.node_id,
                "error": event.data.get("error"),
                "error_type": event.data.get("error_type"),
            }

    return replay


async def replay_run(event_store: Any, run_id: str) -> RunReplay:
    """Load events for a run from an event store and replay them."""

    return replay_events(await event_store.list_by_run(run_id))


def _apply_node_event(node: dict[str, Any], event: WorkflowEvent) -> None:
    if event.type == "node.started":
        node["status"] = "running"
        return
    if event.type == "node.retrying":
        node["status"] = "retrying"
        node["attempt"] = event.data.get("attempt")
        node["error"] = event.data.get("error")
        node["error_type"] = event.data.get("error_type")
        return
    if event.type == "node.finished":
        node.update({key: value for key, value in event.data.items() if key != "messages"})
        return
    if event.type == "human.required":
        node["status"] = "waiting"
        node["pending_action_id"] = event.data.get("pending_action_id")
        node["human_request"] = event.data.get("request")


def _apply_compacted_event(replay: RunReplay, event: WorkflowEvent) -> None:
    snapshot = event.data.get("snapshot") or {}
    replay.status = snapshot.get("status", event.data.get("status"))
    replay.nodes = dict(snapshot.get("nodes") or {})
    replay.messages = dict(snapshot.get("messages") or event.data.get("messages") or {})
    replay.waiting_action_id = snapshot.get("waiting_action_id")
    replay.error = snapshot.get("error")
