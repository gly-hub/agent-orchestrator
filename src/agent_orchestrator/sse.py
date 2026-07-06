"""Adapters from workflow events to message/SSE payloads."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Iterable
from typing import Any

from agent_orchestrator.models import WorkflowEvent

EVENT_NAME_MAP = {
    "run.started": "RUN_STARTED",
    "run.resumed": "RUN_RESUMED",
    "run.compacted": "RUN_COMPACTED",
    "run.waiting": "RUN_WAITING",
    "run.finished": "FINISH",
    "run.failed": "ERROR",
    "node.started": "NODE_STARTED",
    "node.retrying": "NODE_RETRYING",
    "node.failed": "NODE_FAILED",
    "node.finished": "NODE_FINISHED",
    "policy.decision": "POLICY_DECISION",
    "agent.delta": "ADD",
    "agent.output": "AGENT_OUTPUT",
    "agent.tool_use": "AGENT_TOOL_USE",
    "agent.tool_result": "AGENT_TOOL_RESULT",
    "tool.started": "TOOL_USE",
    "tool.finished": "TOOL_RESULT",
    "human.required": "HUMAN_REQUIRED",
    "human.expired": "HUMAN_EXPIRED",
}


def to_message_event(event: WorkflowEvent) -> dict[str, Any]:
    """Convert a WorkflowEvent to a chat-message friendly event envelope."""

    messages = event.data.get("messages", {})
    payload = {
        "event": EVENT_NAME_MAP.get(event.type, event.type.upper().replace(".", "_")),
        "type": event.type,
        "schema_version": event.schema_version,
        "run_id": event.run_id,
        "node_id": event.node_id,
        "up_message_id": messages.get("user_message_id"),
        "down_message_id": messages.get("assistant_message_id"),
        "bubble_id": messages.get("bubble_id"),
        "data": {
            key: value
            for key, value in event.data.items()
            if key not in {"messages"}
        },
    }

    if event.type == "human.required":
        payload["pending_action_id"] = event.data.get("pending_action_id")
        payload["human_request"] = event.data.get("request")
    elif event.type == "agent.delta":
        payload["delta"] = event.data.get("text", "")
    elif event.type == "tool.started":
        payload["tool_use"] = {
            "tool_name": event.data.get("tool_name"),
            "args": event.data.get("args"),
        }
    elif event.type == "tool.finished":
        payload["tool_result"] = {
            "tool_name": event.data.get("tool_name"),
            "output": event.data.get("output"),
        }

    return payload


def encode_sse(payload: dict[str, Any]) -> str:
    """Encode one event envelope as a Server-Sent Events frame."""

    event_name = str(payload.get("event", "message"))
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return f"event: {event_name}\ndata: {data}\n\n"


def events_to_sse(events: Iterable[WorkflowEvent]) -> list[str]:
    """Convert a finite list of workflow events into SSE frames."""

    return [encode_sse(to_message_event(event)) for event in events]


async def stream_sse(events: AsyncIterator[WorkflowEvent]) -> AsyncIterator[str]:
    """Convert an async workflow event stream into SSE frame strings."""

    async for event in events:
        yield encode_sse(to_message_event(event))
