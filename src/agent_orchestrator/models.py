"""Core data structures for the workflow engine."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from agent_orchestrator.validation import validate_workflow_config
from agent_orchestrator.workflow_nodes import WorkflowNode, normalize_workflow_nodes

NodeType = Literal["agent", "tool", "transform", "human", "condition", "parallel", "subflow"]
RunStatus = Literal["running", "waiting_for_user", "completed", "failed"]
NodeStatus = Literal["pending", "running", "success", "failed", "waiting", "skipped"]


@dataclass(slots=True)
class WorkflowConfig:
    """Declarative workflow definition."""

    id: str
    version: int
    nodes: list[WorkflowNode]
    edges: list[dict[str, Any]] = field(default_factory=list)
    policy: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any], *, validate: bool = True) -> WorkflowConfig:
        config = cls(
            id=data["id"],
            version=int(data.get("version", 1)),
            nodes=normalize_workflow_nodes(list(data.get("nodes", []))),
            edges=list(data.get("edges", [])),
            policy=dict(data.get("policy", {})),
        )
        if validate:
            validate_workflow_config(config)
        return config


@dataclass(slots=True)
class StartRunRequest:
    """Input for a new workflow run."""

    message: str
    context: dict[str, Any] = field(default_factory=dict)
    run_id: str | None = None
    user_message_id: str | None = None
    assistant_message_id: str | None = None
    bubble_id: str | None = None


@dataclass(slots=True)
class ResumeRunRequest:
    """Input for resuming a waiting workflow run."""

    pending_action_id: str
    decision: dict[str, Any]


@dataclass(slots=True)
class PendingAction:
    """A resumable wait point, usually created by a human node."""

    id: str
    run_id: str
    node_id: str
    action_type: str
    request: dict[str, Any]
    status: Literal["pending", "approved", "rejected", "expired"] = "pending"
    decision: dict[str, Any] | None = None
    created_at_ms: int = 0
    expires_at_ms: int | None = None


@dataclass(slots=True)
class RunState:
    """Mutable state shared by all nodes in a workflow run."""

    run_id: str
    workflow_id: str
    workflow_version: int
    status: RunStatus
    state: dict[str, Any]
    current_node_id: str | None = None
    waiting_action_id: str | None = None


@dataclass(slots=True)
class WorkflowEvent:
    """Internal event emitted by the engine and convertible to SSE."""

    type: str
    run_id: str
    node_id: str | None = None
    data: dict[str, Any] = field(default_factory=dict)
    schema_version: int = 1


def workflow_event_from_dict(data: dict[str, Any]) -> WorkflowEvent:
    """Deserialize a workflow event, accepting pre-versioned event payloads."""

    payload = dict(data)
    payload.setdefault("schema_version", 1)
    payload.setdefault("data", {})
    payload.setdefault("node_id", None)
    return WorkflowEvent(**payload)


def workflow_event_to_dict(event: WorkflowEvent) -> dict[str, Any]:
    """Serialize a workflow event with an explicit schema version."""

    return asdict(event)


AgentHandler = Callable[[dict[str, Any], RunState], AsyncIterator[WorkflowEvent]]
ToolHandler = Callable[[dict[str, Any], RunState], Awaitable[dict[str, Any]]]


@dataclass(slots=True)
class AgentDefinition:
    name: str
    handler: AgentHandler


@dataclass(slots=True)
class ToolDefinition:
    name: str
    handler: ToolHandler
    requires_confirmation: bool = False
    permissions: tuple[str, ...] = ()
    risk_level: Literal["low", "medium", "high"] = "low"
    confirmation_policy: Literal["never", "always", "risk_based"] = "never"
    input_schema: dict[str, Any] | None = None
    output_schema: dict[str, Any] | None = None
