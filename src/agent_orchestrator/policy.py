"""Tool permission and confirmation policy."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from agent_orchestrator.models import RunState, ToolDefinition

logger = logging.getLogger(__name__)

RiskLevel = Literal["low", "medium", "high"]
ConfirmationPolicy = Literal["never", "always", "risk_based"]
PolicyDecisionKind = Literal["allow", "confirm", "deny"]


@dataclass(slots=True)
class ToolPolicyDecision:
    decision: PolicyDecisionKind
    reason: str = ""
    missing_permissions: list[str] = field(default_factory=list)

    @classmethod
    def allow(cls) -> ToolPolicyDecision:
        return cls(decision="allow")

    @classmethod
    def confirm(cls, reason: str) -> ToolPolicyDecision:
        return cls(decision="confirm", reason=reason)

    @classmethod
    def deny(cls, reason: str, missing_permissions: list[str] | None = None) -> ToolPolicyDecision:
        return cls(decision="deny", reason=reason, missing_permissions=missing_permissions or [])


class ToolPolicyGate(Protocol):
    def evaluate(
        self,
        *,
        tool: ToolDefinition,
        args: dict[str, Any],
        run_state: RunState,
        node: dict[str, Any],
    ) -> ToolPolicyDecision: ...


class DefaultToolPolicyGate:
    """Default policy gate using run context permissions and tool metadata."""

    def evaluate(
        self,
        *,
        tool: ToolDefinition,
        args: dict[str, Any],
        run_state: RunState,
        node: dict[str, Any],
    ) -> ToolPolicyDecision:
        missing = _missing_permissions(tool.permissions, run_state.state.get("context", {}))
        if missing:
            return ToolPolicyDecision.deny(
                reason=f"missing required permission(s): {', '.join(missing)}",
                missing_permissions=missing,
            )

        confirmation_policy = tool.confirmation_policy
        if tool.requires_confirmation:
            confirmation_policy = "always"
        node_confirmation_policy = node.get("confirmation_policy")
        if node_confirmation_policy in {"never", "always", "risk_based"}:
            confirmation_policy = node_confirmation_policy

        if confirmation_policy == "always":
            return ToolPolicyDecision.confirm("tool confirmation policy requires approval")
        if confirmation_policy == "risk_based" and tool.risk_level == "high":
            return ToolPolicyDecision.confirm("high risk tool requires approval")
        return ToolPolicyDecision.allow()


def _missing_permissions(required: tuple[str, ...], context: dict[str, Any]) -> list[str]:
    if not required:
        return []
    granted = context.get("permissions") or context.get("user_permissions") or []
    granted_set = set(granted)
    return [permission for permission in required if permission not in granted_set]
