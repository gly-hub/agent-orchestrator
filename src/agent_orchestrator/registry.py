"""Agent and tool registries."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal, cast

from agent_orchestrator.exceptions import RegistryError
from agent_orchestrator.models import AgentDefinition, AgentHandler, ToolDefinition, ToolHandler


class AgentRegistry:
    def __init__(self) -> None:
        self._items: dict[str, AgentDefinition] = {}

    def register(self, name: str, handler: AgentHandler) -> None:
        _validate_name(name, "agent")
        _validate_handler(handler, "agent")
        self._items[name] = AgentDefinition(name=name, handler=handler)

    def get(self, name: str) -> AgentDefinition:
        try:
            return self._items[name]
        except KeyError as exc:
            raise RegistryError(f"agent not registered: {name}") from exc


class ToolRegistry:
    def __init__(self) -> None:
        self._items: dict[str, ToolDefinition] = {}

    def register(
        self,
        name: str,
        handler: ToolHandler,
        *,
        requires_confirmation: bool = False,
        permissions: list[str] | tuple[str, ...] = (),
        risk_level: Literal["low", "medium", "high"] = "low",
        confirmation_policy: Literal["never", "always", "risk_based"] | None = None,
        input_schema: dict[str, Any] | None = None,
        output_schema: dict[str, Any] | None = None,
    ) -> None:
        _validate_name(name, "tool")
        _validate_handler(handler, "tool")
        _validate_permissions(permissions)
        if risk_level not in {"low", "medium", "high"}:
            raise RegistryError("tool risk_level must be one of: low, medium, high")
        if confirmation_policy is None:
            confirmation_policy = "always" if requires_confirmation else "never"
        elif confirmation_policy not in {"never", "always", "risk_based"}:
            raise RegistryError(
                "tool confirmation_policy must be one of: never, always, risk_based"
            )
        self._items[name] = ToolDefinition(
            name=name,
            handler=handler,
            requires_confirmation=requires_confirmation,
            permissions=tuple(permissions),
            risk_level=cast(Literal["low", "medium", "high"], risk_level),
            confirmation_policy=cast(Literal["never", "always", "risk_based"], confirmation_policy),
            input_schema=input_schema,
            output_schema=output_schema,
        )

    def get(self, name: str) -> ToolDefinition:
        try:
            return self._items[name]
        except KeyError as exc:
            raise RegistryError(f"tool not registered: {name}") from exc


def _validate_name(name: str, item_type: str) -> None:
    if not isinstance(name, str) or not name:
        raise RegistryError(f"{item_type} name is required")


def _validate_handler(handler: Callable[..., Any], item_type: str) -> None:
    if not callable(handler):
        raise RegistryError(f"{item_type} handler must be callable")


def _validate_permissions(permissions: list[str] | tuple[str, ...]) -> None:
    if not isinstance(permissions, list | tuple) or not all(
        isinstance(permission, str) for permission in permissions
    ):
        raise RegistryError("tool permissions must be a string list")
