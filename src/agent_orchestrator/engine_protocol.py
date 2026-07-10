"""Unified Protocol for engine access from mixins."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any, Protocol

from agent_orchestrator.models import PendingAction, RunState, WorkflowConfig, WorkflowEvent


class EngineProtocol(Protocol):
    """Structural type describing the engine interface used by all mixins."""

    execution: Any
    agents: Any
    tools: Any
    checkpoints: Any
    event_store: Any
    artifact_store: Any
    artifact_threshold_bytes: int | None
    pending_action_ttl_ms: int | None
    policy_gate: Any
    raise_on_error: bool
    error_observer: Any
    observer: Any
    _pending_action_futures: dict[str, asyncio.Future[dict[str, Any]]]
    _live_waiting_action_ids: set[str]

    async def _event(
        self,
        event_type: str,
        run_state: RunState,
        *,
        node_id: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> WorkflowEvent: ...

    def _run_node_with_retry(
        self,
        node: dict[str, Any],
        run_state: RunState,
    ) -> AsyncIterator[WorkflowEvent]: ...

    async def _render_node_value(
        self,
        node: dict[str, Any],
        run_state: RunState,
        value: Any,
    ) -> Any: ...

    async def _save_node_output(
        self,
        run_state: RunState,
        node_id: str,
        output: Any,
        *,
        node: dict[str, Any],
    ) -> None: ...

    async def _record_event(self, event: WorkflowEvent) -> WorkflowEvent: ...

    def _continue(self, run_state: RunState) -> AsyncIterator[WorkflowEvent]: ...

    def _now_ms(self) -> int: ...

    def _expires_at_ms(self) -> int | None: ...

    async def _pause_for_action(self, run_state: RunState, action: PendingAction) -> None: ...

    async def _observe_run_failed(self, exc: Exception, run_state: RunState) -> None: ...

    async def _run_failed_event(self, run_state: RunState, exc: Exception) -> WorkflowEvent: ...

    def _publish_resume_event(self, event: WorkflowEvent) -> None: ...

    def _close_resume_event_queues(self, run_id: str) -> None: ...

    def _create_child_engine(
        self,
        child_workflow: WorkflowConfig,
        *,
        checkpoints: Any = None,
    ) -> Any: ...
