"""Runtime helper mixin for the workflow engine."""
# pyright: reportAttributeAccessIssue=false

from __future__ import annotations

from typing import Any

from agent_orchestrator.models import RunState, StartRunRequest, WorkflowEvent


class EngineRuntimeMixin:
    """Delegate runtime services from the engine to ``WorkflowExecutionContext``."""

    def _new_run_state(self, request: StartRunRequest) -> RunState:
        return self.execution.new_run_state(self.workflow.id, self.workflow.version, request)

    async def _save_node_output(
        self,
        run_state: RunState,
        node_id: str,
        output: Any,
        *,
        node: dict[str, Any],
    ) -> None:
        await self.execution.save_node_output(run_state, node_id, output, node=node)

    async def _maybe_store_output_artifact(
        self,
        run_state: RunState,
        node_id: str,
        node: dict[str, Any],
        output: Any,
    ) -> Any:
        return await self.execution.maybe_store_output_artifact(run_state, node_id, node, output)

    def _now_ms(self) -> int:
        return self.execution.now_ms()

    def _expires_at_ms(self) -> int | None:
        return self.execution.expires_at_ms()

    async def _render_node_value(
        self,
        node: dict[str, Any],
        run_state: RunState,
        value: Any,
    ) -> Any:
        return await self.execution.render_node_value(node, run_state, value)

    async def _event(
        self,
        event_type: str,
        run_state: RunState,
        *,
        node_id: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> WorkflowEvent:
        return await self.execution.event(event_type, run_state, node_id=node_id, data=data)

    async def _normalize_child_event(
        self,
        event: WorkflowEvent,
        run_state: RunState,
        node_id: str,
    ) -> WorkflowEvent:
        return await self.execution.normalize_child_event(event, run_state, node_id)

    async def _record_event(self, event: WorkflowEvent) -> WorkflowEvent:
        return await self.execution.record_event(event)

    async def _observe_run_failed(self, exc: Exception, run_state: RunState) -> None:
        await self._observe_error(exc, run_state)
        await self.execution.observe(
            "run.failed",
            run_state,
            node_id=run_state.current_node_id,
            data={"error": str(exc), "error_type": type(exc).__name__},
        )

    async def _run_failed_event(self, run_state: RunState, exc: Exception) -> WorkflowEvent:
        data = {
            "error": str(exc),
            "error_type": type(exc).__name__,
            "status": run_state.status,
            "messages": run_state.state.get("messages", {}),
        }
        try:
            return await self._event(
                "run.failed",
                run_state,
                node_id=run_state.current_node_id,
                data=data,
            )
        except Exception:
            return WorkflowEvent(
                type="run.failed",
                run_id=run_state.run_id,
                node_id=run_state.current_node_id,
                data=data,
            )

    async def _observe_error(self, exc: Exception, run_state: RunState) -> None:
        if self.error_observer is None:
            return
        result = self.error_observer(exc, run_state)
        if result is not None:
            await result
