"""Execution context and node lifecycle helpers."""

from __future__ import annotations

import time
import uuid
from typing import Any

from agent_orchestrator.artifacts import ArtifactStore, estimate_json_size, resolve_artifacts
from agent_orchestrator.events import EventStore
from agent_orchestrator.models import RunState, StartRunRequest, WorkflowEvent
from agent_orchestrator.observability import WorkflowObservation, WorkflowObserver, notify_observer
from agent_orchestrator.runtime import record_event
from agent_orchestrator.state import render_template


class WorkflowExecutionContext:
    """Runtime services shared by workflow executors."""

    def __init__(
        self,
        *,
        event_store: EventStore,
        artifact_store: ArtifactStore | None,
        artifact_threshold_bytes: int | None,
        pending_action_ttl_ms: int | None,
        observer: WorkflowObserver | None = None,
    ) -> None:
        self.event_store = event_store
        self.artifact_store = artifact_store
        self.artifact_threshold_bytes = artifact_threshold_bytes
        self.pending_action_ttl_ms = pending_action_ttl_ms
        self.observer = observer

    def new_run_state(self, workflow_id: str, workflow_version: int, request: StartRunRequest) -> RunState:
        return RunState(
            run_id=request.run_id or f"run_{uuid.uuid4().hex}",
            workflow_id=workflow_id,
            workflow_version=workflow_version,
            status="running",
            state={
                "input": {"message": request.message},
                "context": request.context,
                "messages": {
                    "user_message_id": request.user_message_id or f"msg_user_{uuid.uuid4().hex}",
                    "assistant_message_id": request.assistant_message_id
                    or f"msg_asst_{uuid.uuid4().hex}",
                    "bubble_id": request.bubble_id or f"bubble_{uuid.uuid4().hex}",
                },
                "nodes": {},
            },
        )

    def now_ms(self) -> int:
        return int(time.time() * 1000)

    def expires_at_ms(self) -> int | None:
        if self.pending_action_ttl_ms is None:
            return None
        return self.now_ms() + self.pending_action_ttl_ms

    def start_node(self, run_state: RunState, node_id: str) -> dict[str, Any]:
        run_state.current_node_id = node_id
        node_record = run_state.state.setdefault("nodes", {}).setdefault(node_id, {})
        node_record["status"] = "running"
        node_record["started_at_ms"] = self.now_ms()
        return node_record

    async def observe_node_started(self, run_state: RunState, node_id: str) -> None:
        await self.observe(
            "node.started",
            run_state,
            node_id=node_id,
            data={"status": run_state.status},
        )

    def fail_node(self, run_state: RunState, node_id: str, exc: Exception) -> dict[str, Any]:
        node_record = run_state.state.setdefault("nodes", {}).setdefault(node_id, {})
        node_record["status"] = "failed"
        node_record["error"] = str(exc)
        node_record["error_type"] = type(exc).__name__
        node_record["output"] = {
            "failed": True,
            "error": str(exc),
            "error_type": type(exc).__name__,
        }
        return node_record

    async def observe_node_failed(
        self,
        run_state: RunState,
        node_id: str,
        node_record: dict[str, Any],
    ) -> None:
        await self.observe(
            "node.failed",
            run_state,
            node_id=node_id,
            data={
                "error": node_record.get("error"),
                "error_type": node_record.get("error_type"),
                "duration_ms": node_record.get("duration_ms"),
            },
        )

    def finish_node(self, run_state: RunState, node_id: str) -> dict[str, Any]:
        node_record = run_state.state.setdefault("nodes", {}).setdefault(node_id, {})
        if node_record.get("status") not in {"success", "failed"}:
            node_record["status"] = "success"
        node_record["finished_at_ms"] = self.now_ms()
        node_record["duration_ms"] = node_record["finished_at_ms"] - node_record["started_at_ms"]
        return node_record

    async def observe_node_finished(
        self,
        run_state: RunState,
        node_id: str,
        node_record: dict[str, Any],
    ) -> None:
        await self.observe(
            "node.finished",
            run_state,
            node_id=node_id,
            data={
                "status": node_record.get("status"),
                "attempt": node_record.get("attempt"),
                "duration_ms": node_record.get("duration_ms"),
            },
        )

    async def save_node_output(
        self,
        run_state: RunState,
        node_id: str,
        output: Any,
        *,
        node: dict[str, Any],
    ) -> None:
        stored_output = await self.maybe_store_output_artifact(run_state, node_id, node, output)
        run_state.state.setdefault("nodes", {}).setdefault(node_id, {}).update(
            {"status": "success", "output": stored_output}
        )

    async def maybe_store_output_artifact(
        self,
        run_state: RunState,
        node_id: str,
        node: dict[str, Any],
        output: Any,
    ) -> Any:
        if self.artifact_store is None:
            return output

        output_artifact = bool(node.get("output_artifact"))
        threshold = node.get("artifact_threshold_bytes", self.artifact_threshold_bytes)
        if not output_artifact and threshold is not None:
            output_artifact = estimate_json_size(output) > int(threshold)
        if not output_artifact:
            return output

        ref = await self.artifact_store.put(
            run_id=run_state.run_id,
            node_id=node_id,
            name="output",
            value=output,
            metadata={"workflow_id": run_state.workflow_id, "workflow_version": run_state.workflow_version},
        )
        return {"artifact_ref": ref}

    async def render_node_value(
        self,
        node: dict[str, Any],
        run_state: RunState,
        value: Any,
    ) -> Any:
        rendered = render_template(value, run_state.state)
        if not node.get("resolve_input_artifacts"):
            return rendered
        return await resolve_artifacts(rendered, self.artifact_store)

    async def event(
        self,
        event_type: str,
        run_state: RunState,
        *,
        node_id: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> WorkflowEvent:
        payload = dict(data or {})
        payload.setdefault("status", run_state.status)
        payload.setdefault("messages", run_state.state.get("messages", {}))
        event = WorkflowEvent(
            type=event_type,
            run_id=run_state.run_id,
            node_id=node_id,
            data=payload,
        )
        return await self.record_event(event)

    async def normalize_child_event(
        self,
        event: WorkflowEvent,
        run_state: RunState,
        node_id: str,
    ) -> WorkflowEvent:
        payload = dict(event.data)
        payload.setdefault("status", run_state.status)
        payload.setdefault("messages", run_state.state.get("messages", {}))
        normalized = WorkflowEvent(
            type=event.type,
            run_id=run_state.run_id,
            node_id=event.node_id or node_id,
            data=payload,
        )
        return await self.record_event(normalized)

    async def record_event(self, event: WorkflowEvent) -> WorkflowEvent:
        try:
            recorded = await record_event(self.event_store, event)
        except Exception as exc:
            await notify_observer(
                self.observer,
                WorkflowObservation(
                    type="event.append_failed",
                    run_id=event.run_id,
                    node_id=event.node_id,
                    data={
                        "event_type": event.type,
                        "error": str(exc),
                        "error_type": type(exc).__name__,
                    },
                ),
            )
            raise
        await notify_observer(
            self.observer,
            WorkflowObservation(
                type="event.appended",
                run_id=event.run_id,
                node_id=event.node_id,
                data={"event_type": event.type},
            ),
        )
        return recorded

    async def observe(
        self,
        observation_type: str,
        run_state: RunState,
        *,
        node_id: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        await notify_observer(
            self.observer,
            WorkflowObservation(
                type=observation_type,
                run_id=run_state.run_id,
                node_id=node_id,
                data=dict(data or {}),
            ),
        )
