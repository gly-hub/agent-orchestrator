"""Subflow node execution helpers."""

from __future__ import annotations

from collections.abc import AsyncIterator
from copy import deepcopy
from typing import Any, Protocol, cast

from agent_orchestrator.exceptions import WorkflowError
from agent_orchestrator.models import RunState, WorkflowConfig, WorkflowEvent
from agent_orchestrator.runtime import EVENT_BUFFER
from agent_orchestrator.state import render_template


class _SubflowEngine(Protocol):
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

    async def _event(
        self,
        event_type: str,
        run_state: RunState,
        *,
        node_id: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> WorkflowEvent: ...

    async def _record_event(self, event: WorkflowEvent) -> WorkflowEvent: ...

    def _continue(self, run_state: RunState) -> AsyncIterator[WorkflowEvent]: ...


class SubflowNodeExecutorMixin:
    """Executor for inline reusable workflow nodes."""

    async def _run_subflow_node(
        self,
        node: dict[str, Any],
        run_state: RunState,
    ) -> AsyncIterator[WorkflowEvent]:
        engine = cast(_SubflowEngine, self)
        workflow_data = deepcopy(node["workflow"])
        workflow_data.setdefault("id", f"{run_state.workflow_id}.{node['id']}")
        workflow_data.setdefault("version", run_state.workflow_version)
        child_workflow = WorkflowConfig.from_dict(workflow_data)
        child_input = await engine._render_node_value(node, run_state, node.get("input", {}))
        if not isinstance(child_input, dict):
            child_input = {"value": child_input}

        child_state = RunState(
            run_id=run_state.run_id,
            workflow_id=child_workflow.id,
            workflow_version=child_workflow.version,
            status="running",
            current_node_id=None,
            waiting_action_id=None,
            state={
                "input": child_input,
                "context": run_state.state.get("context", {}),
                "messages": run_state.state.get("messages", {}),
                "nodes": {},
            },
        )
        engine_factory = cast(Any, type(self))
        child_engine = engine_factory(
            child_workflow,
            agents=engine.agents,
            tools=engine.tools,
            checkpoints=engine.checkpoints,
            event_store=engine.event_store,
            artifact_store=engine.artifact_store,
            artifact_threshold_bytes=engine.artifact_threshold_bytes,
            pending_action_ttl_ms=engine.pending_action_ttl_ms,
            policy_gate=engine.policy_gate,
            raise_on_error=engine.raise_on_error,
            error_observer=engine.error_observer,
            observer=engine.observer,
        )
        buffer: list[WorkflowEvent] = []
        token = EVENT_BUFFER.set(buffer)
        try:
            child_runtime = cast(_SubflowEngine, child_engine)
            async for _ in child_runtime._continue(child_state):
                pass
        finally:
            EVENT_BUFFER.reset(token)

        for event in buffer:
            if event.type in {"run.started", "run.resumed", "run.finished"}:
                continue
            if event.type == "run.waiting":
                raise WorkflowError("subflow nodes do not support waiting actions")
            if event.type == "run.failed":
                raise WorkflowError(event.data.get("error", "subflow failed"))
            yield await engine._record_event(self._namespace_subflow_event(event, node["id"]))

        prefixed_nodes = {
            f"{node['id']}.{child_node_id}": deepcopy(record)
            for child_node_id, record in child_state.state.get("nodes", {}).items()
        }
        run_state.state.setdefault("nodes", {}).update(prefixed_nodes)
        selected_output = self._subflow_output(node, child_workflow, child_state)
        output = {
            "workflow_id": child_workflow.id,
            "status": child_state.status,
            "nodes": prefixed_nodes,
            "output": selected_output,
        }
        await engine._save_node_output(run_state, node["id"], output, node=node)
        yield await engine._event(
            "subflow.finished",
            run_state,
            node_id=node["id"],
            data={
                "workflow_id": child_workflow.id,
                "status": child_state.status,
                "output": output["output"],
            },
        )

    def _namespace_subflow_event(self, event: WorkflowEvent, subflow_node_id: str) -> WorkflowEvent:
        data = dict(event.data)
        data["subflow_node_id"] = subflow_node_id
        data["subflow_event_type"] = event.type
        node_id = f"{subflow_node_id}.{event.node_id}" if event.node_id else subflow_node_id
        return WorkflowEvent(
            type=f"subflow.{event.type}",
            run_id=event.run_id,
            node_id=node_id,
            data=data,
        )

    def _subflow_output(
        self,
        node: dict[str, Any],
        workflow: WorkflowConfig,
        child_state: RunState,
    ) -> Any:
        if "output" in node:
            return render_template(node["output"], child_state.state)
        output_node_id = None
        if workflow.nodes:
            output_node_id = workflow.nodes[-1]["id"]
        if output_node_id is None:
            return None
        return deepcopy(child_state.state.get("nodes", {}).get(output_node_id, {}).get("output"))
