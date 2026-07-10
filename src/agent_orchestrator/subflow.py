"""Subflow node execution helpers."""

from __future__ import annotations

import logging
import uuid
from collections.abc import AsyncIterator
from copy import deepcopy
from dataclasses import asdict
from typing import Any, cast

from agent_orchestrator.checkpoint import InMemoryCheckpointStore
from agent_orchestrator.engine_protocol import EngineProtocol
from agent_orchestrator.exceptions import WorkflowError
from agent_orchestrator.models import PendingAction, RunState, WorkflowConfig, WorkflowEvent
from agent_orchestrator.runtime import EVENT_BUFFER, drain
from agent_orchestrator.state import render_template

logger = logging.getLogger(__name__)



class SubflowNodeExecutorMixin:
    """Executor for inline reusable workflow nodes."""

    async def _run_subflow_node(
        self,
        node: dict[str, Any],
        run_state: RunState,
    ) -> AsyncIterator[WorkflowEvent]:
        engine = cast(EngineProtocol, self)
        node_record = run_state.state.setdefault("nodes", {}).setdefault(node["id"], {})

        if "_subflow_child_state" in node_record and "approval" in node_record:
            async for event in self._resume_subflow_node(node, run_state, engine):
                yield event
            return

        workflow_data = deepcopy(node["workflow"])
        workflow_data.setdefault("id", f"{run_state.workflow_id}.{node['id']}")
        workflow_data.setdefault("version", run_state.workflow_version)
        child_workflow = WorkflowConfig.from_dict(workflow_data)
        logger.debug("subflow node %s executing child workflow %s", node["id"], child_workflow.id)
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
                "context": deepcopy(run_state.state.get("context", {})),
                "messages": deepcopy(run_state.state.get("messages", {})),
                "nodes": {},
            },
        )

        async for event in self._execute_subflow_child(
            engine, node, run_state, child_workflow, child_state,
        ):
            yield event

    async def _execute_subflow_child(
        self,
        engine: Any,
        node: dict[str, Any],
        run_state: RunState,
        child_workflow: WorkflowConfig,
        child_state: RunState,
    ) -> AsyncIterator[WorkflowEvent]:
        child_checkpoints = InMemoryCheckpointStore()
        child_engine = cast(EngineProtocol, self)._create_child_engine(child_workflow, checkpoints=child_checkpoints)
        buffer: list[WorkflowEvent] = []
        token = EVENT_BUFFER.set(buffer)
        try:
            child_runtime = cast(EngineProtocol, child_engine)
            await drain(child_runtime._continue(child_state))
        finally:
            EVENT_BUFFER.reset(token)

        waiting_event: WorkflowEvent | None = None
        for event in buffer:
            if event.type in {"run.started", "run.resumed", "run.finished"}:
                continue
            if event.type == "run.waiting":
                waiting_event = event
                continue
            if event.type == "run.failed":
                raise WorkflowError(event.data.get("error", "subflow failed"))
            yield await engine._record_event(self._namespace_subflow_event(event, node["id"]))

        if waiting_event is not None:
            async for event in self._pause_subflow_for_human(
                engine, node, run_state, child_state, child_workflow, child_checkpoints, waiting_event,
            ):
                yield event
            return

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

    async def _pause_subflow_for_human(
        self,
        engine: Any,
        node: dict[str, Any],
        run_state: RunState,
        child_state: RunState,
        child_workflow: WorkflowConfig,
        child_checkpoints: InMemoryCheckpointStore,
        waiting_event: WorkflowEvent,
    ) -> AsyncIterator[WorkflowEvent]:
        child_action_id = waiting_event.data.get("pending_action_id", "")
        child_action = await child_checkpoints.load_action(child_action_id)

        node_record = run_state.state.setdefault("nodes", {}).setdefault(node["id"], {})
        node_record["_subflow_child_state"] = asdict(child_state)
        node_record["_subflow_child_workflow"] = {
            "id": child_workflow.id,
            "version": child_workflow.version,
            "nodes": [dict(n) for n in child_workflow.nodes],
            "edges": list(child_workflow.edges),
            "policy": dict(child_workflow.policy),
        }
        node_record["_subflow_child_human_node_id"] = child_action.node_id

        action = PendingAction(
            id=f"pa_{uuid.uuid4().hex}",
            run_id=run_state.run_id,
            node_id=node["id"],
            action_type="subflow_human",
            request={
                **child_action.request,
                "child_pending_action_id": child_action_id,
                "child_human_node_id": child_action.node_id,
            },
            created_at_ms=engine._now_ms(),
            expires_at_ms=engine._expires_at_ms(),
        )
        logger.debug(
            "subflow node %s pausing for human node %s in child workflow",
            node["id"],
            child_action.node_id,
        )
        yield await engine._event(
            "human.required",
            run_state,
            node_id=node["id"],
            data={
                "pending_action_id": action.id,
                "request": action.request,
            },
        )
        await engine._pause_for_action(run_state, action)

    async def _resume_subflow_node(
        self,
        node: dict[str, Any],
        run_state: RunState,
        engine: Any,
    ) -> AsyncIterator[WorkflowEvent]:
        node_record = run_state.state["nodes"][node["id"]]
        child_state_data = node_record.pop("_subflow_child_state")
        child_workflow_data = node_record.pop("_subflow_child_workflow")
        child_human_node_id = node_record.pop("_subflow_child_human_node_id")
        decision = node_record.pop("approval")

        child_state = RunState(**child_state_data)
        child_state.status = "running"
        child_state.waiting_action_id = None
        child_human_record = child_state.state["nodes"][child_human_node_id]
        child_human_record["status"] = "success"
        child_human_record["output"] = decision
        child_human_record.pop("_dag_outgoing_processed", None)
        child_scheduler = child_state.state.setdefault("_internal", {}).setdefault("scheduler", {})
        child_scheduler["waiting_actions"] = {}

        child_workflow = WorkflowConfig.from_dict(child_workflow_data, validate=False)
        logger.debug("subflow node %s resuming child workflow %s", node["id"], child_workflow.id)

        async for event in self._execute_subflow_child(
            engine, node, run_state, child_workflow, child_state,
        ):
            yield event

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
