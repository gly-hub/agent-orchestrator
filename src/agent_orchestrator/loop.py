"""Loop node execution helpers."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from copy import deepcopy
from typing import Any, Protocol, cast

from agent_orchestrator.exceptions import WorkflowError
from agent_orchestrator.models import RunState, WorkflowConfig, WorkflowEvent
from agent_orchestrator.runtime import EVENT_BUFFER, drain
from agent_orchestrator.state import evaluate_when, render_template

logger = logging.getLogger(__name__)

DEFAULT_MAX_ITERATIONS = 100


class _LoopEngine(Protocol):
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


class LoopNodeExecutorMixin:
    """Executor for loop workflow nodes that repeat a body while a condition holds."""

    async def _run_loop_node(
        self,
        node: dict[str, Any],
        run_state: RunState,
    ) -> AsyncIterator[WorkflowEvent]:
        engine = cast(_LoopEngine, self)
        max_iterations = int(node.get("max_iterations", DEFAULT_MAX_ITERATIONS))
        condition = node.get("condition")
        body_data = node.get("body")
        if not body_data or not isinstance(body_data, dict):
            raise WorkflowError(f"loop node {node['id']} requires a body with nodes")

        logger.debug("loop node %s starting, max_iterations=%d", node["id"], max_iterations)

        all_iteration_nodes: dict[str, dict[str, Any]] = {}
        iteration_outputs: list[Any] = []
        iteration = 0

        while iteration < max_iterations:
            loop_state = self._build_loop_state(node, run_state, iteration, iteration_outputs)
            if condition and iteration > 0 and not evaluate_when(condition, loop_state):
                    logger.debug("loop node %s condition false at iteration %d", node["id"], iteration)
                    break

            yield await engine._event(
                "loop.iteration_started",
                run_state,
                node_id=node["id"],
                data={"iteration": iteration},
            )

            child_state, child_workflow = self._create_loop_child(
                node, run_state, body_data, iteration,
            )
            buffer: list[WorkflowEvent] = []
            token = EVENT_BUFFER.set(buffer)
            error: Exception | None = None
            try:
                child_engine = self._create_child_engine(engine, child_workflow)
                child_runtime = cast(_LoopEngine, child_engine)
                await drain(child_runtime._continue(child_state))
            except Exception as exc:
                error = exc
            finally:
                EVENT_BUFFER.reset(token)

            for event in buffer:
                if event.type in {"run.started", "run.resumed", "run.finished"}:
                    continue
                if event.type == "run.waiting":
                    raise WorkflowError("loop body nodes do not support waiting actions")
                if event.type == "run.failed":
                    error = WorkflowError(event.data.get("error", "loop iteration failed"))
                yield await engine._record_event(
                    self._namespace_loop_event(event, node["id"], iteration)
                )

            prefixed_nodes = {
                f"{node['id']}.iteration_{iteration}.{child_node_id}": deepcopy(record)
                for child_node_id, record in child_state.state.get("nodes", {}).items()
            }
            all_iteration_nodes.update(prefixed_nodes)
            run_state.state.setdefault("nodes", {}).update(prefixed_nodes)

            if error is not None:
                raise error

            last_output = self._loop_iteration_output(child_workflow, child_state)
            iteration_outputs.append(last_output)

            yield await engine._event(
                "loop.iteration_finished",
                run_state,
                node_id=node["id"],
                data={"iteration": iteration, "output": last_output},
            )

            iteration += 1

        output = {
            "iterations": iteration,
            "outputs": iteration_outputs,
            "last_output": iteration_outputs[-1] if iteration_outputs else None,
        }
        await engine._save_node_output(run_state, node["id"], output, node=node)

    def _build_loop_state(
        self,
        node: dict[str, Any],
        run_state: RunState,
        iteration: int,
        iteration_outputs: list[Any],
    ) -> dict[str, Any]:
        loop_state = deepcopy(run_state.state)
        loop_state.setdefault("nodes", {}).setdefault(node["id"], {})["output"] = {
            "iterations": iteration,
            "outputs": iteration_outputs,
            "last_output": iteration_outputs[-1] if iteration_outputs else None,
        }
        loop_state["loop"] = {
            "iteration": iteration,
            "outputs": iteration_outputs,
        }
        return loop_state

    def _create_loop_child(
        self,
        node: dict[str, Any],
        run_state: RunState,
        body_data: dict[str, Any],
        iteration: int,
    ) -> tuple[RunState, WorkflowConfig]:
        workflow_data = deepcopy(body_data)
        workflow_data.setdefault("id", f"{run_state.workflow_id}.{node['id']}.iteration_{iteration}")
        workflow_data.setdefault("version", run_state.workflow_version)
        child_workflow = WorkflowConfig.from_dict(workflow_data)

        child_input = render_template(node.get("input", run_state.state.get("input", {})), run_state.state)
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
                "loop": {
                    "iteration": iteration,
                },
            },
        )
        return child_state, child_workflow

    def _create_child_engine(self, engine: Any, child_workflow: WorkflowConfig) -> Any:
        engine_factory = type(cast(Any, self))
        return engine_factory(
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

    def _namespace_loop_event(
        self,
        event: WorkflowEvent,
        loop_node_id: str,
        iteration: int,
    ) -> WorkflowEvent:
        data = dict(event.data)
        data["loop_node_id"] = loop_node_id
        data["loop_iteration"] = iteration
        data["loop_event_type"] = event.type
        node_id = (
            f"{loop_node_id}.iteration_{iteration}.{event.node_id}"
            if event.node_id
            else loop_node_id
        )
        return WorkflowEvent(
            type=f"loop.{event.type}",
            run_id=event.run_id,
            node_id=node_id,
            data=data,
        )

    def _loop_iteration_output(
        self,
        workflow: WorkflowConfig,
        child_state: RunState,
    ) -> Any:
        if not workflow.nodes:
            return None
        output_node_id = workflow.nodes[-1]["id"]
        return deepcopy(
            child_state.state.get("nodes", {}).get(output_node_id, {}).get("output")
        )
