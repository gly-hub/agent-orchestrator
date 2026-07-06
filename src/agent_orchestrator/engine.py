"""Workflow execution engine."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from agent_orchestrator.artifacts import ArtifactStore
from agent_orchestrator.checkpoint import CheckpointStore, InMemoryCheckpointStore
from agent_orchestrator.engine_runtime import EngineRuntimeMixin
from agent_orchestrator.events import EventStore, NoopEventStore
from agent_orchestrator.exceptions import WaitingForUser
from agent_orchestrator.execution import WorkflowExecutionContext
from agent_orchestrator.models import (
    PendingAction,
    ResumeRunRequest,
    RunState,
    StartRunRequest,
    WorkflowConfig,
    WorkflowEvent,
)
from agent_orchestrator.node_executors import BasicNodeExecutorMixin
from agent_orchestrator.observability import WorkflowObserver
from agent_orchestrator.parallel import ParallelNodeExecutorMixin
from agent_orchestrator.policy import DefaultToolPolicyGate, ToolPolicyGate
from agent_orchestrator.registry import AgentRegistry, ToolRegistry
from agent_orchestrator.retry import retry_delay_ms, should_retry
from agent_orchestrator.router import WorkflowRouter
from agent_orchestrator.subflow import SubflowNodeExecutorMixin
from agent_orchestrator.validation import validate_workflow_config

EngineErrorObserver = Callable[[Exception, RunState], Awaitable[None] | None]


class WorkflowEngine(
    EngineRuntimeMixin,
    BasicNodeExecutorMixin,
    ParallelNodeExecutorMixin,
    SubflowNodeExecutorMixin,
):
    """Execute configured workflows and stream normalized events.

    By default, node/runtime exceptions are converted into a terminal
    ``run.failed`` event so streaming clients can receive an explicit failure
    payload. Set ``raise_on_error=True`` when embedding the engine in services
    that should also propagate exceptions to the caller.
    """

    def __init__(
        self,
        workflow: WorkflowConfig,
        *,
        agents: AgentRegistry,
        tools: ToolRegistry,
        checkpoints: CheckpointStore | None = None,
        event_store: EventStore | None = None,
        artifact_store: ArtifactStore | None = None,
        artifact_threshold_bytes: int | None = None,
        pending_action_ttl_ms: int | None = 600_000,
        run_lease_ttl_ms: int | None = 600_000,
        policy_gate: ToolPolicyGate | None = None,
        raise_on_error: bool = False,
        error_observer: EngineErrorObserver | None = None,
        observer: WorkflowObserver | None = None,
    ) -> None:
        validate_workflow_config(workflow)
        self.workflow = workflow
        self.agents = agents
        self.tools = tools
        self.checkpoints = checkpoints or InMemoryCheckpointStore()
        self.event_store = event_store or NoopEventStore()
        self.artifact_store = artifact_store
        self.artifact_threshold_bytes = artifact_threshold_bytes
        self.pending_action_ttl_ms = pending_action_ttl_ms
        self.run_lease_ttl_ms = run_lease_ttl_ms
        self.policy_gate = policy_gate or DefaultToolPolicyGate()
        self.raise_on_error = raise_on_error
        self.error_observer = error_observer
        self.observer = observer
        self.execution = WorkflowExecutionContext(
            event_store=self.event_store,
            artifact_store=self.artifact_store,
            artifact_threshold_bytes=self.artifact_threshold_bytes,
            pending_action_ttl_ms=self.pending_action_ttl_ms,
            observer=self.observer,
        )
        self._nodes: dict[str, dict[str, Any]] = {node["id"]: dict(node) for node in workflow.nodes}
        self._router = WorkflowRouter(workflow)

    async def start(self, request: StartRunRequest) -> AsyncIterator[WorkflowEvent]:
        run_state = self._new_run_state(request)
        async for event in self._advance_public_run(run_state, initial_event_type="run.started"):
            yield event

    async def resume(
        self,
        *,
        pending_action_id: str | None = None,
        decision: dict[str, Any] | None = None,
        request: ResumeRunRequest | None = None,
    ) -> AsyncIterator[WorkflowEvent]:
        if request:
            pending_action_id = request.pending_action_id
            decision = request.decision
        if not pending_action_id or decision is None:
            raise ValueError("pending_action_id and decision are required")

        run_state = await self.checkpoints.resolve_action(pending_action_id, decision)
        async for event in self._advance_public_run(
            run_state,
            initial_event_type="run.resumed",
            initial_data={"pending_action_id": pending_action_id, "decision": decision},
        ):
            yield event

    async def _advance_public_run(
        self,
        run_state: RunState,
        *,
        initial_event_type: str,
        initial_data: dict[str, Any] | None = None,
    ) -> AsyncIterator[WorkflowEvent]:
        try:
            if self.run_lease_ttl_ms is None:
                yield await self._event(initial_event_type, run_state, data=initial_data)
                async for event in self._continue(run_state):
                    yield event
                return

            lease_run = getattr(self.checkpoints, "lease_run", None)
            if lease_run is None:
                yield await self._event(initial_event_type, run_state, data=initial_data)
                async for event in self._continue(run_state):
                    yield event
                return

            async with lease_run(run_state.run_id, ttl_ms=self.run_lease_ttl_ms):
                yield await self._event(initial_event_type, run_state, data=initial_data)
                async for event in self._continue(run_state):
                    yield event
        except Exception as exc:
            if self.raise_on_error and run_state.status == "failed":
                raise
            run_state.status = "failed"
            await self._observe_run_failed(exc, run_state)
            yield await self._run_failed_event(
                run_state,
                exc,
            )
            if self.raise_on_error:
                raise

    async def resume_expired_action(
        self,
        pending_action_id: str,
    ) -> AsyncIterator[WorkflowEvent]:
        action = await self.checkpoints.load_action(pending_action_id)
        if action.request.get("on_timeout") is None:
            await self.checkpoints.expire_action(pending_action_id)
            run_state = await self.checkpoints.load_run(action.run_id)
            run_state.status = "failed"
            yield await self._event(
                "human.expired",
                run_state,
                node_id=action.node_id,
                data={"pending_action_id": pending_action_id},
            )
            return

        async for event in self.resume(
            pending_action_id=pending_action_id,
            decision={"decision": "timeout"},
        ):
            yield event

    async def resume_expired_actions(
        self,
        *,
        now_ms: int | None = None,
    ) -> list[WorkflowEvent]:
        now_ms = now_ms if now_ms is not None else self._now_ms()
        events: list[WorkflowEvent] = []
        for action in await self.checkpoints.list_expired_actions(now_ms):
            async for event in self.resume_expired_action(action.id):
                events.append(event)
        return events

    async def _continue(self, run_state: RunState) -> AsyncIterator[WorkflowEvent]:
        node_id = self._router.next_node_id(run_state)

        try:
            while node_id:
                node = self._nodes[node_id]
                self.execution.start_node(run_state, node_id)
                await self.execution.observe_node_started(run_state, node_id)
                yield await self._event("node.started", run_state, node_id=node_id)

                try:
                    async for event in self._run_node_with_retry(node, run_state):
                        yield event
                except WaitingForUser:
                    raise
                except Exception as exc:
                    if not self._router.has_error_edge(node_id):
                        raise
                    node_record = self.execution.fail_node(run_state, node_id, exc)
                    await self.execution.observe_node_failed(run_state, node_id, node_record)
                    yield await self._event(
                        "node.failed",
                        run_state,
                        node_id=node_id,
                        data=node_record,
                    )

                node_record = self.execution.finish_node(run_state, node_id)
                await self.execution.observe_node_finished(run_state, node_id, node_record)
                yield await self._event("node.finished", run_state, node_id=node_id, data=node_record)
                node_id = self._router.next_node_id(run_state)

            run_state.status = "completed"
            yield await self._event("run.finished", run_state)
        except WaitingForUser as exc:
            run_state.status = "waiting_for_user"
            run_state.waiting_action_id = exc.pending_action_id
            await self.execution.observe(
                "run.waiting",
                run_state,
                node_id=run_state.current_node_id,
                data={"pending_action_id": exc.pending_action_id},
            )
            yield await self._event(
                "run.waiting",
                run_state,
                node_id=run_state.current_node_id,
                data={"pending_action_id": exc.pending_action_id},
            )
        except Exception as exc:
            run_state.status = "failed"
            await self._observe_run_failed(exc, run_state)
            yield await self._run_failed_event(
                run_state,
                exc,
            )
            if self.raise_on_error:
                raise

    async def _run_node_with_retry(
        self,
        node: dict[str, Any],
        run_state: RunState,
    ) -> AsyncIterator[WorkflowEvent]:
        retry = node.get("retry", {})
        max_attempts = int(retry.get("max_attempts", 1))
        delay_ms = int(retry.get("delay_ms", 0))
        max_delay_ms = retry.get("max_delay_ms")
        backoff_multiplier = float(retry.get("backoff_multiplier", 1))
        retry_on = tuple(str(item) for item in retry.get("retry_on", []))
        attempt = 1

        while True:
            node_record = run_state.state.setdefault("nodes", {}).setdefault(node["id"], {})
            node_record["attempt"] = attempt
            try:
                async for event in self._run_node_with_timeout(node, run_state):
                    yield event
                return
            except WaitingForUser:
                raise
            except Exception as exc:
                node_record["error"] = str(exc)
                node_record["error_type"] = type(exc).__name__
                if attempt >= max_attempts or not should_retry(exc, retry_on):
                    node_record["status"] = "failed"
                    raise
                next_delay_ms = retry_delay_ms(
                    base_delay_ms=delay_ms,
                    max_delay_ms=max_delay_ms,
                    backoff_multiplier=backoff_multiplier,
                    attempt=attempt,
                )
                yield await self._event(
                    "node.retrying",
                    run_state,
                    node_id=node["id"],
                    data={
                        "attempt": attempt,
                        "next_attempt": attempt + 1,
                        "max_attempts": max_attempts,
                        "delay_ms": next_delay_ms,
                        "error": str(exc),
                        "error_type": type(exc).__name__,
                    },
                )
                attempt += 1
                if next_delay_ms > 0:
                    await asyncio.sleep(next_delay_ms / 1000)

    async def _run_node_with_timeout(
        self,
        node: dict[str, Any],
        run_state: RunState,
    ) -> AsyncIterator[WorkflowEvent]:
        timeout_ms = node.get("timeout_ms")
        if timeout_ms is None:
            async for event in self._run_node(node, run_state):
                yield event
            return

        iterator = self._run_node(node, run_state).__aiter__()
        deadline = asyncio.get_running_loop().time() + (int(timeout_ms) / 1000)
        try:
            while True:
                remaining_seconds = deadline - asyncio.get_running_loop().time()
                if remaining_seconds <= 0:
                    raise TimeoutError
                try:
                    event = await asyncio.wait_for(anext(iterator), timeout=remaining_seconds)
                except StopAsyncIteration:
                    return
                yield event
        except TimeoutError:
            close = getattr(iterator, "aclose", None)
            if close is not None:
                await close()
            raise

    async def _run_node(
        self,
        node: dict[str, Any],
        run_state: RunState,
    ) -> AsyncIterator[WorkflowEvent]:
        node_type = node["type"]
        if node_type == "agent":
            async for event in self._run_agent_node(node, run_state):
                yield event
            return
        if node_type == "tool":
            async for event in self._run_tool_node(node, run_state):
                yield event
            return
        if node_type == "transform":
            await self._run_transform_node(node, run_state)
            return
        if node_type == "human":
            async for event in self._run_human_node(node, run_state):
                yield event
            return
        if node_type == "condition":
            await self._run_condition_node(node, run_state)
            return
        if node_type == "parallel":
            async for event in self._run_parallel_node(node, run_state):
                yield event
            return
        if node_type == "subflow":
            async for event in self._run_subflow_node(node, run_state):
                yield event
            return
        raise ValueError(f"unsupported node type: {node_type}")

    async def _pause_for_action(self, run_state: RunState, action: PendingAction) -> None:
        run_state.status = "waiting_for_user"
        run_state.waiting_action_id = action.id
        node_record = run_state.state.setdefault("nodes", {}).setdefault(action.node_id, {})
        node_record["status"] = "waiting"
        await self.checkpoints.save_waiting(run_state, action)
        raise WaitingForUser(action.id)
