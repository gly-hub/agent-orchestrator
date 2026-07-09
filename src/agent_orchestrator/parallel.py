"""Parallel node execution helpers."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator, Callable
from copy import deepcopy
from dataclasses import asdict, dataclass
from typing import Any, Protocol, cast

from agent_orchestrator.checkpoint import InMemoryCheckpointStore
from agent_orchestrator.exceptions import WaitingForUser, WorkflowError
from agent_orchestrator.models import PendingAction, RunState, WorkflowConfig, WorkflowEvent
from agent_orchestrator.runtime import EVENT_BUFFER, EventBuffer, drain
from agent_orchestrator.state import render_template

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class _ParallelEventItem:
    event: WorkflowEvent


@dataclass(slots=True)
class _ParallelResultItem:
    result: dict[str, Any]


_ParallelQueueItem = _ParallelEventItem | _ParallelResultItem


class _QueueEventSink:
    def __init__(self, queue: asyncio.Queue[_ParallelQueueItem]) -> None:
        self.queue = queue

    async def append(self, event: WorkflowEvent) -> None:
        await self.queue.put(_ParallelEventItem(event))


async def _append_to_event_sink(sink: EventBuffer, event: WorkflowEvent) -> None:
    if sink is None:
        return
    if isinstance(sink, list):
        sink.append(event)
        return
    await sink.append(event)


class _NamespacedParallelWorkflowEventSink:
    def __init__(
        self,
        parent_sink: EventBuffer,
        branch_id: str,
        namespace_event: Callable[[WorkflowEvent, str], WorkflowEvent],
    ) -> None:
        self.parent_sink = parent_sink
        self.branch_id = branch_id
        self.namespace_event = namespace_event
        self.control_events: list[WorkflowEvent] = []

    async def append(self, event: WorkflowEvent) -> None:
        if event.type in {"run.started", "run.resumed", "run.finished"}:
            return
        if event.type in {"run.waiting", "run.failed"}:
            self.control_events.append(event)
        namespaced = self.namespace_event(event, self.branch_id)
        await _append_to_event_sink(self.parent_sink, namespaced)


@dataclass(slots=True)
class _ParallelResultMerger:
    branches: list[dict[str, Any]]
    results_by_branch: dict[str, dict[str, Any]]
    run_state: RunState

    def merge(self) -> tuple[dict[str, Any], list[dict[str, str]]]:
        branch_outputs: dict[str, Any] = {}
        failed_branches: list[dict[str, str]] = []
        branch_nodes: dict[str, dict[str, Any]] = {}
        nodes_state = self.run_state.state.setdefault("nodes", {})

        for branch in self.branches:
            result = self.results_by_branch[branch["id"]]
            for node_id, record in result["records"].items():
                nodes_state[node_id] = deepcopy(record)
                if node_id != result["branch_id"]:
                    branch_nodes[node_id] = deepcopy(record)
            branch_record = nodes_state.get(result["branch_id"], {})
            branch_outputs[result["branch_id"]] = deepcopy(branch_record.get("output"))
            if result["error"] is not None:
                failed_branches.append(
                    {
                        "id": result["branch_id"],
                        "error": result["error"],
                        "error_type": result["error_type"],
                    }
                )

        output = {
            "branches": branch_outputs,
            "failed_branches": failed_branches,
        }
        if branch_nodes:
            output["nodes"] = branch_nodes
        return output, failed_branches


class _ParallelEngine(Protocol):
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

    def _run_node_with_retry(
        self,
        node: dict[str, Any],
        run_state: RunState,
    ) -> AsyncIterator[WorkflowEvent]: ...

    def _continue(self, run_state: RunState) -> AsyncIterator[WorkflowEvent]: ...

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

    def _now_ms(self) -> int: ...

    def _expires_at_ms(self) -> int | None: ...

    async def _pause_for_action(self, run_state: RunState, action: PendingAction) -> None: ...


class _ParallelBranchRunner:
    def __init__(self, owner: ParallelNodeExecutorMixinProtocol) -> None:
        self.owner = owner

    async def run_task(
        self,
        branch: dict[str, Any],
        run_state: RunState,
        original_node_ids: set[str],
        queue: asyncio.Queue[_ParallelQueueItem],
    ) -> None:
        try:
            result = await self.collect_branch(branch, run_state, original_node_ids)
        except Exception as exc:
            result = {
                "branch_id": branch["id"],
                "records": {},
                "error": str(exc),
                "error_type": type(exc).__name__,
            }
        await queue.put(_ParallelResultItem(result))

    async def collect_branch(
        self,
        branch: dict[str, Any],
        run_state: RunState,
        original_node_ids: set[str],
    ) -> dict[str, Any]:
        if "workflow" in branch and "type" not in branch:
            return await self.collect_workflow_branch(branch, run_state)

        branch_state = deepcopy(run_state)
        branch_state.current_node_id = branch["id"]
        error: Exception | None = None
        try:
            await self.owner._execute_branch_node(branch, branch_state)
        except Exception as exc:
            error = exc

        records = {}
        for node_id, record in branch_state.state.get("nodes", {}).items():
            if node_id == branch["id"] or node_id not in original_node_ids:
                records[node_id] = deepcopy(record)

        return {
            "branch_id": branch["id"],
            "records": records,
            "error": str(error) if error else None,
            "error_type": type(error).__name__ if error else None,
        }

    async def collect_workflow_branch(
        self,
        branch: dict[str, Any],
        run_state: RunState,
    ) -> dict[str, Any]:
        return await self.owner._collect_parallel_workflow_branch(branch, run_state)


class _ParallelScheduler:
    def __init__(
        self,
        owner: ParallelNodeExecutorMixinProtocol,
        branches: list[dict[str, Any]],
        run_state: RunState,
    ) -> None:
        self.owner = owner
        self.branches = branches
        self.run_state = run_state
        self.original_node_ids = set(run_state.state.get("nodes", {}))
        self.event_queue: asyncio.Queue[_ParallelQueueItem] = asyncio.Queue()
        self.runner = _ParallelBranchRunner(owner)

    def start(self) -> tuple[list[asyncio.Task[None]], EventBuffer]:
        parent_event_sink = EVENT_BUFFER.get()
        token = EVENT_BUFFER.set(_QueueEventSink(self.event_queue))
        try:
            tasks = [
                asyncio.create_task(
                    self.runner.run_task(
                        branch,
                        self.run_state,
                        self.original_node_ids,
                        self.event_queue,
                    ),
                    name=f"parallel:{branch['id']}",
                )
                for branch in self.branches
            ]
        finally:
            EVENT_BUFFER.reset(token)
        return tasks, parent_event_sink

class ParallelNodeExecutorMixinProtocol(Protocol):
    async def _execute_branch_node(self, node: dict[str, Any], run_state: RunState) -> None: ...

    async def _collect_parallel_workflow_branch(
        self,
        branch: dict[str, Any],
        run_state: RunState,
    ) -> dict[str, Any]: ...

    async def _record_parallel_event(
        self,
        event: WorkflowEvent,
        parent_event_sink: EventBuffer,
    ) -> WorkflowEvent: ...


def _branch_contains_human(branch: dict[str, Any]) -> bool:
    if branch.get("type") == "human":
        return True
    workflow = branch.get("workflow")
    if isinstance(workflow, dict):
        for node in workflow.get("nodes", []):
            if isinstance(node, dict) and _branch_contains_human(node):
                return True
    return False


class ParallelNodeExecutorMixin:
    """Executor for parallel workflow nodes."""

    async def _run_parallel_node(
        self,
        node: dict[str, Any],
        run_state: RunState,
    ) -> AsyncIterator[WorkflowEvent]:
        branches = list(node.get("branches", []))
        logger.debug("parallel node %s starting %d branches", node["id"], len(branches))

        has_human = any(_branch_contains_human(b) for b in branches)
        if has_human:
            async for event in self._run_parallel_node_sequential(node, run_state, branches):
                yield event
        else:
            async for event in self._run_parallel_node_concurrent(node, run_state, branches):
                yield event

    async def _run_parallel_node_concurrent(
        self,
        node: dict[str, Any],
        run_state: RunState,
        branches: list[dict[str, Any]],
    ) -> AsyncIterator[WorkflowEvent]:
        engine = cast(_ParallelEngine, self)
        scheduler = _ParallelScheduler(cast(ParallelNodeExecutorMixinProtocol, self), branches, run_state)
        tasks, parent_event_sink = scheduler.start()
        results_by_branch: dict[str, dict[str, Any]] = {}
        try:
            remaining_results = len(tasks)
            while remaining_results:
                item = await scheduler.event_queue.get()
                if isinstance(item, _ParallelEventItem):
                    yield await self._record_parallel_event(item.event, parent_event_sink)
                    continue
                result = item.result
                results_by_branch[result["branch_id"]] = result
                remaining_results -= 1
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        output, failed_branches = _ParallelResultMerger(
            branches,
            results_by_branch,
            run_state,
        ).merge()
        await engine._save_node_output(run_state, node["id"], output, node=node)

        failure_policy = node.get("failure_policy", node.get("partial_failure_policy", "fail"))
        if failed_branches and failure_policy == "fail":
            failed_ids = ", ".join(branch["id"] for branch in failed_branches)
            raise WorkflowError(f"parallel branches failed: {failed_ids}")

    async def _run_parallel_node_sequential(
        self,
        node: dict[str, Any],
        run_state: RunState,
        branches: list[dict[str, Any]],
    ) -> AsyncIterator[WorkflowEvent]:
        engine = cast(_ParallelEngine, self)
        node_record = run_state.state.setdefault("nodes", {}).setdefault(node["id"], {})
        results_by_branch: dict[str, dict[str, Any]] = node_record.pop("_seq_completed", {})
        original_node_ids = set(run_state.state.get("nodes", {}))
        resumed_branch_id = node_record.pop("_seq_waiting_branch", None)
        resumed_decision = node_record.pop("approval", None)

        for branch in branches:
            if branch["id"] in results_by_branch:
                continue

            if branch["id"] == resumed_branch_id and resumed_decision is not None:
                result = await self._resume_sequential_branch(
                    engine, branch, run_state, node, resumed_decision, node_record,
                )
            else:
                result = await self._run_sequential_branch(
                    engine, branch, run_state, node, original_node_ids,
                )

            if result is not None and result.get("_waiting"):
                node_record["_seq_completed"] = results_by_branch
                action = result["_action"]
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
                return

            if result is None:
                return

            results_by_branch[branch["id"]] = result

        output, failed_branches = _ParallelResultMerger(
            branches,
            results_by_branch,
            run_state,
        ).merge()
        await engine._save_node_output(run_state, node["id"], output, node=node)

        failure_policy = node.get("failure_policy", node.get("partial_failure_policy", "fail"))
        if failed_branches and failure_policy == "fail":
            failed_ids = ", ".join(branch["id"] for branch in failed_branches)
            raise WorkflowError(f"parallel branches failed: {failed_ids}")

    async def _run_sequential_branch(
        self,
        engine: Any,
        branch: dict[str, Any],
        run_state: RunState,
        node: dict[str, Any],
        original_node_ids: set[str],
    ) -> dict[str, Any] | None:
        if "workflow" in branch and "type" not in branch:
            return await self._run_sequential_workflow_branch(
                engine, branch, run_state, node,
            )

        branch_state = deepcopy(run_state)
        branch_state.current_node_id = branch["id"]
        error: Exception | None = None
        try:
            await self._execute_branch_node(branch, branch_state)
        except Exception as exc:
            error = exc

        records = {}
        for nid, record in branch_state.state.get("nodes", {}).items():
            if nid == branch["id"] or nid not in original_node_ids:
                records[nid] = deepcopy(record)

        return {
            "branch_id": branch["id"],
            "records": records,
            "error": str(error) if error else None,
            "error_type": type(error).__name__ if error else None,
        }

    async def _run_sequential_workflow_branch(
        self,
        engine: Any,
        branch: dict[str, Any],
        run_state: RunState,
        node: dict[str, Any],
    ) -> dict[str, Any] | None:
        child_checkpoints = InMemoryCheckpointStore()
        workflow_data = deepcopy(branch["workflow"])
        workflow_data.setdefault("id", f"{run_state.workflow_id}.{branch['id']}")
        workflow_data.setdefault("version", run_state.workflow_version)
        child_workflow = WorkflowConfig.from_dict(workflow_data)
        child_input = render_template(branch.get("input", run_state.state.get("input", {})), run_state.state)
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

        result = await self._execute_parallel_child(
            engine, branch, run_state, node, child_workflow, child_state, child_checkpoints,
        )
        return result

    async def _execute_parallel_child(
        self,
        engine: Any,
        branch: dict[str, Any],
        run_state: RunState,
        node: dict[str, Any],
        child_workflow: WorkflowConfig,
        child_state: RunState,
        child_checkpoints: InMemoryCheckpointStore,
    ) -> dict[str, Any] | None:
        engine_factory = cast(Any, type(self))
        child_engine = engine_factory(
            child_workflow,
            agents=engine.agents,
            tools=engine.tools,
            checkpoints=child_checkpoints,
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
        error: Exception | None = None
        try:
            child_runtime = cast(_ParallelEngine, child_engine)
            await drain(child_runtime._continue(child_state))
        except Exception as exc:
            error = exc
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
                error = WorkflowError(event.data.get("error", "parallel workflow branch failed"))
            await engine._record_event(self._namespace_branch_event(event, branch["id"]))

        if waiting_event is not None:
            child_action_id = waiting_event.data.get("pending_action_id", "")
            child_action = await child_checkpoints.load_action(child_action_id)

            node_record = run_state.state.setdefault("nodes", {}).setdefault(node["id"], {})
            completed = node_record.get("_seq_completed", {})
            node_record["_seq_completed"] = completed
            node_record["_seq_waiting_branch"] = branch["id"]
            node_record["_seq_child_state"] = asdict(child_state)
            node_record["_seq_child_workflow"] = {
                "id": child_workflow.id,
                "version": child_workflow.version,
                "nodes": [dict(n) for n in child_workflow.nodes],
                "edges": list(child_workflow.edges),
                "policy": dict(child_workflow.policy),
            }
            node_record["_seq_child_human_node_id"] = child_action.node_id

            action = PendingAction(
                id=f"pa_{uuid.uuid4().hex}",
                run_id=run_state.run_id,
                node_id=node["id"],
                action_type="parallel_human",
                request={
                    **child_action.request,
                    "child_pending_action_id": child_action_id,
                    "child_human_node_id": child_action.node_id,
                    "parallel_branch_id": branch["id"],
                },
                created_at_ms=engine._now_ms(),
                expires_at_ms=engine._expires_at_ms(),
            )
            return {"_waiting": True, "_action": action}

        prefixed_records = {
            f"{branch['id']}.{nid}": deepcopy(record)
            for nid, record in child_state.state.get("nodes", {}).items()
        }
        selected_output = self._parallel_workflow_output(branch, child_workflow, child_state)
        branch_record = {
            "status": "failed" if error else "success",
            "output": selected_output,
            "workflow_id": child_workflow.id,
            "nodes": prefixed_records,
        }
        if error:
            branch_record["error"] = str(error)
            branch_record["error_type"] = type(error).__name__
            branch_record["output"] = {
                "failed": True,
                "error": str(error),
                "error_type": type(error).__name__,
            }
        records = {branch["id"]: branch_record, **prefixed_records}
        return {
            "branch_id": branch["id"],
            "records": records,
            "error": str(error) if error else None,
            "error_type": type(error).__name__ if error else None,
        }

    async def _resume_sequential_branch(
        self,
        engine: Any,
        branch: dict[str, Any],
        run_state: RunState,
        node: dict[str, Any],
        decision: dict[str, Any],
        node_record: dict[str, Any],
    ) -> dict[str, Any] | None:
        child_state_data = node_record.pop("_seq_child_state")
        child_workflow_data = node_record.pop("_seq_child_workflow")
        child_human_node_id = node_record.pop("_seq_child_human_node_id")

        child_state = RunState(**child_state_data)
        child_state.status = "running"
        child_state.waiting_action_id = None
        child_human_record = child_state.state["nodes"][child_human_node_id]
        child_human_record["status"] = "success"
        child_human_record["output"] = decision

        child_workflow = WorkflowConfig.from_dict(child_workflow_data, validate=False)
        child_checkpoints = InMemoryCheckpointStore()

        return await self._execute_parallel_child(
            engine, branch, run_state, node, child_workflow, child_state, child_checkpoints,
        )

    async def _record_parallel_event(
        self,
        event: WorkflowEvent,
        parent_event_sink: EventBuffer,
    ) -> WorkflowEvent:
        engine = cast(_ParallelEngine, self)
        token = EVENT_BUFFER.set(parent_event_sink)
        try:
            return await engine._record_event(event)
        finally:
            EVENT_BUFFER.reset(token)

    async def _collect_parallel_workflow_branch(
        self,
        branch: dict[str, Any],
        run_state: RunState,
    ) -> dict[str, Any]:
        engine = cast(_ParallelEngine, self)
        workflow_data = deepcopy(branch["workflow"])
        workflow_data.setdefault("id", f"{run_state.workflow_id}.{branch['id']}")
        workflow_data.setdefault("version", run_state.workflow_version)
        child_workflow = WorkflowConfig.from_dict(workflow_data)
        child_input = render_template(branch.get("input", run_state.state.get("input", {})), run_state.state)
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
        parent_event_sink = EVENT_BUFFER.get()
        child_event_sink = _NamespacedParallelWorkflowEventSink(
            parent_event_sink,
            branch["id"],
            self._namespace_branch_event,
        )
        token = EVENT_BUFFER.set(child_event_sink)
        error: Exception | None = None
        try:
            child_runtime = cast(_ParallelEngine, child_engine)
            await drain(child_runtime._continue(child_state))
        except Exception as exc:
            error = exc
        finally:
            EVENT_BUFFER.reset(token)

        for event in child_event_sink.control_events:
            if event.type == "run.waiting":
                error = WorkflowError("parallel workflow branches do not support waiting actions")
            if event.type == "run.failed":
                error = WorkflowError(event.data.get("error", "parallel workflow branch failed"))

        prefixed_records = {
            f"{branch['id']}.{node_id}": deepcopy(record)
            for node_id, record in child_state.state.get("nodes", {}).items()
        }
        selected_output = self._parallel_workflow_output(branch, child_workflow, child_state)
        branch_record = {
            "status": "failed" if error else "success",
            "output": selected_output,
            "workflow_id": child_workflow.id,
            "nodes": prefixed_records,
        }
        if error:
            branch_record["error"] = str(error)
            branch_record["error_type"] = type(error).__name__
            branch_record["output"] = {
                "failed": True,
                "error": str(error),
                "error_type": type(error).__name__,
            }
        records = {branch["id"]: branch_record, **prefixed_records}
        return {
            "branch_id": branch["id"],
            "records": records,
            "error": str(error) if error else None,
            "error_type": type(error).__name__ if error else None,
        }

    def _namespace_branch_event(self, event: WorkflowEvent, branch_id: str) -> WorkflowEvent:
        data = dict(event.data)
        data["parallel_branch_id"] = branch_id
        data["parallel_event_type"] = event.type
        node_id = f"{branch_id}.{event.node_id}" if event.node_id else branch_id
        return WorkflowEvent(
            type=f"parallel.{event.type}",
            run_id=event.run_id,
            node_id=node_id,
            data=data,
        )

    def _parallel_workflow_output(
        self,
        branch: dict[str, Any],
        workflow: WorkflowConfig,
        child_state: RunState,
    ) -> Any:
        if "output" in branch:
            return render_template(branch["output"], child_state.state)
        if not workflow.nodes:
            return None
        output_node_id = workflow.nodes[-1]["id"]
        return deepcopy(child_state.state.get("nodes", {}).get(output_node_id, {}).get("output"))

    async def _execute_branch_node(self, node: dict[str, Any], run_state: RunState) -> None:
        engine = cast(_ParallelEngine, self)
        node_record = run_state.state.setdefault("nodes", {}).setdefault(node["id"], {})
        node_record["status"] = "running"
        node_record["started_at_ms"] = engine._now_ms()
        await engine._event("node.started", run_state, node_id=node["id"])

        try:
            await drain(engine._run_node_with_retry(node, run_state))
        except WaitingForUser as exc:
            node_record["status"] = "failed"
            node_record["error"] = "parallel branches do not support waiting actions"
            node_record["error_type"] = type(exc).__name__
            node_record["output"] = {
                "failed": True,
                "error": node_record["error"],
                "error_type": node_record["error_type"],
            }
            await engine._event("node.failed", run_state, node_id=node["id"], data=node_record)
            raise WorkflowError(node_record["error"]) from exc
        except Exception as exc:
            node_record["status"] = "failed"
            node_record["error"] = str(exc)
            node_record["error_type"] = type(exc).__name__
            node_record["output"] = {
                "failed": True,
                "error": str(exc),
                "error_type": type(exc).__name__,
            }
            await engine._event("node.failed", run_state, node_id=node["id"], data=node_record)
            raise
        finally:
            node_record = run_state.state.setdefault("nodes", {}).setdefault(node["id"], {})
            if node_record.get("status") not in {"success", "failed"}:
                node_record["status"] = "success"
            node_record["finished_at_ms"] = engine._now_ms()
            node_record["duration_ms"] = node_record["finished_at_ms"] - node_record["started_at_ms"]
            await engine._event("node.finished", run_state, node_id=node["id"], data=node_record)
