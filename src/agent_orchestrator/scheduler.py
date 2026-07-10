"""DAG scheduler for workflow execution."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import AsyncIterator
from dataclasses import dataclass, replace
from typing import Any

from agent_orchestrator.engine_protocol import EngineProtocol
from agent_orchestrator.exceptions import WaitingForUser
from agent_orchestrator.models import RunState, WorkflowConfig, WorkflowEvent
from agent_orchestrator.state import evaluate_when

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class SchedulerEdge:
    id: str
    from_id: str
    to_id: str
    data: dict[str, Any]


@dataclass(slots=True)
class _SchedulerEventItem:
    event: WorkflowEvent


@dataclass(slots=True)
class _SchedulerResultItem:
    node_id: str
    error: Exception | None = None
    waiting_action_id: str | None = None


_SchedulerQueueItem = _SchedulerEventItem | _SchedulerResultItem



class WorkflowGraph:
    """Runtime graph index built only from explicit workflow edges."""

    def __init__(self, workflow: WorkflowConfig) -> None:
        self.node_ids = [node["id"] for node in workflow.nodes]
        self.edges: list[SchedulerEdge] = []

        for index, edge in enumerate(workflow.edges):
            edge_id = str(edge.get("id") or f"edge:{index}:{edge['from']}->{edge['to']}")
            self.edges.append(
                SchedulerEdge(
                    id=edge_id,
                    from_id=edge["from"],
                    to_id=edge["to"],
                    data=dict(edge),
                )
            )

        self.outgoing: dict[str, list[SchedulerEdge]] = defaultdict(list)
        self.incoming: dict[str, list[SchedulerEdge]] = defaultdict(list)
        for edge in self.edges:
            self.outgoing[edge.from_id].append(edge)
            self.incoming[edge.to_id].append(edge)

        self.entry_node_ids = [
            node_id for node_id in self.node_ids if not self.incoming.get(node_id)
        ]

    def has_error_edge(self, node_id: str) -> bool:
        return any(bool(edge.data.get("on_error")) for edge in self.outgoing.get(node_id, []))


class DagScheduler:
    """Execute a workflow as a DAG of ready nodes."""

    def __init__(
        self,
        engine: EngineProtocol,
        workflow: WorkflowConfig,
        nodes: dict[str, dict[str, Any]],
    ) -> None:
        self.engine = engine
        self.workflow = workflow
        self.nodes = nodes
        self.graph = WorkflowGraph(workflow)

    async def run(self, run_state: RunState) -> AsyncIterator[WorkflowEvent]:
        self._ensure_scheduler_state(run_state)

        try:
            while True:
                self._process_completed_nodes(run_state)
                ready = self._ready_node_ids(run_state)
                if ready:
                    async for event in self._run_ready_nodes(run_state, ready):
                        yield event
                    continue

                running = self._running_node_ids(run_state)
                if running:
                    continue

                waiting_actions = self._waiting_actions(run_state)
                if waiting_actions:
                    await self._save_waiting_checkpoint(run_state, waiting_actions)
                    run_state.status = "waiting_for_user"
                    pending_action_ids = sorted(waiting_actions)
                    data: dict[str, Any] = {"pending_action_ids": pending_action_ids}
                    if len(pending_action_ids) == 1:
                        data["pending_action_id"] = pending_action_ids[0]
                        run_state.waiting_action_id = pending_action_ids[0]
                    else:
                        run_state.waiting_action_id = None
                    logger.info(
                        "run %s waiting for user actions %s",
                        run_state.run_id,
                        ", ".join(pending_action_ids),
                    )
                    await self.engine.execution.observe(
                        "run.waiting",
                        run_state,
                        node_id=run_state.current_node_id,
                        data=data,
                    )
                    yield await self.engine._event(
                        "run.waiting",
                        run_state,
                        node_id=run_state.current_node_id,
                        data=data,
                    )
                    return

                if self._mark_unactivated_pending_nodes_skipped(run_state):
                    continue
                run_state.status = "completed"
                logger.info("run %s completed", run_state.run_id)
                yield await self.engine._event("run.finished", run_state)
                return
        except Exception as exc:
            run_state.status = "failed"
            logger.error(
                "run %s failed at node %s: %s",
                run_state.run_id,
                run_state.current_node_id,
                exc,
            )
            await self.engine._observe_run_failed(exc, run_state)
            yield await self.engine._run_failed_event(run_state, exc)
            if self.engine.raise_on_error:
                raise

    def _ensure_scheduler_state(self, run_state: RunState) -> None:
        run_state.state.setdefault("nodes", {})
        internal = run_state.state.setdefault("_internal", {})
        edge_state = internal.setdefault("edges", {})
        scheduler = internal.setdefault("scheduler", {})
        scheduler.setdefault("entry_node_ids", list(self.graph.entry_node_ids))
        scheduler.setdefault("ready_node_ids", [])
        scheduler.setdefault("running_node_ids", [])
        scheduler.setdefault("waiting_actions", {})
        scheduler.setdefault("completed_node_ids", [])
        scheduler.setdefault("failed_node_ids", [])
        scheduler.setdefault("skipped_node_ids", [])

        for edge in self.graph.edges:
            edge_state.setdefault(
                edge.id,
                {
                    "from": edge.from_id,
                    "to": edge.to_id,
                    "status": "inactive",
                    "reason": None,
                },
            )

        for node_id in self.graph.entry_node_ids:
            self._node_record(run_state, node_id).setdefault("activated", True)

    async def _run_ready_nodes(
        self,
        run_state: RunState,
        node_ids: list[str],
    ) -> AsyncIterator[WorkflowEvent]:
        queue: asyncio.Queue[_SchedulerQueueItem] = asyncio.Queue()
        tasks: list[asyncio.Task[None]] = []
        remaining = 0

        def start_nodes(start_node_ids: list[str]) -> None:
            nonlocal remaining
            for node_id in start_node_ids:
                record = self._node_record(run_state, node_id)
                if record.get("status", "pending") != "pending":
                    continue
                record["status"] = "ready"
                self._scheduler(run_state)["ready_node_ids"].append(node_id)
                tasks.append(
                    asyncio.create_task(
                        self._run_one_node(node_id, run_state, queue),
                        name=f"dag:{node_id}",
                    )
                )
                remaining += 1

        start_nodes(node_ids)
        try:
            while remaining:
                item = await queue.get()
                if isinstance(item, _SchedulerEventItem):
                    yield item.event
                    continue
                remaining -= 1
                if item.error is not None:
                    run_state.current_node_id = item.node_id
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    raise item.error
                self._process_completed_nodes(run_state)
                start_nodes(self._ready_node_ids(run_state))
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_one_node(
        self,
        node_id: str,
        run_state: RunState,
        queue: asyncio.Queue[_SchedulerQueueItem],
    ) -> None:
        node = self.nodes[node_id]
        node_run_state = replace(run_state, current_node_id=node_id)
        scheduler = self._scheduler(run_state)
        self._remove_once(scheduler["ready_node_ids"], node_id)
        scheduler["running_node_ids"].append(node_id)
        self.engine.execution.start_node(node_run_state, node_id)
        await self.engine.execution.observe_node_started(node_run_state, node_id)
        await queue.put(_SchedulerEventItem(await self.engine._event("node.started", node_run_state, node_id=node_id)))

        try:
            try:
                async for event in self.engine._run_node_with_retry(node, node_run_state):
                    await queue.put(_SchedulerEventItem(event))
            except WaitingForUser as exc:
                self._remove_once(scheduler["running_node_ids"], node_id)
                run_state.status = "running"
                run_state.waiting_action_id = None
                self._waiting_actions(run_state).setdefault(
                    exc.pending_action_id,
                    {"node_id": node_id, "action_type": "human"},
                )
                await queue.put(_SchedulerResultItem(node_id=node_id, waiting_action_id=exc.pending_action_id))
                return
            except Exception as exc:
                if not self.graph.has_error_edge(node_id):
                    self._remove_once(scheduler["running_node_ids"], node_id)
                    await queue.put(_SchedulerResultItem(node_id=node_id, error=exc))
                    return
                node_record = self.engine.execution.fail_node(node_run_state, node_id, exc)
                await self.engine.execution.observe_node_failed(node_run_state, node_id, node_record)
                await queue.put(
                    _SchedulerEventItem(
                        await self.engine._event("node.failed", node_run_state, node_id=node_id, data=node_record)
                    )
                )
                node_record["finished_at_ms"] = self.engine.execution.now_ms()
                node_record["duration_ms"] = node_record["finished_at_ms"] - node_record.get("started_at_ms", 0)
                self._remove_once(scheduler["running_node_ids"], node_id)
                scheduler["failed_node_ids"].append(node_id)
                self._activate_outgoing_edges(run_state, node_id)
                await queue.put(_SchedulerResultItem(node_id=node_id))
                return

            node_record = self.engine.execution.finish_node(node_run_state, node_id)
            await self.engine.execution.observe_node_finished(node_run_state, node_id, node_record)
            await queue.put(
                _SchedulerEventItem(
                    await self.engine._event("node.finished", node_run_state, node_id=node_id, data=node_record)
                )
            )
            self._remove_once(scheduler["running_node_ids"], node_id)
            if node_record.get("status") == "failed":
                scheduler["failed_node_ids"].append(node_id)
            else:
                scheduler["completed_node_ids"].append(node_id)
            self._activate_outgoing_edges(run_state, node_id)
            await queue.put(_SchedulerResultItem(node_id=node_id))
        except Exception as exc:
            self._remove_once(scheduler["running_node_ids"], node_id)
            await queue.put(_SchedulerResultItem(node_id=node_id, error=exc))

    def _process_completed_nodes(self, run_state: RunState) -> None:
        for node_id in self.graph.node_ids:
            record = self._node_record(run_state, node_id)
            if record.get("_dag_outgoing_processed"):
                continue
            if record.get("status") in {"success", "failed"}:
                self._activate_outgoing_edges(run_state, node_id)

    def _activate_outgoing_edges(self, run_state: RunState, node_id: str) -> None:
        record = self._node_record(run_state, node_id)
        if record.get("_dag_outgoing_processed"):
            return

        failed = record.get("status") == "failed"
        for edge in self.graph.outgoing.get(node_id, []):
            is_error_edge = bool(edge.data.get("on_error"))
            if failed != is_error_edge:
                self._set_edge_status(run_state, edge, "skipped", "status_mismatch")
                continue
            if evaluate_when(edge.data.get("when"), run_state.state):
                self._set_edge_status(run_state, edge, "active", None)
                target = self._node_record(run_state, edge.to_id)
                target["activated"] = True
            else:
                self._set_edge_status(run_state, edge, "skipped", "when_false")

        record["_dag_outgoing_processed"] = True

    def _ready_node_ids(self, run_state: RunState) -> list[str]:
        ready: list[str] = []
        for node_id in self.graph.node_ids:
            record = self._node_record(run_state, node_id)
            if record.get("status", "pending") != "pending":
                continue
            if not record.get("activated"):
                continue
            if self._is_node_ready(run_state, node_id):
                ready.append(node_id)
        return ready

    def _is_node_ready(self, run_state: RunState, node_id: str) -> bool:
        incoming = self.graph.incoming.get(node_id, [])
        if not incoming:
            return True

        join_policy = self.nodes[node_id].get("join_policy", "all_active")
        if join_policy == "any":
            return any(
                self._edge_record(run_state, edge).get("status") == "active"
                for edge in incoming
            )
        if join_policy not in {"all_active", "all_success"}:
            raise ValueError(f"unsupported join_policy for {node_id}: {join_policy}")

        saw_active_input = False
        for edge in incoming:
            edge_record = self._edge_record(run_state, edge)
            status = edge_record.get("status")
            if status == "active":
                saw_active_input = True
                if join_policy == "all_success":
                    predecessor = self._node_record(run_state, edge.from_id)
                    if predecessor.get("status") != "success":
                        return False
                continue
            if status == "skipped":
                continue
            if join_policy == "all_success":
                return False
            predecessor = self._node_record(run_state, edge.from_id)
            if predecessor.get("activated") and predecessor.get("status", "pending") != "skipped":
                return False
        return saw_active_input

    def _mark_unactivated_pending_nodes_skipped(self, run_state: RunState) -> bool:
        scheduler = self._scheduler(run_state)
        changed = False
        for node_id in self.graph.node_ids:
            record = self._node_record(run_state, node_id)
            if record.get("status", "pending") != "pending" or record.get("activated"):
                continue
            incoming = self.graph.incoming.get(node_id, [])
            if not incoming:
                continue
            if not all(self._edge_record(run_state, edge).get("status") == "skipped" for edge in incoming):
                continue
            if record.get("status", "pending") == "pending" and not record.get("activated"):
                record["status"] = "skipped"
                scheduler["skipped_node_ids"].append(node_id)
                changed = True
                for edge in self.graph.outgoing.get(node_id, []):
                    self._set_edge_status(run_state, edge, "skipped", "source_skipped")
        return changed

    async def _save_waiting_checkpoint(
        self,
        run_state: RunState,
        waiting_actions: dict[str, Any],
    ) -> None:
        for action_id in waiting_actions:
            action = await self.engine.checkpoints.load_action(action_id)
            await self.engine.checkpoints.save_waiting(run_state, action)

    def _set_edge_status(
        self,
        run_state: RunState,
        edge: SchedulerEdge,
        status: str,
        reason: str | None,
    ) -> None:
        edge_record = self._edge_record(run_state, edge)
        edge_record["status"] = status
        edge_record["reason"] = reason

    def _node_record(self, run_state: RunState, node_id: str) -> dict[str, Any]:
        return run_state.state.setdefault("nodes", {}).setdefault(node_id, {"status": "pending"})

    def _edge_record(self, run_state: RunState, edge: SchedulerEdge) -> dict[str, Any]:
        return run_state.state.setdefault("_internal", {}).setdefault("edges", {}).setdefault(
            edge.id,
            {
                "from": edge.from_id,
                "to": edge.to_id,
                "status": "inactive",
                "reason": None,
            },
        )

    def _scheduler(self, run_state: RunState) -> dict[str, Any]:
        return run_state.state.setdefault("_internal", {}).setdefault("scheduler", {})

    def _waiting_actions(self, run_state: RunState) -> dict[str, Any]:
        return self._scheduler(run_state).setdefault("waiting_actions", {})

    def _running_node_ids(self, run_state: RunState) -> list[str]:
        return list(self._scheduler(run_state).get("running_node_ids", []))

    def _remove_once(self, values: list[str], value: str) -> None:
        try:
            values.remove(value)
        except ValueError:
            return
