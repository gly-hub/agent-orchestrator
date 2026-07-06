"""Workflow graph routing helpers."""

from __future__ import annotations

from typing import Any

from agent_orchestrator.models import RunState, WorkflowConfig
from agent_orchestrator.state import evaluate_when


class WorkflowRouter:
    """Resolve the next workflow node from run state and edge rules."""

    def __init__(self, workflow: WorkflowConfig) -> None:
        self.workflow = workflow
        self._node_ids = [node["id"] for node in workflow.nodes]
        self._edges_by_from: dict[str, list[dict[str, Any]]] = {}
        for edge in workflow.edges:
            self._edges_by_from.setdefault(edge["from"], []).append(edge)

    def next_node_id(self, run_state: RunState) -> str | None:
        if run_state.current_node_id is None:
            return self._node_ids[0] if self._node_ids else None

        current_record = run_state.state.get("nodes", {}).get(run_state.current_node_id, {})
        if current_record.get("status") == "pending":
            return run_state.current_node_id

        edges = self._edges_by_from.get(run_state.current_node_id, [])
        if edges:
            for edge in edges:
                if not self._edge_matches_status(edge, current_record):
                    continue
                if evaluate_when(edge.get("when"), run_state.state):
                    return edge["to"]
            return None

        current_idx = self._node_ids.index(run_state.current_node_id)
        next_idx = current_idx + 1
        return self._node_ids[next_idx] if next_idx < len(self._node_ids) else None

    def has_error_edge(self, node_id: str) -> bool:
        return any(bool(edge.get("on_error")) for edge in self._edges_by_from.get(node_id, []))

    def _edge_matches_status(self, edge: dict[str, Any], current_record: dict[str, Any]) -> bool:
        on_error = bool(edge.get("on_error"))
        failed = current_record.get("status") == "failed"
        return failed if on_error else not failed
