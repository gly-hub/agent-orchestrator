"""Workflow config graph validation helpers."""

from __future__ import annotations

from typing import Any

from agent_orchestrator.exceptions import StateResolutionError, WorkflowConfigError
from agent_orchestrator.state import validate_when_syntax


def validate_edges(edges: list[dict[str, Any]], node_ids: set[str]) -> None:
    for index, edge in enumerate(edges):
        if not isinstance(edge, dict):
            raise WorkflowConfigError(f"edge at index {index} must be a mapping")

        from_id = edge.get("from")
        to_id = edge.get("to")
        if from_id not in node_ids:
            raise WorkflowConfigError(f"edge at index {index} references unknown from node: {from_id}")
        if to_id not in node_ids:
            raise WorkflowConfigError(f"edge at index {index} references unknown to node: {to_id}")
        if "on_error" in edge and not isinstance(edge["on_error"], bool):
            raise WorkflowConfigError(f"edge at index {index} on_error must be a boolean")
        if "when" in edge:
            validate_when_expression(f"edge {index}", edge["when"])


def validate_when_expression(label: str, expression: Any) -> None:
    if expression is None:
        return
    if not isinstance(expression, str):
        raise WorkflowConfigError(f"{label} when must be a string")

    expr = expression.strip()
    if not expr:
        return
    try:
        validate_when_syntax(expr)
    except StateResolutionError as exc:
        raise WorkflowConfigError(
            f"{label} when uses unsupported syntax: {exc}"
        ) from exc


def validate_no_simple_cycles(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> None:
    adjacency: dict[str, list[str]] = {node["id"]: [] for node in nodes}
    for edge in edges:
        adjacency[edge["from"]].append(edge["to"])

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node_id: str, stack: list[str]) -> None:
        if node_id in visiting:
            cycle = " -> ".join([*stack, node_id])
            raise WorkflowConfigError(f"workflow contains a cycle: {cycle}")
        if node_id in visited:
            return

        visiting.add(node_id)
        for child_id in adjacency[node_id]:
            visit(child_id, [*stack, node_id])
        visiting.remove(node_id)
        visited.add(node_id)

    for node_id in adjacency:
        visit(node_id, [])
