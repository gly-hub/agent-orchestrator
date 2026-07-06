"""Typed workflow node definitions and normalization helpers."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Literal, NotRequired, TypedDict, cast


class BaseNode(TypedDict):
    id: str
    type: str
    input: NotRequired[Any]
    output: NotRequired[Any]
    input_schema: NotRequired[dict[str, Any]]
    output_schema: NotRequired[dict[str, Any]]
    retry: NotRequired[dict[str, Any]]
    timeout_ms: NotRequired[int]
    on_timeout: NotRequired[str | dict[str, Any]]
    output_artifact: NotRequired[bool]
    artifact_threshold_bytes: NotRequired[int]
    resolve_input_artifacts: NotRequired[bool]


class AgentNode(BaseNode):
    type: Literal["agent"]
    agent: str


class ToolNode(BaseNode):
    type: Literal["tool"]
    tool: str
    args: NotRequired[dict[str, Any]]
    title: NotRequired[str]
    permissions: NotRequired[list[str]]
    required_permissions: NotRequired[list[str]]
    risk_level: NotRequired[Literal["low", "medium", "high"]]
    confirmation_policy: NotRequired[Literal["never", "always", "risk_based"]]


class TransformNode(BaseNode):
    type: Literal["transform"]


class HumanNode(BaseNode):
    type: Literal["human"]
    title: NotRequired[str]
    message: NotRequired[str]
    options: NotRequired[list[dict[str, Any]]]
    fields: NotRequired[list[dict[str, Any]]]
    response_schema: NotRequired[dict[str, Any]]


class ConditionCase(TypedDict):
    when: str
    value: Any


class ConditionNode(BaseNode):
    type: Literal["condition"]
    cases: NotRequired[list[ConditionCase]]
    default: NotRequired[Any]


class ParallelWorkflowBranch(TypedDict):
    id: str
    input: NotRequired[Any]
    output: NotRequired[Any]
    workflow: SubflowWorkflow


class ParallelNode(BaseNode):
    type: Literal["parallel"]
    branches: list[WorkflowNode | ParallelWorkflowBranch]
    failure_policy: NotRequired[Literal["fail", "continue"]]
    partial_failure_policy: NotRequired[Literal["fail", "continue"]]


class SubflowWorkflow(TypedDict):
    id: NotRequired[str]
    version: NotRequired[int]
    nodes: list[WorkflowNode]
    edges: NotRequired[list[dict[str, Any]]]
    policy: NotRequired[dict[str, Any]]


class SubflowNode(BaseNode):
    type: Literal["subflow"]
    workflow: SubflowWorkflow


WorkflowNode = AgentNode | ToolNode | TransformNode | HumanNode | ConditionNode | ParallelNode | SubflowNode


def normalize_workflow_node(node: dict[str, Any]) -> WorkflowNode:
    """Return a defensive, recursively normalized workflow node mapping."""

    normalized = deepcopy(node)
    if normalized.get("type") == "parallel":
        branches = []
        for branch in normalized.get("branches", []):
            branch_data = cast(dict[str, Any], branch)
            if "workflow" in branch_data and "type" not in branch_data:
                workflow = dict(branch_data["workflow"])
                workflow["nodes"] = [
                    normalize_workflow_node(cast(dict[str, Any], child))
                    for child in workflow.get("nodes", [])
                ]
                workflow["edges"] = list(workflow.get("edges", []))
                workflow["policy"] = dict(workflow.get("policy", {}))
                branch_data = dict(branch_data)
                branch_data["workflow"] = workflow
                branches.append(branch_data)
            else:
                branches.append(normalize_workflow_node(branch_data))
        normalized["branches"] = branches
    elif normalized.get("type") == "subflow" and isinstance(normalized.get("workflow"), dict):
        workflow = dict(normalized["workflow"])
        workflow["nodes"] = [
            normalize_workflow_node(cast(dict[str, Any], child))
            for child in workflow.get("nodes", [])
        ]
        workflow["edges"] = list(workflow.get("edges", []))
        workflow["policy"] = dict(workflow.get("policy", {}))
        normalized["workflow"] = workflow
    return cast(WorkflowNode, normalized)


def normalize_workflow_nodes(nodes: list[dict[str, Any]]) -> list[WorkflowNode]:
    """Normalize a workflow node list without changing the public dict API."""

    return [normalize_workflow_node(node) for node in nodes]
