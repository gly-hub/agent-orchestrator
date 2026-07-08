"""Workflow configuration validation."""

from __future__ import annotations

from typing import Any

from agent_orchestrator.exceptions import WorkflowConfigError
from agent_orchestrator.validation_graph import (
    validate_edges,
    validate_no_simple_cycles,
    validate_when_expression,
)
from agent_orchestrator.validation_schema import validate_schema

SUPPORTED_NODE_TYPES = {"agent", "tool", "transform", "human", "condition", "parallel", "subflow", "loop"}
REQUIRED_FIELDS_BY_TYPE = {
    "agent": {"agent"},
    "tool": {"tool"},
    "transform": set(),
    "human": set(),
    "condition": set(),
    "parallel": set(),
    "subflow": {"workflow"},
    "loop": {"body"},
}


def validate_workflow_config(config: Any) -> None:
    """Validate the structural shape of a workflow config.

    Accepts any object with `id`, `nodes`, and `edges` attributes so it can be
    used by dataclass configs without creating import cycles.
    """

    if not getattr(config, "id", None):
        raise WorkflowConfigError("workflow id is required")

    nodes = getattr(config, "nodes", None)
    if not isinstance(nodes, list) or not nodes:
        raise WorkflowConfigError("workflow must contain at least one node")

    edges = getattr(config, "edges", None)
    if edges is None:
        edges = []
    if not isinstance(edges, list):
        raise WorkflowConfigError("workflow edges must be a list")

    node_ids = _validate_nodes(nodes)
    validate_edges(edges, node_ids)
    _validate_policy(getattr(config, "policy", {}))
    validate_no_simple_cycles(nodes, edges)


def _validate_nodes(nodes: list[dict[str, Any]]) -> set[str]:
    seen: set[str] = set()
    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            raise WorkflowConfigError(f"node at index {index} must be a mapping")

        node_id = node.get("id")
        if not isinstance(node_id, str) or not node_id:
            raise WorkflowConfigError(f"node at index {index} must have a non-empty id")
        if node_id in seen:
            raise WorkflowConfigError(f"duplicate node id: {node_id}")
        seen.add(node_id)

        node_type = node.get("type")
        if node_type not in SUPPORTED_NODE_TYPES:
            raise WorkflowConfigError(f"unsupported node type for {node_id}: {node_type}")

        required = REQUIRED_FIELDS_BY_TYPE[node_type]
        missing = sorted(field for field in required if field not in node)
        if missing:
            raise WorkflowConfigError(
                f"node {node_id} missing required field(s): {', '.join(missing)}"
            )

        _validate_node_options(node_id, node)

    return seen


def _validate_node_options(node_id: str, node: dict[str, Any]) -> None:
    if "permissions" in node:
        _validate_string_list(node["permissions"], f"node {node_id} permissions")
    if "required_permissions" in node:
        _validate_string_list(node["required_permissions"], f"node {node_id} required_permissions")
    confirmation_policy = node.get("confirmation_policy")
    if confirmation_policy is not None and confirmation_policy not in {"never", "always", "risk_based"}:
        raise WorkflowConfigError(f"node {node_id} confirmation_policy is unsupported")

    retry = node.get("retry")
    if retry is not None:
        if not isinstance(retry, dict):
            raise WorkflowConfigError(f"node {node_id} retry must be a mapping")
        max_attempts = retry.get("max_attempts", 1)
        if not isinstance(max_attempts, int) or max_attempts < 1:
            raise WorkflowConfigError(f"node {node_id} retry.max_attempts must be >= 1")
        delay_ms = retry.get("delay_ms", 0)
        if not isinstance(delay_ms, int) or delay_ms < 0:
            raise WorkflowConfigError(f"node {node_id} retry.delay_ms must be >= 0")
        max_delay_ms = retry.get("max_delay_ms")
        if max_delay_ms is not None and (not isinstance(max_delay_ms, int) or max_delay_ms < 0):
            raise WorkflowConfigError(f"node {node_id} retry.max_delay_ms must be >= 0")
        backoff_multiplier = retry.get("backoff_multiplier", 1)
        if not isinstance(backoff_multiplier, int | float) or backoff_multiplier < 0:
            raise WorkflowConfigError(f"node {node_id} retry.backoff_multiplier must be >= 0")
        retry_on = retry.get("retry_on", [])
        if retry_on is not None and (
            not isinstance(retry_on, list) or not all(isinstance(item, str) for item in retry_on)
        ):
            raise WorkflowConfigError(f"node {node_id} retry.retry_on must be a string list")

    timeout_ms = node.get("timeout_ms")
    if timeout_ms is not None and (not isinstance(timeout_ms, int) or timeout_ms <= 0):
        raise WorkflowConfigError(f"node {node_id} timeout_ms must be > 0")

    on_timeout = node.get("on_timeout")
    if on_timeout is not None and not isinstance(on_timeout, str | dict):
        raise WorkflowConfigError(f"node {node_id} on_timeout must be a string or mapping")

    if "artifact_threshold_bytes" in node:
        threshold = node["artifact_threshold_bytes"]
        if not isinstance(threshold, int) or threshold < 0:
            raise WorkflowConfigError(f"node {node_id} artifact_threshold_bytes must be >= 0")
    if "output_artifact" in node and not isinstance(node["output_artifact"], bool):
        raise WorkflowConfigError(f"node {node_id} output_artifact must be a boolean")
    if "resolve_input_artifacts" in node and not isinstance(node["resolve_input_artifacts"], bool):
        raise WorkflowConfigError(f"node {node_id} resolve_input_artifacts must be a boolean")

    if node.get("type") == "condition":
        cases = node.get("cases", [])
        if not isinstance(cases, list):
            raise WorkflowConfigError(f"node {node_id} cases must be a list")
        for index, case in enumerate(cases):
            if not isinstance(case, dict):
                raise WorkflowConfigError(f"node {node_id} case at index {index} must be a mapping")
            if "when" not in case or "value" not in case:
                raise WorkflowConfigError(
                    f"node {node_id} case at index {index} must contain when and value"
                )
            validate_when_expression(node_id, case["when"])

    if node.get("type") == "parallel":
        branches = node.get("branches", [])
        if not isinstance(branches, list) or not branches:
            raise WorkflowConfigError(f"node {node_id} branches must be a non-empty list")
        branch_ids: set[str] = set()
        for index, branch in enumerate(branches):
            if not isinstance(branch, dict):
                raise WorkflowConfigError(f"node {node_id} branch at index {index} must be a mapping")
            branch_id = branch.get("id")
            if not isinstance(branch_id, str) or not branch_id:
                raise WorkflowConfigError(f"node {node_id} branch at index {index} must have a non-empty id")
            if branch_id == node_id or branch_id in branch_ids:
                raise WorkflowConfigError(f"node {node_id} has duplicate branch id: {branch_id}")
            branch_ids.add(branch_id)
            if "workflow" in branch and "type" not in branch:
                _validate_parallel_workflow_branch(node_id, branch_id, branch)
                continue
            branch_type = branch.get("type")
            if branch_type == "parallel":
                raise WorkflowConfigError(f"node {node_id} branch {branch_id} cannot be a nested parallel node")
            if branch_type not in SUPPORTED_NODE_TYPES:
                raise WorkflowConfigError(f"unsupported branch type for {branch_id}: {branch_type}")
            missing = sorted(field for field in REQUIRED_FIELDS_BY_TYPE[branch_type] if field not in branch)
            if missing:
                raise WorkflowConfigError(
                    f"branch {branch_id} missing required field(s): {', '.join(missing)}"
                )
            _validate_node_options(branch_id, branch)

        failure_policy = node.get("failure_policy", node.get("partial_failure_policy", "fail"))
        if failure_policy not in {"fail", "continue"}:
            raise WorkflowConfigError(f"node {node_id} failure_policy is unsupported")

    if node.get("type") == "subflow":
        workflow = node.get("workflow")
        if not isinstance(workflow, dict):
            raise WorkflowConfigError(f"node {node_id} workflow must be a mapping")
        if "nodes" not in workflow:
            raise WorkflowConfigError(f"node {node_id} workflow must contain nodes")
        child_config = _InlineWorkflowConfig(
            id=str(workflow.get("id", f"{node_id}.workflow")),
            nodes=list(workflow.get("nodes", [])),
            edges=list(workflow.get("edges", [])),
            policy=dict(workflow.get("policy", {})),
        )
        validate_workflow_config(child_config)

    if node.get("type") == "human":
        response_schema = node.get("response_schema")
        if response_schema is not None:
            validate_schema(node_id, "response_schema", response_schema)

    if node.get("type") == "loop":
        body = node.get("body")
        if not isinstance(body, dict):
            raise WorkflowConfigError(f"node {node_id} body must be a mapping")
        if "nodes" not in body:
            raise WorkflowConfigError(f"node {node_id} body must contain nodes")
        max_iterations = node.get("max_iterations")
        if max_iterations is not None and (not isinstance(max_iterations, int) or max_iterations < 1):
            raise WorkflowConfigError(f"node {node_id} max_iterations must be >= 1")
        condition = node.get("condition")
        if condition is not None:
            validate_when_expression(node_id, condition)
        child_config = _InlineWorkflowConfig(
            id=str(body.get("id", f"{node_id}.body")),
            nodes=list(body.get("nodes", [])),
            edges=list(body.get("edges", [])),
            policy=dict(body.get("policy", {})),
        )
        validate_workflow_config(child_config)
        if _workflow_contains_node_type(child_config.nodes, "human"):
            raise WorkflowConfigError(f"node {node_id} body cannot contain human nodes")

    for schema_field in ("input_schema", "output_schema"):
        schema = node.get(schema_field)
        if schema is not None:
            validate_schema(node_id, schema_field, schema)


def _validate_parallel_workflow_branch(
    node_id: str,
    branch_id: str,
    branch: dict[str, Any],
) -> None:
    workflow = branch.get("workflow")
    if not isinstance(workflow, dict):
        raise WorkflowConfigError(f"node {node_id} branch {branch_id} workflow must be a mapping")
    if "nodes" not in workflow:
        raise WorkflowConfigError(f"node {node_id} branch {branch_id} workflow must contain nodes")
    child_config = _InlineWorkflowConfig(
        id=str(workflow.get("id", f"{node_id}.{branch_id}.workflow")),
        nodes=list(workflow.get("nodes", [])),
        edges=list(workflow.get("edges", [])),
        policy=dict(workflow.get("policy", {})),
    )
    validate_workflow_config(child_config)


def _validate_policy(policy: Any) -> None:
    if policy is None:
        return
    if not isinstance(policy, dict):
        raise WorkflowConfigError("workflow policy must be a mapping")
    for field in ("tool_allowlist", "allowed_tools"):
        if field in policy:
            _validate_string_list(policy[field], f"workflow policy.{field}")


def _validate_string_list(value: Any, label: str) -> None:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise WorkflowConfigError(f"{label} must be a string list")


class _InlineWorkflowConfig:
    def __init__(
        self,
        *,
        id: str,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
        policy: dict[str, Any],
    ) -> None:
        self.id = id
        self.nodes = nodes
        self.edges = edges
        self.policy = policy


def _workflow_contains_node_type(nodes: list[dict[str, Any]], node_type: str) -> bool:
    for node in nodes:
        if node.get("type") == node_type:
            return True
        if node.get("type") == "parallel":
            branch_nodes = []
            for branch in node.get("branches", []):
                if isinstance(branch, dict) and "workflow" in branch and "type" not in branch:
                    workflow = branch.get("workflow", {})
                    if isinstance(workflow, dict):
                        branch_nodes.extend(list(workflow.get("nodes", [])))
                else:
                    branch_nodes.append(branch)
            if _workflow_contains_node_type(branch_nodes, node_type):
                return True
        if node.get("type") == "subflow":
            workflow = node.get("workflow", {})
            if isinstance(workflow, dict) and _workflow_contains_node_type(list(workflow.get("nodes", [])), node_type):
                return True
        if node.get("type") == "loop":
            body = node.get("body", {})
            if isinstance(body, dict) and _workflow_contains_node_type(list(body.get("nodes", [])), node_type):
                return True
    return False
