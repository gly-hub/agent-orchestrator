import pytest

from agent_orchestrator import WorkflowConfig, normalize_workflow_node
from agent_orchestrator.exceptions import WorkflowConfigError


def test_workflow_config_rejects_duplicate_node_ids():
    with pytest.raises(WorkflowConfigError, match="duplicate node id"):
        WorkflowConfig.from_dict(
            {
                "id": "bad",
                "nodes": [
                    {"id": "a", "type": "human"},
                    {"id": "a", "type": "human"},
                ],
            }
        )

def test_workflow_config_rejects_unknown_edge_target():
    with pytest.raises(WorkflowConfigError, match="unknown to node"):
        WorkflowConfig.from_dict(
            {
                "id": "bad",
                "nodes": [{"id": "a", "type": "human"}],
                "edges": [{"from": "a", "to": "missing"}],
            }
        )

def test_workflow_config_rejects_missing_agent_field():
    with pytest.raises(WorkflowConfigError, match="missing required"):
        WorkflowConfig.from_dict(
            {
                "id": "bad",
                "nodes": [{"id": "agent_without_name", "type": "agent"}],
            }
        )

def test_workflow_config_rejects_cycles():
    with pytest.raises(WorkflowConfigError, match="cycle"):
        WorkflowConfig.from_dict(
            {
                "id": "bad",
                "nodes": [
                    {"id": "a", "type": "human"},
                    {"id": "b", "type": "human"},
                ],
                "edges": [
                    {"from": "a", "to": "b"},
                    {"from": "b", "to": "a"},
                ],
            }
        )

def test_workflow_config_rejects_unsupported_edge_when_syntax():
    with pytest.raises(WorkflowConfigError, match="unsupported syntax"):
        WorkflowConfig.from_dict(
            {
                "id": "bad-when",
                "nodes": [
                    {"id": "a", "type": "human"},
                    {"id": "b", "type": "human"},
                ],
                "edges": [
                    {"from": "a", "to": "b", "when": "contains({{context.tags}}, 'vip')"},
                ],
            }
        )

def test_workflow_config_rejects_bare_string_when_literal():
    with pytest.raises(WorkflowConfigError, match="unsupported condition literal"):
        WorkflowConfig.from_dict(
            {
                "id": "bad-bare-string",
                "nodes": [
                    {"id": "a", "type": "human"},
                    {"id": "b", "type": "human"},
                ],
                "edges": [
                    {"from": "a", "to": "b", "when": "{{context.level}} == vip"},
                ],
            }
        )

def test_workflow_config_rejects_inline_template_when_operand():
    with pytest.raises(WorkflowConfigError, match="whole operand"):
        WorkflowConfig.from_dict(
            {
                "id": "bad-inline-template",
                "nodes": [
                    {"id": "a", "type": "human"},
                    {"id": "b", "type": "human"},
                ],
                "edges": [
                    {
                        "from": "a",
                        "to": "b",
                        "when": "'level={{context.level}}' == 'level=vip'",
                    },
                ],
            }
        )

def test_workflow_config_rejects_empty_when_clause():
    with pytest.raises(WorkflowConfigError, match="empty condition"):
        WorkflowConfig.from_dict(
            {
                "id": "bad-empty-clause",
                "nodes": [
                    {"id": "a", "type": "human"},
                    {"id": "b", "type": "human"},
                ],
                "edges": [
                    {"from": "a", "to": "b", "when": "{{context.enabled}} and"},
                ],
            }
        )

def test_workflow_config_rejects_non_string_condition_when():
    with pytest.raises(WorkflowConfigError, match="when must be a string"):
        WorkflowConfig.from_dict(
            {
                "id": "bad-condition-when",
                "nodes": [
                    {
                        "id": "route",
                        "type": "condition",
                        "cases": [
                            {"when": {"path": "context.level"}, "value": "vip"},
                        ],
                    },
                ],
            }
        )

def test_workflow_config_normalizes_nodes_defensively():
    source = {
        "id": "typed",
        "nodes": [
            {
                "id": "fanout",
                "type": "parallel",
                "branches": [
                    {"id": "lookup", "type": "tool", "tool": "lookup"},
                ],
            }
        ],
    }

    config = WorkflowConfig.from_dict(source)
    source["nodes"][0]["branches"][0]["tool"] = "changed"

    assert config.nodes[0]["branches"][0]["tool"] == "lookup"

def test_normalize_workflow_node_recurses_into_subflow():
    node = normalize_workflow_node(
        {
            "id": "child",
            "type": "subflow",
            "workflow": {
                "nodes": [
                    {"id": "echo", "type": "tool", "tool": "echo"},
                ],
            },
        }
    )

    assert node["workflow"]["nodes"][0]["id"] == "echo"
    assert node["workflow"]["edges"] == []
