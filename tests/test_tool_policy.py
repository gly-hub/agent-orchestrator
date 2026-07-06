import sys
import unittest
from pathlib import Path

PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from agent_orchestrator import (
    AgentRegistry,
    StartRunRequest,
    ToolRegistry,
    WorkflowConfig,
    WorkflowEngine,
    to_message_event,
)


class ToolPolicyTest(unittest.IsolatedAsyncioTestCase):
    async def test_tool_confirmation_pauses_before_tool_execution(self):
        agents = AgentRegistry()
        tools = ToolRegistry()
        calls = []

        async def risky_tool(args, run_state):
            calls.append(args)
            return {"ok": True}

        tools.register("risky_tool", risky_tool, requires_confirmation=True)
        workflow = WorkflowConfig.from_dict(
            {
                "id": "tool-confirm",
                "version": 1,
                "nodes": [
                    {
                        "id": "risky",
                        "type": "tool",
                        "tool": "risky_tool",
                        "args": {"command": "{{input.message}}"},
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        first_events = [
            event async for event in engine.start(StartRunRequest(message="deploy"))
        ]

        self.assertEqual(calls, [])
        self.assertEqual(first_events[-1].type, "run.waiting")
        pending_action_id = first_events[-1].data["pending_action_id"]

        second_events = [
            event
            async for event in engine.resume(
                pending_action_id=pending_action_id,
                decision={"decision": "approve"},
            )
        ]

        self.assertEqual(calls, [{"command": "deploy"}])
        self.assertEqual(second_events[-1].type, "run.finished")

    async def test_tool_policy_denies_missing_permission(self):
        agents = AgentRegistry()
        tools = ToolRegistry()
        calls = []

        async def deploy_tool(args, run_state):
            calls.append(args)
            return {"ok": True}

        tools.register("deploy", deploy_tool, permissions=["deploy:write"])
        workflow = WorkflowConfig.from_dict(
            {
                "id": "permission-denied",
                "version": 1,
                "nodes": [
                    {
                        "id": "deploy",
                        "type": "tool",
                        "tool": "deploy",
                        "args": {"service": "{{input.message}}"},
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        events = [event async for event in engine.start(StartRunRequest(message="api"))]

        self.assertEqual(calls, [])
        self.assertEqual(events[-1].type, "run.failed")
        self.assertEqual(events[-1].data["error_type"], "PermissionDenied")
        self.assertIn("deploy:write", events[-1].data["error"])

    async def test_tool_policy_allows_granted_permission(self):
        agents = AgentRegistry()
        tools = ToolRegistry()
        calls = []

        async def deploy_tool(args, run_state):
            calls.append(args)
            return {"ok": True}

        tools.register("deploy", deploy_tool, permissions=["deploy:write"])
        workflow = WorkflowConfig.from_dict(
            {
                "id": "permission-allowed",
                "version": 1,
                "nodes": [
                    {
                        "id": "deploy",
                        "type": "tool",
                        "tool": "deploy",
                        "args": {"service": "{{input.message}}"},
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        events = [
            event
            async for event in engine.start(
                StartRunRequest(
                    message="api",
                    context={"permissions": ["deploy:write"]},
                )
            )
        ]

        self.assertEqual(calls, [{"service": "api"}])
        self.assertEqual(events[-1].type, "run.finished")

    async def test_tool_policy_risk_based_confirmation(self):
        agents = AgentRegistry()
        tools = ToolRegistry()
        calls = []

        async def risky_tool(args, run_state):
            calls.append(args)
            return {"ok": True}

        tools.register(
            "risky",
            risky_tool,
            permissions=["ops:write"],
            risk_level="high",
            confirmation_policy="risk_based",
        )
        workflow = WorkflowConfig.from_dict(
            {
                "id": "risk-confirm",
                "version": 1,
                "nodes": [
                    {
                        "id": "risky",
                        "type": "tool",
                        "tool": "risky",
                        "args": {"target": "{{input.message}}"},
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        first_events = [
            event
            async for event in engine.start(
                StartRunRequest(
                    message="prod",
                    context={"permissions": ["ops:write"]},
                )
            )
        ]

        self.assertEqual(calls, [])
        self.assertEqual(first_events[-1].type, "run.waiting")
        required = [event for event in first_events if event.type == "human.required"][0]
        request = required.data["request"]
        self.assertEqual(request["risk_level"], "high")
        self.assertEqual(request["permissions"], ["ops:write"])

        second_events = [
            event
            async for event in engine.resume(
                pending_action_id=first_events[-1].data["pending_action_id"],
                decision={"decision": "approve"},
            )
        ]

        self.assertEqual(calls, [{"target": "prod"}])
        self.assertEqual(second_events[-1].type, "run.finished")

    async def test_workflow_tool_allowlist_denies_unlisted_tool(self):
        agents = AgentRegistry()
        tools = ToolRegistry()
        calls = []

        async def deploy_tool(args, run_state):
            calls.append(args)
            return {"ok": True}

        tools.register("deploy", deploy_tool)
        workflow = WorkflowConfig.from_dict(
            {
                "id": "tool-allowlist-deny",
                "version": 1,
                "policy": {"tool_allowlist": ["query_profile"]},
                "nodes": [
                    {
                        "id": "deploy",
                        "type": "tool",
                        "tool": "deploy",
                        "args": {"service": "{{input.message}}"},
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        events = [event async for event in engine.start(StartRunRequest(message="api"))]

        self.assertEqual(calls, [])
        policy_events = [event for event in events if event.type == "policy.decision"]
        self.assertEqual(policy_events[0].data["decision"], "deny")
        self.assertIn("deploy", policy_events[0].data["reason"])
        self.assertEqual(events[-1].type, "run.failed")
        self.assertEqual(events[-1].data["error_type"], "PermissionDenied")

    async def test_workflow_tool_allowlist_allows_listed_tool(self):
        agents = AgentRegistry()
        tools = ToolRegistry()
        calls = []

        async def deploy_tool(args, run_state):
            calls.append(args)
            return {"ok": True}

        tools.register("deploy", deploy_tool)
        workflow = WorkflowConfig.from_dict(
            {
                "id": "tool-allowlist-allow",
                "version": 1,
                "policy": {"tool_allowlist": ["deploy"]},
                "nodes": [
                    {
                        "id": "deploy",
                        "type": "tool",
                        "tool": "deploy",
                        "args": {"service": "{{input.message}}"},
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        events = [event async for event in engine.start(StartRunRequest(message="api"))]

        self.assertEqual(calls, [{"service": "api"}])
        policy_events = [event for event in events if event.type == "policy.decision"]
        self.assertEqual(policy_events[0].data["decision"], "allow")
        self.assertEqual(events[-1].type, "run.finished")

    async def test_tool_node_permission_override_replaces_tool_permissions(self):
        agents = AgentRegistry()
        tools = ToolRegistry()
        calls = []

        async def deploy_tool(args, run_state):
            calls.append(args)
            return {"ok": True}

        tools.register("deploy", deploy_tool, permissions=["deploy:write"])
        workflow = WorkflowConfig.from_dict(
            {
                "id": "node-permission-override",
                "version": 1,
                "nodes": [
                    {
                        "id": "deploy",
                        "type": "tool",
                        "tool": "deploy",
                        "permissions": ["deploy:staging"],
                        "args": {"service": "{{input.message}}"},
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        events = [
            event
            async for event in engine.start(
                StartRunRequest(
                    message="api",
                    context={"permissions": ["deploy:staging"]},
                )
            )
        ]

        self.assertEqual(calls, [{"service": "api"}])
        policy_event = [event for event in events if event.type == "policy.decision"][0]
        self.assertEqual(policy_event.data["permissions"], ["deploy:staging"])
        self.assertEqual(events[-1].type, "run.finished")

    async def test_tool_node_confirmation_override_emits_policy_decision_event(self):
        agents = AgentRegistry()
        tools = ToolRegistry()
        calls = []

        async def deploy_tool(args, run_state):
            calls.append(args)
            return {"ok": True}

        tools.register("deploy", deploy_tool)
        workflow = WorkflowConfig.from_dict(
            {
                "id": "node-confirmation-override",
                "version": 1,
                "nodes": [
                    {
                        "id": "deploy",
                        "type": "tool",
                        "tool": "deploy",
                        "confirmation_policy": "always",
                        "args": {"service": "{{input.message}}"},
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        first_events = [event async for event in engine.start(StartRunRequest(message="api"))]
        policy_event = [event for event in first_events if event.type == "policy.decision"][0]
        message_event = to_message_event(policy_event)

        self.assertEqual(calls, [])
        self.assertEqual(policy_event.data["decision"], "confirm")
        self.assertEqual(message_event["event"], "POLICY_DECISION")
        self.assertEqual(message_event["schema_version"], 1)
        self.assertEqual(first_events[-1].type, "run.waiting")
