from agent_orchestrator import (
    AgentRegistry,
    StartRunRequest,
    ToolRegistry,
    WorkflowConfig,
    WorkflowEngine,
    to_message_event,
)


async def test_tool_confirmation_pauses_before_tool_execution():
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

    assert calls == []
    assert first_events[-1].type == "run.waiting"
    pending_action_id = first_events[-1].data["pending_action_id"]

    second_events = [
        event
        async for event in engine.resume(
            pending_action_id=pending_action_id,
            decision={"decision": "approve"},
        )
    ]

    assert calls == [{"command": "deploy"}]
    assert second_events[-1].type == "run.finished"

async def test_tool_policy_denies_missing_permission():
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

    assert calls == []
    assert events[-1].type == "run.failed"
    assert events[-1].data["error_type"] == "PermissionDenied"
    assert "deploy:write" in events[-1].data["error"]

async def test_tool_policy_allows_granted_permission():
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

    assert calls == [{"service": "api"}]
    assert events[-1].type == "run.finished"

async def test_tool_policy_risk_based_confirmation():
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

    assert calls == []
    assert first_events[-1].type == "run.waiting"
    required = [event for event in first_events if event.type == "human.required"][0]
    request = required.data["request"]
    assert request["risk_level"] == "high"
    assert request["permissions"] == ["ops:write"]

    second_events = [
        event
        async for event in engine.resume(
            pending_action_id=first_events[-1].data["pending_action_id"],
            decision={"decision": "approve"},
        )
    ]

    assert calls == [{"target": "prod"}]
    assert second_events[-1].type == "run.finished"

async def test_workflow_tool_allowlist_denies_unlisted_tool():
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

    assert calls == []
    policy_events = [event for event in events if event.type == "policy.decision"]
    assert policy_events[0].data["decision"] == "deny"
    assert "deploy" in policy_events[0].data["reason"]
    assert events[-1].type == "run.failed"
    assert events[-1].data["error_type"] == "PermissionDenied"

async def test_workflow_tool_allowlist_allows_listed_tool():
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

    assert calls == [{"service": "api"}]
    policy_events = [event for event in events if event.type == "policy.decision"]
    assert policy_events[0].data["decision"] == "allow"
    assert events[-1].type == "run.finished"

async def test_tool_node_permission_override_replaces_tool_permissions():
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

    assert calls == [{"service": "api"}]
    policy_event = [event for event in events if event.type == "policy.decision"][0]
    assert policy_event.data["permissions"] == ["deploy:staging"]
    assert events[-1].type == "run.finished"

async def test_tool_node_confirmation_override_emits_policy_decision_event():
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

    assert calls == []
    assert policy_event.data["decision"] == "confirm"
    assert message_event["event"] == "POLICY_DECISION"
    assert message_event["schema_version"] == 1
    assert first_events[-1].type == "run.waiting"
