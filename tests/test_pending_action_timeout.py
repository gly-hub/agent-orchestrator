import sys
import unittest
from pathlib import Path

PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from agent_orchestrator import (
    AgentRegistry,
    InMemoryCheckpointStore,
    StartRunRequest,
    ToolRegistry,
    WorkflowConfig,
    WorkflowEngine,
    WorkflowEvent,
)


class PendingActionTimeoutTest(unittest.IsolatedAsyncioTestCase):
    async def test_expired_human_node_uses_on_timeout_decision(self):
        agents = AgentRegistry()
        tools = ToolRegistry()

        async def timeout_agent(agent_input, run_state):
            yield WorkflowEvent(
                type="agent.output",
                run_id=run_state.run_id,
                node_id="timeout_handler",
                data={"decision": agent_input["decision"], "timed_out": agent_input["timed_out"]},
            )

        agents.register("timeout_handler", timeout_agent)
        workflow = WorkflowConfig.from_dict(
            {
                "id": "timeout-default",
                "version": 1,
                "nodes": [
                    {
                        "id": "confirm",
                        "type": "human",
                        "title": "确认",
                        "options": [{"id": "approve", "label": "确认"}],
                        "on_timeout": {"decision": "timeout", "reason": "expired"},
                    },
                    {
                        "id": "timeout_handler",
                        "type": "agent",
                        "agent": "timeout_handler",
                        "input": {
                            "decision": "{{nodes.confirm.output.decision}}",
                            "timed_out": "{{nodes.confirm.output.timed_out}}",
                        },
                    },
                ],
                "edges": [
                    {
                        "from": "confirm",
                        "to": "timeout_handler",
                        "when": "{{nodes.confirm.output.decision}} == 'timeout'",
                    }
                ],
            }
        )

        engine = WorkflowEngine(
            workflow,
            agents=agents,
            tools=tools,
            pending_action_ttl_ms=-1,
        )
        first_events = [event async for event in engine.start(StartRunRequest(message="hello"))]
        pending_action_id = first_events[-1].data["pending_action_id"]

        second_events = [
            event
            async for event in engine.resume(
                pending_action_id=pending_action_id,
                decision={"decision": "approve"},
            )
        ]

        final = [
            event
            for event in second_events
            if event.node_id == "timeout_handler" and event.type == "node.finished"
        ][0]
        self.assertEqual(final.data["output"], {"decision": "timeout", "timed_out": True})

    async def test_resume_expired_actions_applies_on_timeout_decision(self):
        agents = AgentRegistry()
        tools = ToolRegistry()

        async def timeout_agent(agent_input, run_state):
            yield WorkflowEvent(
                type="agent.output",
                run_id=run_state.run_id,
                node_id="timeout_handler",
                data={"decision": agent_input["decision"], "timed_out": agent_input["timed_out"]},
            )

        agents.register("timeout_handler", timeout_agent)
        workflow = WorkflowConfig.from_dict(
            {
                "id": "timeout-scanner",
                "version": 1,
                "nodes": [
                    {
                        "id": "confirm",
                        "type": "human",
                        "title": "确认",
                        "on_timeout": {"decision": "timeout"},
                    },
                    {
                        "id": "timeout_handler",
                        "type": "agent",
                        "agent": "timeout_handler",
                        "input": {
                            "decision": "{{nodes.confirm.output.decision}}",
                            "timed_out": "{{nodes.confirm.output.timed_out}}",
                        },
                    },
                ],
            }
        )
        engine = WorkflowEngine(
            workflow,
            agents=agents,
            tools=tools,
            pending_action_ttl_ms=-1,
        )

        [event async for event in engine.start(StartRunRequest(message="hello"))]
        events = await engine.resume_expired_actions()

        self.assertEqual(events[0].type, "run.resumed")
        final = [
            event for event in events if event.node_id == "timeout_handler" and event.type == "node.finished"
        ][0]
        self.assertEqual(final.data["output"], {"decision": "timeout", "timed_out": True})

    async def test_resume_expired_actions_marks_action_expired_without_on_timeout(self):
        agents = AgentRegistry()
        tools = ToolRegistry()
        checkpoints = InMemoryCheckpointStore()
        workflow = WorkflowConfig.from_dict(
            {
                "id": "timeout-no-default",
                "version": 1,
                "nodes": [{"id": "confirm", "type": "human", "title": "确认"}],
            }
        )
        engine = WorkflowEngine(
            workflow,
            agents=agents,
            tools=tools,
            checkpoints=checkpoints,
            pending_action_ttl_ms=-1,
        )

        first_events = [event async for event in engine.start(StartRunRequest(message="hello"))]
        pending_action_id = first_events[-1].data["pending_action_id"]
        events = await engine.resume_expired_actions()
        action = await checkpoints.load_action(pending_action_id)

        self.assertEqual(events[0].type, "human.expired")
        self.assertEqual(action.status, "expired")
