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
    to_message_event,
)


async def planner_agent(agent_input, run_state):
    yield WorkflowEvent(
        type="agent.delta",
        run_id=run_state.run_id,
        node_id="planner",
        data={"text": "planning"},
    )
    yield WorkflowEvent(
        type="agent.output",
        run_id=run_state.run_id,
        node_id="planner",
        data={
            "required_user_fields": ["level"],
            "requires_confirmation": True,
            "tools": ["query_profile"],
        },
    )


async def executor_agent(agent_input, run_state):
    yield WorkflowEvent(
        type="agent.output",
        run_id=run_state.run_id,
        node_id="executor",
        data={"answer": f"done for {agent_input['profile']['level']}"},
    )


async def query_profile(args, run_state):
    return {"profile": {"user_id": args["user_id"], "level": "vip"}}


class WorkflowEngineTest(unittest.IsolatedAsyncioTestCase):
    async def test_human_node_resume_continues_same_run(self):
        agents = AgentRegistry()
        agents.register("planner", planner_agent)
        agents.register("executor", executor_agent)

        tools = ToolRegistry()
        tools.register("query_profile", query_profile)

        workflow = WorkflowConfig.from_dict(
            {
                "id": "demo",
                "version": 1,
                "nodes": [
                    {
                        "id": "planner",
                        "type": "agent",
                        "agent": "planner",
                        "input": {"message": "{{input.message}}"},
                    },
                    {
                        "id": "query_profile",
                        "type": "tool",
                        "tool": "query_profile",
                        "args": {"user_id": "{{context.user_id}}"},
                    },
                    {
                        "id": "confirm_apply",
                        "type": "human",
                        "title": "确认执行",
                        "message": "是否继续处理 {{nodes.query_profile.output.profile.level}} 用户？",
                        "options": [
                            {"id": "approve", "label": "确认"},
                            {"id": "reject", "label": "取消"},
                        ],
                    },
                    {
                        "id": "executor",
                        "type": "agent",
                        "agent": "executor",
                        "input": {
                            "message": "{{input.message}}",
                            "profile": "{{nodes.query_profile.output.profile}}",
                        },
                    },
                ],
            }
        )

        checkpoints = InMemoryCheckpointStore()
        engine = WorkflowEngine(
            workflow,
            agents=agents,
            tools=tools,
            checkpoints=checkpoints,
        )

        first_events = [
            event
            async for event in engine.start(
                StartRunRequest(message="hello", context={"user_id": "u_1"})
            )
        ]

        self.assertEqual(first_events[-1].type, "run.waiting")
        required = [event for event in first_events if event.type == "human.required"][0]
        pending_action_id = required.data["pending_action_id"]
        run_id = required.run_id
        assistant_id = required.data["messages"]["assistant_message_id"]

        second_events = [
            event
            async for event in engine.resume(
                pending_action_id=pending_action_id,
                decision={"decision": "approve"},
            )
        ]

        self.assertEqual(second_events[0].type, "run.resumed")
        self.assertEqual(second_events[-1].type, "run.finished")
        self.assertTrue(all(event.run_id == run_id for event in second_events))
        self.assertTrue(
            all(
                event.data["messages"]["assistant_message_id"] == assistant_id
                for event in second_events
                if "messages" in event.data
            )
        )

    async def test_sse_message_events_keep_same_down_message_id_across_resume(self):
        agents = AgentRegistry()
        agents.register("planner", planner_agent)
        agents.register("executor", executor_agent)

        tools = ToolRegistry()
        tools.register("query_profile", query_profile)

        workflow = WorkflowConfig.from_dict(
            {
                "id": "demo",
                "version": 1,
                "nodes": [
                    {"id": "planner", "type": "agent", "agent": "planner"},
                    {
                        "id": "query_profile",
                        "type": "tool",
                        "tool": "query_profile",
                        "args": {"user_id": "{{context.user_id}}"},
                    },
                    {
                        "id": "confirm",
                        "type": "human",
                        "title": "确认",
                        "options": [{"id": "approve", "label": "确认"}],
                    },
                    {
                        "id": "executor",
                        "type": "agent",
                        "agent": "executor",
                        "input": {"profile": "{{nodes.query_profile.output.profile}}"},
                    },
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        first_payloads = [
            to_message_event(event)
            async for event in engine.start(
                StartRunRequest(message="hello", context={"user_id": "u_1"})
            )
        ]
        self.assertNotIn("FINISH", [payload["event"] for payload in first_payloads])

        pending_action_id = first_payloads[-1]["data"]["pending_action_id"]
        down_message_id = first_payloads[-1]["down_message_id"]

        second_payloads = [
            to_message_event(event)
            async for event in engine.resume(
                pending_action_id=pending_action_id,
                decision={"decision": "approve"},
            )
        ]

        self.assertEqual(second_payloads[-1]["event"], "FINISH")
        self.assertTrue(
            all(payload["down_message_id"] == down_message_id for payload in second_payloads)
        )

    async def test_transform_and_condition_edges(self):
        agents = AgentRegistry()
        tools = ToolRegistry()

        async def final_agent(agent_input, run_state):
            yield WorkflowEvent(
                type="agent.output",
                run_id=run_state.run_id,
                node_id="final",
                data={"summary": agent_input["summary"]},
            )

        agents.register("final", final_agent)

        workflow = WorkflowConfig.from_dict(
            {
                "id": "branching",
                "version": 1,
                "nodes": [
                    {
                        "id": "compose",
                        "type": "transform",
                        "input": {"level": "{{context.level}}"},
                        "output": {
                            "summary": "level={{input.level}}",
                            "is_vip": "{{input.level}} == 'vip'",
                        },
                    },
                    {
                        "id": "final",
                        "type": "agent",
                        "agent": "final",
                        "input": {"summary": "{{nodes.compose.output.summary}}"},
                    },
                ],
                "edges": [
                    {
                        "from": "compose",
                        "to": "final",
                        "when": "{{nodes.compose.output.summary}} == 'level=vip'",
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        events = [
            event
            async for event in engine.start(
                StartRunRequest(message="hello", context={"level": "vip"})
            )
        ]

        self.assertEqual(events[-1].type, "run.finished")
        final = [event for event in events if event.node_id == "final" and event.type == "node.finished"][0]
        self.assertEqual(final.data["output"]["summary"], "level=vip")

    async def test_condition_node_writes_decision_for_branching(self):
        agents = AgentRegistry()
        tools = ToolRegistry()

        async def final_agent(agent_input, run_state):
            yield WorkflowEvent(
                type="agent.output",
                run_id=run_state.run_id,
                node_id="vip_handler",
                data={"route": agent_input["route"]},
            )

        agents.register("vip_handler", final_agent)

        workflow = WorkflowConfig.from_dict(
            {
                "id": "condition-node",
                "version": 1,
                "nodes": [
                    {
                        "id": "route",
                        "type": "condition",
                        "input": {"level": "{{context.level | default('normal')}}"},
                        "cases": [
                            {"when": "{{input.level}} == 'vip'", "value": "vip"},
                            {"when": "{{input.level}} == 'normal'", "value": "normal"},
                        ],
                        "default": "fallback",
                    },
                    {
                        "id": "vip_handler",
                        "type": "agent",
                        "agent": "vip_handler",
                        "input": {"route": "{{nodes.route.output.value}}"},
                    },
                ],
                "edges": [
                    {
                        "from": "route",
                        "to": "vip_handler",
                        "when": "{{nodes.route.output.value}} == 'vip'",
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        events = [
            event
            async for event in engine.start(
                StartRunRequest(message="hello", context={"level": "vip"})
            )
        ]

        condition = [
            event for event in events if event.node_id == "route" and event.type == "node.finished"
        ][0]
        final = [
            event
            for event in events
            if event.node_id == "vip_handler" and event.type == "node.finished"
        ][0]
        self.assertEqual(condition.data["output"]["value"], "vip")
        self.assertEqual(final.data["output"]["route"], "vip")

if __name__ == "__main__":
    unittest.main()
