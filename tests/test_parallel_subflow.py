import asyncio
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
)
from agent_orchestrator.exceptions import WorkflowConfigError


class ParallelSubflowTest(unittest.IsolatedAsyncioTestCase):
    async def test_parallel_node_runs_branches_concurrently_and_merges_outputs(self):
        agents = AgentRegistry()
        tools = ToolRegistry()
        b_started = asyncio.Event()
        calls = []

        async def tool_a(args, run_state):
            calls.append("a-start")
            await asyncio.wait_for(b_started.wait(), timeout=1)
            calls.append("a-finish")
            return {"value": "a"}

        async def tool_b(args, run_state):
            calls.append("b-start")
            b_started.set()
            calls.append("b-finish")
            return {"value": "b"}

        tools.register("tool_a", tool_a)
        tools.register("tool_b", tool_b)
        workflow = WorkflowConfig.from_dict(
            {
                "id": "parallel-merge",
                "version": 1,
                "nodes": [
                    {
                        "id": "fanout",
                        "type": "parallel",
                        "branches": [
                            {"id": "a", "type": "tool", "tool": "tool_a"},
                            {"id": "b", "type": "tool", "tool": "tool_b"},
                        ],
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        events = [event async for event in engine.start(StartRunRequest(message="go"))]
        finished = [event for event in events if event.type == "node.finished" and event.node_id == "fanout"][0]

        self.assertIn("b-start", calls)
        self.assertLess(calls.index("b-start"), calls.index("a-finish"))
        self.assertEqual(
            finished.data["output"],
            {
                "branches": {
                    "a": {"value": "a"},
                    "b": {"value": "b"},
                },
                "failed_branches": [],
            },
        )
        self.assertEqual(events[-1].type, "run.finished")

    async def test_parallel_node_streams_branch_events_as_they_happen(self):
        agents = AgentRegistry()
        tools = ToolRegistry()

        async def slow_tool(args, run_state):
            await asyncio.sleep(0.01)
            return {"value": "slow"}

        async def fast_tool(args, run_state):
            return {"value": "fast"}

        tools.register("slow", slow_tool)
        tools.register("fast", fast_tool)
        workflow = WorkflowConfig.from_dict(
            {
                "id": "parallel-order",
                "version": 1,
                "nodes": [
                    {
                        "id": "fanout",
                        "type": "parallel",
                        "branches": [
                            {"id": "slow", "type": "tool", "tool": "slow"},
                            {"id": "fast", "type": "tool", "tool": "fast"},
                        ],
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        events = [event async for event in engine.start(StartRunRequest(message="go"))]
        branch_finishes = [
            event.node_id
            for event in events
            if event.type == "node.finished" and event.node_id in {"slow", "fast"}
        ]

        self.assertEqual(branch_finishes, ["fast", "slow"])

    async def test_parallel_node_cancels_branch_tasks_when_stream_closes(self):
        agents = AgentRegistry()
        tools = ToolRegistry()
        branch_started = asyncio.Event()
        branch_cancelled = asyncio.Event()

        async def slow_tool(args, run_state):
            branch_started.set()
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                branch_cancelled.set()
                raise

        tools.register("slow", slow_tool)
        workflow = WorkflowConfig.from_dict(
            {
                "id": "parallel-cancel",
                "version": 1,
                "nodes": [
                    {
                        "id": "fanout",
                        "type": "parallel",
                        "branches": [{"id": "slow", "type": "tool", "tool": "slow"}],
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)
        stream = engine.start(StartRunRequest(message="go")).__aiter__()

        self.assertEqual((await anext(stream)).type, "run.started")
        self.assertEqual((await anext(stream)).type, "node.started")
        self.assertEqual((await anext(stream)).type, "node.started")
        await asyncio.wait_for(branch_started.wait(), timeout=1)

        await stream.aclose()

        await asyncio.wait_for(branch_cancelled.wait(), timeout=1)

    async def test_parallel_node_continue_policy_keeps_failed_branch_output(self):
        agents = AgentRegistry()
        tools = ToolRegistry()

        async def ok_tool(args, run_state):
            return {"ok": True}

        async def fail_tool(args, run_state):
            raise RuntimeError("boom")

        tools.register("ok", ok_tool)
        tools.register("fail", fail_tool)
        workflow = WorkflowConfig.from_dict(
            {
                "id": "parallel-continue",
                "version": 1,
                "nodes": [
                    {
                        "id": "fanout",
                        "type": "parallel",
                        "failure_policy": "continue",
                        "branches": [
                            {"id": "ok", "type": "tool", "tool": "ok"},
                            {"id": "fail", "type": "tool", "tool": "fail"},
                        ],
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        events = [event async for event in engine.start(StartRunRequest(message="go"))]
        finished = [event for event in events if event.type == "node.finished" and event.node_id == "fanout"][0]

        self.assertEqual(events[-1].type, "run.finished")
        self.assertEqual(finished.data["output"]["branches"]["ok"], {"ok": True})
        self.assertEqual(finished.data["output"]["branches"]["fail"]["failed"], True)
        self.assertEqual(finished.data["output"]["failed_branches"][0]["id"], "fail")

    async def test_parallel_node_runs_multi_node_workflow_branch(self):
        agents = AgentRegistry()
        tools = ToolRegistry()

        async def lookup_tool(args, run_state):
            return {"profile": {"user_id": args["user_id"], "level": "vip"}}

        tools.register("lookup", lookup_tool)
        workflow = WorkflowConfig.from_dict(
            {
                "id": "parallel-workflow-branch",
                "version": 1,
                "nodes": [
                    {
                        "id": "fanout",
                        "type": "parallel",
                        "branches": [
                            {
                                "id": "profile",
                                "input": {"user_id": "{{context.user_id}}"},
                                "output": {"level": "{{nodes.decorate.output.level}}"},
                                "workflow": {
                                    "nodes": [
                                        {
                                            "id": "lookup",
                                            "type": "tool",
                                            "tool": "lookup",
                                            "args": {"user_id": "{{input.user_id}}"},
                                        },
                                        {
                                            "id": "decorate",
                                            "type": "transform",
                                            "input": {
                                                "level": "{{nodes.lookup.output.profile.level}}",
                                            },
                                        },
                                    ],
                                },
                            }
                        ],
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        events = [
            event
            async for event in engine.start(
                StartRunRequest(message="go", context={"user_id": "u_1"})
            )
        ]
        finished = [event for event in events if event.type == "node.finished" and event.node_id == "fanout"][0]
        branch_started = [
            event for event in events if event.type == "parallel.node.started" and event.node_id == "profile.lookup"
        ][0]

        self.assertEqual(branch_started.data["parallel_branch_id"], "profile")
        self.assertEqual(finished.data["output"]["branches"]["profile"], {"level": "vip"})
        self.assertIn("profile.lookup", finished.data["output"]["nodes"])
        self.assertIn("profile.decorate", finished.data["output"]["nodes"])
        self.assertEqual(events[-1].type, "run.finished")

    async def test_parallel_node_default_failure_policy_fails_run(self):
        agents = AgentRegistry()
        tools = ToolRegistry()

        async def fail_tool(args, run_state):
            raise RuntimeError("boom")

        tools.register("fail", fail_tool)
        workflow = WorkflowConfig.from_dict(
            {
                "id": "parallel-fail",
                "version": 1,
                "nodes": [
                    {
                        "id": "fanout",
                        "type": "parallel",
                        "branches": [
                            {"id": "fail", "type": "tool", "tool": "fail"},
                        ],
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        events = [event async for event in engine.start(StartRunRequest(message="go"))]

        self.assertEqual(events[-1].type, "run.failed")
        self.assertEqual(events[-1].data["error_type"], "WorkflowError")
        self.assertIn("parallel branches failed: fail", events[-1].data["error"])

    async def test_parallel_node_rejects_human_branch(self):
        with self.assertRaisesRegex(WorkflowConfigError, "cannot be a human node"):
            WorkflowConfig.from_dict(
                {
                    "id": "parallel-human",
                    "version": 1,
                    "nodes": [
                        {
                            "id": "fanout",
                            "type": "parallel",
                            "branches": [
                                {"id": "confirm", "type": "human", "title": "确认"},
                            ],
                        }
                    ],
                }
            )

    async def test_subflow_node_runs_reusable_workflow_and_selects_output(self):
        agents = AgentRegistry()
        tools = ToolRegistry()

        async def lookup_tool(args, run_state):
            return {"profile": {"user_id": args["user_id"], "level": "vip"}}

        tools.register("lookup", lookup_tool)
        workflow = WorkflowConfig.from_dict(
            {
                "id": "subflow-parent",
                "version": 1,
                "nodes": [
                    {
                        "id": "profile_flow",
                        "type": "subflow",
                        "input": {"user_id": "{{context.user_id}}"},
                        "output": {"level": "{{nodes.decorate.output.level}}"},
                        "workflow": {
                            "id": "profile-lookup",
                            "version": 1,
                            "nodes": [
                                {
                                    "id": "lookup",
                                    "type": "tool",
                                    "tool": "lookup",
                                    "args": {"user_id": "{{input.user_id}}"},
                                },
                                {
                                    "id": "decorate",
                                    "type": "transform",
                                    "input": {
                                        "level": "{{nodes.lookup.output.profile.level}}",
                                    },
                                },
                            ],
                        },
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        events = [
            event
            async for event in engine.start(
                StartRunRequest(message="go", context={"user_id": "u_1"})
            )
        ]
        finished = [
            event
            for event in events
            if event.type == "node.finished" and event.node_id == "profile_flow"
        ][0]

        self.assertEqual(finished.data["output"]["workflow_id"], "profile-lookup")
        self.assertEqual(finished.data["output"]["output"], {"level": "vip"})
        self.assertIn("profile_flow.lookup", finished.data["output"]["nodes"])
        self.assertIn("profile_flow.decorate", finished.data["output"]["nodes"])
        self.assertEqual(events[-1].type, "run.finished")

    async def test_subflow_node_namespaces_child_events(self):
        agents = AgentRegistry()
        tools = ToolRegistry()

        async def echo_tool(args, run_state):
            return {"echo": args["value"]}

        tools.register("echo", echo_tool)
        workflow = WorkflowConfig.from_dict(
            {
                "id": "subflow-events",
                "version": 1,
                "nodes": [
                    {
                        "id": "child",
                        "type": "subflow",
                        "input": {"value": "{{input.message}}"},
                        "workflow": {
                            "nodes": [
                                {
                                    "id": "echo",
                                    "type": "tool",
                                    "tool": "echo",
                                    "args": {"value": "{{input.value}}"},
                                }
                            ],
                        },
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        events = [event async for event in engine.start(StartRunRequest(message="hello"))]
        child_started = [
            event
            for event in events
            if event.type == "subflow.node.started" and event.node_id == "child.echo"
        ][0]
        subflow_finished = [event for event in events if event.type == "subflow.finished"][0]

        self.assertEqual(child_started.data["subflow_node_id"], "child")
        self.assertEqual(child_started.data["subflow_event_type"], "node.started")
        self.assertEqual(subflow_finished.data["output"], {"echo": "hello"})

    async def test_subflow_node_namespaces_parallel_branch_events(self):
        agents = AgentRegistry()
        tools = ToolRegistry()

        async def first_tool(args, run_state):
            return {"value": "first"}

        async def second_tool(args, run_state):
            return {"value": "second"}

        tools.register("first", first_tool)
        tools.register("second", second_tool)
        workflow = WorkflowConfig.from_dict(
            {
                "id": "subflow-parallel-events",
                "version": 1,
                "nodes": [
                    {
                        "id": "child",
                        "type": "subflow",
                        "workflow": {
                            "nodes": [
                                {
                                    "id": "fanout",
                                    "type": "parallel",
                                    "branches": [
                                        {"id": "first", "type": "tool", "tool": "first"},
                                        {"id": "second", "type": "tool", "tool": "second"},
                                    ],
                                }
                            ],
                        },
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        events = [event async for event in engine.start(StartRunRequest(message="go"))]
        raw_branch_events = [
            event
            for event in events
            if event.node_id in {"first", "second"} and event.type.startswith("node.")
        ]
        namespaced_branch_events = [
            event
            for event in events
            if event.node_id in {"child.first", "child.second"}
            and event.type == "subflow.node.started"
        ]

        self.assertEqual(raw_branch_events, [])
        self.assertEqual(
            [event.node_id for event in namespaced_branch_events],
            ["child.first", "child.second"],
        )
        self.assertEqual(events[-1].type, "run.finished")

    async def test_subflow_node_failure_fails_parent_run(self):
        agents = AgentRegistry()
        tools = ToolRegistry()

        async def fail_tool(args, run_state):
            raise RuntimeError("boom")

        tools.register("fail", fail_tool)
        workflow = WorkflowConfig.from_dict(
            {
                "id": "subflow-failure",
                "version": 1,
                "nodes": [
                    {
                        "id": "child",
                        "type": "subflow",
                        "workflow": {
                            "nodes": [
                                {"id": "fail", "type": "tool", "tool": "fail"},
                            ],
                        },
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        events = [event async for event in engine.start(StartRunRequest(message="go"))]

        self.assertEqual(events[-1].type, "run.failed")
        self.assertEqual(events[-1].data["error_type"], "WorkflowError")
        self.assertIn("boom", events[-1].data["error"])

    async def test_subflow_node_rejects_human_workflow(self):
        with self.assertRaisesRegex(WorkflowConfigError, "workflow cannot contain human nodes"):
            WorkflowConfig.from_dict(
                {
                    "id": "subflow-human",
                    "version": 1,
                    "nodes": [
                        {
                            "id": "child",
                            "type": "subflow",
                            "workflow": {
                                "nodes": [
                                    {"id": "confirm", "type": "human", "title": "确认"},
                                ],
                            },
                        }
                    ],
                }
            )
