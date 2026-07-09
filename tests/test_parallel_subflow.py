import asyncio

from agent_orchestrator import (
    AgentRegistry,
    StartRunRequest,
    ToolRegistry,
    WorkflowConfig,
    WorkflowEngine,
)


async def test_parallel_node_runs_branches_concurrently_and_merges_outputs():
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

    assert "b-start" in calls
    assert calls.index("b-start") < calls.index("a-finish")
    assert finished.data["output"] == {
        "branches": {
            "a": {"value": "a"},
            "b": {"value": "b"},
        },
        "failed_branches": [],
    }
    assert events[-1].type == "run.finished"

async def test_parallel_node_streams_branch_events_as_they_happen():
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

    assert branch_finishes == ["fast", "slow"]

async def test_parallel_workflow_branch_streams_child_events_while_child_runs():
    agents = AgentRegistry()
    tools = ToolRegistry()
    release_child = asyncio.Event()

    async def blocking_tool(args, run_state):
        await release_child.wait()
        return {"value": "done"}

    tools.register("blocking", blocking_tool)
    workflow = WorkflowConfig.from_dict(
        {
            "id": "parallel-workflow-live-events",
            "version": 1,
            "nodes": [
                {
                    "id": "fanout",
                    "type": "parallel",
                    "branches": [
                        {
                            "id": "child",
                            "workflow": {
                                "nodes": [
                                    {"id": "block", "type": "tool", "tool": "blocking"},
                                ],
                            },
                        }
                    ],
                }
            ],
        }
    )
    engine = WorkflowEngine(workflow, agents=agents, tools=tools)
    stream = engine.start(StartRunRequest(message="go")).__aiter__()

    async def next_child_start():
        for _ in range(10):
            event = await anext(stream)
            if event.type == "parallel.node.started" and event.node_id == "child.block":
                return event
        raise AssertionError("child workflow start event was not streamed")

    try:
        child_started = await asyncio.wait_for(next_child_start(), timeout=1)
        assert child_started.data["parallel_branch_id"] == "child"
        assert not release_child.is_set()
    finally:
        release_child.set()

    remaining = [event async for event in stream]
    assert remaining[-1].type == "run.finished"

async def test_parallel_node_cancels_branch_tasks_when_stream_closes():
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

    assert (await anext(stream)).type == "run.started"
    assert (await anext(stream)).type == "node.started"
    assert (await anext(stream)).type == "node.started"
    await asyncio.wait_for(branch_started.wait(), timeout=1)

    await stream.aclose()

    await asyncio.wait_for(branch_cancelled.wait(), timeout=1)

async def test_parallel_node_continue_policy_keeps_failed_branch_output():
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

    assert events[-1].type == "run.finished"
    assert finished.data["output"]["branches"]["ok"] == {"ok": True}
    assert finished.data["output"]["branches"]["fail"]["failed"] is True
    assert finished.data["output"]["failed_branches"][0]["id"] == "fail"

async def test_parallel_node_runs_multi_node_workflow_branch():
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

    assert branch_started.data["parallel_branch_id"] == "profile"
    assert finished.data["output"]["branches"]["profile"] == {"level": "vip"}
    assert "profile.lookup" in finished.data["output"]["nodes"]
    assert "profile.decorate" in finished.data["output"]["nodes"]
    assert events[-1].type == "run.finished"

async def test_parallel_node_default_failure_policy_fails_run():
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

    assert events[-1].type == "run.failed"
    assert events[-1].data["error_type"] == "WorkflowError"
    assert "parallel branches failed: fail" in events[-1].data["error"]

async def test_parallel_node_accepts_human_branch():
    workflow = WorkflowConfig.from_dict(
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
    assert workflow.id == "parallel-human"

async def test_subflow_node_runs_reusable_workflow_and_selects_output():
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

    assert finished.data["output"]["workflow_id"] == "profile-lookup"
    assert finished.data["output"]["output"] == {"level": "vip"}
    assert "profile_flow.lookup" in finished.data["output"]["nodes"]
    assert "profile_flow.decorate" in finished.data["output"]["nodes"]
    assert events[-1].type == "run.finished"

async def test_subflow_node_namespaces_child_events():
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

    assert child_started.data["subflow_node_id"] == "child"
    assert child_started.data["subflow_event_type"] == "node.started"
    assert subflow_finished.data["output"] == {"echo": "hello"}

async def test_subflow_node_namespaces_parallel_branch_events():
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

    assert raw_branch_events == []
    assert [event.node_id for event in namespaced_branch_events] == ["child.first", "child.second"]
    assert events[-1].type == "run.finished"

async def test_subflow_node_failure_fails_parent_run():
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

    assert events[-1].type == "run.failed"
    assert events[-1].data["error_type"] == "WorkflowError"
    assert "boom" in events[-1].data["error"]

async def test_subflow_node_accepts_human_workflow():
    workflow = WorkflowConfig.from_dict(
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
    assert workflow.id == "subflow-human"

async def test_subflow_with_human_node_pauses_and_resumes():
    agents = AgentRegistry()
    tools = ToolRegistry()

    async def greet(args, run_state):
        return {"greeting": f"Hello {args.get('name', 'world')}"}

    tools.register("greet", greet)
    workflow = WorkflowConfig.from_dict(
        {
            "id": "subflow-human-flow",
            "version": 1,
            "nodes": [
                {
                    "id": "child",
                    "type": "subflow",
                    "workflow": {
                        "nodes": [
                            {"id": "ask", "type": "human", "title": "Enter name"},
                            {"id": "say_hi", "type": "tool", "tool": "greet",
                             "args": {"name": "{{nodes.ask.output.name}}"}},
                        ],
                    },
                }
            ],
        }
    )
    engine = WorkflowEngine(workflow, agents=agents, tools=tools, raise_on_error=True)

    events = [e async for e in engine.start(StartRunRequest(message="go"))]

    waiting = [e for e in events if e.type == "run.waiting"]
    assert len(waiting) == 1
    human_required = [e for e in events if e.type == "human.required"]
    assert len(human_required) == 1
    action_id = human_required[0].data["pending_action_id"]

    resumed_events = [
        e async for e in engine.resume(
            pending_action_id=action_id,
            decision={"decision": "approve", "name": "Alice"},
        )
    ]

    assert resumed_events[-1].type == "run.finished"
    node_finished = [e for e in resumed_events if e.type == "node.finished" and e.node_id == "child"]
    assert len(node_finished) == 1
    output = node_finished[0].data["output"]
    assert output["output"] == {"greeting": "Hello Alice"}

async def test_parallel_workflow_branch_with_human_pauses_and_resumes():
    agents = AgentRegistry()
    tools = ToolRegistry()

    async def process(args, run_state):
        return {"result": f"processed {args.get('value', '')}"}

    tools.register("process", process)
    workflow = WorkflowConfig.from_dict(
        {
            "id": "parallel-human-flow",
            "version": 1,
            "nodes": [
                {
                    "id": "fanout",
                    "type": "parallel",
                    "branches": [
                        {
                            "id": "auto",
                            "workflow": {
                                "nodes": [
                                    {"id": "step", "type": "tool", "tool": "process",
                                     "args": {"value": "auto"}},
                                ],
                            },
                        },
                        {
                            "id": "manual",
                            "workflow": {
                                "nodes": [
                                    {"id": "confirm", "type": "human", "title": "Confirm"},
                                    {"id": "act", "type": "tool", "tool": "process",
                                     "args": {"value": "{{nodes.confirm.output.choice}}"}},
                                ],
                            },
                        },
                    ],
                }
            ],
        }
    )
    engine = WorkflowEngine(workflow, agents=agents, tools=tools, raise_on_error=True)

    events = [e async for e in engine.start(StartRunRequest(message="go"))]

    waiting = [e for e in events if e.type == "run.waiting"]
    assert len(waiting) == 1
    human_required = [e for e in events if e.type == "human.required"]
    assert len(human_required) == 1
    action_id = human_required[0].data["pending_action_id"]

    resumed_events = [
        e async for e in engine.resume(
            pending_action_id=action_id,
            decision={"decision": "approve", "choice": "manual_value"},
        )
    ]

    assert resumed_events[-1].type == "run.finished"
    node_finished = [e for e in resumed_events if e.type == "node.finished" and e.node_id == "fanout"]
    assert len(node_finished) == 1
    output = node_finished[0].data["output"]
    assert output["branches"]["auto"] == {"result": "processed auto"}
    assert output["branches"]["manual"] == {"result": "processed manual_value"}
