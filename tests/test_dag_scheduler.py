import asyncio

from agent_orchestrator import (
    AgentRegistry,
    InMemoryCheckpointStore,
    StartRunRequest,
    ToolRegistry,
    WorkflowConfig,
    WorkflowEngine,
    WorkflowEvent,
)


async def test_dag_scheduler_runs_ready_branches_concurrently_and_joins():
    agents = AgentRegistry()
    tools = ToolRegistry()
    b_started = asyncio.Event()
    calls: list[str] = []

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
            "id": "dag-join",
            "version": 1,
            "nodes": [
                {"id": "start", "type": "transform", "output": {"ok": True}},
                {"id": "a", "type": "tool", "tool": "tool_a"},
                {"id": "b", "type": "tool", "tool": "tool_b"},
                {
                    "id": "join",
                    "type": "transform",
                    "input": {
                        "a": "{{nodes.a.output.value}}",
                        "b": "{{nodes.b.output.value}}",
                    },
                },
            ],
            "edges": [
                {"from": "start", "to": "a"},
                {"from": "start", "to": "b"},
                {"from": "a", "to": "join"},
                {"from": "b", "to": "join"},
            ],
        }
    )
    engine = WorkflowEngine(workflow, agents=agents, tools=tools)

    events = [event async for event in engine.start(StartRunRequest(message="go"))]

    assert "b-start" in calls
    assert calls.index("b-start") < calls.index("a-finish")
    join_finished = [
        event for event in events if event.type == "node.finished" and event.node_id == "join"
    ][0]
    assert join_finished.data["output"] == {"a": "a", "b": "b"}
    assert events[-1].type == "run.finished"


async def test_concurrent_agent_events_keep_their_node_id():
    agents = AgentRegistry()
    tools = ToolRegistry()
    a_can_finish = asyncio.Event()
    b_started = asyncio.Event()
    seen_current_node_ids: list[tuple[str, str | None]] = []

    async def agent_a(agent_input, run_state):
        seen_current_node_ids.append(("a", run_state.current_node_id))
        await asyncio.wait_for(b_started.wait(), timeout=1)
        yield WorkflowEvent(
            type="agent.delta",
            run_id=run_state.run_id,
            node_id=run_state.current_node_id,
            data={"text": "a"},
        )
        a_can_finish.set()
        yield WorkflowEvent(
            type="agent.output",
            run_id=run_state.run_id,
            node_id=run_state.current_node_id,
            data={"text": "a"},
        )

    async def agent_b(agent_input, run_state):
        seen_current_node_ids.append(("b", run_state.current_node_id))
        b_started.set()
        await asyncio.wait_for(a_can_finish.wait(), timeout=1)
        yield WorkflowEvent(
            type="agent.delta",
            run_id=run_state.run_id,
            node_id=run_state.current_node_id,
            data={"text": "b"},
        )
        yield WorkflowEvent(
            type="agent.output",
            run_id=run_state.run_id,
            node_id=run_state.current_node_id,
            data={"text": "b"},
        )

    agents.register("agent_a", agent_a)
    agents.register("agent_b", agent_b)

    workflow = WorkflowConfig.from_dict(
        {
            "id": "dag-agent-event-node-id",
            "version": 1,
            "nodes": [
                {"id": "a", "type": "agent", "agent": "agent_a"},
                {"id": "b", "type": "agent", "agent": "agent_b"},
            ],
        }
    )
    engine = WorkflowEngine(workflow, agents=agents, tools=tools)

    events = [event async for event in engine.start(StartRunRequest(message="go"))]

    assert sorted(seen_current_node_ids) == [("a", "a"), ("b", "b")]
    agent_events = [
        event
        for event in events
        if event.type in {"agent.delta", "agent.output"}
    ]
    assert [(event.type, event.node_id) for event in agent_events] == [
        ("agent.delta", "a"),
        ("agent.output", "a"),
        ("agent.delta", "b"),
        ("agent.output", "b"),
    ]


async def test_condition_skips_unselected_path_so_join_can_continue():
    agents = AgentRegistry()
    tools = ToolRegistry()

    workflow = WorkflowConfig.from_dict(
        {
            "id": "dag-condition-join",
            "version": 1,
            "nodes": [
                {
                    "id": "route",
                    "type": "condition",
                    "input": {"kind": "{{context.kind}}"},
                    "cases": [
                        {"when": "{{input.kind}} == 'vip'", "value": "vip"},
                        {"when": "{{input.kind}} == 'normal'", "value": "normal"},
                    ],
                },
                {"id": "vip", "type": "transform", "output": {"path": "vip"}},
                {"id": "normal", "type": "transform", "output": {"path": "normal"}},
                {
                    "id": "join",
                    "type": "transform",
                    "input": {"path": "{{nodes.vip.output.path}}"},
                },
            ],
            "edges": [
                {
                    "from": "route",
                    "to": "vip",
                    "when": "{{nodes.route.output.value}} == 'vip'",
                },
                {
                    "from": "route",
                    "to": "normal",
                    "when": "{{nodes.route.output.value}} == 'normal'",
                },
                {"from": "vip", "to": "join"},
                {"from": "normal", "to": "join"},
            ],
        }
    )
    engine = WorkflowEngine(workflow, agents=agents, tools=tools)

    events = [
        event
        async for event in engine.start(StartRunRequest(message="go", context={"kind": "vip"}))
    ]

    assert events[-1].type == "run.finished"
    assert not any(event.type == "node.started" and event.node_id == "normal" for event in events)
    join_finished = [
        event for event in events if event.type == "node.finished" and event.node_id == "join"
    ][0]
    assert join_finished.data["output"] == {"path": "vip"}


async def test_all_success_join_continues_after_unselected_branch_chain_is_skipped():
    agents = AgentRegistry()
    tools = ToolRegistry()

    workflow = WorkflowConfig.from_dict(
        {
            "id": "dag-condition-chain-skip-join",
            "version": 1,
            "nodes": [
                {
                    "id": "route",
                    "type": "condition",
                    "input": {"kind": "{{context.kind}}"},
                    "cases": [{"when": "{{input.kind}} == 'fast'", "value": "fast"}],
                    "default": "slow",
                },
                {"id": "fast", "type": "transform", "output": {"value": "fast"}},
                {"id": "slow_parent", "type": "transform", "output": {"value": "parent"}},
                {"id": "late", "type": "transform", "output": {"value": "late"}},
                {
                    "id": "join",
                    "type": "transform",
                    "join_policy": "all_success",
                    "input": {"value": "{{nodes.fast.output.value}}"},
                },
            ],
            "edges": [
                {
                    "from": "route",
                    "to": "fast",
                    "when": "{{nodes.route.output.value}} == 'fast'",
                },
                {
                    "from": "route",
                    "to": "slow_parent",
                    "when": "{{nodes.route.output.value}} == 'slow'",
                },
                {"from": "slow_parent", "to": "late"},
                {"from": "fast", "to": "join"},
                {"from": "late", "to": "join"},
            ],
        }
    )
    engine = WorkflowEngine(workflow, agents=agents, tools=tools)

    events = [
        event
        async for event in engine.start(StartRunRequest(message="go", context={"kind": "fast"}))
    ]

    assert events[-1].type == "run.finished"
    assert not any(event.type == "node.started" and event.node_id == "slow_parent" for event in events)
    assert not any(event.type == "node.started" and event.node_id == "late" for event in events)
    assert any(event.type == "node.finished" and event.node_id == "join" for event in events)


async def test_downstream_node_is_not_skipped_before_unselected_branch_propagates():
    agents = AgentRegistry()
    tools = ToolRegistry()

    workflow = WorkflowConfig.from_dict(
        {
            "id": "dag-do-not-skip-downstream-end-early",
            "version": 1,
            "nodes": [
                {
                    "id": "route",
                    "type": "condition",
                    "input": {"kind": "{{context.kind}}"},
                    "cases": [{"when": "{{input.kind}} == 'fast'", "value": "fast"}],
                    "default": "slow",
                },
                {"id": "fast", "type": "transform", "output": {"value": "fast"}},
                {"id": "end", "type": "transform", "input": {"value": "{{nodes.final.output.value}}"}},
                {"id": "slow_parent", "type": "transform", "output": {"value": "parent"}},
                {"id": "late", "type": "transform", "output": {"value": "late"}},
                {
                    "id": "final",
                    "type": "transform",
                    "join_policy": "all_success",
                    "input": {"value": "{{nodes.fast.output.value}}"},
                },
            ],
            "edges": [
                {
                    "from": "route",
                    "to": "fast",
                    "when": "{{nodes.route.output.value}} == 'fast'",
                },
                {
                    "from": "route",
                    "to": "slow_parent",
                    "when": "{{nodes.route.output.value}} == 'slow'",
                },
                {"from": "slow_parent", "to": "late"},
                {"from": "fast", "to": "final"},
                {"from": "late", "to": "final"},
                {"from": "final", "to": "end"},
            ],
        }
    )
    engine = WorkflowEngine(workflow, agents=agents, tools=tools)

    events = [
        event
        async for event in engine.start(StartRunRequest(message="go", context={"kind": "fast"}))
    ]

    assert events[-1].type == "run.finished"
    assert any(event.type == "node.finished" and event.node_id == "final" for event in events)
    assert any(event.type == "node.finished" and event.node_id == "end" for event in events)


async def test_all_success_join_waits_for_inactive_branch_to_resolve():
    agents = AgentRegistry()
    tools = ToolRegistry()
    release_parent = asyncio.Event()
    events_seen: list[tuple[str, str | None]] = []

    async def fast(args, run_state):
        return {"value": "fast"}

    async def slow_parent(args, run_state):
        await asyncio.wait_for(release_parent.wait(), timeout=1)
        return {"value": "parent"}

    tools.register("fast", fast)
    tools.register("slow_parent", slow_parent)

    workflow = WorkflowConfig.from_dict(
        {
            "id": "dag-all-success-waits-for-inactive",
            "version": 1,
            "nodes": [
                {"id": "route", "type": "condition", "output": {"value": "go"}},
                {"id": "fast", "type": "tool", "tool": "fast"},
                {"id": "slow_parent", "type": "tool", "tool": "slow_parent"},
                {"id": "late", "type": "transform", "output": {"value": "late"}},
                {
                    "id": "join",
                    "type": "transform",
                    "join_policy": "all_success",
                    "input": {
                        "fast": "{{nodes.fast.output.value}}",
                        "late": "{{nodes.late.output.value}}",
                    },
                },
            ],
            "edges": [
                {"from": "route", "to": "fast"},
                {"from": "fast", "to": "join"},
                {"from": "slow_parent", "to": "late"},
                {"from": "late", "to": "join"},
            ],
        }
    )
    engine = WorkflowEngine(workflow, agents=agents, tools=tools)

    async def collect_events():
        async for event in engine.start(StartRunRequest(message="go")):
            events_seen.append((event.type, event.node_id))
            if event.type == "node.finished" and event.node_id == "fast":
                await asyncio.sleep(0)
                assert ("node.started", "join") not in events_seen
                release_parent.set()

    await collect_events()

    assert events_seen.index(("node.finished", "late")) < events_seen.index(("node.started", "join"))
    assert events_seen[-1] == ("run.finished", None)


async def test_human_wait_does_not_block_independent_ready_nodes():
    agents = AgentRegistry()
    tools = ToolRegistry()

    async def slow_tool(args, run_state):
        await asyncio.sleep(0.01)
        return {"value": "auto"}

    tools.register("slow", slow_tool)
    checkpoints = InMemoryCheckpointStore()
    workflow = WorkflowConfig.from_dict(
        {
            "id": "dag-human-concurrent",
            "version": 1,
            "nodes": [
                {"id": "confirm", "type": "human", "title": "Confirm"},
                {"id": "auto", "type": "tool", "tool": "slow"},
                {
                    "id": "join",
                    "type": "transform",
                    "input": {
                        "decision": "{{nodes.confirm.output.decision}}",
                        "auto": "{{nodes.auto.output.value}}",
                    },
                },
            ],
            "edges": [
                {"from": "confirm", "to": "join"},
                {"from": "auto", "to": "join"},
            ],
        }
    )
    engine = WorkflowEngine(workflow, agents=agents, tools=tools, checkpoints=checkpoints)

    first_events = [event async for event in engine.start(StartRunRequest(message="go"))]

    assert first_events[-1].type == "run.waiting"
    assert any(event.type == "node.finished" and event.node_id == "auto" for event in first_events)
    required = [event for event in first_events if event.type == "human.required"][0]
    pending_action_id = required.data["pending_action_id"]

    resumed_events = [
        event
        async for event in engine.resume(
            pending_action_id=pending_action_id,
            decision={"decision": "approve"},
        )
    ]

    assert resumed_events[-1].type == "run.finished"
    join_finished = [
        event for event in resumed_events if event.type == "node.finished" and event.node_id == "join"
    ][0]
    assert join_finished.data["output"] == {"decision": "approve", "auto": "auto"}


async def test_join_policy_any_runs_after_first_active_predecessor():
    agents = AgentRegistry()
    tools = ToolRegistry()
    release_slow = asyncio.Event()
    calls: list[str] = []

    async def fast(args, run_state):
        calls.append("fast")
        return {"value": "fast"}

    async def slow(args, run_state):
        await release_slow.wait()
        calls.append("slow")
        return {"value": "slow"}

    tools.register("fast", fast)
    tools.register("slow", slow)

    workflow = WorkflowConfig.from_dict(
        {
            "id": "dag-join-any",
            "version": 1,
            "nodes": [
                {"id": "fast", "type": "tool", "tool": "fast"},
                {"id": "slow", "type": "tool", "tool": "slow"},
                {
                    "id": "first",
                    "type": "transform",
                    "join_policy": "any",
                    "input": {"value": "{{nodes.fast.output.value}}"},
                },
            ],
            "edges": [
                {"from": "fast", "to": "first"},
                {"from": "slow", "to": "first"},
            ],
        }
    )
    engine = WorkflowEngine(workflow, agents=agents, tools=tools)

    events = []
    async for event in engine.start(StartRunRequest(message="go")):
        events.append(event)
        if event.type == "node.finished" and event.node_id == "first":
            release_slow.set()

    first_finished_index = next(
        index
        for index, event in enumerate(events)
        if event.type == "node.finished" and event.node_id == "first"
    )
    slow_finished_index = next(
        index
        for index, event in enumerate(events)
        if event.type == "node.finished" and event.node_id == "slow"
    )
    assert first_finished_index < slow_finished_index
    assert calls == ["fast", "slow"]
