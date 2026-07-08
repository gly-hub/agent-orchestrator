import pytest

from agent_orchestrator import (
    AgentRegistry,
    StartRunRequest,
    ToolRegistry,
    WorkflowConfig,
    WorkflowEngine,
)
from agent_orchestrator.exceptions import WorkflowConfigError, WorkflowError


async def test_basic_loop_fixed_iterations():
    tools = ToolRegistry()
    counter = {"n": 0}

    async def increment(args, run_state):
        counter["n"] += 1
        return {"count": counter["n"]}

    tools.register("increment", increment)
    workflow = WorkflowConfig.from_dict({
        "id": "loop-test",
        "version": 1,
        "nodes": [{
            "id": "counter_loop",
            "type": "loop",
            "max_iterations": 3,
            "body": {
                "nodes": [
                    {"id": "inc", "type": "tool", "tool": "increment"},
                ],
            },
        }],
    })
    engine = WorkflowEngine(
        workflow, agents=AgentRegistry(), tools=tools, raise_on_error=True,
    )
    events = [e async for e in engine.start(StartRunRequest(message="go"))]

    assert counter["n"] == 3
    finished = [e for e in events if e.type == "node.finished" and e.node_id == "counter_loop"]
    assert len(finished) == 1
    output = finished[0].data["output"]
    assert output["iterations"] == 3
    assert len(output["outputs"]) == 3
    assert output["last_output"] == {"count": 3}

async def test_loop_condition_exit():
    tools = ToolRegistry()
    call_count = {"n": 0}

    async def check(args, run_state):
        call_count["n"] += 1
        return {"done": call_count["n"] >= 2}

    tools.register("check", check)
    workflow = WorkflowConfig.from_dict({
        "id": "cond-loop",
        "version": 1,
        "nodes": [{
            "id": "poll",
            "type": "loop",
            "max_iterations": 10,
            "condition": "{{nodes.poll.output.last_output.done}} != true",
            "body": {
                "nodes": [
                    {"id": "chk", "type": "tool", "tool": "check"},
                ],
            },
        }],
    })
    engine = WorkflowEngine(
        workflow, agents=AgentRegistry(), tools=tools, raise_on_error=True,
    )
    events = [e async for e in engine.start(StartRunRequest(message="go"))]

    assert call_count["n"] == 2
    finished = [e for e in events if e.type == "node.finished" and e.node_id == "poll"]
    output = finished[0].data["output"]
    assert output["iterations"] == 2

async def test_loop_max_iterations_cap():
    tools = ToolRegistry()
    counter = {"n": 0}

    async def noop(args, run_state):
        counter["n"] += 1
        return {"ok": True}

    tools.register("noop", noop)
    workflow = WorkflowConfig.from_dict({
        "id": "cap-loop",
        "version": 1,
        "nodes": [{
            "id": "capped",
            "type": "loop",
            "max_iterations": 5,
            "condition": "1 == 1",
            "body": {
                "nodes": [
                    {"id": "op", "type": "tool", "tool": "noop"},
                ],
            },
        }],
    })
    engine = WorkflowEngine(
        workflow, agents=AgentRegistry(), tools=tools, raise_on_error=True,
    )
    events = [e async for e in engine.start(StartRunRequest(message="go"))]

    assert counter["n"] == 5
    finished = [e for e in events if e.type == "node.finished" and e.node_id == "capped"]
    assert finished[0].data["output"]["iterations"] == 5

async def test_loop_zero_iterations_when_condition_false():
    tools = ToolRegistry()
    workflow = WorkflowConfig.from_dict({
        "id": "no-iter-loop",
        "version": 1,
        "nodes": [
            {
                "id": "setup",
                "type": "transform",
                "input": {"ready": True},
            },
            {
                "id": "loop_node",
                "type": "loop",
                "max_iterations": 10,
                "condition": "{{nodes.setup.output.ready}} != true",
                "body": {
                    "nodes": [
                        {"id": "never", "type": "transform", "input": {"x": 1}},
                    ],
                },
            },
        ],
    })
    engine = WorkflowEngine(
        workflow, agents=AgentRegistry(), tools=tools, raise_on_error=True,
    )
    events = [e async for e in engine.start(StartRunRequest(message="go"))]

    finished = [e for e in events if e.type == "node.finished" and e.node_id == "loop_node"]
    output = finished[0].data["output"]
    assert output["iterations"] == 1
    assert len(output["outputs"]) == 1

async def test_loop_events_are_namespaced():
    tools = ToolRegistry()

    async def echo(args, run_state):
        return {"echoed": True}

    tools.register("echo", echo)
    workflow = WorkflowConfig.from_dict({
        "id": "ns-loop",
        "version": 1,
        "nodes": [{
            "id": "my_loop",
            "type": "loop",
            "max_iterations": 2,
            "body": {
                "nodes": [
                    {"id": "step", "type": "tool", "tool": "echo"},
                ],
            },
        }],
    })
    engine = WorkflowEngine(
        workflow, agents=AgentRegistry(), tools=tools, raise_on_error=True,
    )
    events = [e async for e in engine.start(StartRunRequest(message="go"))]

    loop_events = [e for e in events if e.type.startswith("loop.")]
    iteration_started = [e for e in events if e.type == "loop.iteration_started"]
    iteration_finished = [e for e in events if e.type == "loop.iteration_finished"]

    assert len(iteration_started) == 2
    assert len(iteration_finished) == 2
    assert len(loop_events) > 0

    namespaced = [e for e in loop_events if e.node_id and "iteration_0" in e.node_id]
    assert len(namespaced) > 0

async def test_loop_iteration_output_accessible_in_state():
    tools = ToolRegistry()
    call_count = {"n": 0}

    async def accumulate(args, run_state):
        call_count["n"] += 1
        return {"step": call_count["n"]}

    tools.register("accumulate", accumulate)
    workflow = WorkflowConfig.from_dict({
        "id": "acc-loop",
        "version": 1,
        "nodes": [{
            "id": "acc",
            "type": "loop",
            "max_iterations": 3,
            "body": {
                "nodes": [
                    {"id": "do", "type": "tool", "tool": "accumulate"},
                ],
            },
        }],
    })
    engine = WorkflowEngine(
        workflow, agents=AgentRegistry(), tools=tools, raise_on_error=True,
    )
    events = [e async for e in engine.start(StartRunRequest(message="go"))]

    finished = [e for e in events if e.type == "node.finished" and e.node_id == "acc"]
    output = finished[0].data["output"]
    assert output["iterations"] == 3
    assert output["outputs"] == [{"step": 1}, {"step": 2}, {"step": 3}]

async def test_loop_body_error_propagates():
    tools = ToolRegistry()

    async def fail_tool(args, run_state):
        raise RuntimeError("boom")

    tools.register("fail_tool", fail_tool)
    workflow = WorkflowConfig.from_dict({
        "id": "err-loop",
        "version": 1,
        "nodes": [{
            "id": "bad_loop",
            "type": "loop",
            "max_iterations": 3,
            "body": {
                "nodes": [
                    {"id": "fail", "type": "tool", "tool": "fail_tool"},
                ],
            },
        }],
    })
    engine = WorkflowEngine(
        workflow, agents=AgentRegistry(), tools=tools, raise_on_error=True,
    )
    with pytest.raises(WorkflowError):
        [e async for e in engine.start(StartRunRequest(message="go"))]


def test_loop_missing_body():
    with pytest.raises(WorkflowConfigError):
        WorkflowConfig.from_dict({
            "id": "bad",
            "version": 1,
            "nodes": [{"id": "l", "type": "loop"}],
        })

def test_loop_body_must_have_nodes():
    with pytest.raises(WorkflowConfigError):
        WorkflowConfig.from_dict({
            "id": "bad",
            "version": 1,
            "nodes": [{"id": "l", "type": "loop", "body": {}}],
        })

def test_loop_invalid_max_iterations():
    with pytest.raises(WorkflowConfigError):
        WorkflowConfig.from_dict({
            "id": "bad",
            "version": 1,
            "nodes": [{
                "id": "l",
                "type": "loop",
                "max_iterations": 0,
                "body": {"nodes": [{"id": "x", "type": "transform"}]},
            }],
        })

def test_loop_human_in_body_rejected():
    with pytest.raises(WorkflowConfigError):
        WorkflowConfig.from_dict({
            "id": "bad",
            "version": 1,
            "nodes": [{
                "id": "l",
                "type": "loop",
                "body": {
                    "nodes": [{"id": "h", "type": "human"}],
                },
            }],
        })
