from agent_orchestrator import RunState, WorkflowConfig
from agent_orchestrator.retry import retry_delay_ms, should_retry
from agent_orchestrator.router import WorkflowRouter


async def test_workflow_router_resolves_sequential_conditional_and_error_edges():
    workflow = WorkflowConfig.from_dict(
        {
            "id": "router-demo",
            "version": 1,
            "nodes": [
                {"id": "start", "type": "transform"},
                {"id": "vip", "type": "transform"},
                {"id": "fallback", "type": "transform"},
                {"id": "recover", "type": "transform"},
            ],
            "edges": [
                {
                    "from": "start",
                    "to": "vip",
                    "when": "{{context.level}} == 'vip'",
                },
                {
                    "from": "start",
                    "to": "recover",
                    "on_error": True,
                },
            ],
        }
    )
    router = WorkflowRouter(workflow)
    run_state = RunState(
        run_id="run_router",
        workflow_id="router-demo",
        workflow_version=1,
        status="running",
        current_node_id=None,
        state={"context": {"level": "vip"}, "nodes": {}},
    )

    assert router.next_node_id(run_state) == "start"

    run_state.current_node_id = "start"
    run_state.state["nodes"]["start"] = {"status": "success"}
    assert router.next_node_id(run_state) == "vip"

    run_state.state["nodes"]["start"] = {"status": "failed"}
    assert router.next_node_id(run_state) == "recover"
    assert router.has_error_edge("start")
    assert not router.has_error_edge("vip")

    run_state.current_node_id = "vip"
    run_state.state["nodes"]["vip"] = {"status": "pending"}
    assert router.next_node_id(run_state) == "vip"

async def test_retry_helpers_match_error_type_and_calculate_backoff():
    error = RuntimeError("temporary")

    assert should_retry(error, ())
    assert should_retry(error, ("RuntimeError",))
    assert should_retry(error, ("builtins.RuntimeError",))
    assert not should_retry(error, ("ValueError",))
    assert retry_delay_ms(
        base_delay_ms=100,
        max_delay_ms=250,
        backoff_multiplier=2,
        attempt=3,
    ) == 250
