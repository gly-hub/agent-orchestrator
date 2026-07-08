"""Service-style demo that emits SSE frames for start and resume phases.

Demonstrates: to_message_event conversion, stream_sse helper, two-phase flow.
Chinese strings in node configs are sample UI labels.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(PROJECT_SRC))

from agent_orchestrator import (
    AgentRegistry,
    StartRunRequest,
    ToolRegistry,
    WorkflowConfig,
    WorkflowEngine,
    WorkflowEvent,
    stream_sse,
    to_message_event,
)


async def planner(agent_input, run_state):
    yield WorkflowEvent(
        type="agent.delta",
        run_id=run_state.run_id,
        node_id="planner",
        data={"text": "正在分析请求"},
    )
    yield WorkflowEvent(
        type="agent.output",
        run_id=run_state.run_id,
        node_id="planner",
        data={"needs_confirm": True},
    )


async def executor(agent_input, run_state):
    yield WorkflowEvent(
        type="agent.delta",
        run_id=run_state.run_id,
        node_id="executor",
        data={"text": f"已执行：{agent_input['command']}"},
    )


def build_engine() -> WorkflowEngine:
    agents = AgentRegistry()
    agents.register("planner", planner)
    agents.register("executor", executor)

    tools = ToolRegistry()
    workflow = WorkflowConfig.from_dict(
        {
            "id": "service-demo",
            "version": 1,
            "nodes": [
                {"id": "planner", "type": "agent", "agent": "planner"},
                {
                    "id": "confirm",
                    "type": "human",
                    "title": "确认执行",
                    "message": "是否允许继续执行该请求？",
                    "options": [
                        {"id": "approve", "label": "确认"},
                        {"id": "reject", "label": "取消"},
                    ],
                },
                {
                    "id": "executor",
                    "type": "agent",
                    "agent": "executor",
                    "input": {"command": "{{input.message}}"},
                },
            ],
        }
    )
    return WorkflowEngine(workflow, agents=agents, tools=tools)


async def main() -> None:
    engine = build_engine()
    pending_action_id = ""

    print("== POST /chat/stream ==")
    events = []
    async for event in engine.start(StartRunRequest(message="deploy service")):
        events.append(event)
        print(to_message_event(event))

    for event in events:
        if event.type == "human.required":
            pending_action_id = event.data["pending_action_id"]

    print("== POST /runs/{run_id}/resume/stream ==")
    async for frame in stream_sse(
        engine.resume(
            pending_action_id=pending_action_id,
            decision={"decision": "approve"},
        )
    ):
        print(frame, end="")


if __name__ == "__main__":
    asyncio.run(main())
