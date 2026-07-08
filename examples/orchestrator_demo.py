"""Run a small in-memory workflow with a human checkpoint.

Demonstrates: agent nodes, tool nodes, human confirmation, and resume flow.
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
)


async def planner(agent_input, run_state):
    yield WorkflowEvent(
        type="agent.output",
        run_id=run_state.run_id,
        node_id="planner",
        data={"requires_confirmation": True},
    )


async def executor(agent_input, run_state):
    yield WorkflowEvent(
        type="agent.delta",
        run_id=run_state.run_id,
        node_id="executor",
        data={"text": f"处理完成：{agent_input['profile']['level']} 用户"},
    )


async def query_profile(args, run_state):
    return {"profile": {"user_id": args["user_id"], "level": "vip"}}


async def main() -> None:
    agents = AgentRegistry()
    agents.register("planner", planner)
    agents.register("executor", executor)

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
                    "title": "确认继续",
                    "message": "是否继续处理该请求？",
                    "options": [
                        {"id": "approve", "label": "确认"},
                        {"id": "reject", "label": "取消"},
                    ],
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

    pending_action_id = None
    print("== phase 1 ==")
    async for event in engine.start(
        StartRunRequest(message="帮我处理这个用户", context={"user_id": "u_1"})
    ):
        print(event)
        if event.type == "human.required":
            pending_action_id = event.data["pending_action_id"]

    if not pending_action_id:
        return

    print("== phase 2 ==")
    async for event in engine.resume(
        pending_action_id=pending_action_id,
        decision={"decision": "approve"},
    ):
        print(event)


if __name__ == "__main__":
    asyncio.run(main())
