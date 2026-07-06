"""Run a resumable workflow using SQLite checkpoint and event stores."""

from __future__ import annotations

import asyncio
import tempfile
import sys
from pathlib import Path

PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(PROJECT_SRC))

from agent_orchestrator import (
    AgentRegistry,
    SQLiteCheckpointStore,
    SQLiteEventStore,
    StartRunRequest,
    ToolRegistry,
    WorkflowConfig,
    WorkflowEngine,
    WorkflowEvent,
    replay_run,
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
        type="agent.output",
        run_id=run_state.run_id,
        node_id="executor",
        data={"answer": f"处理完成：{agent_input['profile']['level']} 用户"},
    )


async def query_profile(args, run_state):
    return {"profile": {"user_id": args["user_id"], "level": "vip"}}


def build_workflow() -> WorkflowConfig:
    return WorkflowConfig.from_dict(
        {
            "id": "sqlite-demo",
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


async def main() -> None:
    agents = AgentRegistry()
    agents.register("planner", planner)
    agents.register("executor", executor)

    tools = ToolRegistry()
    tools.register("query_profile", query_profile)

    workflow = build_workflow()

    with tempfile.TemporaryDirectory() as tmp:
        sqlite_path = Path(tmp) / "workflow.sqlite"
        events = SQLiteEventStore(sqlite_path)

        first_engine = WorkflowEngine(
            workflow,
            agents=agents,
            tools=tools,
            checkpoints=SQLiteCheckpointStore(sqlite_path),
            event_store=events,
        )

        pending_action_id = None
        run_id = None
        print("== phase 1: start and checkpoint ==")
        async for event in first_engine.start(
            StartRunRequest(message="帮我处理这个用户", context={"user_id": "u_1"})
        ):
            print(event)
            run_id = event.run_id
            if event.type == "human.required":
                pending_action_id = event.data["pending_action_id"]

        if not pending_action_id or not run_id:
            return

        second_engine = WorkflowEngine(
            workflow,
            agents=agents,
            tools=tools,
            checkpoints=SQLiteCheckpointStore(sqlite_path),
            event_store=events,
        )

        print("== phase 2: resume from another engine ==")
        async for event in second_engine.resume(
            pending_action_id=pending_action_id,
            decision={"decision": "approve"},
        ):
            print(event)

        replay = await replay_run(events, run_id)
        print("== replay ==")
        print({"run_id": replay.run_id, "status": replay.status, "events": len(replay.workflow_events)})


if __name__ == "__main__":
    asyncio.run(main())
