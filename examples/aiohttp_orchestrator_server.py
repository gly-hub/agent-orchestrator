"""Minimal aiohttp server for the workflow engine.

Run:
    python3 examples/aiohttp_orchestrator_server.py

Then:
    curl -N -X POST http://127.0.0.1:8088/chat/stream \
      -H 'Content-Type: application/json' \
      -d '{"message":"deploy service"}'
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from aiohttp import web

PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(PROJECT_SRC))

from agent_orchestrator import (  # noqa: E402
    AgentRegistry,
    StartRunRequest,
    ToolRegistry,
    WorkflowConfig,
    WorkflowEngine,
    WorkflowEvent,
    stream_sse,
)


async def planner(agent_input: dict[str, Any], run_state):
    yield WorkflowEvent(
        type="agent.delta",
        run_id=run_state.run_id,
        node_id="planner",
        data={"text": "正在分析请求..."},
    )
    yield WorkflowEvent(
        type="agent.output",
        run_id=run_state.run_id,
        node_id="planner",
        data={"needs_confirm": True},
    )


async def executor(agent_input: dict[str, Any], run_state):
    yield WorkflowEvent(
        type="agent.delta",
        run_id=run_state.run_id,
        node_id="executor",
        data={"text": f"已继续执行：{agent_input['message']}"},
    )


def build_engine() -> WorkflowEngine:
    agents = AgentRegistry()
    agents.register("planner", planner)
    agents.register("executor", executor)

    tools = ToolRegistry()
    workflow = WorkflowConfig.from_dict(
        {
            "id": "aiohttp-demo",
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
                    "input": {"message": "{{input.message}}"},
                },
            ],
        }
    )
    return WorkflowEngine(workflow, agents=agents, tools=tools)


async def write_sse_response(request: web.Request, frames) -> web.StreamResponse:
    response = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )
    await response.prepare(request)

    async for frame in frames:
        await response.write(frame.encode("utf-8"))

    await response.write_eof()
    return response


async def chat_stream(request: web.Request) -> web.StreamResponse:
    body = await request.json()
    engine: WorkflowEngine = request.app["engine"]
    events = engine.start(
        StartRunRequest(
            message=body["message"],
            context=body.get("context", {}),
            run_id=body.get("run_id"),
            user_message_id=body.get("user_message_id"),
            assistant_message_id=body.get("assistant_message_id"),
            bubble_id=body.get("bubble_id"),
        )
    )
    return await write_sse_response(request, stream_sse(events))


async def confirm_action(request: web.Request) -> web.Response:
    pending_action_id = request.match_info["pending_action_id"]
    body = await request.json()
    decision = body.get("decision", "approve")
    payload = body.get("payload", {})
    return web.json_response(
        {
            "pending_action_id": pending_action_id,
            "decision": {"decision": decision, **payload},
            "resume_url": f"/runs/{request.match_info['run_id']}/resume/stream"
            f"?pending_action_id={pending_action_id}",
        }
    )


async def resume_stream(request: web.Request) -> web.StreamResponse:
    body = await _read_optional_json(request)
    pending_action_id = body.get("pending_action_id") or request.query.get("pending_action_id")
    if not pending_action_id:
        raise web.HTTPBadRequest(text="pending_action_id is required")

    decision = body.get("decision") or {"decision": request.query.get("decision", "approve")}
    engine: WorkflowEngine = request.app["engine"]
    return await write_sse_response(
        request,
        stream_sse(engine.resume(pending_action_id=pending_action_id, decision=decision)),
    )


async def _read_optional_json(request: web.Request) -> dict[str, Any]:
    if request.can_read_body:
        raw = await request.read()
        if raw:
            return json.loads(raw)
    return {}


def create_app() -> web.Application:
    app = web.Application()
    app["engine"] = build_engine()
    app.router.add_post("/chat/stream", chat_stream)
    app.router.add_post(
        "/runs/{run_id}/actions/{pending_action_id}/confirm",
        confirm_action,
    )
    app.router.add_post("/runs/{run_id}/resume/stream", resume_stream)
    app.router.add_get("/runs/{run_id}/resume/stream", resume_stream)
    return app


def main() -> None:
    web.run_app(create_app(), host="127.0.0.1", port=8088)


if __name__ == "__main__":
    main()
