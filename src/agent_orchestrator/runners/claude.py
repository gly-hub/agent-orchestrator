"""Claude Agent SDK runner adapter.

This module intentionally imports `claude_agent_sdk` lazily so the core package
can be used without installing the SDK.
"""

from __future__ import annotations

import importlib
import json
from dataclasses import dataclass, field
from typing import Any

from agent_orchestrator.models import RunState, WorkflowEvent


@dataclass(slots=True)
class ClaudeAgentRunnerConfig:
    """Configuration passed to ClaudeAgentOptions."""

    options: dict[str, Any] = field(default_factory=dict)
    prompt_template: str | None = None


class ClaudeAgentRunner:
    """Adapter that turns Claude Agent SDK messages into WorkflowEvents."""

    def __init__(
        self,
        config: ClaudeAgentRunnerConfig | None = None,
        *,
        sdk_module: Any | None = None,
    ) -> None:
        self.config = config or ClaudeAgentRunnerConfig()
        self._sdk_module = sdk_module

    async def __call__(self, agent_input: dict[str, Any], run_state: RunState):
        sdk = self._sdk_module or importlib.import_module("claude_agent_sdk")
        options = sdk.ClaudeAgentOptions(**self.config.options)
        client = sdk.ClaudeSDKClient(options=options)
        prompt = self._build_prompt(agent_input)

        text_parts: list[str] = []
        final_text_parts: list[str] = []
        saw_stream_delta = False
        await client.connect()
        try:
            await client.query(prompt)
            async for msg in client.receive_response():
                if self._is_sdk_type(msg, sdk, "StreamEvent"):
                    text = self._extract_stream_text_delta(getattr(msg, "event", None))
                    if text:
                        saw_stream_delta = True
                        text_parts.append(text)
                        yield WorkflowEvent(
                            type="agent.delta",
                            run_id=run_state.run_id,
                            node_id=run_state.current_node_id,
                            data={"text": text},
                        )
                elif isinstance(msg, sdk.AssistantMessage):
                    message_text_parts: list[str] = []
                    for block in msg.content:
                        if isinstance(block, sdk.TextBlock):
                            message_text_parts.append(block.text)
                            if not saw_stream_delta:
                                text_parts.append(block.text)
                                yield WorkflowEvent(
                                    type="agent.delta",
                                    run_id=run_state.run_id,
                                    node_id=run_state.current_node_id,
                                    data={"text": block.text},
                                )
                        elif isinstance(block, sdk.ToolUseBlock):
                            yield WorkflowEvent(
                                type="agent.tool_use",
                                run_id=run_state.run_id,
                                node_id=run_state.current_node_id,
                                data={
                                    "tool_name": getattr(block, "name", ""),
                                    "tool_input": getattr(block, "input", None),
                                    "tool_id": getattr(block, "id", None),
                                },
                            )
                        elif isinstance(block, sdk.ToolResultBlock):
                            yield WorkflowEvent(
                                type="agent.tool_result",
                                run_id=run_state.run_id,
                                node_id=run_state.current_node_id,
                                data={
                                    "tool_use_id": getattr(block, "tool_use_id", None),
                                    "content": getattr(block, "content", None),
                                    "is_error": getattr(block, "is_error", None),
                                },
                            )
                    if message_text_parts:
                        final_text_parts.extend(message_text_parts)
                elif isinstance(msg, sdk.ResultMessage):
                    if getattr(msg, "result", None):
                        final_text_parts = [str(msg.result)]
                    break

            yield WorkflowEvent(
                type="agent.output",
                run_id=run_state.run_id,
                node_id=run_state.current_node_id,
                data={"text": "".join(final_text_parts or text_parts)},
            )
        finally:
            await client.disconnect()

    def _build_prompt(self, agent_input: dict[str, Any]) -> str:
        if self.config.prompt_template:
            return self.config.prompt_template.format(**agent_input)
        if "message" in agent_input:
            return str(agent_input["message"])
        return json.dumps(agent_input, ensure_ascii=False, indent=2, sort_keys=True)

    @staticmethod
    def _is_sdk_type(msg: Any, sdk: Any, type_name: str) -> bool:
        sdk_type = getattr(sdk, type_name, None)
        if sdk_type is not None:
            return isinstance(msg, sdk_type)
        return msg.__class__.__name__ == type_name

    @staticmethod
    def _extract_stream_text_delta(event: Any) -> str:
        if not isinstance(event, dict):
            return ""

        event_type = event.get("type")
        if event_type == "content_block_delta":
            delta = event.get("delta")
            if isinstance(delta, dict):
                delta_type = delta.get("type")
                if delta_type == "text_delta":
                    return str(delta.get("text") or "")
                if delta_type == "input_json_delta":
                    return ""

        if event_type == "content_block_start":
            block = event.get("content_block")
            if isinstance(block, dict) and block.get("type") == "text":
                return str(block.get("text") or "")

        return ""
