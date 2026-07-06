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
        await client.connect()
        try:
            await client.query(prompt)
            async for msg in client.receive_response():
                if isinstance(msg, sdk.AssistantMessage):
                    for block in msg.content:
                        if isinstance(block, sdk.TextBlock):
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
                elif isinstance(msg, sdk.ResultMessage):
                    break

            yield WorkflowEvent(
                type="agent.output",
                run_id=run_state.run_id,
                node_id=run_state.current_node_id,
                data={"text": "".join(text_parts)},
            )
        finally:
            await client.disconnect()

    def _build_prompt(self, agent_input: dict[str, Any]) -> str:
        if self.config.prompt_template:
            return self.config.prompt_template.format(**agent_input)
        if "message" in agent_input:
            return str(agent_input["message"])
        return json.dumps(agent_input, ensure_ascii=False, indent=2, sort_keys=True)
