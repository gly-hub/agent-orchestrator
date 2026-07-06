"""Built-in workflow node executors."""
# pyright: reportAttributeAccessIssue=false

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from copy import deepcopy
from dataclasses import replace
from typing import Any

from agent_orchestrator.exceptions import PermissionDenied
from agent_orchestrator.models import PendingAction, RunState, ToolDefinition, WorkflowEvent
from agent_orchestrator.schema import validate_schema_value
from agent_orchestrator.state import evaluate_when, render_template


class BasicNodeExecutorMixin:
    """Executors for built-in non-composite node types."""

    async def _run_agent_node(
        self,
        node: dict[str, Any],
        run_state: RunState,
    ) -> AsyncIterator[WorkflowEvent]:
        agent_name = render_template(node["agent"], run_state.state)
        agent = self.agents.get(agent_name)
        agent_input = await self._render_node_value(node, run_state, node.get("input", {}))
        text_parts: list[str] = []
        output: dict[str, Any] | None = None

        async for event in agent.handler(agent_input, run_state):
            if event.type == "agent.delta":
                text = event.data.get("text")
                if text:
                    text_parts.append(str(text))
            elif event.type == "agent.output":
                output = dict(event.data)
            yield await self._normalize_child_event(event, run_state, node["id"])

        final_output = output if output is not None else {"text": "".join(text_parts)}
        validate_schema_value(final_output, node.get("output_schema"), label=f"agent {agent_name} output")
        await self._save_node_output(
            run_state,
            node["id"],
            final_output,
            node=node,
        )

    async def _run_tool_node(
        self,
        node: dict[str, Any],
        run_state: RunState,
    ) -> AsyncIterator[WorkflowEvent]:
        tool_name = render_template(node["tool"], run_state.state)
        tool = self.tools.get(tool_name)
        tool = self._effective_tool_for_node(tool, node)
        args = await self._render_node_value(node, run_state, node.get("args", {}))
        input_schema = node.get("input_schema") or tool.input_schema
        output_schema = node.get("output_schema") or tool.output_schema
        validate_schema_value(args, input_schema, label=f"tool {tool_name} input")
        node_record = run_state.state.setdefault("nodes", {}).setdefault(node["id"], {})

        approval = node_record.get("approval")
        policy_decision = self._evaluate_tool_policy(
            tool_name=tool_name,
            tool=tool,
            args=args,
            run_state=run_state,
            node=node,
        )
        yield await self._event(
            "policy.decision",
            run_state,
            node_id=node["id"],
            data={
                "tool_name": tool_name,
                "decision": policy_decision.decision,
                "reason": policy_decision.reason,
                "missing_permissions": policy_decision.missing_permissions,
                "permissions": list(tool.permissions),
                "risk_level": tool.risk_level,
                "confirmation_policy": tool.confirmation_policy,
            },
        )
        if policy_decision.decision == "deny":
            raise PermissionDenied(policy_decision.reason)

        if policy_decision.decision == "confirm" and not approval:
            action = PendingAction(
                id=f"pa_{uuid.uuid4().hex}",
                run_id=run_state.run_id,
                node_id=node["id"],
                action_type="tool_confirmation",
                request={
                    "title": node.get("title", "确认工具调用"),
                    "tool_name": tool_name,
                    "tool_input": args,
                    "risk_level": tool.risk_level,
                    "permissions": list(tool.permissions),
                    "reason": policy_decision.reason,
                    "options": [
                        {"id": "approve", "label": "确认"},
                        {"id": "reject", "label": "取消"},
                    ],
                    "on_timeout": deepcopy(node.get("on_timeout")),
                },
                created_at_ms=self._now_ms(),
                expires_at_ms=self._expires_at_ms(),
            )
            yield await self._event(
                "human.required",
                run_state,
                node_id=node["id"],
                data={
                    "pending_action_id": action.id,
                    "request": action.request,
                },
            )
            await self._pause_for_action(run_state, action)

        if approval and approval.get("decision") != "approve":
            await self._save_node_output(
                run_state,
                node["id"],
                {"cancelled": True, "decision": approval},
                node=node,
            )
            return

        yield await self._event(
            "tool.started",
            run_state,
            node_id=node["id"],
            data={"tool_name": tool_name, "args": args},
        )
        output = await tool.handler(args, run_state)
        validate_schema_value(output, output_schema, label=f"tool {tool_name} output")
        await self._save_node_output(run_state, node["id"], output, node=node)
        yield await self._event(
            "tool.finished",
            run_state,
            node_id=node["id"],
            data={"tool_name": tool_name, "output": output},
        )

    def _effective_tool_for_node(self, tool: ToolDefinition, node: dict[str, Any]) -> ToolDefinition:
        updates: dict[str, Any] = {}
        permission_override = node.get("permissions", node.get("required_permissions"))
        if permission_override is not None:
            updates["permissions"] = tuple(permission_override)
        node_confirmation_policy = node.get("confirmation_policy")
        if node_confirmation_policy in {"never", "always", "risk_based"}:
            updates["confirmation_policy"] = node_confirmation_policy
            updates["requires_confirmation"] = node_confirmation_policy == "always"
        node_risk_level = node.get("risk_level")
        if node_risk_level in {"low", "medium", "high"}:
            updates["risk_level"] = node_risk_level
        return replace(tool, **updates) if updates else tool

    def _evaluate_tool_policy(
        self,
        *,
        tool_name: str,
        tool: ToolDefinition,
        args: dict[str, Any],
        run_state: RunState,
        node: dict[str, Any],
    ):
        allowed_tools = self._workflow_allowed_tools()
        if allowed_tools is not None and tool_name not in allowed_tools:
            from agent_orchestrator.policy import ToolPolicyDecision

            return ToolPolicyDecision.deny(
                reason=f"tool is not allowed by workflow policy: {tool_name}",
            )
        return self.policy_gate.evaluate(
            tool=tool,
            args=args,
            run_state=run_state,
            node=node,
        )

    def _workflow_allowed_tools(self) -> set[str] | None:
        allowlist = self.workflow.policy.get("tool_allowlist")
        if allowlist is None:
            allowlist = self.workflow.policy.get("allowed_tools")
        if allowlist is None:
            return None
        return set(allowlist)

    async def _run_transform_node(self, node: dict[str, Any], run_state: RunState) -> None:
        transform_input = await self._render_node_value(node, run_state, node.get("input", {}))
        output_template = node.get("output")
        if output_template is None:
            output = transform_input
        else:
            local_state = deepcopy(run_state.state)
            local_state["input"] = transform_input
            output = render_template(output_template, local_state)
        await self._save_node_output(run_state, node["id"], output, node=node)

    async def _run_condition_node(self, node: dict[str, Any], run_state: RunState) -> None:
        condition_input = await self._render_node_value(node, run_state, node.get("input", {}))
        local_state = deepcopy(run_state.state)
        local_state["input"] = condition_input

        for index, case in enumerate(node.get("cases", [])):
            if evaluate_when(case.get("when"), local_state):
                await self._save_node_output(
                    run_state,
                    node["id"],
                    {
                        "matched": True,
                        "matched_index": index,
                        "value": render_template(case.get("value"), local_state),
                    },
                    node=node,
                )
                return

        await self._save_node_output(
            run_state,
            node["id"],
            {
                "matched": False,
                "matched_index": None,
                "value": render_template(node.get("default"), local_state),
            },
            node=node,
        )

    async def _run_human_node(
        self,
        node: dict[str, Any],
        run_state: RunState,
    ) -> AsyncIterator[WorkflowEvent]:
        action = PendingAction(
            id=f"pa_{uuid.uuid4().hex}",
            run_id=run_state.run_id,
            node_id=node["id"],
            action_type="human",
            request={
                "title": render_template(node.get("title", "需要确认"), run_state.state),
                "message": render_template(node.get("message", ""), run_state.state),
                "options": render_template(node.get("options", []), run_state.state),
                "fields": render_template(node.get("fields", []), run_state.state),
                "response_schema": deepcopy(node.get("response_schema")),
                "on_timeout": deepcopy(node.get("on_timeout")),
            },
            created_at_ms=self._now_ms(),
            expires_at_ms=self._expires_at_ms(),
        )
        yield await self._event(
            "human.required",
            run_state,
            node_id=node["id"],
            data={
                "pending_action_id": action.id,
                "request": action.request,
            },
        )
        await self._pause_for_action(run_state, action)
