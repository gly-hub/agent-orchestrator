"""Redis-backed persistence stores for production services."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict
from typing import Any

from agent_orchestrator.checkpoint import BaseCheckpointStore
from agent_orchestrator.exceptions import WorkflowError
from agent_orchestrator.models import (
    PendingAction,
    RunState,
    WorkflowEvent,
    workflow_event_from_dict,
    workflow_event_to_dict,
)
from agent_orchestrator.schema import validate_schema_value

logger = logging.getLogger(__name__)

try:
    from redis.asyncio import Redis
except ImportError:  # pragma: no cover - exercised when optional dependency is missing.
    Redis = None  # type: ignore[assignment]


class RedisCheckpointStore(BaseCheckpointStore):
    """Redis checkpoint store with atomic resume markers and expiry indexes."""

    def __init__(
        self,
        *,
        url: str | None = None,
        client: Any | None = None,
        prefix: str = "agent-orchestrator",
        action_ttl_seconds: int | None = None,
        run_ttl_seconds: int | None = None,
    ) -> None:
        if client is None:
            if Redis is None:
                raise WorkflowError("redis store requires the optional redis package")
            client = Redis.from_url(url or "redis://localhost:6379/0", decode_responses=True)
        self.client = client
        self.prefix = prefix.rstrip(":")
        self.action_ttl_seconds = action_ttl_seconds
        self.run_ttl_seconds = run_ttl_seconds

    async def save_waiting(self, run_state: RunState, action: PendingAction) -> None:
        run_payload = _json_dumps(asdict(run_state))
        action_payload = _json_dumps(asdict(action))
        pipe = self.client.pipeline(transaction=True)
        pipe.set(self._run_key(run_state.run_id), run_payload)
        pipe.set(self._action_key(action.id), action_payload)
        pipe.zadd(self._expiry_key(), {action.id: action.expires_at_ms or _NO_EXPIRY_SCORE})
        if self.action_ttl_seconds is not None:
            pipe.expire(self._action_key(action.id), self.action_ttl_seconds)
        if self.run_ttl_seconds is not None:
            pipe.expire(self._run_key(run_state.run_id), self.run_ttl_seconds)
        await pipe.execute()

    async def load_run(self, run_id: str) -> RunState:
        payload = await self.client.get(self._run_key(run_id))
        if payload is None:
            raise WorkflowError(f"run checkpoint not found: {run_id}")
        return RunState(**json.loads(payload))

    async def load_action(self, pending_action_id: str) -> PendingAction:
        payload = await self.client.get(self._action_key(pending_action_id))
        if payload is None:
            raise WorkflowError(f"pending action not found: {pending_action_id}")
        return PendingAction(**json.loads(payload))

    async def _save_action(self, action: PendingAction) -> None:
        pipe = self.client.pipeline(transaction=True)
        pipe.set(self._action_key(action.id), _json_dumps(asdict(action)))
        if action.status == "pending":
            pipe.zadd(self._expiry_key(), {action.id: action.expires_at_ms or _NO_EXPIRY_SCORE})
        else:
            pipe.zrem(self._expiry_key(), action.id)
        await pipe.execute()

    async def _mark_executed_once(self, pending_action_id: str) -> bool:
        key = self._executed_key(pending_action_id)
        result = bool(await self.client.set(key, _now_ms(), nx=True))
        if result and self.action_ttl_seconds is not None:
            await self.client.expire(key, self.action_ttl_seconds)
        return result

    async def _acquire_run_lease(self, run_id: str, owner_id: str, ttl_ms: int) -> bool:
        return bool(
            await self.client.set(
                self._lease_key(run_id),
                owner_id,
                nx=True,
                px=ttl_ms,
            )
        )

    async def _release_run_lease(self, run_id: str, owner_id: str) -> None:
        await self.client.eval(
            _RELEASE_LEASE_SCRIPT,
            1,
            self._lease_key(run_id),
            owner_id,
        )

    async def resolve_action(self, pending_action_id: str, decision: dict) -> RunState:
        if not isinstance(decision, dict):
            raise WorkflowError("decision must be a mapping")
        action = await self.load_action(pending_action_id)
        try:
            resolved_decision = self._validated_decision_for_action(action, decision)
        except WorkflowError:
            if action.status == "expired":
                await self._save_action(action)
            raise
        if not hasattr(self.client, "eval"):
            logger.warning(
                "Redis client does not support EVAL; "
                "resolve_action falls back to a non-atomic sequence "
                "that is unsafe under concurrent resumes",
            )
            if action.status != "pending":
                raise WorkflowError(f"pending action already resolved: {pending_action_id}")
            if not await self._mark_executed_once(pending_action_id):
                raise WorkflowError(f"pending action already resumed: {pending_action_id}")

        action.status = "approved" if resolved_decision.get("decision") == "approve" else "rejected"
        action.decision = resolved_decision
        run_state = await self.load_run(action.run_id)
        self._apply_resolution(run_state, action, pending_action_id, resolved_decision)

        if not hasattr(self.client, "eval"):
            pipe = self.client.pipeline(transaction=True)
            pipe.set(self._action_key(action.id), _json_dumps(asdict(action)))
            pipe.zrem(self._expiry_key(), action.id)
            pipe.set(self._run_key(run_state.run_id), _json_dumps(asdict(run_state)))
            await pipe.execute()
            return run_state

        now_ms = _now_ms()
        allow_expired = "1" if action.expires_at_ms is not None and action.expires_at_ms <= now_ms else "0"
        result = await self.client.eval(
            _RESOLVE_ACTION_SCRIPT,
            4,
            self._action_key(action.id),
            self._executed_key(action.id),
            self._run_key(run_state.run_id),
            self._expiry_key(),
            action.id,
            _json_dumps(asdict(action)),
            _json_dumps(asdict(run_state)),
            str(now_ms),
            allow_expired,
        )
        if result == "missing":
            raise WorkflowError(f"pending action not found: {pending_action_id}")
        if result == "resolved":
            raise WorkflowError(f"pending action already resolved: {pending_action_id}")
        if result == "resumed":
            raise WorkflowError(f"pending action already resumed: {pending_action_id}")
        if result == "expired":
            raise WorkflowError(f"pending action expired: {pending_action_id}")
        if result != "ok":
            raise WorkflowError(f"pending action resume failed: {pending_action_id}")
        return run_state

    async def list_expired_actions(self, now_ms: int) -> list[PendingAction]:
        ids = await self.client.zrangebyscore(self._expiry_key(), min=0, max=now_ms)
        actions = []
        for action_id in ids:
            try:
                action = await self.load_action(action_id)
            except WorkflowError:
                await self.client.zrem(self._expiry_key(), action_id)
                continue
            if action.status == "pending" and action.expires_at_ms is not None and action.expires_at_ms <= now_ms:
                actions.append(action)
        return actions

    def _validated_decision_for_action(self, action: PendingAction, decision: dict) -> dict:
        now_ms = _now_ms()
        expired = action.expires_at_ms is not None and action.expires_at_ms <= now_ms
        if expired and action.request.get("on_timeout") is None:
            action.status = "expired"
            raise WorkflowError(f"pending action expired: {action.id}")
        if expired:
            decision = self._resolve_timeout_decision(action, decision)
        validate_schema_value(decision, action.request.get("response_schema"), label="decision")
        return decision

    def _run_key(self, run_id: str) -> str:
        return f"{self.prefix}:run:{run_id}"

    def _action_key(self, action_id: str) -> str:
        return f"{self.prefix}:action:{action_id}"

    def _executed_key(self, action_id: str) -> str:
        return f"{self.prefix}:executed:{action_id}"

    def _lease_key(self, run_id: str) -> str:
        return f"{self.prefix}:run-lease:{run_id}"

    def _expiry_key(self) -> str:
        return f"{self.prefix}:actions:expiry"

    async def aclose(self) -> None:
        await self.client.aclose()


class RedisEventStore:
    """Redis event log store using per-run lists."""

    def __init__(
        self,
        *,
        url: str | None = None,
        client: Any | None = None,
        prefix: str = "agent-orchestrator",
        max_events_per_run: int | None = None,
        migration_registry: Any = None,
    ) -> None:
        if client is None:
            if Redis is None:
                raise WorkflowError("redis store requires the optional redis package")
            client = Redis.from_url(url or "redis://localhost:6379/0", decode_responses=True)
        self.client = client
        self.prefix = prefix.rstrip(":")
        self.max_events_per_run = max_events_per_run
        self.migration_registry = migration_registry

    async def append(self, event: WorkflowEvent) -> None:
        key = self._events_key(event.run_id)
        pipe = self.client.pipeline(transaction=True)
        pipe.rpush(key, _json_dumps(workflow_event_to_dict(event)))
        if self.max_events_per_run is not None:
            pipe.ltrim(key, -self.max_events_per_run, -1)
        await pipe.execute()

    async def list_by_run(self, run_id: str) -> list[WorkflowEvent]:
        rows = await self.client.lrange(self._events_key(run_id), 0, -1)
        return [workflow_event_from_dict(json.loads(row), migration_registry=self.migration_registry) for row in rows]

    async def replace_run(self, run_id: str, events: list[WorkflowEvent]) -> None:
        key = self._events_key(run_id)
        pipe = self.client.pipeline(transaction=True)
        pipe.delete(key)
        if events:
            pipe.rpush(key, *[_json_dumps(workflow_event_to_dict(event)) for event in events])
        await pipe.execute()

    def _events_key(self, run_id: str) -> str:
        return f"{self.prefix}:events:{run_id}"

    async def aclose(self) -> None:
        await self.client.aclose()


def _json_dumps(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _now_ms() -> int:
    return int(time.time() * 1000)


_NO_EXPIRY_SCORE = 9_999_999_999_999

_RELEASE_LEASE_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    redis.call("DEL", KEYS[1])
    return "released"
end
return "ignored"
"""

_RESOLVE_ACTION_SCRIPT = """
local current_payload = redis.call("GET", KEYS[1])
if not current_payload then
    return "missing"
end

local current_action = cjson.decode(current_payload)
if current_action["status"] ~= "pending" then
    return "resolved"
end

local expires_at_ms = current_action["expires_at_ms"]
local now_ms = tonumber(ARGV[4])
local allow_expired = ARGV[5] == "1"
if expires_at_ms ~= cjson.null and expires_at_ms ~= nil and tonumber(expires_at_ms) <= now_ms and not allow_expired then
    return "expired"
end

if redis.call("EXISTS", KEYS[2]) == 1 then
    return "resumed"
end

redis.call("SET", KEYS[2], now_ms)
redis.call("SET", KEYS[1], ARGV[2])
redis.call("ZREM", KEYS[4], ARGV[1])
redis.call("SET", KEYS[3], ARGV[3])
return "ok"
"""
