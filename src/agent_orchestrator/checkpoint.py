"""Checkpoint storage for resumable workflow runs."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
from typing import Protocol

from agent_orchestrator.exceptions import WorkflowError
from agent_orchestrator.models import PendingAction, RunState
from agent_orchestrator.schema import validate_schema_value


class CheckpointStore(Protocol):
    """Persistence API for resumable workflow runs."""

    async def save_waiting(self, run_state: RunState, action: PendingAction) -> None: ...

    async def load_run(self, run_id: str) -> RunState: ...

    async def load_action(self, pending_action_id: str) -> PendingAction: ...

    async def list_expired_actions(self, now_ms: int) -> list[PendingAction]: ...

    async def expire_action(self, pending_action_id: str) -> PendingAction: ...

    async def resolve_action(self, pending_action_id: str, decision: dict) -> RunState: ...

    def lease_run(
        self,
        run_id: str,
        *,
        owner_id: str | None = None,
        ttl_ms: int = 60_000,
    ) -> AbstractAsyncContextManager[None]: ...


class BaseCheckpointStore:
    """Shared resolution logic for checkpoint store implementations."""

    async def save_waiting(self, run_state: RunState, action: PendingAction) -> None:
        raise NotImplementedError

    async def load_run(self, run_id: str) -> RunState:
        raise NotImplementedError

    async def load_action(self, pending_action_id: str) -> PendingAction:
        raise NotImplementedError

    async def _save_action(self, action: PendingAction) -> None:
        raise NotImplementedError

    async def _mark_executed_once(self, pending_action_id: str) -> bool:
        raise NotImplementedError

    async def list_expired_actions(self, now_ms: int) -> list[PendingAction]:
        raise NotImplementedError

    @asynccontextmanager
    async def lease_run(
        self,
        run_id: str,
        *,
        owner_id: str | None = None,
        ttl_ms: int = 60_000,
    ) -> AsyncIterator[None]:
        """Hold a best-effort execution lease for a run while it is advancing."""

        token = owner_id or f"lease_{uuid.uuid4().hex}"
        acquired = await self._acquire_run_lease(run_id, token, ttl_ms)
        if not acquired:
            raise WorkflowError(f"run is already being executed: {run_id}")
        try:
            yield
        finally:
            await self._release_run_lease(run_id, token)

    async def _acquire_run_lease(self, run_id: str, owner_id: str, ttl_ms: int) -> bool:
        return True

    async def _release_run_lease(self, run_id: str, owner_id: str) -> None:
        return None

    async def expire_action(self, pending_action_id: str) -> PendingAction:
        action = await self.load_action(pending_action_id)
        if action.status == "pending":
            action.status = "expired"
            await self._save_action(action)
        return action

    async def resolve_action(self, pending_action_id: str, decision: dict) -> RunState:
        action = await self.load_action(pending_action_id)
        now_ms = int(time.time() * 1000)
        expired = action.expires_at_ms is not None and action.expires_at_ms <= now_ms
        if expired and action.request.get("on_timeout") is None:
            action.status = "expired"
            await self._save_action(action)
            raise WorkflowError(f"pending action expired: {pending_action_id}")
        if expired:
            decision = self._resolve_timeout_decision(action, decision)
        if action.status != "pending":
            raise WorkflowError(f"pending action already resolved: {pending_action_id}")
        _validate_decision(action, decision)
        if not await self._mark_executed_once(pending_action_id):
            raise WorkflowError(f"pending action already resumed: {pending_action_id}")

        action.status = "approved" if decision.get("decision") == "approve" else "rejected"
        action.decision = decision
        await self._save_action(action)

        run_state = await self.load_run(action.run_id)
        run_state.status = "running"
        run_state.waiting_action_id = None
        node_record = run_state.state.setdefault("nodes", {}).setdefault(action.node_id, {})
        if action.action_type == "human":
            node_record["status"] = "success"
            node_record["output"] = decision
        else:
            node_record["status"] = "pending"
            node_record["approval"] = decision
        return run_state

    def _resolve_timeout_decision(
        self,
        action: PendingAction,
        decision: dict,
    ) -> dict:
        on_timeout = action.request.get("on_timeout")
        if on_timeout is None:
            return decision
        if isinstance(on_timeout, str):
            return {"decision": on_timeout, "timed_out": True}
        if isinstance(on_timeout, dict):
            timeout_decision = dict(on_timeout)
            timeout_decision.setdefault("timed_out", True)
            return timeout_decision
        raise WorkflowError("pending action on_timeout must be a string or mapping")


class InMemoryCheckpointStore(BaseCheckpointStore):
    """In-memory store suitable for demos and tests.

    Production code should replace this with Redis/DB-backed storage.
    """

    def __init__(self) -> None:
        self._runs: dict[str, RunState] = {}
        self._actions: dict[str, PendingAction] = {}
        self._executed_actions: set[str] = set()
        self._leases: dict[str, tuple[str, int]] = {}

    async def save_waiting(self, run_state: RunState, action: PendingAction) -> None:
        self._runs[run_state.run_id] = deepcopy(run_state)
        self._actions[action.id] = deepcopy(action)

    async def load_run(self, run_id: str) -> RunState:
        try:
            return deepcopy(self._runs[run_id])
        except KeyError as exc:
            raise WorkflowError(f"run checkpoint not found: {run_id}") from exc

    async def load_action(self, pending_action_id: str) -> PendingAction:
        try:
            return deepcopy(self._actions[pending_action_id])
        except KeyError as exc:
            raise WorkflowError(f"pending action not found: {pending_action_id}") from exc

    async def _save_action(self, action: PendingAction) -> None:
        self._actions[action.id] = deepcopy(action)

    async def _mark_executed_once(self, pending_action_id: str) -> bool:
        if pending_action_id in self._executed_actions:
            return False
        self._executed_actions.add(pending_action_id)
        return True

    async def list_expired_actions(self, now_ms: int) -> list[PendingAction]:
        return [
            deepcopy(action)
            for action in self._actions.values()
            if _is_expired_pending(action, now_ms)
        ]

    async def _acquire_run_lease(self, run_id: str, owner_id: str, ttl_ms: int) -> bool:
        now_ms = int(time.time() * 1000)
        current = self._leases.get(run_id)
        if current is not None:
            _, expires_at_ms = current
            if expires_at_ms > now_ms:
                return False
        self._leases[run_id] = (owner_id, now_ms + ttl_ms)
        return True

    async def _release_run_lease(self, run_id: str, owner_id: str) -> None:
        current = self._leases.get(run_id)
        if current is not None and current[0] == owner_id:
            self._leases.pop(run_id, None)


class FileCheckpointStore(BaseCheckpointStore):
    """JSON-file checkpoint store suitable for local services and integration tests."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.runs_dir = self.root / "runs"
        self.actions_dir = self.root / "actions"
        self.executed_dir = self.root / "executed"
        self.leases_dir = self.root / "leases"
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.actions_dir.mkdir(parents=True, exist_ok=True)
        self.executed_dir.mkdir(parents=True, exist_ok=True)
        self.leases_dir.mkdir(parents=True, exist_ok=True)

    async def save_waiting(self, run_state: RunState, action: PendingAction) -> None:
        await asyncio.to_thread(self._save_waiting_sync, deepcopy(run_state), deepcopy(action))

    def _save_waiting_sync(self, run_state: RunState, action: PendingAction) -> None:
        self._write_json(self._run_path(run_state.run_id), _run_state_to_dict(run_state))
        self._write_json(self._action_path(action.id), asdict(action))

    async def load_run(self, run_id: str) -> RunState:
        return await asyncio.to_thread(self._load_run_sync, run_id)

    def _load_run_sync(self, run_id: str) -> RunState:
        path = self._run_path(run_id)
        if not path.exists():
            raise WorkflowError(f"run checkpoint not found: {run_id}")
        return _run_state_from_dict(self._read_json(path))

    async def load_action(self, pending_action_id: str) -> PendingAction:
        return await asyncio.to_thread(self._load_action_sync, pending_action_id)

    def _load_action_sync(self, pending_action_id: str) -> PendingAction:
        path = self._action_path(pending_action_id)
        if not path.exists():
            raise WorkflowError(f"pending action not found: {pending_action_id}")
        return PendingAction(**self._read_json(path))

    async def _save_action(self, action: PendingAction) -> None:
        await asyncio.to_thread(self._save_action_sync, deepcopy(action))

    def _save_action_sync(self, action: PendingAction) -> None:
        self._write_json(self._action_path(action.id), asdict(action))

    async def _mark_executed_once(self, pending_action_id: str) -> bool:
        return await asyncio.to_thread(self._mark_executed_once_sync, pending_action_id)

    def _mark_executed_once_sync(self, pending_action_id: str) -> bool:
        marker = self.executed_dir / f"{pending_action_id}.lock"
        try:
            fd = marker.open("x", encoding="utf-8")
        except FileExistsError:
            return False
        with fd:
            fd.write(str(int(time.time() * 1000)))
        return True

    async def _acquire_run_lease(self, run_id: str, owner_id: str, ttl_ms: int) -> bool:
        return await asyncio.to_thread(self._acquire_run_lease_sync, run_id, owner_id, ttl_ms)

    def _acquire_run_lease_sync(self, run_id: str, owner_id: str, ttl_ms: int) -> bool:
        path = self._lease_path(run_id)
        now_ms = int(time.time() * 1000)
        expires_at_ms = now_ms + ttl_ms
        payload = {"owner_id": owner_id, "expires_at_ms": expires_at_ms}
        try:
            fd = path.open("x", encoding="utf-8")
        except FileExistsError:
            try:
                existing = self._read_json(path)
            except json.JSONDecodeError:
                existing = {}
            if int(existing.get("expires_at_ms", 0)) > now_ms:
                return False
            self._write_json(path, payload)
            return True
        with fd:
            fd.write(json.dumps(payload, separators=(",", ":")))
        return True

    async def _release_run_lease(self, run_id: str, owner_id: str) -> None:
        await asyncio.to_thread(self._release_run_lease_sync, run_id, owner_id)

    def _release_run_lease_sync(self, run_id: str, owner_id: str) -> None:
        path = self._lease_path(run_id)
        if not path.exists():
            return
        try:
            payload = self._read_json(path)
        except json.JSONDecodeError:
            return
        if payload.get("owner_id") == owner_id:
            path.unlink(missing_ok=True)

    async def list_expired_actions(self, now_ms: int) -> list[PendingAction]:
        return await asyncio.to_thread(self._list_expired_actions_sync, now_ms)

    def _list_expired_actions_sync(self, now_ms: int) -> list[PendingAction]:
        actions = []
        for path in self.actions_dir.glob("*.json"):
            action = PendingAction(**self._read_json(path))
            if _is_expired_pending(action, now_ms):
                actions.append(action)
        return actions

    def _run_path(self, run_id: str) -> Path:
        return self.runs_dir / f"{run_id}.json"

    def _action_path(self, pending_action_id: str) -> Path:
        return self.actions_dir / f"{pending_action_id}.json"

    def _lease_path(self, run_id: str) -> Path:
        return self.leases_dir / f"{run_id}.json"

    def _read_json(self, path: Path) -> dict:
        return json.loads(path.read_text(encoding="utf-8"))

    def _write_json(self, path: Path, data: dict) -> None:
        temp = path.with_suffix(".tmp")
        temp.write_text(
            json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        temp.replace(path)


def _run_state_to_dict(run_state: RunState) -> dict:
    return asdict(run_state)


def _run_state_from_dict(data: dict) -> RunState:
    return RunState(**data)


def _validate_decision(action: PendingAction, decision: dict) -> None:
    if not isinstance(decision, dict):
        raise WorkflowError("decision must be a mapping")
    validate_schema_value(decision, action.request.get("response_schema"), label="decision")


def _is_expired_pending(action: PendingAction, now_ms: int) -> bool:
    return (
        action.status == "pending"
        and action.expires_at_ms is not None
        and action.expires_at_ms <= now_ms
    )
