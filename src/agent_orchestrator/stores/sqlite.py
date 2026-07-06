"""SQLite-backed persistence stores using only the Python standard library."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from dataclasses import asdict
from pathlib import Path
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


class SQLiteCheckpointStore(BaseCheckpointStore):
    """SQLite checkpoint store for local services and small deployments."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    async def save_waiting(self, run_state: RunState, action: PendingAction) -> None:
        await asyncio.to_thread(self._save_waiting_sync, run_state, action)

    def _save_waiting_sync(self, run_state: RunState, action: PendingAction) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO runs(run_id, payload, updated_at_ms)
                VALUES(?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    payload = excluded.payload,
                    updated_at_ms = excluded.updated_at_ms
                """,
                (
                    run_state.run_id,
                    _json_dumps(asdict(run_state)),
                    _now_ms(),
                ),
            )
            conn.execute(
                """
                INSERT INTO actions(action_id, run_id, status, expires_at_ms, payload, updated_at_ms)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(action_id) DO UPDATE SET
                    run_id = excluded.run_id,
                    status = excluded.status,
                    expires_at_ms = excluded.expires_at_ms,
                    payload = excluded.payload,
                    updated_at_ms = excluded.updated_at_ms
                """,
                _action_row(action),
            )

    async def load_run(self, run_id: str) -> RunState:
        return await asyncio.to_thread(self._load_run_sync, run_id)

    def _load_run_sync(self, run_id: str) -> RunState:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        if row is None:
            raise WorkflowError(f"run checkpoint not found: {run_id}")
        return RunState(**json.loads(row["payload"]))

    async def load_action(self, pending_action_id: str) -> PendingAction:
        return await asyncio.to_thread(self._load_action_sync, pending_action_id)

    def _load_action_sync(self, pending_action_id: str) -> PendingAction:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload FROM actions WHERE action_id = ?",
                (pending_action_id,),
            ).fetchone()
        if row is None:
            raise WorkflowError(f"pending action not found: {pending_action_id}")
        return PendingAction(**json.loads(row["payload"]))

    async def _save_action(self, action: PendingAction) -> None:
        await asyncio.to_thread(self._save_action_sync, action)

    def _save_action_sync(self, action: PendingAction) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO actions(action_id, run_id, status, expires_at_ms, payload, updated_at_ms)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(action_id) DO UPDATE SET
                    run_id = excluded.run_id,
                    status = excluded.status,
                    expires_at_ms = excluded.expires_at_ms,
                    payload = excluded.payload,
                    updated_at_ms = excluded.updated_at_ms
                """,
                _action_row(action),
            )

    async def _mark_executed_once(self, pending_action_id: str) -> bool:
        return await asyncio.to_thread(self._mark_executed_once_sync, pending_action_id)

    def _mark_executed_once_sync(self, pending_action_id: str) -> bool:
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO executed_actions(pending_action_id, executed_at_ms)
                    VALUES(?, ?)
                    """,
                    (pending_action_id, _now_ms()),
                )
        except sqlite3.IntegrityError:
            return False
        return True

    async def _acquire_run_lease(self, run_id: str, owner_id: str, ttl_ms: int) -> bool:
        return await asyncio.to_thread(self._acquire_run_lease_sync, run_id, owner_id, ttl_ms)

    def _acquire_run_lease_sync(self, run_id: str, owner_id: str, ttl_ms: int) -> bool:
        now_ms = _now_ms()
        expires_at_ms = now_ms + ttl_ms
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT owner_id, expires_at_ms FROM run_leases WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                if row is not None and int(row["expires_at_ms"]) > now_ms:
                    conn.rollback()
                    return False
                conn.execute(
                    """
                    INSERT INTO run_leases(run_id, owner_id, expires_at_ms)
                    VALUES(?, ?, ?)
                    ON CONFLICT(run_id) DO UPDATE SET
                        owner_id = excluded.owner_id,
                        expires_at_ms = excluded.expires_at_ms
                    """,
                    (run_id, owner_id, expires_at_ms),
                )
                conn.commit()
                return True
            except Exception:
                conn.rollback()
                raise

    async def _release_run_lease(self, run_id: str, owner_id: str) -> None:
        await asyncio.to_thread(self._release_run_lease_sync, run_id, owner_id)

    def _release_run_lease_sync(self, run_id: str, owner_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM run_leases WHERE run_id = ? AND owner_id = ?",
                (run_id, owner_id),
            )

    async def resolve_action(self, pending_action_id: str, decision: dict) -> RunState:
        if not isinstance(decision, dict):
            raise WorkflowError("decision must be a mapping")
        return await asyncio.to_thread(self._resolve_action_sync, pending_action_id, decision)

    def _resolve_action_sync(self, pending_action_id: str, decision: dict) -> RunState:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                action_row = conn.execute(
                    "SELECT payload FROM actions WHERE action_id = ?",
                    (pending_action_id,),
                ).fetchone()
                if action_row is None:
                    raise WorkflowError(f"pending action not found: {pending_action_id}")

                action = PendingAction(**json.loads(action_row["payload"]))
                try:
                    resolved_decision = self._validated_decision_for_action(action, decision)
                except WorkflowError:
                    if action.status == "expired":
                        conn.execute(
                            """
                            UPDATE actions
                            SET status = ?, payload = ?, updated_at_ms = ?
                            WHERE action_id = ?
                            """,
                            (action.status, _json_dumps(asdict(action)), _now_ms(), action.id),
                        )
                        conn.commit()
                    raise

                if action.status != "pending":
                    raise WorkflowError(f"pending action already resolved: {pending_action_id}")
                try:
                    conn.execute(
                        """
                        INSERT INTO executed_actions(pending_action_id, executed_at_ms)
                        VALUES(?, ?)
                        """,
                        (pending_action_id, _now_ms()),
                    )
                except sqlite3.IntegrityError as exc:
                    raise WorkflowError(f"pending action already resumed: {pending_action_id}") from exc

                action.status = "approved" if resolved_decision.get("decision") == "approve" else "rejected"
                action.decision = resolved_decision
                conn.execute(
                    """
                    UPDATE actions
                    SET status = ?, payload = ?, updated_at_ms = ?
                    WHERE action_id = ?
                    """,
                    (action.status, _json_dumps(asdict(action)), _now_ms(), action.id),
                )

                run_row = conn.execute(
                    "SELECT payload FROM runs WHERE run_id = ?",
                    (action.run_id,),
                ).fetchone()
                if run_row is None:
                    raise WorkflowError(f"run checkpoint not found: {action.run_id}")

                run_state = RunState(**json.loads(run_row["payload"]))
                run_state.status = "running"
                run_state.waiting_action_id = None
                node_record = run_state.state.setdefault("nodes", {}).setdefault(action.node_id, {})
                if action.action_type == "human":
                    node_record["status"] = "success"
                    node_record["output"] = resolved_decision
                else:
                    node_record["status"] = "pending"
                    node_record["approval"] = resolved_decision
                conn.execute(
                    """
                    UPDATE runs
                    SET payload = ?, updated_at_ms = ?
                    WHERE run_id = ?
                    """,
                    (_json_dumps(asdict(run_state)), _now_ms(), run_state.run_id),
                )
                conn.commit()
                return run_state
            except Exception:
                conn.rollback()
                raise

    async def list_expired_actions(self, now_ms: int) -> list[PendingAction]:
        return await asyncio.to_thread(self._list_expired_actions_sync, now_ms)

    def _list_expired_actions_sync(self, now_ms: int) -> list[PendingAction]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT payload FROM actions
                WHERE status = 'pending'
                  AND expires_at_ms IS NOT NULL
                  AND expires_at_ms <= ?
                ORDER BY expires_at_ms ASC, action_id ASC
                """,
                (now_ms,),
            ).fetchall()
        return [PendingAction(**json.loads(row["payload"])) for row in rows]

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS actions (
                    action_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    expires_at_ms INTEGER,
                    payload TEXT NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_actions_expiry
                    ON actions(status, expires_at_ms);
                CREATE TABLE IF NOT EXISTS executed_actions (
                    pending_action_id TEXT PRIMARY KEY,
                    executed_at_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS run_leases (
                    run_id TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    expires_at_ms INTEGER NOT NULL
                );
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.isolation_level = None
        return conn

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


class SQLiteEventStore:
    """SQLite event log store for audit and replay."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    async def append(self, event: WorkflowEvent) -> None:
        await asyncio.to_thread(self._append_sync, event)

    def _append_sync(self, event: WorkflowEvent) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO events(run_id, event_type, node_id, payload, created_at_ms)
                VALUES(?, ?, ?, ?, ?)
                """,
                (
                    event.run_id,
                    event.type,
                    event.node_id,
                    _json_dumps(workflow_event_to_dict(event)),
                    _now_ms(),
                ),
            )

    async def list_by_run(self, run_id: str) -> list[WorkflowEvent]:
        return await asyncio.to_thread(self._list_by_run_sync, run_id)

    def _list_by_run_sync(self, run_id: str) -> list[WorkflowEvent]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT payload FROM events
                WHERE run_id = ?
                ORDER BY id ASC
                """,
                (run_id,),
            ).fetchall()
        return [workflow_event_from_dict(json.loads(row["payload"])) for row in rows]

    async def replace_run(self, run_id: str, events: list[WorkflowEvent]) -> None:
        await asyncio.to_thread(self._replace_run_sync, run_id, events)

    def _replace_run_sync(self, run_id: str, events: list[WorkflowEvent]) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM events WHERE run_id = ?", (run_id,))
            conn.executemany(
                """
                INSERT INTO events(run_id, event_type, node_id, payload, created_at_ms)
                VALUES(?, ?, ?, ?, ?)
                """,
                [
                    (
                        event.run_id,
                        event.type,
                        event.node_id,
                        _json_dumps(workflow_event_to_dict(event)),
                        _now_ms(),
                    )
                    for event in events
                ],
            )

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    node_id TEXT,
                    payload TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_events_run_id
                    ON events(run_id, id);
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn


def _action_row(action: PendingAction) -> tuple[Any, ...]:
    return (
        action.id,
        action.run_id,
        action.status,
        action.expires_at_ms,
        _json_dumps(asdict(action)),
        _now_ms(),
    )


def _json_dumps(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _now_ms() -> int:
    return int(time.time() * 1000)
