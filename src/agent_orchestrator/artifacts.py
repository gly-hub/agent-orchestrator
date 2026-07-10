"""Artifact persistence interfaces and built-in stores."""

from __future__ import annotations

import json
import uuid
from copy import deepcopy
from pathlib import Path
from typing import Any, Protocol

from agent_orchestrator.exceptions import WorkflowError


class ArtifactStore(Protocol):
    """Persistence API for large node outputs or external payloads."""

    async def put(
        self,
        *,
        run_id: str,
        node_id: str,
        name: str,
        value: Any,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]: ...

    async def get(self, ref: dict[str, Any]) -> Any: ...


class InMemoryArtifactStore:
    """In-memory artifact store suitable for tests and demos."""

    def __init__(self) -> None:
        self._items: dict[str, Any] = {}

    async def put(
        self,
        *,
        run_id: str,
        node_id: str,
        name: str,
        value: Any,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        artifact_id = f"art_{uuid.uuid4().hex}"
        self._items[artifact_id] = deepcopy(value)
        return _artifact_ref(
            artifact_id=artifact_id,
            run_id=run_id,
            node_id=node_id,
            name=name,
            store="memory",
            metadata=metadata,
        )

    async def get(self, ref: dict[str, Any]) -> Any:
        artifact_id = ref.get("artifact_id")
        if not isinstance(artifact_id, str):
            raise WorkflowError("artifact ref missing artifact_id")
        try:
            return deepcopy(self._items[artifact_id])
        except KeyError as exc:
            raise WorkflowError(f"artifact not found: {artifact_id}") from exc


class FileArtifactStore:
    """JSON artifact store suitable for local services and integration tests."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    async def put(
        self,
        *,
        run_id: str,
        node_id: str,
        name: str,
        value: Any,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        artifact_id = f"art_{uuid.uuid4().hex}"
        path = self.root / f"{artifact_id}.json"
        path.write_text(json.dumps(value, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        return _artifact_ref(
            artifact_id=artifact_id,
            run_id=run_id,
            node_id=node_id,
            name=name,
            store="file",
            uri=str(path),
            metadata=metadata,
        )

    async def get(self, ref: dict[str, Any]) -> Any:
        uri = ref.get("uri")
        if not uri:
            raise WorkflowError("artifact ref missing uri")
        path = Path(uri).resolve()
        if not path.is_relative_to(self.root.resolve()):
            raise WorkflowError(f"artifact path outside root: {uri}")
        if not path.exists():
            raise WorkflowError(f"artifact not found: {uri}")
        return json.loads(path.read_text(encoding="utf-8"))


async def resolve_artifacts(value: Any, artifact_store: ArtifactStore | None) -> Any:
    """Recursively replace artifact refs with their stored values."""

    if artifact_store is None:
        return value
    if _is_artifact_ref_wrapper(value):
        return await artifact_store.get(value["artifact_ref"])
    if isinstance(value, dict):
        return {
            key: await resolve_artifacts(item, artifact_store)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [await resolve_artifacts(item, artifact_store) for item in value]
    return value


def estimate_json_size(value: Any) -> int:
    """Return the UTF-8 JSON size for a value."""

    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _is_artifact_ref_wrapper(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value.keys()) == {"artifact_ref"}
        and isinstance(value["artifact_ref"], dict)
        and "artifact_id" in value["artifact_ref"]
    )


def _artifact_ref(
    *,
    artifact_id: str,
    run_id: str,
    node_id: str,
    name: str,
    store: str,
    uri: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ref = {
        "artifact_id": artifact_id,
        "run_id": run_id,
        "node_id": node_id,
        "name": name,
        "store": store,
        "metadata": dict(metadata or {}),
    }
    if uri:
        ref["uri"] = uri
    return ref
