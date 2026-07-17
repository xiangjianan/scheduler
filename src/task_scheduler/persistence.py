import asyncio
import copy
import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol

from .exceptions import PersistenceError

SCHEMA_VERSION = 1


class StateStore(Protocol):
    async def load(self) -> dict[str, Any] | None: ...

    async def save(self, snapshot: Mapping[str, Any]) -> None: ...


class JsonStateStore:
    """Single-writer, atomic JSON snapshot store."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    async def load(self) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        try:
            snapshot = await asyncio.to_thread(self._load_sync)
        except (OSError, json.JSONDecodeError) as exc:
            raise PersistenceError(f"Could not load state file '{self.path}': {exc}") from exc
        if not isinstance(snapshot, dict):
            raise PersistenceError("Scheduler state must be a JSON object")
        version = snapshot.get("schema_version")
        if version != SCHEMA_VERSION:
            raise PersistenceError(
                f"Unsupported scheduler state schema version {version!r}; expected {SCHEMA_VERSION}"
            )
        if not isinstance(snapshot.get("tasks"), dict):
            raise PersistenceError("Scheduler state field 'tasks' must be an object")
        return snapshot

    async def save(self, snapshot: Mapping[str, Any]) -> None:
        try:
            await asyncio.to_thread(self._save_sync, copy.deepcopy(dict(snapshot)))
        except (OSError, TypeError, ValueError) as exc:
            raise PersistenceError(f"Could not save state file '{self.path}': {exc}") from exc

    def _load_sync(self) -> dict[str, Any]:
        with self.path.open(encoding="utf-8") as file:
            return json.load(file)

    def _save_sync(self, snapshot: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        try:
            with temporary.open("w", encoding="utf-8") as file:
                json.dump(snapshot, file, ensure_ascii=False, indent=2)
                file.write("\n")
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary, self.path)
        finally:
            if temporary.exists():
                temporary.unlink()
