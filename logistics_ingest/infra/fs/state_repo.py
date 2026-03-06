from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from logistics_ingest.infra.fs.file_ops import file_readable, file_signature


def iso_now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def ensure_state_shape(raw: dict[str, Any] | None) -> dict[str, Any]:
    state = raw or {}
    if not isinstance(state.get("files"), dict):
        state["files"] = {}
    if not isinstance(state.get("processed_hashes"), dict):
        state["processed_hashes"] = {}
    return state


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return ensure_state_shape({})
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        data = {}
    return ensure_state_shape(data if isinstance(data, dict) else {})


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def mark_file_seen(path: Path, state: dict[str, Any], now_ts: float) -> dict[str, Any]:
    signature, size, mtime_ns = file_signature(path)
    record = state["files"].setdefault(str(path), {})
    record["signature"] = signature
    record["size"] = size
    record["mtime_ns"] = mtime_ns
    record["stable_since"] = now_ts
    record["last_seen_at"] = iso_now()
    return record


def confirm_file_settled(path: Path, record: dict[str, Any], recheck_seconds: float) -> bool:
    expected_signature = str(record.get("signature") or "")
    delay = max(0.0, float(recheck_seconds))
    if delay > 0:
        time.sleep(delay)

    try:
        signature, size, mtime_ns = file_signature(path)
    except Exception as exc:
        record["last_error"] = f"restat_failed:{exc}"
        return False

    record["last_seen_at"] = iso_now()
    if signature != expected_signature:
        record["signature"] = signature
        record["size"] = size
        record["mtime_ns"] = mtime_ns
        record["stable_since"] = time.time()
        record["last_error"] = "file_changed_during_recheck"
        return False

    if not file_readable(path):
        record["last_error"] = "file_not_readable"
        return False

    record["last_error"] = ""
    return True


__all__ = [
    "confirm_file_settled",
    "ensure_state_shape",
    "iso_now",
    "load_state",
    "mark_file_seen",
    "save_state",
]
