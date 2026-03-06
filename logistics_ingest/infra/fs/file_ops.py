from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path


def file_signature(path: Path) -> tuple[str, int, int]:
    stat = path.stat()
    signature = f"{stat.st_size}:{stat.st_mtime_ns}"
    return signature, int(stat.st_size), int(stat.st_mtime_ns)


def file_readable(path: Path) -> bool:
    try:
        with path.open("rb"):
            return True
    except Exception:
        return False


def compute_file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def file_hash_or_empty(path: Path) -> str:
    try:
        return compute_file_hash(path)
    except Exception:
        return ""


def archive_file(path: Path, archive_root: Path) -> Path:
    archive_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    candidate = archive_root / f"{stamp}_{path.name}"
    counter = 1
    while candidate.exists():
        candidate = archive_root / f"{stamp}_{counter}_{path.name}"
        counter += 1
    path.replace(candidate)
    return candidate


__all__ = [
    "archive_file",
    "compute_file_hash",
    "file_hash_or_empty",
    "file_readable",
    "file_signature",
]
