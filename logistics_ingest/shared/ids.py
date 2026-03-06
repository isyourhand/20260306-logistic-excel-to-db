from __future__ import annotations

import uuid


def new_batch_id() -> str:
    return str(uuid.uuid4())


def ensure_uuid(value: str) -> str:
    return str(uuid.UUID(value))

