from __future__ import annotations

import re

INVALID_FS_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')

def normalize_text(text: str | None) -> str:
    if not text:
        return ""
    return str(text).replace("\n", " ").replace("\r", " ").strip()


def safe_name(name: str) -> str:
    cleaned = INVALID_FS_CHARS.sub("_", str(name or "")).strip()
    return cleaned or "unnamed"
