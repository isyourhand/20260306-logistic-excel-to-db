from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

EXCEL_PATTERNS = ("*.xlsx", "*.xlsm", "*.xltx", "*.xltm")


def _clean_env_value(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
        return text[1:-1]
    return text


def _load_env_file(path: Path) -> None:
    if not path.exists() or not path.is_file():
        return

    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or any(ch.isspace() for ch in key):
            continue
        value = value.strip()
        # Keep literal # in quoted values; trim inline comments for unquoted values.
        if value and value[0] not in {'"', "'"}:
            value = value.split(" #", 1)[0].rstrip()
        os.environ.setdefault(key, _clean_env_value(value))


@lru_cache(maxsize=1)
def ensure_env_loaded() -> None:
    root = project_root()
    # Priority: .env > .env.local > .env.example (fallback)
    for filename in (".env", ".env.local", ".env.example"):
        _load_env_file(root / filename)


@dataclass(frozen=True)
class Settings:
    pg_dsn: str
    deepseek_api_key: str
    source_root: Path


def load_settings() -> Settings:
    ensure_env_loaded()
    return Settings(
        pg_dsn=os.getenv("PG_DSN", ""),
        deepseek_api_key=os.getenv("DEEPSEEK_API_KEY", ""),
        source_root=Path(
            os.getenv("LOGISTICS_SOURCE_ROOT", r"D:\wx_lj\WXWork\1688854674811621\Cache\File")
        ),
    )


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def data_root() -> Path:
    ensure_env_loaded()
    custom = os.getenv("LOGISTICS_DATA_DIR", "").strip()
    if custom:
        return Path(custom).resolve()
    return project_root() / "data"


def default_update_dir() -> Path:
    return data_root() / "update_excel"


def default_archive_root() -> Path:
    return data_root() / "archive_excel"


def default_out_dir() -> Path:
    return data_root() / "out"
