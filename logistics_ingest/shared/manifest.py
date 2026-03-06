from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

TRUE_VALUES = {"1", "true", "yes", "y", "on"}


def parse_bool(value: str, default: bool) -> bool:
    text = (value or "").strip().lower()
    if not text:
        return default
    return text in TRUE_VALUES


def read_manifest(manifest_path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with manifest_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return rows
        for line_no, row in enumerate(reader, start=2):
            filename = str(row.get("filename") or "").strip()
            if not filename:
                continue
            enabled = parse_bool(str(row.get("enabled") or ""), True)
            expect_channels = parse_bool(str(row.get("expect_channels") or ""), True)
            rows.append(
                {
                    "line_no": line_no,
                    "filename": filename,
                    "enabled": enabled,
                    "expect_channels": expect_channels,
                }
            )
    return rows

