from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from logistics_ingest.domain.models import InputWorkbook
from logistics_ingest.shared.text_utils import safe_name


def csv_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)
    if not rows:
        return 0
    return max(0, len(rows) - 1)


def build_quality_report(
    selected: list[InputWorkbook],
    metrics: dict[str, Any],
    parser_flags: int,
    min_channels: int,
    min_tiers: int,
    require_channels_for_expected: bool,
    max_parser_flags: int | None,
) -> dict[str, Any]:
    expected_workbooks = [safe_name(x.path.stem) for x in selected if x.expect_channels]
    channels_by_workbook: dict[str, dict[str, int]] = metrics.get("channels_by_workbook", {})

    failures: list[str] = []
    warnings: list[str] = []

    channels_total = int(metrics.get("channels_total_for_batch_workbooks", 0))
    tiers_total = int(metrics.get("tiers_total_for_batch_workbooks", 0))
    if channels_total < min_channels:
        failures.append(f"channels_total<{min_channels} (actual={channels_total})")
    if tiers_total < min_tiers:
        failures.append(f"tiers_total<{min_tiers} (actual={tiers_total})")

    missing_expected = sorted([wb for wb in expected_workbooks if channels_by_workbook.get(wb, {}).get("channels", 0) == 0])
    if missing_expected:
        msg = f"expected workbook has no channels: {', '.join(missing_expected)}"
        if require_channels_for_expected:
            failures.append(msg)
        else:
            warnings.append(msg)

    if max_parser_flags is not None and parser_flags > max_parser_flags:
        failures.append(f"parser_flags>{max_parser_flags} (actual={parser_flags})")

    return {
        "pass": len(failures) == 0,
        "failures": failures,
        "warnings": warnings,
        "parser_flags": parser_flags,
        "expected_workbooks": expected_workbooks,
    }


__all__ = ["build_quality_report", "csv_row_count"]
