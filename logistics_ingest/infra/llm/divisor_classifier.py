from __future__ import annotations

from decimal import Decimal
from typing import Any

from logistics_ingest.domain.rules.divisor_rules import extract_divisor


def infer_divisor(
    rows: list[list[str]],
    workbook: str,
    sheet: str,
    llm_config: Any | None = None,
    audit_rows: list[dict[str, str]] | None = None,
) -> Decimal:
    return extract_divisor(
        rows=rows,
        workbook=workbook,
        sheet=sheet,
        llm_config=llm_config,
        audit_rows=audit_rows,
    )


__all__ = ["infer_divisor"]
