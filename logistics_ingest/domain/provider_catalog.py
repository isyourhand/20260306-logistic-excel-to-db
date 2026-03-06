from __future__ import annotations

import re

CANONICAL_COMPANY_ALIASES: dict[str, tuple[str, ...]] = {
    "九方通逊": ("九方通逊", "九方"),
    "云驼": ("云驼", "深圳云驼"),
    "威飒": ("威飒",),
    "天美通": ("天美通", "天美通国际", "天美通物流"),
}

ALLOWED_SOURCE_COMPANIES = tuple(CANONICAL_COMPANY_ALIASES.keys())


def normalize_workbook_label(workbook_name: str) -> str:
    text = (workbook_name or "").replace("\n", " ").replace("\r", " ").strip()
    text = re.split(r"(?:\d{4}[./年-]\d{1,2}[./月-]\d{1,2}|20\d{2}\.\d{1,2}\.\d{1,2})", text)[0]
    return text.strip(" -_")


def infer_canonical_company_name(workbook_name: str) -> str | None:
    raw_text = (workbook_name or "").replace("\n", " ").replace("\r", " ").strip()
    normalized = normalize_workbook_label(workbook_name)
    for canonical, aliases in CANONICAL_COMPANY_ALIASES.items():
        if any(alias in raw_text or alias in normalized for alias in aliases):
            return canonical
    return None

