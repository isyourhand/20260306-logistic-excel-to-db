from __future__ import annotations

import re

from logistics_ingest.shared.text_utils import normalize_text

BATTERY_POSITIVE_HINTS = ("纯电", "带电", "电池", "带磁", "磁")
BATTERY_NEGATIVE_HINTS_RE = re.compile(
    r"不(?:接|收|走)?\s*(?:带电|纯电|电池|磁)|"
    r"拒(?:接|收)\s*(?:带电|纯电|电池|磁)|"
    r"禁(?:止|收)\s*(?:带电|纯电|电池|磁)|"
    r"仅(?:限)?\s*普货|"
    r"普货[^，,。;；]{0,16}不(?:接|收|走)?\s*(?:带电|纯电|电池|磁)"
)


def has_battery_positive_hint(text: str) -> bool:
    t = normalize_text(text)
    return any(k in t for k in BATTERY_POSITIVE_HINTS)


def has_battery_negative_hint(text: str) -> bool:
    t = normalize_text(text)
    return bool(BATTERY_NEGATIVE_HINTS_RE.search(t))


def infer_cargo_natures(text: str) -> list[str]:
    t = normalize_text(text)
    has_negative_battery = has_battery_negative_hint(t)
    has_positive_battery = has_battery_positive_hint(t)
    if has_negative_battery:
        if any(k in t for k in ("纺织", "木制")):
            return ["general", "textile"]
        return ["general"]
    if has_positive_battery:
        return ["battery"]
    if any(k in t for k in ("纺织", "木制")):
        return ["general", "textile"]
    return ["general"]


__all__ = [
    "BATTERY_POSITIVE_HINTS",
    "BATTERY_NEGATIVE_HINTS_RE",
    "has_battery_positive_hint",
    "has_battery_negative_hint",
    "infer_cargo_natures",
]
