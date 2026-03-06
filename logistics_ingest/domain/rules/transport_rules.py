from __future__ import annotations

from logistics_ingest.shared.text_utils import normalize_text

TRANSPORT_RAIL_HINTS = ("班列", "卡铁", "快铁", "铁路", "中欧", "RAIL", "TRAIN")
TRANSPORT_SEA_HINTS = (
    "海派",
    "海卡",
    "海运",
    "船运",
    "船期",
    "船司",
    "开船",
    "快船",
    "普船",
    "整柜",
    "拼柜",
    "美森",
    "以星",
    "MATSON",
    "COSCO",
    "EMC",
    "直航",
    "QY-OA",
    "卡航",
    "定时达卡派（美转加，OA头程，尾端卡车派送）",
    "定时达快递派（美转加）"
)
TRANSPORT_AIR_HINTS = ("空派", "空运", "航空", "直飞", "航班")


def infer_transport_mode(*parts: str) -> str:
    text = " ".join([normalize_text(p) for p in parts if p]).upper()
    has_rail = any(h in text for h in TRANSPORT_RAIL_HINTS)
    has_sea = any(h in text for h in TRANSPORT_SEA_HINTS)
    has_air = any(h in text for h in TRANSPORT_AIR_HINTS)

    if has_rail:
        return "rail"
    if has_sea and not has_air:
        return "sea"
    if has_air and not has_sea:
        return "air"
    if has_sea and has_air:
        # Mixed contexts (e.g. sheet-level "空海派" + row-level "海派") should bias to sea
        # when explicit sea-route hints are present.
        if any(
            h in text
            for h in (
                "海派",
                "海卡",
                "海运",
                "船运",
                "开船",
                "船期",
                "美森",
                "以星",
                "MATSON",
                "COSCO",
                "EMC",
                "QY-OA",
                "定时达快递派（美转加）",
            )
        ):
            return "sea"
        return "air"
    if has_sea:
        return "sea"
    return "air"


__all__ = [
    "TRANSPORT_RAIL_HINTS",
    "TRANSPORT_SEA_HINTS",
    "TRANSPORT_AIR_HINTS",
    "infer_transport_mode",
]
