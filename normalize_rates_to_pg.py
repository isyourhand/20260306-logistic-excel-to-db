from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from psycopg import connect
from pricing_schema import ensure_pricing_schema
try:
    from openai import OpenAI
except Exception:
    OpenAI = None

try:
    from config import PG_DSN as CONFIG_PG_DSN
    from config import DEEPSEEK_API_KEY as CONFIG_DEEPSEEK_API_KEY
except Exception:
    CONFIG_PG_DSN = ""
    CONFIG_DEEPSEEK_API_KEY = ""


SKIP_SHEET_KEYWORDS = (
    "目录",
    "说明",
    "查询",
    "教程",
    "地址表",
    "须知",
    "区别",
    "列表",
    "反倾销",
    "禁运",
    "发票",
    "船期",
)

HEADER_HINTS = ("KG", "公斤", "重量", "计费", "+")
CHANNEL_COL_HINTS = ("渠道", "国家", "区域", "分区", "目的", "仓", "邮编")
ROW_CHANNEL_COL_HINTS = ("渠道名称", "渠道")
DESTINATION_COL_HINTS = ("末端分区", "国家/仓库代码", "国家/地区", "国家", "区域", "分区", "目的地", "目的", "仓库", "邮编", "仓")
TRANSIT_COL_HINTS = ("时效", "提取", "签收")
NOTE_COL_HINTS = ("备注", "说明")
ROW_SKIP_KEYWORDS = (
    "备注",
    "说明",
    "赔偿",
    "拒收",
    "提醒",
    "收费",
    "附加费",
    "报关",
    "清关",
    "发货",
    "要求",
    "特别",
    "单票",
    "票",
    "住宅",
    "偏远",
    "磁检",
    "商检",
)
CONTEXT_NOISE_TOKENS = {"返回目录"}
CONTEXT_HEADER_TOKENS = {"渠道名称", "国家", "国家/地区", "国家/仓库代码", "时效/备注"}
CHANNEL_TITLE_HINTS = ("空派", "海派", "海卡", "空运", "海运", "专线", "渠道", "包税", "带电", "普货", "卡派")
CONTEXT_PRICING_NOTE_HINTS = ("单价", "/KG", "递延", "自税", "实报实销", "VAT", "EORI")
COMPANY_NAME_HINTS = ("有限公司", "物流", "国际", "集团", "公司")

RANGE_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(?:KG|KGS|公斤)?\s*[-~—–至到]\s*(\d+(?:\.\d+)?)\s*(?:KG|KGS|公斤)?", re.IGNORECASE)
PLUS_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(?:KG|KGS|公斤)?\s*\+", re.IGNORECASE)
LE_RE = re.compile(r"(?:<=|≤)?\s*(\d+(?:\.\d+)?)\s*(?:KG|KGS|公斤)?\s*(?:以下|以内)?", re.IGNORECASE)
PURE_RANGE_CELL_RE = re.compile(r"^\s*\d+(?:\.\d+)?\s*[-~—–至到]\s*\d+(?:\.\d+)?\s*$", re.IGNORECASE)
PURE_PLUS_CELL_RE = re.compile(r"^\s*\d+(?:\.\d+)?\s*\+\s*$", re.IGNORECASE)
PURE_LE_CELL_RE = re.compile(r"^\s*(?:<=|≤)?\s*\d+(?:\.\d+)?\s*(?:以下|以内)?\s*$", re.IGNORECASE)
NUM_RE = re.compile(r"^-?\d+(?:\.\d+)?$")
DECIMAL_RE = re.compile(r"-?\d+(?:\.\d+)?")
STRICT_PRICE_RE = re.compile(
    r"^(?:RMB|USD|\$)?\s*-?\d+(?:\.\d+)?\s*(?:RMB|USD|/KG|元|美元|美金)?$",
    re.IGNORECASE,
)
DIVISOR_RE = re.compile(r"(?:材积|体积)[^0-9]{0,12}/\s*(\d{3,5})")
MIN_WEIGHT_RE = re.compile(r"(?:最低计费重|不足)\s*(\d+(?:\.\d+)?)\s*KG", re.IGNORECASE)
DAYS_RANGE_RE = re.compile(r"(\d{1,3})\s*[-~—–]\s*(\d{1,3})\s*(?:个)?\s*(?:自然日|天|工作日)")
DAYS_SINGLE_RE = re.compile(r"(\d{1,3})\s*(?:个)?\s*(?:自然日|天|工作日)")
FBA_CODE_RE = re.compile(r"\b[A-Z]{3}\d\b")
EXTENDED_FBA_CODE_RE = re.compile(r"^[A-Z]{2,}(?:-[A-Z]{2,}[A-Z0-9]*)+\d$")
ZIP_PREFIX_RE = re.compile(r"邮编[^0-9A-Z]*([0-9](?:[、,，/\-]\d)*)")
US_ZONE_RE = re.compile(r"(?:美东|美西|美中|美国)")
BRACKET_DIGITS_RE = re.compile(r"[（(]([^）)]{1,30})[）)]")
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
)
TRANSPORT_AIR_HINTS = ("空派", "空运", "航空", "直飞", "航班")
BATTERY_POSITIVE_HINTS = ("纯电", "带电", "电池", "带磁", "磁")
BATTERY_NEGATIVE_HINTS_RE = re.compile(
    r"不(?:接|收|走)?\s*(?:带电|纯电|电池|磁)|"
    r"拒(?:接|收)\s*(?:带电|纯电|电池|磁)|"
    r"禁(?:止|收)\s*(?:带电|纯电|电池|磁)|"
    r"仅(?:限)?\s*普货|"
    r"普货[^，,。;；]{0,16}不(?:接|收|走)?\s*(?:带电|纯电|电池|磁)"
)
DEFAULT_VOLUMETRIC_DIVISOR = Decimal("6000")

COUNTRY_ALIASES = [
    ("US", ("美国", "UNITED STATES", "USA", "AMERICA")),
    ("CA", ("加拿大", "CANADA")),
    ("UK", ("英国", "UNITED KINGDOM", "ENGLAND", "BRITAIN", "GREAT BRITAIN")),
    ("DE", ("德国", "GERMANY")),
    ("FR", ("法国", "FRANCE")),
    ("IT", ("意大利", "ITALY")),
    ("ES", ("西班牙", "SPAIN")),
    ("NL", ("荷兰", "NETHERLANDS")),
    ("PL", ("波兰", "POLAND")),
    ("CZ", ("捷克", "CZECH")),
    ("HU", ("匈牙利", "HUNGARY")),
    ("BE", ("比利时", "BELGIUM")),
    ("DK", ("丹麦", "DENMARK")),
    ("FI", ("芬兰", "FINLAND")),
    ("IE", ("爱尔兰", "IRELAND")),
    ("GR", ("希腊", "GREECE")),
    ("AU", ("澳大利亚", "AUSTRALIA", "澳洲")),
    ("JP", ("日本", "JAPAN")),
]

BROAD_DESTINATION_HINTS = [
    ("EU", ("欧盟", "欧洲", "EUROPE")),
    ("US", ("美国", "美线", "美东", "美西", "美中", "UNITED STATES", "USA", "AMERICA")),
    ("CA", ("加拿大", "CANADA")),
    ("UK", ("英国", "UNITED KINGDOM", "ENGLAND", "BRITAIN")),
    ("AU", ("澳大利亚", "AUSTRALIA", "澳洲")),
    ("JP", ("日本", "JAPAN")),
]

EU_COUNTRY_CODES = {"DE", "FR", "IT", "ES", "NL", "PL", "CZ", "HU", "BE", "DK", "FI", "IE", "GR"}
NON_WEIGHT_RANGE_HINTS = ("邮编", "时效", "自然日", "工作日", "提取", "开头")
SURCHARGE_SEGMENT_SPLIT_RE = re.compile(r"[;；。]+")
SURCHARGE_SUBSEGMENT_SPLIT_RE = re.compile(r"[，,、]+")
SURCHARGE_AMOUNT_RE = re.compile(
    r"(?P<prefix>(?:MIN|最低(?:消费|收费)?|起收)?\s*)?"
    r"(?:(?P<lead_currency>RMB|USD|元|美元|美金)\s*\+?\s*)?"
    r"(?P<amount>\d+(?:\.\d+)?)\s*"
    r"(?P<currency>RMB|USD|元|美元|美金)?\s*/\s*"
    r"(?P<unit>KG|KGS|公斤|PCS|件|票|SKU|品名|个|页)"
    r"(?:\s*\*\s*(?P<multiplier>燃油(?:附加费)?))?",
    re.IGNORECASE,
)
SURCHARGE_FIXED_FUEL_RE = re.compile(
    r"(?P<prefix>(?:MIN|最低(?:消费|收费)?|起收)?\s*)?"
    r"(?:(?P<lead_currency>RMB|USD|元|美元|美金)\s*\+?\s*)?"
    r"(?P<amount>\d+(?:\.\d+)?)\s*"
    r"(?P<currency>RMB|USD|元|美元|美金)?\s*"
    r"(?:\*\s*)?(?P<multiplier>燃油(?:附加费)?)",
    re.IGNORECASE,
)
SURCHARGE_IMPLICIT_AMOUNT_RE = re.compile(
    r"\+\s*(?P<amount>\d+(?:\.\d+)?)\s*(?P<currency>RMB|USD|元|美元|美金)?\s*$",
    re.IGNORECASE,
)
SURCHARGE_TRIGGER_PATTERNS = (
    ("超级偏远", "address_type", "ultra_remote_area"),
    ("超偏远", "address_type", "very_remote_area"),
    ("一般偏远", "address_type", "remote_area"),
    ("海外仓地址", "address_type", "overseas_warehouse_address"),
    ("非FBA地址", "address_type", "non_fba_address"),
    ("私人地址", "address_type", "private_address"),
    ("住宅", "address_type", "residential"),
    ("偏远", "address_type", "remote_area"),
    ("指定派送", "delivery_option", "specified_carrier"),
    ("签名服务", "service_option", "signature_service"),
    ("签收证明", "service_option", "proof_of_delivery"),
    ("地址修正费", "service_option", "address_correction"),
    ("进口商使用费", "service_option", "importer_of_record"),
    ("清关费", "service_option", "customs_clearance"),
    ("燃油附加费", "service_option", "fuel_surcharge"),
    ("续页费", "service_option", "continuation_page_fee"),
    ("操作费", "service_option", "handling_fee"),
    ("单独报关", "service_option", "customs_declaration"),
    ("一般贸易报关件", "service_option", "customs_declaration"),
    ("报关费", "service_option", "customs_declaration"),
    ("磁检费", "service_option", "magnetic_inspection"),
    ("提货费", "service_option", "pickup_fee"),
    ("超周长", "package_condition", "oversize_girth"),
    ("超长费", "package_condition", "overlength"),
    ("超重费", "package_condition", "overweight"),
    ("异形费", "package_condition", "irregular_package"),
    ("3D眼镜", "cargo_tag", "3d_glasses"),
    ("成人用品", "cargo_tag", "adult_products"),
    ("内存卡", "cargo_tag", "memory_card"),
    ("纸制品", "cargo_tag", "paper"),
    ("玻璃制品", "cargo_tag", "glass"),
    ("皮革", "cargo_tag", "leather"),
    ("皮制", "cargo_tag", "leather"),
    ("木竹", "cargo_tag", "wood"),
    ("木制", "cargo_tag", "wood"),
    ("竹制", "cargo_tag", "wood"),
    ("含纺织", "cargo_tag", "textile"),
    ("纺织", "cargo_tag", "textile"),
    ("手表", "cargo_tag", "watch"),
    ("带磁", "cargo_tag", "magnetic"),
    ("带电", "cargo_tag", "battery"),
    ("纯电", "cargo_tag", "battery"),
)
CANONICAL_COMPANY_ALIASES = {
    "九方通逊": ("九方通逊", "九方"),
    "云驼": ("云驼", "深圳云驼"),
    "威飒": ("威飒",),
    "天美通": ("天美通", "天美通国际", "天美通物流"),
}
ALLOWED_SOURCE_COMPANIES = tuple(CANONICAL_COMPANY_ALIASES.keys())


def normalize_workbook_label(workbook_name: str) -> str:
    text = normalize_text(workbook_name)
    # Remove common date/version suffixes and keep stable company prefix.
    text = re.split(r"(?:\d{4}[./年-]\d{1,2}[./月-]\d{1,2}|20\d{2}\.\d{1,2}\.\d{1,2})", text)[0]
    return text.strip(" -_")


def infer_canonical_company_name(workbook_name: str) -> str | None:
    raw_text = normalize_text(workbook_name)
    normalized = normalize_workbook_label(workbook_name)
    for canonical, aliases in CANONICAL_COMPANY_ALIASES.items():
        if any(alias in raw_text or alias in normalized for alias in aliases):
            return canonical
    return None


def infer_company_name(workbook_name: str) -> str:
    canonical = infer_canonical_company_name(workbook_name)
    if canonical:
        return canonical

    text = normalize_workbook_label(workbook_name)
    if "-" in text:
        left = text.split("-", 1)[0].strip()
        if len(left) >= 2:
            return left
    return text or workbook_name


@dataclass
class RateRecord:
    channel_code: str
    channel_name: str
    transport_mode: str
    cargo_natures: list[str]
    destination_keyword: str | None
    transit_days_min: int | None
    transit_days_max: int | None
    channel_note: str | None
    destination_country: str | None
    destination_scope: str
    tax_included: bool | None
    source_workbook: str
    source_company: str
    min_weight: Decimal
    max_weight: Decimal | None
    unit_price: Decimal
    currency: str
    volumetric_divisor: Decimal | None
    min_charge: Decimal


@dataclass
class ParseStats:
    sheets_total: int = 0
    sheets_skipped: int = 0
    sheets_parsed: int = 0
    channels: int = 0
    tiers: int = 0
    surcharges: int = 0


@dataclass
class DivisorCandidate:
    row_index: int
    divisor: Decimal
    text: str
    heading: str
    nearby_lines: list[str]


@dataclass
class DivisorLLMDecision:
    applies_to_main_shipping: bool
    confidence: float
    scope: str
    reason: str


@dataclass
class DivisorLLMConfig:
    enabled: bool = False
    api_key: str = ""
    model: str = "deepseek-chat"
    confidence_threshold: float = 0.8
    client: Any | None = None
    cache: dict[str, DivisorLLMDecision | None] | None = None


@dataclass
class SurchargeRuleRecord:
    channel_code: str
    rule_name: str
    trigger_type: str
    trigger_value: str
    calc_method: str
    amount: Decimal
    currency: str
    weight_basis: str | None
    min_charge: Decimal | None
    max_charge: Decimal | None
    requires_fuel_multiplier: bool
    stack_mode: str
    priority: int
    note: str | None
    source_excerpt: str | None


@dataclass
class ShenzhenKGMatrixSection:
    group_row_idx: int
    title_row_idx: int
    unit_row_idx: int
    next_header_idx: int
    row_channel_col: int
    destination_col: int
    transit_col: int | None
    note_col: int | None
    extra_note_cols: list[int]
    weight_cols: dict[int, tuple[Decimal, Decimal | None]]
    tax_by_col: dict[int, bool | None]


DIVISOR_LLM_SYSTEM_PROMPT = (
    "You are a strict logistics pricing parser. "
    "Decide whether a volumetric divisor mention applies to MAIN shipping rate tiers "
    "or only applies to after-shipping services (return/reroute/claims/fees). "
    "Respond with JSON only."
)

def ensure_engine_tables(conn) -> None:
    ensure_pricing_schema(conn, include_indexes=True)


def truncate_engine_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "TRUNCATE pricing_constraints, pricing_rate_tiers, pricing_surcharge_rules, "
            "pricing_channels RESTART IDENTITY CASCADE"
        )
    conn.commit()


def list_sheet_metas(conn, batch_id: str | None):
    where = ""
    params: tuple[Any, ...] = ()
    if batch_id:
        where = "WHERE import_batch_id = %s"
        params = (batch_id,)

    sql = f"""
    SELECT id, workbook_name, sheet_name
    FROM raw_sheet_meta
    {where}
    ORDER BY id
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def load_rows(conn, meta_id: int) -> list[list[str]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT row_data
            FROM raw_sheet_rows
            WHERE meta_id = %s
            ORDER BY row_index
            """,
            (meta_id,),
        )
        return [list(r[0]) for r in cur.fetchall()]


def should_skip_sheet(sheet_name: str) -> bool:
    lower = sheet_name.lower()
    return any(k.lower() in lower for k in SKIP_SHEET_KEYWORDS)


def normalize_text(s: str) -> str:
    return s.replace("\n", " ").replace("\r", " ").strip()


def text_has_hint(cell: str) -> bool:
    up = cell.upper()
    return any(h in up for h in HEADER_HINTS)


def looks_like_weight_header_cell(text: str) -> bool:
    # Prevent zip/time ranges (e.g. 邮编8-9, 时效8-11自然日) from being treated as weight bins.
    if any(k in text for k in NON_WEIGHT_RANGE_HINTS):
        return False
    if any(k in text for k in ("KG", "KGS", "公斤", "计费", "重量")):
        return True
    if PURE_RANGE_CELL_RE.match(text) or PURE_PLUS_CELL_RE.match(text) or PURE_LE_CELL_RE.match(text):
        return True
    return False


def parse_weight_range(header_cell: str) -> tuple[Decimal, Decimal | None] | None:
    text = normalize_text(header_cell).upper()
    if not text:
        return None
    if not looks_like_weight_header_cell(text):
        return None

    m = RANGE_RE.search(text)
    if m:
        a = Decimal(m.group(1))
        b = Decimal(m.group(2))
        if b >= a:
            return a, b

    m = PLUS_RE.search(text)
    if m:
        a = Decimal(m.group(1))
        return a, None

    if any(x in text for x in ("以下", "以内", "<=", "≤")):
        m = LE_RE.search(text)
        if m:
            b = Decimal(m.group(1))
            return Decimal("0"), b

    return None


def parse_numeric(cell: str) -> Decimal | None:
    t = normalize_text(cell)
    if not t:
        return None
    if t in {"*", "/", "-", "--", "单询", "暂停", "渠道暂停"}:
        return None
    t_no_comma = t.replace(",", "")
    if NUM_RE.match(t_no_comma):
        return Decimal(t_no_comma)
    if STRICT_PRICE_RE.match(t_no_comma):
        m = DECIMAL_RE.search(t_no_comma)
        if m:
            return Decimal(m.group(0))
    return None


def infer_transport_mode(*parts: str) -> str:
    text = " ".join([normalize_text(p) for p in parts if p]).upper()
    if any(h in text for h in TRANSPORT_RAIL_HINTS):
        return "rail"
    if any(h in text for h in TRANSPORT_AIR_HINTS):
        return "air"
    if any(h in text for h in TRANSPORT_SEA_HINTS):
        return "sea"
    return "air"


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


def infer_currency(text: str) -> str:
    t = text.upper()
    if "USD" in t or "$" in t:
        return "USD"
    return "CNY"


def summarize_row_text(row: list[str], max_tokens: int = 3) -> str:
    tokens = [normalize_text(c) for c in row if normalize_text(c)]
    if not tokens:
        return ""
    unique = list(dict.fromkeys(tokens))
    if len(unique) == 1:
        return unique[0]
    return " | ".join(unique[:max_tokens])


def flatten_row_text(row: list[str]) -> str:
    tokens = [normalize_text(c) for c in row if normalize_text(c)]
    if not tokens:
        return ""
    return " ".join(dict.fromkeys(tokens))


def normalize_surcharge_cell_text(text: str) -> str:
    if not text:
        return ""
    lines = [normalize_text(part) for part in re.split(r"[\r\n]+", text) if normalize_text(part)]
    if not lines:
        return ""
    return "\n".join(lines)


def has_surcharge_amount_pattern(text: str) -> bool:
    sample = normalize_text(text)
    if not sample:
        return False
    if SURCHARGE_AMOUNT_RE.search(sample) or SURCHARGE_FIXED_FUEL_RE.search(sample):
        return True
    for raw_line in re.split(r"[\r\n]+", text):
        line = normalize_text(raw_line)
        if not line:
            continue
        for part in SURCHARGE_SUBSEGMENT_SPLIT_RE.split(line):
            if SURCHARGE_IMPLICIT_AMOUNT_RE.search(normalize_text(part)):
                return True
    return False


def extract_surcharge_triggers(text: str) -> list[tuple[str, str]]:
    remaining = normalize_text(text)
    hits: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for needle, trigger_type, trigger_value in SURCHARGE_TRIGGER_PATTERNS:
        if needle in remaining and (trigger_type, trigger_value) not in seen:
            hits.append((trigger_type, trigger_value))
            seen.add((trigger_type, trigger_value))
            remaining = remaining.replace(needle, " ")
    return hits


def normalize_surcharge_currency(raw: str) -> str:
    token = normalize_text(raw).upper()
    if token in {"USD", "美元", "美金"}:
        return "USD"
    return "CNY"


def normalize_surcharge_unit(raw: str) -> tuple[str | None, str | None]:
    unit = normalize_text(raw).upper()
    if unit in {"KG", "KGS", "公斤"}:
        return "per_kg", "unknown"
    if unit in {"PCS", "件"}:
        return "per_piece", None
    if unit == "票":
        return "per_ticket", None
    if unit == "页":
        return "per_page", None
    if unit in {"SKU", "品名", "个"}:
        return "per_sku", None
    return None, None


def is_min_charge_match(match: re.Match[str]) -> bool:
    prefix = normalize_text(match.group("prefix") or "").upper()
    return prefix.startswith("MIN") or "最低" in prefix or "起收" in prefix


def build_surcharge_rule_rows(
    *,
    channel_codes: list[str],
    triggers: list[tuple[str, str]],
    calc_method: str,
    amount: Decimal,
    currency: str,
    weight_basis: str | None,
    min_charge: Decimal | None,
    max_charge: Decimal | None,
    requires_fuel_multiplier: bool,
    stack_mode: str,
    priority: int,
    note: str,
) -> list[SurchargeRuleRecord]:
    rules: list[SurchargeRuleRecord] = []
    for trigger_type, trigger_value in triggers:
        rule_name = f"{trigger_type}:{trigger_value}"
        for channel_code in channel_codes:
            rules.append(
                SurchargeRuleRecord(
                    channel_code=channel_code,
                    rule_name=rule_name,
                    trigger_type=trigger_type,
                    trigger_value=trigger_value,
                    calc_method=calc_method,
                    amount=amount,
                    currency=currency,
                    weight_basis=weight_basis,
                    min_charge=min_charge,
                    max_charge=max_charge,
                    requires_fuel_multiplier=requires_fuel_multiplier,
                    stack_mode=stack_mode,
                    priority=priority,
                    note=note,
                    source_excerpt=note,
                )
            )
    return rules


def build_surcharge_segments(row_text: str) -> list[str]:
    segments: list[str] = []
    seen: set[str] = set()
    for raw_line in row_text.splitlines() or [row_text]:
        line = normalize_text(raw_line)
        if not line:
            continue
        for raw_segment in SURCHARGE_SEGMENT_SPLIT_RE.split(line):
            segment = normalize_text(raw_segment)
            if not segment:
                continue

            subsegments = [
                normalize_text(part) for part in SURCHARGE_SUBSEGMENT_SPLIT_RE.split(segment) if normalize_text(part)
            ]
            if len(subsegments) > 1:
                emitted = False
                idx = 0
                while idx < len(subsegments):
                    part = subsegments[idx]
                    part_has_amount = has_surcharge_amount_pattern(part)
                    part_has_triggers = bool(extract_surcharge_triggers(part))
                    if part_has_triggers and not part_has_amount:
                        combined = part
                        probe = idx + 1
                        while probe < len(subsegments):
                            next_part = subsegments[probe]
                            next_has_amount = has_surcharge_amount_pattern(next_part)
                            next_has_triggers = bool(extract_surcharge_triggers(next_part))
                            if next_has_amount and not next_has_triggers:
                                combined = normalize_text(f"{combined} {next_part}")
                                probe += 1
                                continue
                            break
                        if combined != part and has_surcharge_amount_pattern(combined):
                            if combined not in seen:
                                seen.add(combined)
                                segments.append(combined)
                            emitted = True
                            idx = probe
                            continue
                    if part_has_amount:
                        if part not in seen:
                            seen.add(part)
                            segments.append(part)
                        emitted = True
                    idx += 1
                if emitted:
                    continue

            if segment not in seen:
                seen.add(segment)
                segments.append(segment)
    return segments


def parse_surcharge_segment(
    segment: str,
    channel_codes: list[str],
) -> list[SurchargeRuleRecord]:
    triggers = extract_surcharge_triggers(segment)
    if not triggers:
        return []
    if any(trigger_value == "fuel_surcharge" for _, trigger_value in triggers):
        triggers = [(trigger_type, trigger_value) for trigger_type, trigger_value in triggers if trigger_value == "fuel_surcharge"]

    matches = list(SURCHARGE_AMOUNT_RE.finditer(segment))
    primary_matches = [m for m in matches if not is_min_charge_match(m)]
    stack_mode = "highest_only" if "不叠加" in segment else "stackable"
    note = segment[:500]
    if len(primary_matches) == 1:
        primary = primary_matches[0]
        calc_method, weight_basis = normalize_surcharge_unit(primary.group("unit") or "")
        if not calc_method:
            return []

        min_charge: Decimal | None = None
        for match in matches:
            if match == primary or not is_min_charge_match(match):
                continue
            min_calc_method, _ = normalize_surcharge_unit(match.group("unit") or "")
            if min_calc_method in {"per_ticket", "fixed"}:
                min_charge = Decimal(match.group("amount"))
                break
        if min_charge is None:
            for match in SURCHARGE_FIXED_FUEL_RE.finditer(segment):
                if is_min_charge_match(match):
                    min_charge = Decimal(match.group("amount"))
                    break

        return build_surcharge_rule_rows(
            channel_codes=channel_codes,
            triggers=triggers,
            calc_method=calc_method,
            amount=Decimal(primary.group("amount")),
            currency=normalize_surcharge_currency(primary.group("currency") or primary.group("lead_currency") or ""),
            weight_basis=weight_basis,
            min_charge=min_charge,
            max_charge=None,
            requires_fuel_multiplier=bool(primary.group("multiplier")),
            stack_mode=stack_mode,
            priority=100,
            note=note,
        )

    implicit_matches = list(SURCHARGE_IMPLICIT_AMOUNT_RE.finditer(segment))
    implicit_triggers = [(trigger_type, trigger_value) for trigger_type, trigger_value in triggers if trigger_type == "cargo_tag"]
    if len(implicit_matches) == 1 and implicit_triggers:
        match = implicit_matches[0]
        return build_surcharge_rule_rows(
            channel_codes=channel_codes,
            triggers=implicit_triggers,
            calc_method="per_kg",
            amount=Decimal(match.group("amount")),
            currency=normalize_surcharge_currency(match.group("currency") or ""),
            weight_basis="unknown",
            min_charge=None,
            max_charge=None,
            requires_fuel_multiplier=False,
            stack_mode=stack_mode,
            priority=100,
            note=note,
        )

    fixed_fuel_matches = list(SURCHARGE_FIXED_FUEL_RE.finditer(segment))
    fixed_primary_matches = [match for match in fixed_fuel_matches if not is_min_charge_match(match)]
    if len(fixed_primary_matches) == 1:
        primary = fixed_primary_matches[0]
        min_charge = None
        for match in fixed_fuel_matches:
            if match == primary or not is_min_charge_match(match):
                continue
            min_charge = Decimal(match.group("amount"))
            break
        return build_surcharge_rule_rows(
            channel_codes=channel_codes,
            triggers=triggers,
            calc_method="fixed",
            amount=Decimal(primary.group("amount")),
            currency=normalize_surcharge_currency(primary.group("currency") or primary.group("lead_currency") or ""),
            weight_basis=None,
            min_charge=min_charge,
            max_charge=None,
            requires_fuel_multiplier=True,
            stack_mode=stack_mode,
            priority=100,
            note=note,
        )
    return []


def dedupe_surcharge_records(records: list[SurchargeRuleRecord]) -> list[SurchargeRuleRecord]:
    uniq: dict[tuple[Any, ...], SurchargeRuleRecord] = {}
    for record in records:
        key = (
            record.channel_code,
            record.rule_name,
            record.trigger_type,
            record.trigger_value,
            record.calc_method,
            record.amount,
            record.currency,
            record.weight_basis,
            record.min_charge,
            record.max_charge,
            record.requires_fuel_multiplier,
            record.stack_mode,
            record.priority,
        )
        uniq[key] = record
    return list(uniq.values())


def parse_sheet_surcharge_rules(
    rows: list[list[str]],
    rate_records: list[RateRecord],
) -> list[SurchargeRuleRecord]:
    if not rate_records:
        return []

    channel_codes = sorted({record.channel_code for record in rate_records})
    surcharge_records: list[SurchargeRuleRecord] = []
    for row in rows:
        seen_cells: set[str] = set()
        for cell in row:
            cell_text = normalize_surcharge_cell_text(cell)
            if not cell_text or cell_text in seen_cells:
                continue
            seen_cells.add(cell_text)
            if not has_surcharge_amount_pattern(cell_text):
                continue
            for segment in build_surcharge_segments(cell_text):
                surcharge_records.extend(parse_surcharge_segment(segment, channel_codes))
    return dedupe_surcharge_records(surcharge_records)


def find_heading_above(rows: list[list[str]], row_index: int, max_lookback: int = 12) -> str:
    for ridx in range(row_index - 1, max(0, row_index - max_lookback) - 1, -1):
        text = summarize_row_text(rows[ridx], max_tokens=2)
        if not text:
            continue
        if "返回目录" in text or text in CONTEXT_HEADER_TOKENS:
            continue
        if len(text) <= 120:
            return text
    return ""


def collect_nearby_lines(rows: list[list[str]], row_index: int, window: int = 2) -> list[str]:
    lines: list[str] = []
    start = max(0, row_index - window)
    end = min(len(rows) - 1, row_index + window)
    for ridx in range(start, end + 1):
        text = summarize_row_text(rows[ridx], max_tokens=2)
        if text:
            lines.append(f"r{ridx + 1}: {text}")
    return lines


def collect_divisor_candidates(rows: list[list[str]]) -> list[DivisorCandidate]:
    candidates: list[DivisorCandidate] = []
    seen: set[tuple[int, str]] = set()
    for ridx, row in enumerate(rows):
        for cell in row:
            t = normalize_text(cell)
            if not t:
                continue
            m = DIVISOR_RE.search(t)
            if not m:
                continue
            value = Decimal(m.group(1))
            if value <= 0:
                continue
            key = (ridx, str(value))
            if key in seen:
                continue
            seen.add(key)
            candidates.append(
                DivisorCandidate(
                    row_index=ridx + 1,
                    divisor=value,
                    text=t[:300],
                    heading=find_heading_above(rows, ridx),
                    nearby_lines=collect_nearby_lines(rows, ridx, window=2),
                )
            )
            # One candidate per row is enough; merged cells can repeat the same note.
            break
    return candidates


def parse_divisor_llm_decision(text: str) -> DivisorLLMDecision | None:
    raw = text.strip()
    payload: dict[str, Any] | None = None
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            payload = parsed
    except Exception:
        m = re.search(r"\{[\s\S]*\}", raw)
        if m:
            try:
                parsed = json.loads(m.group(0))
                if isinstance(parsed, dict):
                    payload = parsed
            except Exception:
                payload = None
    if payload is None:
        return None

    applies = bool(payload.get("applies_to_main_shipping"))
    confidence_raw = payload.get("confidence", 0)
    try:
        confidence = float(confidence_raw)
    except Exception:
        confidence = 0.0
    if confidence < 0:
        confidence = 0.0
    if confidence > 1:
        confidence = 1.0

    scope = str(payload.get("scope") or "").strip().lower()
    reason = str(payload.get("reason") or "").strip()
    return DivisorLLMDecision(
        applies_to_main_shipping=applies,
        confidence=confidence,
        scope=scope,
        reason=reason,
    )


def llm_decide_divisor_candidate(
    config: DivisorLLMConfig,
    workbook: str,
    sheet: str,
    candidate: DivisorCandidate,
) -> DivisorLLMDecision | None:
    if not config.enabled or config.client is None:
        return None

    cache = config.cache if config.cache is not None else {}
    cache_key = hashlib.md5(
        f"{workbook}|{sheet}|{candidate.row_index}|{candidate.divisor}|{candidate.text}|{candidate.heading}".encode("utf-8")
    ).hexdigest()
    if cache_key in cache:
        return cache[cache_key]

    user_payload = {
        "task": "decide_if_divisor_applies_to_main_shipping_rate_table",
        "workbook": workbook,
        "sheet": sheet,
        "candidate_divisor": str(candidate.divisor),
        "candidate_row_index": candidate.row_index,
        "candidate_line": candidate.text,
        "nearest_heading_above": candidate.heading,
        "nearby_lines": candidate.nearby_lines,
        "output_schema": {
            "applies_to_main_shipping": "boolean",
            "confidence": "number_0_to_1",
            "scope": "main_rate|return_only|unknown",
            "reason": "short_string",
        },
    }
    try:
        resp = config.client.chat.completions.create(
            model=config.model,
            messages=[
                {"role": "system", "content": DIVISOR_LLM_SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
            ],
            stream=False,
            temperature=0.0,
        )
        text = resp.choices[0].message.content or ""
        decision = parse_divisor_llm_decision(text)
    except Exception:
        decision = None

    if config.cache is not None:
        config.cache[cache_key] = decision
    return decision


def extract_divisor(
    rows: list[list[str]],
    workbook: str = "",
    sheet: str = "",
    llm_config: DivisorLLMConfig | None = None,
    audit_rows: list[dict[str, str]] | None = None,
) -> Decimal:
    candidates = collect_divisor_candidates(rows)
    if not candidates:
        return DEFAULT_VOLUMETRIC_DIVISOR

    if llm_config is None or not llm_config.enabled:
        return candidates[0].divisor

    threshold = llm_config.confidence_threshold
    for candidate in candidates:
        decision = llm_decide_divisor_candidate(llm_config, workbook, sheet, candidate)
        if audit_rows is not None:
            if decision is None:
                audit_rows.append(
                    {
                        "workbook": workbook,
                        "sheet": sheet,
                        "row_index": str(candidate.row_index),
                        "context_title": candidate.heading,
                        "destination_text": candidate.text,
                        "destination_scope": "",
                        "destination_country": "",
                        "destination_keyword": "",
                        "transport_mode": "",
                        "divisor_candidate": str(candidate.divisor),
                        "divisor_decision": "llm_parse_failed",
                        "llm_confidence": "0",
                        "flags": "divisor_llm_parse_failed",
                    }
                )
            else:
                decision_flag = (
                    "divisor_candidate_applied"
                    if decision.applies_to_main_shipping and decision.confidence >= threshold
                    else "divisor_candidate_rejected"
                )
                audit_rows.append(
                    {
                        "workbook": workbook,
                        "sheet": sheet,
                        "row_index": str(candidate.row_index),
                        "context_title": candidate.heading,
                        "destination_text": candidate.text,
                        "destination_scope": "",
                        "destination_country": "",
                        "destination_keyword": "",
                        "transport_mode": "",
                        "divisor_candidate": str(candidate.divisor),
                        "divisor_decision": decision.scope or decision.reason or decision_flag,
                        "llm_confidence": f"{decision.confidence:.3f}",
                        "flags": decision_flag,
                    }
                )

        if decision and decision.applies_to_main_shipping and decision.confidence >= threshold:
            return candidate.divisor

    return DEFAULT_VOLUMETRIC_DIVISOR


def extract_min_charge(rows: list[list[str]]) -> Decimal:
    # Keep MVP simple: most sheets are per-kg only; min charge can be added later.
    return Decimal("0")


def extract_transit_days(text: str) -> tuple[int | None, int | None]:
    t = normalize_text(text)
    if not t:
        return None, None
    m = DAYS_RANGE_RE.search(t)
    if m:
        a = int(m.group(1))
        b = int(m.group(2))
        return (a, b) if a <= b else (b, a)
    m = DAYS_SINGLE_RE.search(t)
    if m:
        a = int(m.group(1))
        return a, a
    return None, None


def build_channel_code(seed: str) -> str:
    h = hashlib.md5(seed.encode("utf-8")).hexdigest()[:12]
    return f"AUTO_{h}".upper()


def find_table_headers(rows: list[list[str]]) -> list[tuple[int, dict[int, tuple[Decimal, Decimal | None]]]]:
    headers: list[tuple[int, dict[int, tuple[Decimal, Decimal | None]]]] = []
    for idx, row in enumerate(rows):
        ranges: dict[int, tuple[Decimal, Decimal | None]] = {}
        hinted = 0
        has_channel_hint = False
        for col, cell in enumerate(row):
            c = normalize_text(cell)
            if text_has_hint(c):
                hinted += 1
            if any(h in c for h in CHANNEL_COL_HINTS):
                has_channel_hint = True
            parsed = parse_weight_range(c)
            if parsed:
                ranges[col] = parsed
        if len(ranges) >= 2 and hinted >= 2 and has_channel_hint:
            headers.append((idx, ranges))
    return headers


def infer_sheet_context_title(sheet_name: str) -> str | None:
    text = normalize_text(sheet_name)
    if any(h in text for h in CHANNEL_TITLE_HINTS):
        return text
    return None


def pick_dominant_title(tokens: list[str]) -> str | None:
    if not tokens:
        return None
    unique = list(dict.fromkeys(tokens))
    if len(unique) == 1:
        return unique[0]
    top_text, top_count = Counter(tokens).most_common(1)[0]
    if top_count >= 2 and top_count >= (len(tokens) + 1) // 2:
        return top_text
    return None


def score_context_title(text: str) -> int:
    score = 0
    if any(h in text for h in CHANNEL_TITLE_HINTS):
        score += 4
    if any(h in text for h in ("空派", "海派", "海卡", "空运", "海运", "快铁", "卡铁", "班列")):
        score += 3
    if any(h in text for h in CONTEXT_PRICING_NOTE_HINTS):
        score -= 4
    if any(h in text for h in ("时效", "备注", "提取", "工作日")):
        score -= 3
    if any(h in text for h in COMPANY_NAME_HINTS) and not any(h in text for h in CHANNEL_TITLE_HINTS):
        score -= 3
    return score


def find_context_title(rows: list[list[str]], header_idx: int) -> str | None:
    best_title: str | None = None
    best_score = -10**9
    # Look a bit wider because many workbooks place channel titles several rows above headers.
    for i in range(header_idx - 1, max(-1, header_idx - 10), -1):
        row = rows[i]
        tokens = [normalize_text(c) for c in row if normalize_text(c) and normalize_text(c) not in CONTEXT_NOISE_TOKENS]
        if not tokens:
            continue

        text = pick_dominant_title(tokens)
        if not text:
            continue

        if (
            text
            and len(text) >= 4
            and len(text) <= 80
            and text not in CONTEXT_HEADER_TOKENS
            and not any(k in text for k in ROW_SKIP_KEYWORDS)
        ):
            score = score_context_title(text)
            if score > best_score:
                best_title = text
                best_score = score

    if best_title and best_score >= 1:
        return best_title
    return None


def find_col(row: list[str], hints: tuple[str, ...]) -> int | None:
    for i, cell in enumerate(row):
        t = normalize_text(cell)
        if not t:
            continue
        if any(h in t for h in hints):
            return i
    return None


def find_last_col(row: list[str], hints: tuple[str, ...]) -> int | None:
    for i in range(len(row) - 1, -1, -1):
        t = normalize_text(row[i])
        if not t:
            continue
        if any(h in t for h in hints):
            return i
    return None


def row_is_end(row: list[str]) -> bool:
    non_empty = [normalize_text(c) for c in row if normalize_text(c)]
    if not non_empty:
        return True
    first = non_empty[0]
    if "返回目录" in first:
        return True
    if first.startswith("一、") or first.startswith("二、") or first.startswith("三、"):
        return True
    return False


def normalize_destination_text(text: str) -> str:
    t = normalize_text(text)
    t = t.replace("\u3000", " ")
    t = re.sub(r"[，、/;；|]+", ",", t)
    t = re.sub(r"\s*,\s*", ",", t)
    t = re.sub(r"\s+", " ", t)
    return t.strip(" ,")


def split_destination_tokens(text: str) -> list[str]:
    normalized = normalize_destination_text(text)
    if not normalized:
        return []
    return [x.strip() for x in normalized.split(",") if x.strip()]


def extract_fba_like_codes(text: str) -> list[str]:
    normalized = normalize_destination_text(text).upper()
    if not normalized:
        return []

    standard_codes = list(dict.fromkeys(FBA_CODE_RE.findall(normalized)))
    if standard_codes:
        return standard_codes

    extended_codes: list[str] = []
    for token in split_destination_tokens(normalized):
        if EXTENDED_FBA_CODE_RE.match(token) and token not in extended_codes:
            extended_codes.append(token)
    return extended_codes


def expand_header_row(row: list[str], width: int) -> list[str]:
    expanded: list[str] = []
    last = ""
    for idx in range(width):
        value = normalize_text(row[idx]) if idx < len(row) else ""
        if value:
            last = value
        expanded.append(last)
    return expanded


def match_country_code(text: str) -> str | None:
    upper = normalize_text(text).upper()
    if not upper:
        return None
    for code, aliases in COUNTRY_ALIASES:
        if any(alias.upper() in upper for alias in aliases):
            return code
    return None


def extract_country_codes(destination_text: str) -> list[str]:
    codes: list[str] = []
    for token in split_destination_tokens(destination_text):
        code = match_country_code(token)
        if code and code not in codes:
            codes.append(code)
    if not codes:
        code = match_country_code(destination_text)
        if code:
            codes.append(code)
    return codes


def parse_zone_digits(text: str) -> str | None:
    normalized = normalize_destination_text(text)
    if not normalized:
        return None
    if not US_ZONE_RE.search(normalized):
        return None

    bracket_match = BRACKET_DIGITS_RE.search(text)
    candidate = bracket_match.group(1) if bracket_match else normalized
    digits = re.findall(r"\d", candidate)
    if not digits:
        # Fallback: extract from the full normalized string.
        digits = re.findall(r"\d", normalized)
    if not digits:
        return None
    uniq_digits = list(dict.fromkeys(digits))
    return ",".join(uniq_digits)


def choose_destination_keyword(destination_text: str) -> str | None:
    text = normalize_destination_text(destination_text)
    codes = extract_fba_like_codes(text)
    if codes:
        return ",".join(codes)
    m = ZIP_PREFIX_RE.search(text)
    if m:
        return m.group(1).replace("，", ",").replace("、", ",")
    zone_digits = parse_zone_digits(text)
    if zone_digits:
        return zone_digits
    country_codes = extract_country_codes(text)
    if len(country_codes) > 1:
        return ",".join(country_codes)
    return None


def infer_destination_country(*parts: str) -> str | None:
    destination_text = normalize_destination_text(parts[-1]) if parts else ""
    country_codes = extract_country_codes(destination_text)
    if len(country_codes) == 1:
        return country_codes[0]
    if len(country_codes) > 1:
        if all(c in EU_COUNTRY_CODES for c in country_codes):
            return "EU"
        return None

    joined = " ".join([normalize_text(p) for p in parts if p]).upper()
    for code, hints in BROAD_DESTINATION_HINTS:
        if any(h.upper() in joined for h in hints):
            return code
    return None


def infer_destination_scope(destination_text: str, destination_keyword: str | None, destination_country: str | None) -> str:
    text = normalize_destination_text(destination_text)
    if destination_keyword and extract_fba_like_codes(destination_keyword):
        return "fba_code"
    if "邮编" in text:
        return "zip_prefix"
    if parse_zone_digits(text):
        return "zip_prefix"
    country_codes = extract_country_codes(text)
    if len(country_codes) > 1:
        return "country_list"
    if len(country_codes) == 1:
        return "country"
    if destination_country:
        return "region" if destination_country == "EU" else "country"
    return "any"


def infer_tax_included(*parts: str) -> bool | None:
    text = " ".join([p for p in parts if p])
    if any(k in text for k in ("不包税", "递延", "自税")):
        return False
    if "包税" in text:
        return True
    return None


def has_any_hint(text: str, hints: tuple[str, ...]) -> bool:
    upper_text = text.upper()
    return any(h in upper_text for h in hints)


def detect_parser_flags(
    sheet: str,
    context_title: str | None,
    transport_mode: str,
    destination_text: str,
    destination_scope: str,
    destination_country: str | None,
    destination_keyword: str | None,
    cargo_hint_text: str = "",
) -> list[str]:
    flags: list[str] = []
    if not context_title:
        flags.append("context_missing")
    else:
        if any(h in context_title for h in CONTEXT_PRICING_NOTE_HINTS):
            flags.append("context_looks_like_pricing_note")
        if any(h in context_title for h in COMPANY_NAME_HINTS) and not any(h in context_title for h in CHANNEL_TITLE_HINTS):
            flags.append("context_looks_like_company")

    scope_text = context_title or infer_sheet_context_title(sheet) or ""
    has_air = has_any_hint(scope_text, TRANSPORT_AIR_HINTS)
    has_sea = has_any_hint(scope_text, TRANSPORT_SEA_HINTS)
    has_rail = has_any_hint(scope_text, TRANSPORT_RAIL_HINTS)

    if transport_mode == "sea" and has_air and not has_sea:
        flags.append("mode_conflict_air_markers")
    if transport_mode == "air" and has_sea and not has_air:
        flags.append("mode_conflict_sea_markers")
    if transport_mode == "rail" and not has_rail:
        flags.append("mode_conflict_rail_markers")

    if destination_scope == "country" and destination_country == "EU":
        flags.append("destination_country_too_broad")
    if destination_scope == "country_list" and not destination_keyword:
        flags.append("destination_list_missing_keyword")
    if parse_zone_digits(destination_text) and destination_scope != "zip_prefix":
        flags.append("us_zone_scope_mismatch")
    if (
        any(h in normalize_destination_text(destination_text) for h in ("美东", "美西", "美中"))
        and not destination_keyword
    ):
        flags.append("us_zone_missing_keyword")
    if cargo_hint_text and has_battery_positive_hint(cargo_hint_text) and has_battery_negative_hint(cargo_hint_text):
        flags.append("cargo_battery_hint_conflict")

    return flags


def is_valid_data_row(text: str) -> bool:
    t = normalize_text(text)
    if not t:
        return False
    if len(t) > 120:
        return False
    if t.startswith(("一、", "二、", "三、", "四、", "五、")):
        return False
    if re.match(r"^\\d+[、.]", t):
        return False
    if any(k in t for k in ROW_SKIP_KEYWORDS):
        return False
    return True


def count_numeric_prices_in_row(row: list[str], weight_cols: dict[int, tuple[Decimal, Decimal | None]]) -> int:
    count = 0
    for col in weight_cols:
        if col < len(row) and parse_numeric(row[col]) is not None:
            count += 1
    return count


def iter_header_sections(
    headers: list[tuple[int, dict[int, tuple[Decimal, Decimal | None]]]],
    rows_len: int,
) -> list[tuple[int, dict[int, tuple[Decimal, Decimal | None]], int]]:
    sections: list[tuple[int, dict[int, tuple[Decimal, Decimal | None]], int]] = []
    for idx, (header_idx, weight_cols) in enumerate(headers):
        next_header_idx = headers[idx + 1][0] if idx + 1 < len(headers) else rows_len
        sections.append((header_idx, weight_cols, next_header_idx))
    return sections


def looks_like_shenzhen_kg_matrix_header(rows: list[list[str]], idx: int) -> bool:
    if idx + 2 >= len(rows):
        return False
    group_row = flatten_row_text(rows[idx])
    title_row = flatten_row_text(rows[idx + 1])
    unit_row = rows[idx + 2]
    if "收货仓库" not in group_row or "深圳" not in group_row:
        return False
    if "产品名称" not in title_row or "仓库代码" not in title_row:
        return False
    return any(parse_weight_range(normalize_text(cell)) is not None for cell in unit_row)


def build_shenzhen_kg_matrix_sections(rows: list[list[str]]) -> list[ShenzhenKGMatrixSection]:
    start_indices: list[int] = []
    idx = 0
    while idx < len(rows):
        if looks_like_shenzhen_kg_matrix_header(rows, idx):
            start_indices.append(idx)
            idx += 3
            continue
        idx += 1

    sections: list[ShenzhenKGMatrixSection] = []
    for pos, start_idx in enumerate(start_indices):
        group_row = rows[start_idx]
        title_row = rows[start_idx + 1]
        unit_row = rows[start_idx + 2]
        next_header_idx = start_indices[pos + 1] if pos + 1 < len(start_indices) else len(rows)

        width = max(len(group_row), len(title_row), len(unit_row))
        expanded_groups = expand_header_row(group_row, width)
        expanded_titles = expand_header_row(title_row, width)
        expanded_units = expand_header_row(unit_row, width)

        weight_cols: dict[int, tuple[Decimal, Decimal | None]] = {}
        tax_by_col: dict[int, bool | None] = {}
        for col in range(width):
            unit_text = expanded_units[col]
            tax_text = expanded_titles[col]
            group_text = expanded_groups[col]
            parsed = parse_weight_range(unit_text)
            if parsed is None:
                continue
            if "深圳" not in group_text:
                continue
            if "包税" not in tax_text or "不包税" in tax_text or "按方" in tax_text:
                continue
            weight_cols[col] = parsed
            tax_by_col[col] = True

        if not weight_cols:
            continue

        row_channel_col = find_col(title_row, ("产品名称",)) or 0
        destination_col = find_col(title_row, ("仓库代码",))
        if destination_col is None:
            continue

        transit_col = find_col(group_row, ("参考时效", "提取", "签收"))
        note_col = find_last_col(group_row, NOTE_COL_HINTS)
        extra_note_candidates = [
            find_col(group_row, ("船司",)),
            find_col(group_row, ("船期",)),
            find_col(group_row, ("目的港",)),
            find_col(group_row, ("赔偿时效", "理赔时效")),
        ]
        extra_note_cols = [
            col
            for col in extra_note_candidates
            if col is not None and col != transit_col and col != note_col
        ]

        sections.append(
            ShenzhenKGMatrixSection(
                group_row_idx=start_idx,
                title_row_idx=start_idx + 1,
                unit_row_idx=start_idx + 2,
                next_header_idx=next_header_idx,
                row_channel_col=row_channel_col,
                destination_col=destination_col,
                transit_col=transit_col,
                note_col=note_col,
                extra_note_cols=extra_note_cols,
                weight_cols=weight_cols,
                tax_by_col=tax_by_col,
            )
        )
    return sections


def collect_note_parts_from_cols(row: list[str], cols: list[int]) -> list[str]:
    parts: list[str] = []
    for col in cols:
        if col >= len(row):
            continue
        value = normalize_text(row[col])
        if value and value not in {"*", "-", "--"} and value not in parts:
            parts.append(value)
    return parts


def extract_text_from_col(row: list[str], col: int | None) -> str:
    if col is not None and col < len(row):
        text = normalize_text(row[col])
        if text:
            return text
    return next((normalize_text(c) for c in row if normalize_text(c)), "")


def build_channel_name_seed(
    context_title: str | None,
    row_channel_text: str,
    destination_text: str,
) -> str:
    row_channel = normalize_text(row_channel_text)
    destination = normalize_destination_text(destination_text)
    context = normalize_text(context_title or "")
    if context:
        if row_channel and row_channel != context and row_channel != destination and row_channel not in context:
            return f"{context} | {row_channel}"
        return context
    return row_channel or destination


def build_row_rate_records(
    workbook: str,
    sheet: str,
    source_company: str,
    currency: str,
    divisor: Decimal,
    min_charge: Decimal,
    context_title: str | None,
    transit_col: int | None,
    note_col: int | None,
    weight_cols: dict[int, tuple[Decimal, Decimal | None]],
    row: list[str],
    row_index: int,
    row_channel_text: str,
    destination_text: str,
    audit_rows: list[dict[str, str]] | None = None,
    tax_included_override: bool | None = None,
    transport_mode_override: str | None = None,
    extra_note_parts: list[str] | None = None,
) -> list[RateRecord]:
    destination_text = normalize_destination_text(destination_text)
    channel_name_seed = build_channel_name_seed(context_title, row_channel_text, destination_text)
    destination_keyword = choose_destination_keyword(destination_text)
    destination_country = infer_destination_country(
        workbook,
        sheet,
        context_title or "",
        destination_text,
    )
    destination_scope = infer_destination_scope(destination_text, destination_keyword, destination_country)

    transit_text = normalize_text(row[transit_col]) if transit_col is not None and transit_col < len(row) else ""
    note_text = normalize_text(row[note_col]) if note_col is not None and note_col < len(row) else ""
    dmin, dmax = extract_transit_days(transit_text)
    if dmin is None:
        dmin, dmax = extract_transit_days(f"{note_text} {context_title or ''}")
    tax_included = infer_tax_included(sheet, context_title or "", destination_text, note_text)
    if tax_included_override is not None:
        tax_included = tax_included_override

    transport_mode = infer_transport_mode(workbook, sheet, context_title or "", channel_name_seed, destination_text)
    if transport_mode_override is not None:
        transport_mode = transport_mode_override
    cargo_hint_text = f"{channel_name_seed} {context_title or ''} {sheet}"
    cargo_natures = infer_cargo_natures(cargo_hint_text)
    parser_flags = detect_parser_flags(
        sheet=sheet,
        context_title=context_title,
        transport_mode=transport_mode,
        destination_text=destination_text,
        destination_scope=destination_scope,
        destination_country=destination_country,
        destination_keyword=destination_keyword,
        cargo_hint_text=cargo_hint_text,
    )
    if audit_rows is not None and parser_flags:
        audit_rows.append(
            {
                "workbook": workbook,
                "sheet": sheet,
                "row_index": str(row_index),
                "context_title": context_title or "",
                "destination_text": destination_text,
                "destination_scope": destination_scope,
                "destination_country": destination_country or "",
                "destination_keyword": destination_keyword or "",
                "transport_mode": transport_mode,
                "flags": ",".join(parser_flags),
            }
        )

    channel_note_parts = [x for x in [context_title, transit_text] if x]
    if extra_note_parts:
        channel_note_parts.extend([x for x in extra_note_parts if x])
    if note_text:
        channel_note_parts.append(note_text)
    if parser_flags:
        channel_note_parts.append(f"[parser_flags:{','.join(parser_flags)}]")
    channel_note = " | ".join(channel_note_parts) or None
    channel_name = f"{sheet} | {channel_name_seed} | {destination_text}"
    code_seed = f"{workbook}|{sheet}|{channel_name_seed}|{destination_text}|{transport_mode}|{','.join(cargo_natures)}"
    channel_code = build_channel_code(code_seed)

    records: list[RateRecord] = []
    for col, (wmin, wmax) in weight_cols.items():
        if col >= len(row):
            continue
        unit_price = parse_numeric(row[col])
        if unit_price is None:
            continue

        records.append(
            RateRecord(
                channel_code=channel_code,
                channel_name=channel_name[:250],
                transport_mode=transport_mode,
                cargo_natures=cargo_natures,
                destination_country=destination_country,
                destination_scope=destination_scope,
                tax_included=tax_included,
                source_workbook=workbook,
                source_company=source_company,
                destination_keyword=destination_keyword,
                transit_days_min=dmin,
                transit_days_max=dmax,
                channel_note=channel_note,
                min_weight=wmin,
                max_weight=wmax,
                unit_price=unit_price,
                currency=currency,
                volumetric_divisor=divisor,
                min_charge=min_charge,
            )
        )
    return records


def dedupe_rate_records(records: list[RateRecord]) -> list[RateRecord]:
    uniq: dict[tuple[Any, ...], RateRecord] = {}
    for record in records:
        key = (
            record.channel_code,
            record.min_weight,
            record.max_weight,
            record.unit_price,
            record.currency,
            record.volumetric_divisor,
            record.min_charge,
        )
        uniq[key] = record
    return list(uniq.values())


def parse_shenzhen_kg_matrix_records(
    workbook: str,
    sheet: str,
    rows: list[list[str]],
    source_company: str,
    currency: str,
    divisor: Decimal,
    min_charge: Decimal,
    audit_rows: list[dict[str, str]] | None = None,
) -> list[RateRecord]:
    sections = build_shenzhen_kg_matrix_sections(rows)
    if not sections:
        return []

    records: list[RateRecord] = []
    for section in sections:
        current_row_channel = ""
        dead_rows = 0
        for ridx in range(section.unit_row_idx + 1, section.next_header_idx):
            row = rows[ridx]
            if row_is_end(row):
                dead_rows += 1
                if dead_rows >= 2:
                    break
                continue

            row_channel_candidate = normalize_text(row[section.row_channel_col]) if section.row_channel_col < len(row) else ""
            if row_channel_candidate and row_channel_candidate != "产品名称":
                current_row_channel = row_channel_candidate

            if not current_row_channel:
                dead_rows += 1
                if dead_rows >= 3:
                    break
                continue

            destination_text = normalize_text(row[section.destination_col]) if section.destination_col < len(row) else ""
            if not destination_text:
                if count_numeric_prices_in_row(row, section.weight_cols) == 0:
                    dead_rows += 1
                    if dead_rows >= 3:
                        break
                continue

            if not is_valid_data_row(destination_text):
                dead_rows += 1
                if dead_rows >= 3:
                    break
                continue

            if count_numeric_prices_in_row(row, section.weight_cols) == 0:
                dead_rows += 1
                if dead_rows >= 3:
                    break
                continue

            extra_note_parts = collect_note_parts_from_cols(row, section.extra_note_cols)
            section_records: list[RateRecord] = []
            for col, weight_range in section.weight_cols.items():
                lane_weight_cols = {col: weight_range}
                lane_records = build_row_rate_records(
                    workbook=workbook,
                    sheet=sheet,
                    source_company=source_company,
                    currency=currency,
                    divisor=divisor,
                    min_charge=min_charge,
                    context_title=current_row_channel,
                    transit_col=section.transit_col,
                    note_col=section.note_col,
                    weight_cols=lane_weight_cols,
                    row=row,
                    row_index=ridx + 1,
                    row_channel_text=current_row_channel,
                    destination_text=destination_text,
                    audit_rows=audit_rows,
                    tax_included_override=section.tax_by_col.get(col),
                    transport_mode_override="sea",
                    extra_note_parts=extra_note_parts,
                )
                section_records.extend(lane_records)

            if section_records:
                records.extend(section_records)
                dead_rows = 0
            else:
                dead_rows += 1
                if dead_rows >= 3:
                    break

    return dedupe_rate_records(records)


def parse_sheet_records(
    workbook: str,
    sheet: str,
    rows: list[list[str]],
    audit_rows: list[dict[str, str]] | None = None,
    llm_config: DivisorLLMConfig | None = None,
) -> tuple[list[RateRecord], list[SurchargeRuleRecord]]:
    if should_skip_sheet(sheet):
        return [], []

    divisor = extract_divisor(
        rows,
        workbook=workbook,
        sheet=sheet,
        llm_config=llm_config,
        audit_rows=audit_rows,
    )
    min_charge = extract_min_charge(rows)
    currency = infer_currency(f"{workbook} {sheet}")
    source_company = infer_company_name(workbook)

    matrix_records = parse_shenzhen_kg_matrix_records(
        workbook=workbook,
        sheet=sheet,
        rows=rows,
        source_company=source_company,
        currency=currency,
        divisor=divisor,
        min_charge=min_charge,
        audit_rows=audit_rows,
    )
    if matrix_records:
        surcharge_records = parse_sheet_surcharge_rules(rows, matrix_records)
        return matrix_records, surcharge_records

    headers = find_table_headers(rows)
    if not headers:
        return [], []

    records: list[RateRecord] = []

    for h_idx, weight_cols, next_header_idx in iter_header_sections(headers, len(rows)):
        header_row = rows[h_idx]

        context_title = find_context_title(rows, h_idx) or infer_sheet_context_title(sheet)
        row_channel_col = find_col(header_row, ROW_CHANNEL_COL_HINTS)
        destination_col = find_col(header_row, DESTINATION_COL_HINTS)
        transit_col = find_col(header_row, TRANSIT_COL_HINTS)
        note_col = find_col(header_row, NOTE_COL_HINTS)

        dead_rows = 0
        for ridx in range(h_idx + 1, next_header_idx):
            row = rows[ridx]
            if row_is_end(row):
                dead_rows += 1
                if dead_rows >= 2:
                    break
                continue

            row_channel_text = extract_text_from_col(row, row_channel_col)
            destination_text = extract_text_from_col(row, destination_col)
            primary_text = destination_text or row_channel_text
            if not primary_text:
                continue
            if not is_valid_data_row(primary_text):
                dead_rows += 1
                if dead_rows >= 3:
                    break
                continue

            if count_numeric_prices_in_row(row, weight_cols) == 0:
                dead_rows += 1
                if dead_rows >= 3:
                    break
                continue

            row_records = build_row_rate_records(
                workbook=workbook,
                sheet=sheet,
                source_company=source_company,
                currency=currency,
                divisor=divisor,
                min_charge=min_charge,
                context_title=context_title,
                transit_col=transit_col,
                note_col=note_col,
                weight_cols=weight_cols,
                row=row,
                row_index=ridx + 1,
                row_channel_text=row_channel_text,
                destination_text=destination_text or row_channel_text,
                audit_rows=audit_rows,
            )
            if row_records:
                records.extend(row_records)
                dead_rows = 0
            else:
                dead_rows += 1
                if dead_rows >= 3:
                    break

    deduped_records = dedupe_rate_records(records)
    surcharge_records = parse_sheet_surcharge_rules(rows, deduped_records)
    return deduped_records, surcharge_records


def upsert_rates(
    conn,
    records: list[RateRecord],
    surcharge_rules: list[SurchargeRuleRecord],
) -> tuple[int, int, int]:
    if not records:
        return 0, 0, 0

    channels_map: dict[str, int] = {}
    inserted_channels = 0
    inserted_tiers = 0
    inserted_surcharges = 0

    with conn.cursor() as cur:
        for rec in records:
            if rec.channel_code not in channels_map:
                cur.execute(
                    """
                    INSERT INTO pricing_channels (
                        channel_code, channel_name, transport_mode, cargo_natures,
                        destination_country, destination_scope, tax_included, destination_keyword,
                        source_workbook, source_company,
                        transit_days_min, transit_days_max, active, note
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s)
                    ON CONFLICT (channel_code)
                    DO UPDATE SET
                        channel_name = EXCLUDED.channel_name,
                        transport_mode = EXCLUDED.transport_mode,
                        cargo_natures = EXCLUDED.cargo_natures,
                        destination_country = EXCLUDED.destination_country,
                        destination_scope = EXCLUDED.destination_scope,
                        tax_included = EXCLUDED.tax_included,
                        destination_keyword = EXCLUDED.destination_keyword,
                        source_workbook = EXCLUDED.source_workbook,
                        source_company = EXCLUDED.source_company,
                        transit_days_min = EXCLUDED.transit_days_min,
                        transit_days_max = EXCLUDED.transit_days_max,
                        note = EXCLUDED.note,
                        active = TRUE
                    RETURNING id
                    """,
                    (
                        rec.channel_code,
                        rec.channel_name,
                        rec.transport_mode,
                        rec.cargo_natures,
                        rec.destination_country,
                        rec.destination_scope,
                        rec.tax_included,
                        rec.destination_keyword,
                        rec.source_workbook,
                        rec.source_company,
                        rec.transit_days_min,
                        rec.transit_days_max,
                        rec.channel_note,
                    ),
                )
                channel_id = cur.fetchone()[0]
                channels_map[rec.channel_code] = channel_id
                inserted_channels += 1

        # Remove existing tiers for channels touched in this run, then re-insert fresh tiers.
        touched_ids = sorted(set(channels_map.values()))
        cur.execute("DELETE FROM pricing_rate_tiers WHERE channel_id = ANY(%s)", (touched_ids,))
        cur.execute("DELETE FROM pricing_surcharge_rules WHERE channel_id = ANY(%s)", (touched_ids,))

        tier_rows = [
            (
                channels_map[r.channel_code],
                r.min_weight,
                r.max_weight,
                r.unit_price,
                r.currency,
                r.volumetric_divisor,
                r.min_charge,
            )
            for r in records
        ]
        cur.executemany(
            """
            INSERT INTO pricing_rate_tiers (
                channel_id, min_weight, max_weight, unit_price, currency,
                volumetric_divisor, min_charge, active
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
            """,
            tier_rows,
        )
        inserted_tiers = len(tier_rows)

        surcharge_rows = [
            (
                channels_map[r.channel_code],
                r.rule_name,
                r.trigger_type,
                r.trigger_value,
                r.calc_method,
                r.amount,
                r.currency,
                r.weight_basis,
                r.min_charge,
                r.max_charge,
                r.requires_fuel_multiplier,
                r.stack_mode,
                r.priority,
                r.note,
                r.source_excerpt,
            )
            for r in surcharge_rules
            if r.channel_code in channels_map
        ]
        if surcharge_rows:
            cur.executemany(
                """
                INSERT INTO pricing_surcharge_rules (
                    channel_id, rule_name, trigger_type, trigger_value, calc_method,
                    amount, currency, weight_basis, min_charge, max_charge, requires_fuel_multiplier,
                    stack_mode, priority, active, note, source_excerpt
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s, %s)
                """,
                surcharge_rows,
            )
            inserted_surcharges = len(surcharge_rows)

    conn.commit()
    return inserted_channels, inserted_tiers, inserted_surcharges


def latest_batch_id(conn) -> str | None:
    with conn.cursor() as cur:
        cur.execute("SELECT import_batch_id::text FROM raw_sheet_meta ORDER BY imported_at DESC LIMIT 1")
        row = cur.fetchone()
        return row[0] if row else None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize raw_sheet_* into pricing engine tables")
    parser.add_argument(
        "--dsn",
        default=os.getenv("PG_DSN", CONFIG_PG_DSN),
        help="PostgreSQL DSN (arg > PG_DSN env > config.py)",
    )
    parser.add_argument(
        "--batch-id",
        default="",
        help="Import batch UUID from raw_sheet_meta. Default: latest batch",
    )
    parser.add_argument(
        "--truncate",
        dest="truncate",
        action="store_true",
        help="Truncate pricing engine tables before loading (default: enabled)",
    )
    parser.add_argument(
        "--no-truncate",
        dest="truncate",
        action="store_false",
        help="Disable truncation before loading",
    )
    parser.add_argument(
        "--sheet-like",
        default="",
        help="Optional substring filter for sheet_name, e.g. 空派",
    )
    parser.add_argument(
        "--audit-csv",
        default="",
        help="Optional CSV path to save parser anomaly rows",
    )
    parser.add_argument(
        "--llm-divisor-check",
        dest="llm_divisor_check",
        action="store_true",
        help="Use LLM to decide whether extracted volumetric divisor applies to main shipping rates (default: enabled)",
    )
    parser.add_argument(
        "--no-llm-divisor-check",
        dest="llm_divisor_check",
        action="store_false",
        help="Disable LLM divisor applicability check",
    )
    parser.add_argument(
        "--llm-api-key",
        default=os.getenv("DEEPSEEK_API_KEY", CONFIG_DEEPSEEK_API_KEY),
        help="DeepSeek API key for divisor scope checks",
    )
    parser.add_argument(
        "--llm-model",
        default="deepseek-chat",
        help="DeepSeek model for divisor scope checks",
    )
    parser.add_argument(
        "--llm-divisor-confidence",
        type=float,
        default=0.8,
        help="Minimum confidence (0-1) required to accept LLM divisor decision",
    )
    parser.set_defaults(truncate=True, llm_divisor_check=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.dsn:
        raise SystemExit("Missing DSN. Provide --dsn, PG_DSN, or config.py PG_DSN.")

    stats = ParseStats()
    all_audits: list[dict[str, str]] = []
    threshold = min(1.0, max(0.0, float(args.llm_divisor_confidence)))
    divisor_llm_config = DivisorLLMConfig(
        enabled=bool(args.llm_divisor_check),
        api_key=args.llm_api_key or "",
        model=args.llm_model,
        confidence_threshold=threshold,
        cache={},
    )
    if divisor_llm_config.enabled:
        if OpenAI is None:
            raise SystemExit("openai package is missing. Install it to use --llm-divisor-check.")
        if not divisor_llm_config.api_key:
            raise SystemExit("Missing LLM API key. Set DEEPSEEK_API_KEY or pass --llm-api-key.")
        divisor_llm_config.client = OpenAI(api_key=divisor_llm_config.api_key, base_url="https://api.deepseek.com")
        print(
            "Divisor LLM : enabled "
            f"(model={divisor_llm_config.model}, confidence>={divisor_llm_config.confidence_threshold:.2f})"
        )
    else:
        print("Divisor LLM : disabled")

    with connect(args.dsn) as conn:
        ensure_engine_tables(conn)
        if args.truncate:
            truncate_engine_tables(conn)

        batch_id = args.batch_id or latest_batch_id(conn)
        if not batch_id:
            raise SystemExit("No raw_sheet_meta batch found.")

        metas = list_sheet_metas(conn, batch_id)
        if args.sheet_like:
            like = args.sheet_like.lower()
            metas = [m for m in metas if like.replace('%', '') in str(m[2]).lower()]

        if not metas:
            raise SystemExit(f"No raw_sheet_meta records for batch: {batch_id}")

        print(f"Using batch_id: {batch_id}")
        print(f"Sheets to scan: {len(metas)}")

        all_records: list[RateRecord] = []
        all_surcharge_rules: list[SurchargeRuleRecord] = []
        for meta_id, workbook_name, sheet_name in metas:
            stats.sheets_total += 1
            if should_skip_sheet(sheet_name):
                stats.sheets_skipped += 1
                continue

            rows = load_rows(conn, meta_id)
            records, surcharge_rules = parse_sheet_records(
                workbook_name,
                sheet_name,
                rows,
                audit_rows=all_audits,
                llm_config=divisor_llm_config,
            )
            if records:
                stats.sheets_parsed += 1
                all_records.extend(records)
                all_surcharge_rules.extend(surcharge_rules)

        channels, tiers, surcharges = upsert_rates(conn, all_records, all_surcharge_rules)
        stats.channels = channels
        stats.tiers = tiers
        stats.surcharges = surcharges

    print("Normalization complete.")
    print(f"  sheets_total   : {stats.sheets_total}")
    print(f"  sheets_skipped : {stats.sheets_skipped}")
    print(f"  sheets_parsed  : {stats.sheets_parsed}")
    print(f"  channels_upsert: {stats.channels}")
    print(f"  tiers_inserted : {stats.tiers}")
    print(f"  surcharges_ins : {stats.surcharges}")
    print(f"  parser_flags   : {len(all_audits)}")

    if all_audits:
        flag_counter = Counter()
        for item in all_audits:
            for flag in str(item.get("flags") or "").split(","):
                if flag:
                    flag_counter[flag] += 1
        if flag_counter:
            print("  parser_flag_summary:")
            for key, value in sorted(flag_counter.items(), key=lambda kv: (-kv[1], kv[0])):
                print(f"    {key}: {value}")

    if args.audit_csv:
        output_path = os.path.abspath(args.audit_csv)
        fieldnames = [
            "workbook",
            "sheet",
            "row_index",
            "context_title",
            "destination_text",
            "destination_scope",
            "destination_country",
            "destination_keyword",
            "transport_mode",
            "divisor_candidate",
            "divisor_decision",
            "llm_confidence",
            "flags",
        ]
        with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for item in all_audits:
                writer.writerow({k: item.get(k, "") for k in fieldnames})
        print(f"  audit_csv      : {output_path}")


if __name__ == "__main__":
    main()
