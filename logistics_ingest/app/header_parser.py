from __future__ import annotations

import re
from collections import Counter
from decimal import Decimal

from logistics_ingest.app.normalize_parser_constants import *


def normalize_text(s: str) -> str:
    return s.replace("\n", " ").replace("\r", " ").strip()


DESTINATION_PLACEHOLDERS = {"/", "//", "///", "*", "-", "--", "—", "——", "N/A", "NA", "无"}


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
    if t in {"*", "/", "//", "///", "-", "--", "单询", "暂停", "渠道暂停"}:
        return None
    t_no_comma = t.replace(",", "")
    if NUM_RE.match(t_no_comma):
        return Decimal(t_no_comma)
    if STRICT_PRICE_RE.match(t_no_comma):
        m = DECIMAL_RE.search(t_no_comma)
        if m:
            return Decimal(m.group(0))
    return None


def infer_currency(text: str) -> str:
    t = text.upper()
    if "USD" in t or "$" in t:
        return "USD"
    return "CNY"


def flatten_row_text(row: list[str]) -> str:
    tokens = [normalize_text(c) for c in row if normalize_text(c)]
    if not tokens:
        return ""
    return " ".join(dict.fromkeys(tokens))


def extract_transit_days(text: str) -> tuple[int | None, int | None]:
    t = normalize_text(text)
    if not t:
        return None, None
    # Normalize bracketed unit forms, e.g. "22-28（自然日）".
    t = re.sub(r"[（(]\s*(自然日|工作日|天)\s*[）)]", r"\1", t)
    m = DAYS_RANGE_RE.search(t)
    if m:
        a = int(m.group(1))
        b = int(m.group(2))
        return (a, b) if a <= b else (b, a)
    m = DAYS_SINGLE_RE.search(t)
    if m:
        a = int(m.group(1))
        return a, a
    # Fallback: allow plain ranges when transit hints exist in the same text.
    if any(h in t for h in ("时效", "提取", "签收", "自然日", "工作日", "天")):
        m = re.search(r"(\d{1,3})\s*[-~—–]\s*(\d{1,3})", t)
        if m:
            a = int(m.group(1))
            b = int(m.group(2))
            return (a, b) if a <= b else (b, a)
    return None, None


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


def pick_dominant_title(tokens: list[str]) -> tuple[str | None, int]:
    if not tokens:
        return None, 0
    unique = list(dict.fromkeys(tokens))
    if len(unique) == 1:
        return unique[0], len(tokens)
    top_text, top_count = Counter(tokens).most_common(1)[0]
    if top_count >= 2 and top_count >= (len(tokens) + 1) // 2:
        return top_text, top_count
    return None, 0


def score_context_title(text: str) -> int:
    score = 0
    if any(h in text for h in CHANNEL_TITLE_HINTS):
        score += 4
    if any(h in text for h in ("空派", "海派", "海卡", "空运", "海运", "快铁", "卡铁", "班列", "卡派", "快递派")):
        score += 3
    if any(h in text for h in CONTEXT_PRICING_NOTE_HINTS):
        score -= 4
    if any(h in text for h in ("时效", "备注", "提取", "工作日")):
        score -= 3
    if any(h in text for h in COMPANY_NAME_HINTS) and not any(h in text for h in CHANNEL_TITLE_HINTS):
        score -= 3
    return score


def find_context_title_with_meta(rows: list[list[str]], header_idx: int) -> tuple[str | None, bool]:
    best_title: str | None = None
    best_score = -10**9
    recovered_by_repeat = False
    # Look a bit wider because many workbooks place channel titles several rows above headers.
    for i in range(header_idx - 1, max(-1, header_idx - 10), -1):
        row = rows[i]
        tokens = [normalize_text(c) for c in row if normalize_text(c) and normalize_text(c) not in CONTEXT_NOISE_TOKENS]
        if not tokens:
            continue

        text, top_count = pick_dominant_title(tokens)
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
            # Fallback for merged title rows like "星速达快递派..." repeated across many columns.
            # These rows are often real section titles even when keyword scoring is weak.
            if score <= 0 and top_count >= 3 and any(h in text for h in ("派", "卡派", "快递派", "专线", "渠道")):
                score = 1
            if score > best_score:
                best_title = text
                best_score = score
                recovered_by_repeat = score == 1 and top_count >= 3

    if best_title and best_score >= 1:
        return best_title, recovered_by_repeat
    return None, False


def find_context_title(rows: list[list[str]], header_idx: int) -> str | None:
    title, _ = find_context_title_with_meta(rows, header_idx)
    return title


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


def find_destination_col(row: list[str]) -> int | None:
    # Prefer stable destination identity columns over postal/zone columns.
    # This avoids selecting "邮编" when "仓库代码" is present.
    priority_hints: tuple[tuple[str, ...], ...] = (
        ("国家/仓库代码", "仓库代码", "仓库编码", "仓代码"),
        ("末端分区", "国家/地区", "国家", "地区", "区域", "分区", "目的地", "目的"),
        ("邮编",),
        ("仓库", "仓"),
    )
    for hints in priority_hints:
        idx = find_col(row, hints)
        if idx is not None:
            return idx
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


def expand_header_row(row: list[str], width: int) -> list[str]:
    expanded: list[str] = []
    last = ""
    for idx in range(width):
        value = normalize_text(row[idx]) if idx < len(row) else ""
        if value:
            last = value
        expanded.append(last)
    return expanded


def has_any_hint(text: str, hints: tuple[str, ...]) -> bool:
    upper_text = text.upper()
    return any(h in upper_text for h in hints)


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


def collect_note_parts_from_cols(row: list[str], cols: list[int]) -> list[str]:
    parts: list[str] = []
    for col in cols:
        if col >= len(row):
            continue
        value = normalize_text(row[col])
        if value and value not in {"*", "-", "--"} and value not in parts:
            parts.append(value)
    return parts


def extract_text_from_col(row: list[str], col: int | None, *, fallback_first_non_empty: bool = True) -> str:
    if col is not None and col < len(row):
        text = normalize_text(row[col])
        if text and text.upper() not in DESTINATION_PLACEHOLDERS:
            return text
    if not fallback_first_non_empty:
        return ""
    for c in row:
        t = normalize_text(c)
        if t and t.upper() not in DESTINATION_PLACEHOLDERS:
            return t
    return ""


def infer_tax_included(*parts: str) -> bool | None:
    text = " ".join([p for p in parts if p])
    if any(k in text for k in ("不包税", "递延", "自税")):
        return False
    if "包税" in text:
        return True
    return None


__all__ = [
    "collect_note_parts_from_cols",
    "count_numeric_prices_in_row",
    "expand_header_row",
    "extract_text_from_col",
    "find_destination_col",
    "extract_transit_days",
    "find_col",
    "find_context_title",
    "find_context_title_with_meta",
    "find_last_col",
    "find_table_headers",
    "flatten_row_text",
    "has_any_hint",
    "infer_currency",
    "infer_sheet_context_title",
    "infer_tax_included",
    "is_valid_data_row",
    "iter_header_sections",
    "normalize_text",
    "parse_numeric",
    "parse_weight_range",
    "row_is_end",
]
