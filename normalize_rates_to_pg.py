from __future__ import annotations

import argparse
import hashlib
import os
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from psycopg import connect
from pricing_schema import ensure_pricing_schema

try:
    from config import PG_DSN as CONFIG_PG_DSN
except Exception:
    CONFIG_PG_DSN = ""


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

HEADER_HINTS = ("KG", "公斤", "重量", "计费", "+", "-")
CHANNEL_COL_HINTS = ("渠道", "国家", "区域", "分区", "目的", "仓", "邮编")
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

RANGE_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(?:KG|KGS|公斤)?\s*[-~—–至到]\s*(\d+(?:\.\d+)?)\s*(?:KG|KGS|公斤)?", re.IGNORECASE)
PLUS_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(?:KG|KGS|公斤)?\s*\+", re.IGNORECASE)
LE_RE = re.compile(r"(?:<=|≤)?\s*(\d+(?:\.\d+)?)\s*(?:KG|KGS|公斤)?\s*(?:以下|以内)?", re.IGNORECASE)
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
ZIP_PREFIX_RE = re.compile(r"邮编[^0-9A-Z]*([0-9](?:[、,，/\-]\d)*)")
TRANSPORT_RAIL_HINTS = ("铁", "班列", "卡铁", "快铁", "铁路", "中欧", "RAIL", "TRAIN")
TRANSPORT_SEA_HINTS = ("海", "船", "美森", "MATSON", "OCEAN", "SEA")
TRANSPORT_AIR_HINTS = ("空", "航", "AIR", "UPS", "DHL", "FEDEX", "红单", "快递")
DEFAULT_VOLUMETRIC_DIVISOR = Decimal("6000")

COUNTRY_HINTS = [
    ("US", ("美国", "US ", "USA", "美西", "美东", "美中")),
    ("CA", ("加拿大", "CANADA", "加拿")),
    ("UK", ("英国", "UK ", "ENGLAND", "LONDON")),
    ("EU", ("欧盟", "欧洲", "德国", "法国", "意大利", "西班牙", "荷兰", "波兰")),
    ("AU", ("澳大利亚", "AUSTRALIA", "澳洲")),
    ("JP", ("日本", "JAPAN")),
]


def infer_company_name(workbook_name: str) -> str:
    text = normalize_text(workbook_name)
    # Remove common date/version suffixes and keep stable company prefix.
    text = re.split(r"(?:\d{4}[./年-]\d{1,2}[./月-]\d{1,2}|20\d{2}\.\d{1,2}\.\d{1,2})", text)[0]
    text = text.strip(" -_")
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


def ensure_engine_tables(conn) -> None:
    ensure_pricing_schema(conn, include_indexes=False)


def truncate_engine_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("TRUNCATE pricing_constraints, pricing_rate_tiers, pricing_channels RESTART IDENTITY CASCADE")
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


def parse_weight_range(header_cell: str) -> tuple[Decimal, Decimal | None] | None:
    text = normalize_text(header_cell).upper()
    if not text:
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
    if any(h in text for h in TRANSPORT_SEA_HINTS):
        return "sea"
    if any(h in text for h in TRANSPORT_AIR_HINTS):
        return "air"
    return "air"


def infer_cargo_natures(text: str) -> list[str]:
    t = text
    if any(k in t for k in ("纯电", "带电", "电池", "磁")):
        return ["battery"]
    if any(k in t for k in ("纺织", "木制")):
        return ["general", "textile"]
    return ["general"]


def infer_currency(text: str) -> str:
    t = text.upper()
    if "USD" in t or "$" in t:
        return "USD"
    return "CNY"


def extract_divisor(rows: list[list[str]]) -> Decimal:
    for row in rows:
        for cell in row:
            t = normalize_text(cell)
            if not t:
                continue
            m = DIVISOR_RE.search(t)
            if m:
                value = Decimal(m.group(1))
                if value > 0:
                    return value
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


def find_context_title(rows: list[list[str]], header_idx: int) -> str | None:
    for i in range(header_idx - 1, max(-1, header_idx - 6), -1):
        row = rows[i]
        non_empty = [normalize_text(c) for c in row if normalize_text(c)]
        if not non_empty:
            continue
        unique = list(dict.fromkeys(non_empty))
        if len(unique) == 1:
            text = unique[0]
            if (
                len(text) >= 4
                and len(text) <= 80
                and text not in {"返回目录", "渠道名称", "国家"}
                and not any(k in text for k in ROW_SKIP_KEYWORDS)
            ):
                return text
    return None


def find_col(row: list[str], hints: tuple[str, ...]) -> int | None:
    for i, cell in enumerate(row):
        t = normalize_text(cell)
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


def choose_destination_keyword(destination_text: str) -> str | None:
    codes = list(dict.fromkeys(FBA_CODE_RE.findall(destination_text.upper())))
    if codes:
        return ",".join(codes)
    m = ZIP_PREFIX_RE.search(destination_text)
    if m:
        return m.group(1).replace("，", ",").replace("、", ",")
    return None


def infer_destination_country(*parts: str) -> str | None:
    joined = " ".join([p for p in parts if p]).upper()
    for code, hints in COUNTRY_HINTS:
        if any(h.upper() in joined for h in hints):
            return code
    return None


def infer_destination_scope(destination_text: str, destination_keyword: str | None, destination_country: str | None) -> str:
    text = normalize_text(destination_text)
    if destination_keyword and FBA_CODE_RE.search(destination_keyword.upper()):
        return "fba_code"
    if "邮编" in text:
        return "zip_prefix"
    if destination_country:
        return "country"
    return "any"


def infer_tax_included(*parts: str) -> bool | None:
    text = " ".join([p for p in parts if p])
    if any(k in text for k in ("不包税", "递延", "自税")):
        return False
    if "包税" in text:
        return True
    return None


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


def extract_base_text(row: list[str], channel_col: int | None) -> str:
    if channel_col is not None and channel_col < len(row):
        text = normalize_text(row[channel_col])
        if text:
            return text
    return next((normalize_text(c) for c in row if normalize_text(c)), "")


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
    base_text: str,
) -> list[RateRecord]:
    channel_name_seed = context_title or base_text
    destination_text = base_text
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

    channel_note = " | ".join([x for x in [context_title, transit_text, note_text] if x]) or None
    cargo_natures = infer_cargo_natures(f"{channel_name_seed} {context_title or ''} {sheet}")
    transport_mode = infer_transport_mode(workbook, sheet, context_title or "", destination_text, transit_text, note_text)
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


def parse_sheet_records(workbook: str, sheet: str, rows: list[list[str]]) -> list[RateRecord]:
    if should_skip_sheet(sheet):
        return []

    headers = find_table_headers(rows)
    if not headers:
        return []

    divisor = extract_divisor(rows)
    min_charge = extract_min_charge(rows)
    currency = infer_currency(f"{workbook} {sheet}")
    source_company = infer_company_name(workbook)

    records: list[RateRecord] = []

    for h_idx, weight_cols, next_header_idx in iter_header_sections(headers, len(rows)):
        header_row = rows[h_idx]

        context_title = find_context_title(rows, h_idx)
        channel_col = find_col(header_row, CHANNEL_COL_HINTS)
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

            base_text = extract_base_text(row, channel_col)
            if not base_text:
                continue
            if not is_valid_data_row(base_text):
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
                base_text=base_text,
            )
            if row_records:
                records.extend(row_records)
                dead_rows = 0
            else:
                dead_rows += 1
                if dead_rows >= 3:
                    break

    return dedupe_rate_records(records)


def upsert_rates(conn, records: list[RateRecord]) -> tuple[int, int]:
    if not records:
        return 0, 0

    channels_map: dict[str, int] = {}
    inserted_channels = 0
    inserted_tiers = 0

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

    conn.commit()
    return inserted_channels, inserted_tiers


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
        action="store_true",
        help="Truncate pricing engine tables before loading",
    )
    parser.add_argument(
        "--sheet-like",
        default="",
        help="Optional substring filter for sheet_name, e.g. 空派",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.dsn:
        raise SystemExit("Missing DSN. Provide --dsn, PG_DSN, or config.py PG_DSN.")

    stats = ParseStats()

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
        for meta_id, workbook_name, sheet_name in metas:
            stats.sheets_total += 1
            if should_skip_sheet(sheet_name):
                stats.sheets_skipped += 1
                continue

            rows = load_rows(conn, meta_id)
            records = parse_sheet_records(workbook_name, sheet_name, rows)
            if records:
                stats.sheets_parsed += 1
                all_records.extend(records)

        channels, tiers = upsert_rates(conn, all_records)
        stats.channels = channels
        stats.tiers = tiers

    print("Normalization complete.")
    print(f"  sheets_total   : {stats.sheets_total}")
    print(f"  sheets_skipped : {stats.sheets_skipped}")
    print(f"  sheets_parsed  : {stats.sheets_parsed}")
    print(f"  channels_upsert: {stats.channels}")
    print(f"  tiers_inserted : {stats.tiers}")


if __name__ == "__main__":
    main()
