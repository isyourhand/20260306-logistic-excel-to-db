from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from logistics_ingest.app.header_parser import (
    count_numeric_prices_in_row,
    extract_text_from_col,
    find_col,
    find_context_title_with_meta,
    find_destination_col,
    find_table_headers,
    infer_currency,
    infer_sheet_context_title,
    is_valid_data_row,
    iter_header_sections,
    row_is_end,
)
from logistics_ingest.app.normalize_parser_constants import (
    ROW_CHANNEL_COL_HINTS,
    SKIP_SHEET_KEYWORDS,
    TRANSIT_COL_HINTS,
    NOTE_COL_HINTS,
)
from logistics_ingest.app.row_parser import (
    build_row_rate_records,
    dedupe_rate_records,
    parse_mirrored_route_records,
    parse_multi_origin_sz_matrix_records,
    parse_shenzhen_kg_matrix_records,
)
from logistics_ingest.domain.models import DivisorLLMConfig, RateRecord, SurchargeRuleRecord
from logistics_ingest.domain.provider_catalog import (
    infer_canonical_company_name,
    normalize_workbook_label,
)
from logistics_ingest.domain.rules import (
    divisor_rules as domain_divisor_rules,
    surcharge_rules as domain_surcharge_rules,
)


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
class ParseStats:
    sheets_total: int = 0
    sheets_skipped: int = 0
    sheets_parsed: int = 0
    channels: int = 0
    tiers: int = 0
    surcharges: int = 0


def should_skip_sheet(sheet_name: str) -> bool:
    lower = sheet_name.lower()
    return any(k.lower() in lower for k in SKIP_SHEET_KEYWORDS)


def parse_sheet_surcharge_rules(
    rows: list[list[str]],
    rate_records: list[RateRecord],
) -> list[SurchargeRuleRecord]:
    return domain_surcharge_rules.parse_sheet_surcharge_rules(rows, rate_records)


def extract_divisor(
    rows: list[list[str]],
    workbook: str = "",
    sheet: str = "",
    llm_config: DivisorLLMConfig | None = None,
    audit_rows: list[dict[str, str]] | None = None,
) -> Decimal:
    return domain_divisor_rules.extract_divisor(rows, workbook, sheet, llm_config, audit_rows)


def extract_min_charge(rows: list[list[str]]) -> Decimal:
    # Keep MVP simple: most sheets are per-kg only; min charge can be added later.
    return Decimal("0")


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

    mirrored_route_records = parse_mirrored_route_records(
        workbook=workbook,
        sheet=sheet,
        rows=rows,
        source_company=source_company,
        currency=currency,
        divisor=divisor,
        min_charge=min_charge,
        audit_rows=audit_rows,
    )
    if mirrored_route_records:
        surcharge_records = parse_sheet_surcharge_rules(rows, mirrored_route_records)
        return mirrored_route_records, surcharge_records

    multi_origin_records = parse_multi_origin_sz_matrix_records(
        workbook=workbook,
        sheet=sheet,
        rows=rows,
        source_company=source_company,
        currency=currency,
        divisor=divisor,
        min_charge=min_charge,
        audit_rows=audit_rows,
    )
    if multi_origin_records:
        surcharge_records = parse_sheet_surcharge_rules(rows, multi_origin_records)
        return multi_origin_records, surcharge_records

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

        context_title, context_from_repeat = find_context_title_with_meta(rows, h_idx)
        context_title = context_title or infer_sheet_context_title(sheet)
        row_channel_col = find_col(header_row, ROW_CHANNEL_COL_HINTS)
        destination_col = find_destination_col(header_row)
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

            row_channel_text = extract_text_from_col(row, row_channel_col, fallback_first_non_empty=True)
            destination_text = extract_text_from_col(row, destination_col, fallback_first_non_empty=False)
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
                context_from_repeat=context_from_repeat,
                row_channel_from_fallback=row_channel_col is None,
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


__all__ = [
    "ParseStats",
    "parse_sheet_records",
    "should_skip_sheet",
]
