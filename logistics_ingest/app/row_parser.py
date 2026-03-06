from __future__ import annotations

import hashlib
from decimal import Decimal

from logistics_ingest.app.header_parser import (
    collect_note_parts_from_cols,
    count_numeric_prices_in_row,
    extract_transit_days,
    has_any_hint,
    infer_sheet_context_title,
    infer_tax_included,
    is_valid_data_row,
    normalize_text,
    parse_numeric,
    row_is_end,
)
from logistics_ingest.app.normalize_parser_constants import (
    CHANNEL_TITLE_HINTS,
    COMPANY_NAME_HINTS,
    CONTEXT_PRICING_NOTE_HINTS,
    TRANSPORT_AIR_HINTS,
    TRANSPORT_RAIL_HINTS,
    TRANSPORT_SEA_HINTS,
)
from logistics_ingest.app.section_parser import build_multi_origin_sz_sections, build_shenzhen_kg_matrix_sections
from logistics_ingest.app.section_parser import build_mirrored_route_sections
from logistics_ingest.domain.models import RateRecord
from logistics_ingest.domain.rules import (
    cargo_rules as domain_cargo_rules,
    destination_rules as domain_destination_rules,
    transport_rules as domain_transport_rules,
)


def build_channel_code(seed: str) -> str:
    h = hashlib.md5(seed.encode("utf-8")).hexdigest()[:12]
    return f"AUTO_{h}".upper()


def detect_parser_flags(
    sheet: str,
    context_title: str | None,
    transport_mode: str,
    destination_text: str,
    destination_scope: str,
    destination_country: str | None,
    destination_keyword: str | None,
    cargo_hint_text: str = "",
    context_from_repeat: bool = False,
    transit_from_note_or_context: bool = False,
    row_channel_from_fallback: bool = False,
    channel_name_dest_deduped: bool = False,
) -> list[str]:
    flags: list[str] = []
    if not context_title:
        flags.append("context_missing")
    elif context_from_repeat:
        flags.append("context_recovered_by_repeat_title")
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
    if domain_destination_rules.parse_zone_digits(destination_text) and destination_scope != "zip_prefix":
        flags.append("us_zone_scope_mismatch")
    if (
        any(h in domain_destination_rules.normalize_destination_text(destination_text) for h in ("美东", "美西", "美中"))
        and not destination_keyword
    ):
        flags.append("us_zone_missing_keyword")
    if (
        cargo_hint_text
        and domain_cargo_rules.has_battery_positive_hint(cargo_hint_text)
        and domain_cargo_rules.has_battery_negative_hint(cargo_hint_text)
    ):
        flags.append("cargo_battery_hint_conflict")
    if transit_from_note_or_context:
        flags.append("transit_from_note_or_context")
    if row_channel_from_fallback:
        flags.append("row_channel_from_fallback")
    if channel_name_dest_deduped:
        flags.append("channel_name_dest_deduped")

    return flags


def build_channel_name_seed(
    context_title: str | None,
    row_channel_text: str,
    destination_text: str,
) -> str:
    row_channel = domain_destination_rules.normalize_destination_text(row_channel_text)
    destination = domain_destination_rules.normalize_destination_text(destination_text)
    context = normalize_text(context_title or "")
    if context:
        if row_channel and row_channel != context and row_channel != destination and row_channel not in context:
            return f"{context} | {row_channel}"
        return context
    return row_channel or destination


def build_channel_name(
    sheet: str,
    channel_name_seed: str,
    destination_text: str,
) -> tuple[str, bool]:
    seed = normalize_text(channel_name_seed)
    destination = domain_destination_rules.normalize_destination_text(destination_text)
    sheet_name = normalize_text(sheet)

    deduped = False
    if seed and destination:
        if seed == destination or seed.endswith(f"| {destination}") or destination in seed:
            deduped = True

    parts = [sheet_name]
    if seed:
        parts.append(seed)
    if destination and not deduped:
        parts.append(destination)
    channel_name = " | ".join([p for p in parts if p])
    return channel_name, deduped


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
    context_from_repeat: bool = False,
    row_channel_from_fallback: bool = False,
) -> list[RateRecord]:
    destination_text = domain_destination_rules.normalize_destination_text(destination_text)
    if not destination_text:
        # Skip rows where destination collapses to placeholder/empty after normalization.
        # Example: destination cell "/" or "//" should not create a channel with empty destination.
        return []
    effective_row_channel_text = "" if row_channel_from_fallback else row_channel_text
    channel_name_seed = build_channel_name_seed(context_title, effective_row_channel_text, destination_text)
    destination_keyword = domain_destination_rules.choose_destination_keyword(destination_text)
    destination_country = domain_destination_rules.infer_destination_country(
        workbook,
        sheet,
        context_title or "",
        destination_text,
    )
    destination_scope = domain_destination_rules.infer_destination_scope(destination_text, destination_keyword, destination_country)

    transit_text = normalize_text(row[transit_col]) if transit_col is not None and transit_col < len(row) else ""
    note_text = normalize_text(row[note_col]) if note_col is not None and note_col < len(row) else ""
    dmin, dmax = extract_transit_days(transit_text)
    transit_from_note_or_context = False
    if dmin is None:
        dmin, dmax = extract_transit_days(f"{note_text} {context_title or ''}")
        transit_from_note_or_context = dmin is not None
    tax_included = infer_tax_included(sheet, context_title or "", destination_text, note_text)
    if tax_included_override is not None:
        tax_included = tax_included_override

    transport_mode = domain_transport_rules.infer_transport_mode(
        workbook,
        sheet,
        context_title or "",
        channel_name_seed,
        destination_text,
        transit_text,
        note_text,
    )
    if transport_mode_override is not None:
        transport_mode = transport_mode_override
    cargo_hint_text = f"{channel_name_seed} {context_title or ''} {sheet}"
    channel_name, channel_name_dest_deduped = build_channel_name(sheet, channel_name_seed, destination_text)
    cargo_natures = domain_cargo_rules.infer_cargo_natures(cargo_hint_text)
    parser_flags = detect_parser_flags(
        sheet=sheet,
        context_title=context_title,
        transport_mode=transport_mode,
        destination_text=destination_text,
        destination_scope=destination_scope,
        destination_country=destination_country,
        destination_keyword=destination_keyword,
        cargo_hint_text=cargo_hint_text,
        context_from_repeat=context_from_repeat,
        transit_from_note_or_context=transit_from_note_or_context,
        row_channel_from_fallback=row_channel_from_fallback,
        channel_name_dest_deduped=channel_name_dest_deduped,
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
    uniq: dict[tuple[object, ...], RateRecord] = {}
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


def parse_multi_origin_sz_matrix_records(
    workbook: str,
    sheet: str,
    rows: list[list[str]],
    source_company: str,
    currency: str,
    divisor: Decimal,
    min_charge: Decimal,
    audit_rows: list[dict[str, str]] | None = None,
) -> list[RateRecord]:
    sections = build_multi_origin_sz_sections(rows)
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
            if row_channel_candidate and row_channel_candidate != "渠道名称":
                current_row_channel = row_channel_candidate

            if not current_row_channel:
                dead_rows += 1
                if dead_rows >= 3:
                    break
                continue

            destination_text = normalize_text(row[section.destination_col]) if section.destination_col < len(row) else ""
            if not destination_text or not is_valid_data_row(destination_text):
                dead_rows += 1
                if dead_rows >= 3:
                    break
                continue

            if section.price_col >= len(row) or parse_numeric(row[section.price_col]) is None:
                dead_rows += 1
                if dead_rows >= 3:
                    break
                continue

            extra_note_parts = collect_note_parts_from_cols(row, section.extra_note_cols)
            row_records = build_row_rate_records(
                workbook=workbook,
                sheet=sheet,
                source_company=source_company,
                currency=currency,
                divisor=divisor,
                min_charge=min_charge,
                context_title=current_row_channel,
                transit_col=section.transit_col,
                note_col=section.note_col,
                weight_cols={section.price_col: section.weight_range},
                row=row,
                row_index=ridx + 1,
                row_channel_text=current_row_channel,
                destination_text=destination_text,
                audit_rows=audit_rows,
                tax_included_override=True,
                transport_mode_override="sea",
                extra_note_parts=extra_note_parts,
            )
            if row_records:
                records.extend(row_records)
                dead_rows = 0
            else:
                dead_rows += 1
                if dead_rows >= 3:
                    break

    return dedupe_rate_records(records)


def parse_mirrored_route_records(
    workbook: str,
    sheet: str,
    rows: list[list[str]],
    source_company: str,
    currency: str,
    divisor: Decimal,
    min_charge: Decimal,
    audit_rows: list[dict[str, str]] | None = None,
) -> list[RateRecord]:
    sections = build_mirrored_route_sections(rows)
    if not sections:
        return []

    records: list[RateRecord] = []
    for section in sections:
        dead_rows = 0
        for ridx in range(section.unit_row_idx + 1, section.next_header_idx):
            row = rows[ridx]
            if row_is_end(row):
                dead_rows += 1
                if dead_rows >= 2:
                    break
                continue

            row_records: list[RateRecord] = []
            for block in section.blocks:
                destination_text = normalize_text(row[block.destination_col]) if block.destination_col < len(row) else ""
                if not destination_text or not is_valid_data_row(destination_text):
                    continue
                if count_numeric_prices_in_row(row, block.weight_cols) == 0:
                    continue

                block_records = build_row_rate_records(
                    workbook=workbook,
                    sheet=sheet,
                    source_company=source_company,
                    currency=currency,
                    divisor=divisor,
                    min_charge=min_charge,
                    context_title=block.route_title,
                    transit_col=block.transit_col,
                    note_col=None,
                    weight_cols=block.weight_cols,
                    row=row,
                    row_index=ridx + 1,
                    row_channel_text="",
                    destination_text=destination_text,
                    audit_rows=audit_rows,
                    transport_mode_override="sea",
                )
                row_records.extend(block_records)

            if row_records:
                records.extend(row_records)
                dead_rows = 0
            else:
                dead_rows += 1
                if dead_rows >= 3:
                    break

    return dedupe_rate_records(records)


__all__ = [
    "build_row_rate_records",
    "dedupe_rate_records",
    "detect_parser_flags",
    "parse_mirrored_route_records",
    "parse_multi_origin_sz_matrix_records",
    "parse_shenzhen_kg_matrix_records",
]
