from __future__ import annotations

import re
from decimal import Decimal
from typing import Any

from logistics_ingest.domain.models import SurchargeRuleRecord
from logistics_ingest.shared.text_utils import normalize_text

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


def parse_surcharge_segment(segment: str, channel_codes: list[str]) -> list[SurchargeRuleRecord]:
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


def parse_sheet_surcharge_rules(rows: list[list[str]], rate_records: list[Any]) -> list[SurchargeRuleRecord]:
    if not rate_records:
        return []

    channel_codes = sorted({str(record.channel_code) for record in rate_records if getattr(record, "channel_code", None)})
    if not channel_codes:
        return []

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


__all__ = ["parse_sheet_surcharge_rules"]
