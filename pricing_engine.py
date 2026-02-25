# pricing_engine.py

from __future__ import annotations

import argparse
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
    from config import PG_DSN as CONFIG_PG_DSN
    from config import PRICING_ENGINE_DEFAULTS
except Exception:
    CONFIG_PG_DSN = ""
    PRICING_ENGINE_DEFAULTS = {}

FBA_CODE_RE = re.compile(r"\b[A-Z]{3}\d\b")


@dataclass
class BoxInput:
    gross_weight: Decimal
    length: Decimal
    width: Decimal
    height: Decimal


def infer_target_country(address_text: str) -> str | None:
    s = (address_text or "").upper()
    if any(x in s for x in ("UNITED STATES", "USA", "U.S.", "AMERICA")):
        return "US"
    if any(x in s for x in ("CANADA", "加拿大")):
        return "CA"
    if any(x in s for x in ("UNITED KINGDOM", "UK", "英国", "ENGLAND")):
        return "UK"
    if any(x in s for x in ("AUSTRALIA", "澳大利亚", "澳洲")):
        return "AU"
    if any(x in s for x in ("GERMANY", "FRANCE", "ITALY", "SPAIN", "NETHERLANDS", "POLAND", "欧盟", "欧洲")):
        return "EU"

    # Common US city/state format like "San Bernardino, CA".
    if "," in s:
        tail = s.rsplit(",", 1)[-1].strip()
        if len(tail) == 2 and tail.isalpha():
            return "US"
    return None


def parse_decimal(value: Any, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"Invalid decimal for {field_name}: {value}") from exc


def parse_boxes_json(boxes_json: str) -> list[BoxInput]:
    payload = json.loads(boxes_json)
    if not isinstance(payload, list) or not payload:
        raise ValueError("boxes-json must be a non-empty JSON array")

    boxes: list[BoxInput] = []
    for idx, item in enumerate(payload, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Box {idx} must be an object")
        box = BoxInput(
            gross_weight=parse_decimal(item.get("gross_weight"), f"boxes[{idx}].gross_weight"),
            length=parse_decimal(item.get("length"), f"boxes[{idx}].length"),
            width=parse_decimal(item.get("width"), f"boxes[{idx}].width"),
            height=parse_decimal(item.get("height"), f"boxes[{idx}].height"),
        )
        boxes.append(box)
    return boxes


def ensure_mvp_tables(conn) -> None:
    ensure_pricing_schema(conn, include_indexes=True)


def fetch_candidates(conn, transport_mode: str):
    sql = """
    SELECT
      c.id,
      c.channel_code,
      c.channel_name,
      c.transport_mode,
      c.cargo_natures,
      c.destination_country,
      c.destination_scope,
      c.tax_included,
      c.destination_keyword,
      c.source_workbook,
      c.source_company,
      c.transit_days_min,
      c.transit_days_max,
      c.note,
      t.id AS tier_id,
      t.min_weight,
      t.max_weight,
      t.unit_price,
      t.currency,
      t.volumetric_divisor,
      t.min_charge,
      co.max_gross_weight,
      co.max_length,
      co.max_width,
      co.max_height,
      co.max_l_plus_w_plus_h,
      co.note AS constraint_note
    FROM pricing_channels c
    JOIN pricing_rate_tiers t ON t.channel_id = c.id AND t.active = TRUE
    LEFT JOIN pricing_constraints co ON co.channel_id = c.id
    WHERE c.active = TRUE AND c.transport_mode = %s
    ORDER BY c.id, t.min_weight ASC
    """
    with conn.cursor() as cur:
        cur.execute(sql, (transport_mode,))
        cols = [d.name for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return rows


def matches_destination(destination: str, keyword: str | None) -> bool:
    if not keyword:
        return True
    fba_codes = list(dict.fromkeys(FBA_CODE_RE.findall(keyword.upper())))
    if fba_codes:
        destination_upper = destination.upper()
        return any(code in destination_upper for code in fba_codes)
    return keyword.lower() in destination.lower()


def check_constraints(box: BoxInput, row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []

    max_gw = row.get("max_gross_weight")
    if max_gw is not None and box.gross_weight > Decimal(str(max_gw)):
        reasons.append(f"gross_weight>{max_gw}")

    max_length = row.get("max_length")
    if max_length is not None and box.length > Decimal(str(max_length)):
        reasons.append(f"length>{max_length}")

    max_width = row.get("max_width")
    if max_width is not None and box.width > Decimal(str(max_width)):
        reasons.append(f"width>{max_width}")

    max_height = row.get("max_height")
    if max_height is not None and box.height > Decimal(str(max_height)):
        reasons.append(f"height>{max_height}")

    max_lwh = row.get("max_l_plus_w_plus_h")
    if max_lwh is not None and (box.length + box.width + box.height) > Decimal(str(max_lwh)):
        reasons.append(f"l+w+h>{max_lwh}")

    return reasons


def chargeable_weight(box: BoxInput, divisor: Any) -> tuple[Decimal, Decimal]:
    if divisor is None:
        vol = Decimal("0")
    else:
        divisor_dec = Decimal(str(divisor))
        if divisor_dec == 0:
            vol = Decimal("0")
        else:
            vol = (box.length * box.width * box.height) / divisor_dec
    charge = box.gross_weight if box.gross_weight >= vol else vol
    return charge, vol


def evaluate_tier_for_boxes(boxes: list[BoxInput], row: dict[str, Any]) -> tuple[bool, Decimal, list[str], list[dict[str, str]]]:
    min_w = Decimal(str(row["min_weight"]))
    max_w = Decimal(str(row["max_weight"])) if row.get("max_weight") is not None else None
    unit_price = Decimal(str(row["unit_price"]))
    min_charge = Decimal(str(row["min_charge"]))

    total = Decimal("0")
    reasons: list[str] = []
    box_details: list[dict[str, str]] = []

    for idx, box in enumerate(boxes, start=1):
        failed = check_constraints(box, row)
        if failed:
            reasons.append(f"box#{idx}:" + ",".join(failed))
            continue

        charge_w, vol_w = chargeable_weight(box, row.get("volumetric_divisor"))
        if charge_w < min_w:
            reasons.append(f"box#{idx}:chargeable<{min_w}")
            continue
        if max_w is not None and charge_w > max_w:
            reasons.append(f"box#{idx}:chargeable>{max_w}")
            continue

        box_price = charge_w * unit_price
        if box_price < min_charge:
            box_price = min_charge

        total += box_price
        box_details.append(
            {
                "box": str(idx),
                "gross_weight": str(box.gross_weight),
                "volumetric_weight": str(vol_w.quantize(Decimal('0.001'))),
                "chargeable_weight": str(charge_w.quantize(Decimal('0.001'))),
                "box_price": str(box_price.quantize(Decimal('0.01'))),
            }
        )

    if reasons:
        return False, Decimal("0"), reasons, []

    return True, total.quantize(Decimal("0.01")), [], box_details


def build_rejected_channel(
    channel_row: dict[str, Any],
    reason: str,
    details: list[str] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "channel_id": channel_row["id"],
        "channel_code": channel_row["channel_code"],
        "channel_name": channel_row["channel_name"],
        "source_company": channel_row.get("source_company"),
        "reason": reason,
    }
    if details is not None:
        payload["details"] = details
    return payload


def channel_rejection_reason(
    channel_row: dict[str, Any],
    destination: str,
    target_country: str | None,
    cargo_nature: str,
    allow_any_destination: bool,
    tax_requirement: str,
) -> str | None:
    if cargo_nature.lower() not in [str(x).lower() for x in (channel_row.get("cargo_natures") or [])]:
        return f"cargo_nature_not_allowed:{cargo_nature}"

    row_country = (channel_row.get("destination_country") or "").upper()
    row_scope = (channel_row.get("destination_scope") or "any").lower()
    row_tax = channel_row.get("tax_included")

    if target_country:
        if row_country and row_country != target_country:
            return f"country_mismatch:{row_country}->{target_country}"
        if not row_country and row_scope == "any" and not allow_any_destination:
            return "destination_too_broad:any"

    if not matches_destination(destination, channel_row.get("destination_keyword")):
        return f"destination_mismatch:{channel_row.get('destination_keyword')}"

    if tax_requirement == "required" and row_tax is not True:
        return "tax_included_required"
    if tax_requirement == "not_required" and row_tax is not False:
        return "tax_excluded_required"

    return None


def recommend(
    rows: list[dict[str, Any]],
    destination: str,
    target_country: str | None,
    cargo_nature: str,
    boxes: list[BoxInput],
    top_n: int,
    allow_any_destination: bool,
    tax_requirement: str,
) -> dict[str, Any]:
    by_channel: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        by_channel.setdefault(row["id"], []).append(row)

    recommended: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []

    for channel_rows in by_channel.values():
        first = channel_rows[0]
        reject_reason = channel_rejection_reason(
            channel_row=first,
            destination=destination,
            target_country=target_country,
            cargo_nature=cargo_nature,
            allow_any_destination=allow_any_destination,
            tax_requirement=tax_requirement,
        )
        if reject_reason is not None:
            rejected.append(build_rejected_channel(first, reject_reason))
            continue

        tier_passed = False
        tier_fail_reasons: list[str] = []
        best_candidate: dict[str, Any] | None = None

        for row in channel_rows:
            ok, total_price, fail_reasons, box_details = evaluate_tier_for_boxes(boxes, row)
            if not ok:
                tier_fail_reasons.extend([f"tier#{row['tier_id']}:{x}" for x in fail_reasons])
                continue

            tier_passed = True
            candidate = {
                "channel_id": first["id"],
                "channel_code": first["channel_code"],
                "channel_name": first["channel_name"],
                "currency": row["currency"],
                "tier_id": row["tier_id"],
                "total_price": str(total_price),
                "transit_days": {
                    "min": first.get("transit_days_min"),
                    "max": first.get("transit_days_max"),
                },
                "destination_country": first.get("destination_country"),
                "destination_scope": first.get("destination_scope"),
                "tax_included": first.get("tax_included"),
                "source_company": first.get("source_company"),
                "source_workbook": first.get("source_workbook"),
                "rule_hits": [
                    f"transport_mode={first['transport_mode']}",
                    f"cargo_nature={cargo_nature}",
                    f"target_country={target_country or 'UNKNOWN'}",
                    f"tax_requirement={tax_requirement}",
                    f"destination_keyword={first.get('destination_keyword') or 'ANY'}",
                    f"tier=[{row['min_weight']},{row.get('max_weight')}]",
                ],
                "box_details": box_details,
            }
            if best_candidate is None or Decimal(candidate["total_price"]) < Decimal(best_candidate["total_price"]):
                best_candidate = candidate

        if tier_passed and best_candidate is not None:
            recommended.append(best_candidate)
        else:
            rejected.append(build_rejected_channel(first, "no_tier_matched", tier_fail_reasons))

    recommended.sort(
        key=lambda x: (
            Decimal(x["total_price"]),
            x.get("transit_days", {}).get("min") if x.get("transit_days", {}).get("min") is not None else 10**9,
        )
    )

    return {
        "recommended": recommended[:top_n],
        "rejected": rejected,
    }


def summarize_rejected(rejected: list[dict[str, Any]]) -> dict[str, int]:
    counter = Counter()
    for item in rejected:
        reason = str(item.get("reason") or "unknown")
        base_reason = reason.split(":", 1)[0]
        counter[base_reason] += 1
    return dict(counter)


def parse_args() -> argparse.Namespace:
    default_boxes = PRICING_ENGINE_DEFAULTS.get("boxes")
    boxes_json_default = json.dumps(default_boxes, ensure_ascii=False) if default_boxes is not None else None

    parser = argparse.ArgumentParser(description="MVP pricing engine with explainable recommendation output")
    parser.add_argument(
        "--dsn",
        default=os.getenv("PG_DSN", CONFIG_PG_DSN),
        help="PostgreSQL DSN or PG_DSN (env has priority, then config.py)",
    )
    parser.add_argument("--create-schema", action="store_true", help="Create MVP pricing tables if missing")
    parser.add_argument(
        "--schema-only",
        action="store_true",
        help="Only initialize schema and exit (used with --create-schema)",
    )
    parser.add_argument(
        "--warehouse",
        default=PRICING_ENGINE_DEFAULTS.get("warehouse"),
        help="Target FBA warehouse code/name",
    )
    parser.add_argument(
        "--address",
        default=PRICING_ENGINE_DEFAULTS.get("address"),
        help="Target full destination address",
    )
    parser.add_argument(
        "--transport-mode",
        choices=("air", "sea", "rail"),
        default=PRICING_ENGINE_DEFAULTS.get("transport_mode"),
        help="Transport mode",
    )
    parser.add_argument(
        "--cargo-nature",
        default=PRICING_ENGINE_DEFAULTS.get("cargo_nature"),
        help="Cargo nature, e.g. general/battery",
    )
    parser.add_argument(
        "--tax-included",
        choices=("any", "required", "not_required"),
        default=PRICING_ENGINE_DEFAULTS.get("tax_included", "any"),
        help="Tax requirement filter: any, required(包税), not_required(不包税)",
    )
    parser.add_argument(
        "--target-country",
        default=PRICING_ENGINE_DEFAULTS.get("target_country"),
        help="Destination country code override, e.g. US/CA/UK/EU/AU",
    )
    parser.add_argument(
        "--boxes-json",
        default=boxes_json_default,
        help='JSON array: [{"gross_weight": 12, "length": 40, "width": 30, "height": 20}]',
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=int(PRICING_ENGINE_DEFAULTS.get("top_n", 3)),
        help="Number of recommendations to return",
    )
    parser.add_argument(
        "--allow-any-destination",
        action="store_true",
        help="Allow channels with broad/unknown destination scope",
    )
    parser.add_argument(
        "--verbose-rejected",
        action="store_true",
        help="Output full rejected channel list. Default outputs summary only.",
    )
    parser.add_argument(
        "--max-rejected-details",
        type=int,
        default=3,
        help="When --verbose-rejected is set, cap no_tier details per channel",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.dsn:
        raise SystemExit("Missing DSN. Provide --dsn or set PG_DSN.")

    with connect(args.dsn) as conn:
        if args.create_schema:
            ensure_mvp_tables(conn)
            if args.schema_only:
                print("Schema initialized.")
                return

    required = {
        "warehouse": args.warehouse,
        "address": args.address,
        "transport_mode": args.transport_mode,
        "cargo_nature": args.cargo_nature,
        "boxes_json": args.boxes_json,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise SystemExit(f"Missing required args for recommendation: {', '.join(missing)}")

    boxes = parse_boxes_json(args.boxes_json)
    destination = f"{args.warehouse} {args.address}".strip()
    target_country = (args.target_country or infer_target_country(destination) or "").upper() or None

    with connect(args.dsn) as conn:
        rows = fetch_candidates(conn, args.transport_mode)
        if not rows:
            raise SystemExit(
                "No active channels/tiers found for this transport mode. "
                "Use --create-schema first and insert pricing channels + tiers."
            )

        result = recommend(
            rows=rows,
            destination=destination,
            target_country=target_country,
            cargo_nature=args.cargo_nature,
            boxes=boxes,
            top_n=max(1, args.top_n),
            allow_any_destination=args.allow_any_destination,
            tax_requirement=args.tax_included,
        )
    rejected = result.get("rejected", [])
    output: dict[str, Any] = {
        "target_country": target_country,
        "tax_requirement": args.tax_included,
        "recommended": result.get("recommended", []),
        "rejected_count": len(rejected),
        "rejected_summary": summarize_rejected(rejected),
    }

    if args.verbose_rejected:
        max_details = max(0, int(args.max_rejected_details))
        trimmed = []
        for item in rejected:
            cloned = dict(item)
            details = cloned.get("details")
            if isinstance(details, list) and len(details) > max_details:
                cloned["details"] = details[:max_details]
                cloned["details_omitted"] = len(details) - max_details
            trimmed.append(cloned)
        output["rejected"] = trimmed

    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
