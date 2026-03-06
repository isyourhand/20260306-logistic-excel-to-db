from __future__ import annotations

import argparse
import csv
import os
from collections import Counter

from psycopg import connect

from logistics_ingest.app.normalize_parser import ParseStats, parse_sheet_records, should_skip_sheet
from logistics_ingest.domain.models import DivisorLLMConfig, RateRecord, SurchargeRuleRecord
from logistics_ingest.infra.db.pricing_repo import ensure_engine_tables, truncate_engine_tables, upsert_rates
from logistics_ingest.infra.db.raw_repo import latest_batch_id, list_sheet_metas, load_rows
from logistics_ingest.shared.settings import load_settings

try:
    from openai import OpenAI
except Exception:
    OpenAI = None


def parse_args() -> argparse.Namespace:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Normalize raw_sheet_* into pricing engine tables")
    parser.add_argument(
        "--dsn",
        default=settings.pg_dsn,
        help="PostgreSQL DSN (arg > PG_DSN from env/.env)",
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
        default=settings.deepseek_api_key,
        help="DeepSeek API key for divisor scope checks (env/.env: DEEPSEEK_API_KEY)",
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
        raise SystemExit("Missing DSN. Provide --dsn or set PG_DSN.")

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


def run() -> None:
    main()


if __name__ == "__main__":
    main()
