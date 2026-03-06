from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from logistics_ingest.app.pipeline_quality import build_quality_report, csv_row_count
from logistics_ingest.domain.models import InputWorkbook
from logistics_ingest.domain.provider_catalog import ALLOWED_SOURCE_COMPANIES
from logistics_ingest.infra.db.pipeline_metrics import collect_publish_metrics
from logistics_ingest.infra.manifest.input_resolver import (
    resolve_inputs as manifest_resolve_inputs,
    validate_input_providers as manifest_validate_input_providers,
)
from logistics_ingest.shared.logging import configure_console_encoding
from logistics_ingest.shared.settings import default_out_dir, default_update_dir, load_settings, project_root

def run_command(cmd: list[str], cwd: Path, log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    rendered = subprocess.list2cmdline(cmd)
    env = os.environ.copy()
    # Ensure child Python processes can print workbook names with non-ASCII chars
    # (e.g. NBSP U+00A0) on Windows shells with legacy code pages.
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")
    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    content = f"$ {rendered}\n\n{proc.stdout}"
    log_path.write_text(content, encoding="utf-8")
    print(proc.stdout, end="")
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {rendered}")


def parse_args() -> argparse.Namespace:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Run standardized logistics data update pipeline (export -> import -> normalize -> quality gate)")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=default_update_dir(),
        help="Directory containing latest workbook snapshots (default: ./data/update_excel)",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=default_out_dir() / "update_runs",
        help="Root directory for run artifacts/logs (default: ./data/out/update_runs)",
    )
    parser.add_argument(
        "--dsn",
        default=settings.pg_dsn,
        help="PostgreSQL DSN (arg > PG_DSN from env/.env)",
    )
    parser.add_argument(
        "--batch-id",
        default="",
        help="Optional batch UUID for raw import; default generates a new UUID",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Optional manifest CSV. If omitted, all Excel files in input-dir are used.",
    )
    parser.add_argument(
        "--manifest-required",
        action="store_true",
        help="Fail if manifest path is missing",
    )
    parser.add_argument(
        "--data-only",
        action="store_true",
        help="Pass --data-only to export step",
    )
    parser.add_argument(
        "--bounds-mode",
        choices=("effective", "strict"),
        default="effective",
        help="Pass-through to export step",
    )
    parser.add_argument(
        "--truncate",
        dest="truncate",
        action="store_true",
        help="Truncate normalized pricing tables before loading (default: enabled)",
    )
    parser.add_argument(
        "--no-truncate",
        dest="truncate",
        action="store_false",
        help="Disable truncation for normalization",
    )
    parser.add_argument(
        "--llm-divisor-check",
        dest="llm_divisor_check",
        action="store_true",
        help="Enable LLM divisor applicability check for normalization (default: enabled)",
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
        help="DeepSeek API key for divisor checks (env/.env: DEEPSEEK_API_KEY)",
    )
    parser.add_argument(
        "--llm-model",
        default="deepseek-chat",
        help="DeepSeek model for divisor checks",
    )
    parser.add_argument(
        "--llm-divisor-confidence",
        type=float,
        default=0.8,
        help="Minimum confidence threshold for LLM divisor decision",
    )
    parser.add_argument(
        "--min-channels",
        type=int,
        default=1,
        help="Quality gate: minimum total active channels for this batch's workbooks",
    )
    parser.add_argument(
        "--min-tiers",
        type=int,
        default=1,
        help="Quality gate: minimum total active tiers for this batch's workbooks",
    )
    parser.add_argument(
        "--max-parser-flags",
        type=int,
        default=-1,
        help="Quality gate: max parser flags allowed; -1 disables this gate",
    )
    parser.add_argument(
        "--no-require-channels-per-workbook",
        action="store_true",
        help="Do not fail when an expected workbook yields zero channels",
    )
    parser.set_defaults(truncate=True, llm_divisor_check=True)
    return parser.parse_args()


def main() -> None:
    configure_console_encoding()
    args = parse_args()
    if not args.dsn:
        raise SystemExit("Missing DSN. Provide --dsn or set PG_DSN.")

    root = project_root()
    input_dir = args.input_dir.resolve()
    if not input_dir.exists():
        raise SystemExit(f"Input dir not found: {input_dir}")

    manifest_path = args.manifest.resolve() if args.manifest else None
    selected, diagnostics = manifest_resolve_inputs(input_dir, manifest_path, args.manifest_required)
    if not selected:
        raise SystemExit("No enabled Excel files selected for this run.")
    selected, unknown_providers = manifest_validate_input_providers(selected)
    diagnostics["provider_allowlist"] = list(ALLOWED_SOURCE_COMPANIES)
    diagnostics["provider_unknown_files"] = unknown_providers
    if unknown_providers:
        names = ", ".join(item["filename"] for item in unknown_providers)
        allowed = ", ".join(ALLOWED_SOURCE_COMPANIES)
        raise SystemExit(
            "Unknown provider workbook(s): "
            f"{names}. Allowed providers: {allowed}."
        )

    batch_id = args.batch_id.strip() or str(uuid.uuid4())
    try:
        batch_uuid = str(uuid.UUID(batch_id))
    except ValueError as exc:
        raise SystemExit(f"Invalid batch id: {batch_id}") from exc

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = args.output_root.resolve() / f"batch_{ts}_{batch_uuid[:8]}"
    stage_input_dir = run_dir / "input"
    export_out_dir = run_dir / "exported"
    logs_dir = run_dir / "logs"
    audit_csv = run_dir / "parser_audit.csv"
    summary_json = run_dir / "summary.json"

    run_dir.mkdir(parents=True, exist_ok=True)
    stage_input_dir.mkdir(parents=True, exist_ok=True)

    for item in selected:
        shutil.copy2(item.path, stage_input_dir / item.filename)

    pipeline_meta = {
        "run_dir": str(run_dir),
        "batch_id": batch_uuid,
        "selected_files": [
            {
                "filename": x.filename,
                "provider": x.provider,
                "expect_channels": x.expect_channels,
                "source_path": str(x.path),
            }
            for x in selected
        ],
        "manifest_diagnostics": diagnostics,
    }
    (run_dir / "pipeline_meta.json").write_text(
        json.dumps(pipeline_meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"Run dir    : {run_dir}")
    print(f"Batch id   : {batch_uuid}")
    print(f"Input files: {len(selected)}")
    for x in selected:
        print(f"  - {x.filename} [provider={x.provider}] (expect_channels={x.expect_channels})")

    try:
        cmd_export = [
            sys.executable,
            "-m",
            "logistics_ingest.cli.export_grids",
            "--input-dir",
            str(stage_input_dir),
            "--output-dir",
            str(export_out_dir),
            "--bounds-mode",
            args.bounds_mode,
        ]
        if args.data_only:
            cmd_export.append("--data-only")
        run_command(cmd_export, cwd=root, log_path=logs_dir / "01_export.log")

        cmd_import = [
            sys.executable,
            "-m",
            "logistics_ingest.cli.import_raw",
            "--output-dir",
            str(export_out_dir),
            "--dsn",
            args.dsn,
            "--batch-id",
            batch_uuid,
        ]
        run_command(cmd_import, cwd=root, log_path=logs_dir / "02_import.log")

        cmd_norm = [
            sys.executable,
            "-m",
            "logistics_ingest.cli.normalize_rates",
            "--dsn",
            args.dsn,
            "--batch-id",
            batch_uuid,
            "--audit-csv",
            str(audit_csv),
            "--llm-model",
            args.llm_model,
            "--llm-divisor-confidence",
            str(args.llm_divisor_confidence),
        ]
        if args.truncate:
            cmd_norm.append("--truncate")
        else:
            cmd_norm.append("--no-truncate")
        if args.llm_divisor_check:
            cmd_norm.append("--llm-divisor-check")
            if args.llm_api_key:
                cmd_norm.extend(["--llm-api-key", args.llm_api_key])
        else:
            cmd_norm.append("--no-llm-divisor-check")
        run_command(cmd_norm, cwd=root, log_path=logs_dir / "03_normalize.log")
    except Exception as exc:
        summary = {
            "status": "failed",
            "error": str(exc),
            "run_dir": str(run_dir),
            "batch_id": batch_uuid,
            "manifest_diagnostics": diagnostics,
        }
        summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        raise

    parser_flags = csv_row_count(audit_csv)
    metrics = collect_publish_metrics(args.dsn, batch_uuid)
    quality = build_quality_report(
        selected=selected,
        metrics=metrics,
        parser_flags=parser_flags,
        min_channels=max(0, int(args.min_channels)),
        min_tiers=max(0, int(args.min_tiers)),
        require_channels_for_expected=not bool(args.no_require_channels_per_workbook),
        max_parser_flags=None if int(args.max_parser_flags) < 0 else int(args.max_parser_flags),
    )

    summary = {
        "status": "passed" if quality["pass"] else "failed",
        "batch_id": batch_uuid,
        "run_dir": str(run_dir),
        "selected_count": len(selected),
        "manifest_diagnostics": diagnostics,
        "metrics": metrics,
        "quality_gate": quality,
        "artifacts": {
            "pipeline_meta": str(run_dir / "pipeline_meta.json"),
            "audit_csv": str(audit_csv),
            "summary_json": str(summary_json),
            "export_log": str(logs_dir / "01_export.log"),
            "import_log": str(logs_dir / "02_import.log"),
            "normalize_log": str(logs_dir / "03_normalize.log"),
        },
    }
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\nQuality Gate:")
    print(f"  pass              : {quality['pass']}")
    print(f"  channels_total    : {metrics['channels_total_for_batch_workbooks']}")
    print(f"  tiers_total       : {metrics['tiers_total_for_batch_workbooks']}")
    print(f"  parser_flags      : {quality['parser_flags']}")
    if quality["warnings"]:
        print("  warnings:")
        for w in quality["warnings"]:
            print(f"    - {w}")
    if quality["failures"]:
        print("  failures:")
        for f in quality["failures"]:
            print(f"    - {f}")

    print(f"\nSummary saved: {summary_json}")
    if not quality["pass"]:
        raise SystemExit(2)


def run() -> None:
    main()


if __name__ == "__main__":
    main()
