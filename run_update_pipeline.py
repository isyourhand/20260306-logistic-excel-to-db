from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from psycopg import connect

from export_excel_grids import EXCEL_PATTERNS, safe_name
from normalize_rates_to_pg import ALLOWED_SOURCE_COMPANIES, infer_canonical_company_name

try:
    from config import DEEPSEEK_API_KEY as CONFIG_DEEPSEEK_API_KEY
    from config import PG_DSN as CONFIG_PG_DSN
except Exception:
    CONFIG_PG_DSN = ""
    CONFIG_DEEPSEEK_API_KEY = ""

TRUE_VALUES = {"1", "true", "yes", "y", "on"}


def configure_console_encoding() -> None:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


@dataclass
class InputWorkbook:
    filename: str
    path: Path
    expect_channels: bool = True
    provider: str = ""


def parse_bool(value: str, default: bool) -> bool:
    text = (value or "").strip().lower()
    if not text:
        return default
    return text in TRUE_VALUES


def iter_excel_files(input_dir: Path) -> list[Path]:
    files: dict[str, Path] = {}
    for pattern in EXCEL_PATTERNS:
        for p in input_dir.glob(pattern):
            if p.is_file() and not p.name.startswith("~$"):
                files[p.name] = p
    return sorted(files.values(), key=lambda p: p.name.lower())


def read_manifest(manifest_path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with manifest_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return rows
        for line_no, row in enumerate(reader, start=2):
            filename = str(row.get("filename") or "").strip()
            if not filename:
                continue
            enabled = parse_bool(str(row.get("enabled") or ""), True)
            expect_channels = parse_bool(str(row.get("expect_channels") or ""), True)
            rows.append(
                {
                    "line_no": line_no,
                    "filename": filename,
                    "enabled": enabled,
                    "expect_channels": expect_channels,
                }
            )
    return rows


def resolve_inputs(
    input_dir: Path,
    manifest_path: Path | None,
    manifest_required: bool,
) -> tuple[list[InputWorkbook], dict[str, Any]]:
    files = iter_excel_files(input_dir)
    by_name = {p.name: p for p in files}

    diagnostics: dict[str, Any] = {
        "manifest_used": False,
        "manifest_path": str(manifest_path) if manifest_path else "",
        "manifest_missing_entries": [],
        "manifest_extra_files": [],
    }

    if manifest_path is not None:
        if not manifest_path.exists():
            if manifest_required:
                raise SystemExit(f"Manifest is required but not found: {manifest_path}")
            return [InputWorkbook(filename=p.name, path=p, expect_channels=True) for p in files], diagnostics

        manifest_rows = read_manifest(manifest_path)
        diagnostics["manifest_used"] = True

        selected: list[InputWorkbook] = []
        selected_names: set[str] = set()
        for row in manifest_rows:
            if not row["enabled"]:
                continue
            name = row["filename"]
            p = by_name.get(name)
            if p is None:
                diagnostics["manifest_missing_entries"].append(
                    {"line_no": row["line_no"], "filename": name}
                )
                continue
            if name in selected_names:
                continue
            selected_names.add(name)
            selected.append(
                InputWorkbook(
                    filename=name,
                    path=p,
                    expect_channels=bool(row["expect_channels"]),
                )
            )

        diagnostics["manifest_extra_files"] = sorted([p.name for p in files if p.name not in selected_names])
        return selected, diagnostics

    return [InputWorkbook(filename=p.name, path=p, expect_channels=True) for p in files], diagnostics


def validate_input_providers(selected: list[InputWorkbook]) -> tuple[list[InputWorkbook], list[dict[str, str]]]:
    validated: list[InputWorkbook] = []
    unknown: list[dict[str, str]] = []
    for item in selected:
        provider = infer_canonical_company_name(item.path.stem) or infer_canonical_company_name(item.filename)
        if provider is None:
            unknown.append(
                {
                    "filename": item.filename,
                    "workbook_stem": item.path.stem,
                }
            )
            continue
        validated.append(
            InputWorkbook(
                filename=item.filename,
                path=item.path,
                expect_channels=item.expect_channels,
                provider=provider,
            )
        )
    return validated, unknown


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


def csv_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)
    if not rows:
        return 0
    # minus header row
    return max(0, len(rows) - 1)


def collect_publish_metrics(dsn: str, batch_id: str) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "raw_workbook_count": 0,
        "raw_sheet_count": 0,
        "raw_workbooks": [],
        "channels_total_for_batch_workbooks": 0,
        "tiers_total_for_batch_workbooks": 0,
        "channels_by_workbook": {},
    }
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT workbook_name, COUNT(*) AS sheet_count
                FROM raw_sheet_meta
                WHERE import_batch_id = %s
                GROUP BY workbook_name
                ORDER BY workbook_name
                """,
                (batch_id,),
            )
            rows = cur.fetchall()
            metrics["raw_workbook_count"] = len(rows)
            metrics["raw_workbooks"] = [{"workbook_name": x[0], "sheet_count": int(x[1])} for x in rows]
            metrics["raw_sheet_count"] = sum(int(x[1]) for x in rows)

            workbooks = [x[0] for x in rows]
            if workbooks:
                cur.execute(
                    """
                    SELECT
                      c.source_workbook,
                      COUNT(DISTINCT c.id) AS channels,
                      COALESCE(COUNT(t.id), 0) AS tiers
                    FROM pricing_channels c
                    LEFT JOIN pricing_rate_tiers t ON t.channel_id = c.id AND t.active = TRUE
                    WHERE c.active = TRUE
                      AND c.source_workbook = ANY(%s)
                    GROUP BY c.source_workbook
                    ORDER BY c.source_workbook
                    """,
                    (workbooks,),
                )
                crows = cur.fetchall()
                channels_by_workbook: dict[str, dict[str, int]] = {}
                for wb, channels, tiers in crows:
                    channels_by_workbook[str(wb)] = {
                        "channels": int(channels),
                        "tiers": int(tiers),
                    }
                metrics["channels_by_workbook"] = channels_by_workbook
                metrics["channels_total_for_batch_workbooks"] = sum(v["channels"] for v in channels_by_workbook.values())
                metrics["tiers_total_for_batch_workbooks"] = sum(v["tiers"] for v in channels_by_workbook.values())
    return metrics


def build_quality_report(
    selected: list[InputWorkbook],
    metrics: dict[str, Any],
    parser_flags: int,
    min_channels: int,
    min_tiers: int,
    require_channels_for_expected: bool,
    max_parser_flags: int | None,
) -> dict[str, Any]:
    expected_workbooks = [safe_name(x.path.stem) for x in selected if x.expect_channels]
    channels_by_workbook: dict[str, dict[str, int]] = metrics.get("channels_by_workbook", {})

    failures: list[str] = []
    warnings: list[str] = []

    channels_total = int(metrics.get("channels_total_for_batch_workbooks", 0))
    tiers_total = int(metrics.get("tiers_total_for_batch_workbooks", 0))
    if channels_total < min_channels:
        failures.append(f"channels_total<{min_channels} (actual={channels_total})")
    if tiers_total < min_tiers:
        failures.append(f"tiers_total<{min_tiers} (actual={tiers_total})")

    missing_expected = sorted([wb for wb in expected_workbooks if channels_by_workbook.get(wb, {}).get("channels", 0) == 0])
    if missing_expected:
        msg = f"expected workbook has no channels: {', '.join(missing_expected)}"
        if require_channels_for_expected:
            failures.append(msg)
        else:
            warnings.append(msg)

    if max_parser_flags is not None and parser_flags > max_parser_flags:
        failures.append(
            f"parser_flags>{max_parser_flags} (actual={parser_flags})"
        )

    return {
        "pass": len(failures) == 0,
        "failures": failures,
        "warnings": warnings,
        "parser_flags": parser_flags,
        "expected_workbooks": expected_workbooks,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run standardized logistics data update pipeline (export -> import -> normalize -> quality gate)")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path.cwd() / "update_excel",
        help="Directory containing latest workbook snapshots (default: ./update_excel)",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path.cwd() / "out" / "update_runs",
        help="Root directory for run artifacts/logs (default: ./out/update_runs)",
    )
    parser.add_argument(
        "--dsn",
        default=os.getenv("PG_DSN", CONFIG_PG_DSN),
        help="PostgreSQL DSN (arg > env PG_DSN > config.py)",
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
        help="Pass --data-only to export_excel_grids.py",
    )
    parser.add_argument(
        "--bounds-mode",
        choices=("effective", "strict"),
        default="effective",
        help="Pass-through to export_excel_grids.py",
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
        default=os.getenv("DEEPSEEK_API_KEY", CONFIG_DEEPSEEK_API_KEY),
        help="DeepSeek API key for divisor checks",
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

    script_dir = Path(__file__).resolve().parent
    input_dir = args.input_dir.resolve()
    if not input_dir.exists():
        raise SystemExit(f"Input dir not found: {input_dir}")

    manifest_path = args.manifest.resolve() if args.manifest else None
    selected, diagnostics = resolve_inputs(input_dir, manifest_path, args.manifest_required)
    if not selected:
        raise SystemExit("No enabled Excel files selected for this run.")
    selected, unknown_providers = validate_input_providers(selected)
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
            "export_excel_grids.py",
            "--input-dir",
            str(stage_input_dir),
            "--output-dir",
            str(export_out_dir),
            "--bounds-mode",
            args.bounds_mode,
        ]
        if args.data_only:
            cmd_export.append("--data-only")
        run_command(cmd_export, cwd=script_dir, log_path=logs_dir / "01_export.log")

        cmd_import = [
            sys.executable,
            "import_to_pg.py",
            "--output-dir",
            str(export_out_dir),
            "--dsn",
            args.dsn,
            "--batch-id",
            batch_uuid,
        ]
        run_command(cmd_import, cwd=script_dir, log_path=logs_dir / "02_import.log")

        cmd_norm = [
            sys.executable,
            "normalize_rates_to_pg.py",
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
        run_command(cmd_norm, cwd=script_dir, log_path=logs_dir / "03_normalize.log")
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


if __name__ == "__main__":
    main()
