from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from logistics_ingest.domain.provider_catalog import ALLOWED_SOURCE_COMPANIES
from logistics_ingest.infra.excel.workbook_reader import read_workbook_preview
from logistics_ingest.infra.fs.file_ops import compute_file_hash
from logistics_ingest.infra.fs.state_repo import (
    confirm_file_settled,
    iso_now,
    load_state,
    mark_file_seen,
    save_state,
)
from logistics_ingest.infra.fs.file_scanner import (
    determine_watch_dirs as fs_determine_watch_dirs,
    iter_candidate_files as fs_iter_candidate_files,
)
from logistics_ingest.infra.llm.filename_classifier import classify_filename
from logistics_ingest.infra.manifest.manifest_repo import (
    current_snapshot_filenames as repo_current_snapshot_filenames,
)
from logistics_ingest.infra.manifest.snapshot_repo import apply_snapshot_update
from logistics_ingest.shared.logging import configure_console_encoding
from logistics_ingest.shared.settings import (
    default_archive_root,
    default_out_dir,
    default_update_dir,
    load_settings,
    project_root,
)

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

FILENAME_SCAN_KEYWORDS = (*ALLOWED_SOURCE_COMPANIES, "报价")


def filename_has_scan_keyword(filename: str) -> bool:
    name = str(filename or "")
    return any(keyword in name for keyword in FILENAME_SCAN_KEYWORDS)


def parse_args() -> argparse.Namespace:
    settings = load_settings()
    parser = argparse.ArgumentParser(
        description="Watch incoming monthly folders, use LLM filename triage, update data/update_excel/, and optionally trigger the publish pipeline."
    )
    parser.add_argument(
        "--source-root",
        type=Path,
        default=settings.source_root,
        help="Root directory that contains monthly folders like 2026-03, 2026-04",
    )
    parser.add_argument(
        "--update-dir",
        type=Path,
        default=default_update_dir(),
        help="Current active snapshot directory (default: ./data/update_excel)",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=default_update_dir() / "manifest.csv",
        help="Manifest file to rewrite after accepted files are imported",
    )
    parser.add_argument(
        "--archive-root",
        type=Path,
        default=default_archive_root() / "auto_ingest",
        help="Archive directory for replaced snapshot files",
    )
    parser.add_argument(
        "--state-file",
        type=Path,
        default=default_out_dir() / "auto_ingest_state.json",
        help="JSON state file used to avoid reprocessing the same files",
    )
    parser.add_argument(
        "--dsn",
        default=settings.pg_dsn,
        help="PostgreSQL DSN passed through when triggering the pipeline (env/.env: PG_DSN)",
    )
    parser.add_argument(
        "--llm-api-key",
        default=settings.deepseek_api_key,
        help="DeepSeek API key for filename triage (env/.env: DEEPSEEK_API_KEY)",
    )
    parser.add_argument(
        "--llm-model",
        default="deepseek-chat",
        help="DeepSeek model used for filename triage",
    )
    parser.add_argument(
        "--llm-confidence-threshold",
        type=float,
        default=0.8,
        help="Minimum confidence required for an automatic accept decision",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=20.0,
        help="Polling interval in seconds for watch mode",
    )
    parser.add_argument(
        "--stability-seconds",
        type=float,
        default=2.0,
        help="Seconds to wait before a second size/mtime check to confirm the file has settled",
    )
    parser.add_argument(
        "--debounce-seconds",
        type=float,
        default=120.0,
        help="Quiet period before auto-triggering the publish pipeline after accepted files",
    )
    parser.add_argument(
        "--watch-previous-month",
        dest="watch_previous_month",
        action="store_true",
        help="Also watch the previous month's folder if it exists (default: enabled)",
    )
    parser.add_argument(
        "--no-watch-previous-month",
        dest="watch_previous_month",
        action="store_false",
        help="Only watch the current month's folder",
    )
    parser.add_argument(
        "--run-pipeline",
        dest="run_pipeline",
        action="store_true",
        help="Trigger pipeline after accepted files (default: enabled)",
    )
    parser.add_argument(
        "--no-run-pipeline",
        dest="run_pipeline",
        action="store_false",
        help="Do not trigger the publish pipeline automatically",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Scan once, process stable files, optionally run pipeline once, then exit",
    )
    parser.set_defaults(watch_previous_month=True, run_pipeline=True)
    return parser.parse_args()


def determine_watch_dirs(source_root: Path, watch_previous_month: bool) -> list[Path]:
    return fs_determine_watch_dirs(source_root, watch_previous_month)


def iter_candidate_files(watch_dirs: list[Path]) -> list[Path]:
    return fs_iter_candidate_files(watch_dirs, filename_filter=filename_has_scan_keyword)


def build_llm_client(api_key: str) -> Any:
    if OpenAI is None:
        raise SystemExit("openai package is missing. Install it before using the auto-ingest watcher.")
    if not api_key:
        raise SystemExit("Missing LLM API key. Set DEEPSEEK_API_KEY or pass --llm-api-key.")
    return OpenAI(api_key=api_key, base_url="https://api.deepseek.com")


def current_snapshot_filenames(update_dir: Path, manifest_path: Path) -> list[str]:
    return repo_current_snapshot_filenames(update_dir, manifest_path)


def run_publish_pipeline(args: argparse.Namespace) -> None:
    if not args.dsn:
        raise SystemExit("Missing DSN. Provide --dsn or set PG_DSN if you want to auto-run the pipeline.")
    root = project_root()

    cmd = [
        sys.executable,
        "-m",
        "logistics_ingest.cli.run_pipeline",
        "--manifest",
        str(args.manifest.resolve()),
        "--manifest-required",
        "--dsn",
        args.dsn,
        "--llm-model",
        args.llm_model,
    ]
    if args.llm_api_key:
        cmd.extend(["--llm-api-key", args.llm_api_key])

    env = os.environ.copy()
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")
    proc = subprocess.run(
        cmd,
        cwd=str(root),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    print(proc.stdout, end="")
    if proc.returncode != 0:
        raise RuntimeError(f"pipeline run failed with exit code {proc.returncode}")


def process_file(
    path: Path,
    record: dict[str, Any],
    state: dict[str, Any],
    args: argparse.Namespace,
    client: Any,
) -> tuple[bool, str]:
    file_hash = compute_file_hash(path)
    record["file_hash"] = file_hash

    existing_hash = state["processed_hashes"].get(file_hash)
    if existing_hash and str(existing_hash.get("status") or "") in {"accepted", "ignored", "review"}:
        record["status"] = "duplicate"
        record["last_processed_signature"] = record.get("signature", "")
        record["last_processed_at"] = iso_now()
        record["decision"] = {
            "decision": "duplicate",
            "provider": str(existing_hash.get("provider") or ""),
            "reason": "same_hash_already_processed",
        }
        return False, "duplicate"

    workbook_preview = load_workbook_preview(path)
    current_snapshot = current_snapshot_filenames(args.update_dir, args.manifest)
    decision = classify_filename(
        client=client,
        model=args.llm_model,
        path=path,
        workbook_preview=workbook_preview,
        current_snapshot=current_snapshot,
        confidence_threshold=float(args.llm_confidence_threshold),
    )

    record["decision"] = {
        "decision": decision.decision,
        "provider": decision.provider,
        "feed_family": decision.feed_family,
        "replaces_filename": decision.replaces_filename,
        "confidence": decision.confidence,
        "reason": decision.reason,
    }
    record["last_processed_signature"] = record.get("signature", "")
    record["last_processed_at"] = iso_now()

    if decision.decision != "accept":
        record["status"] = decision.decision
        state["processed_hashes"][file_hash] = {
            "status": decision.decision,
            "provider": decision.provider,
            "filename": path.name,
            "processed_at": record["last_processed_at"],
        }
        return False, decision.decision

    update_result = apply_snapshot_update(
        incoming_path=path,
        decision=decision,
        update_dir=args.update_dir,
        manifest_path=args.manifest,
        archive_root=args.archive_root,
    )
    record["status"] = "accepted"
    record["update_result"] = update_result
    state["processed_hashes"][file_hash] = {
        "status": "accepted",
        "provider": decision.provider,
        "filename": path.name,
        "processed_at": record["last_processed_at"],
        "replaces_filename": decision.replaces_filename,
    }
    return True, update_result.get("status", "accepted")


def scan_once(
    args: argparse.Namespace,
    client: Any,
) -> tuple[int, int]:
    state = load_state(args.state_file)
    now_ts = time.time()
    watch_dirs = determine_watch_dirs(args.source_root, bool(args.watch_previous_month))
    if not watch_dirs:
        print(f"No watch folders found under {args.source_root}")
        save_state(args.state_file, state)
        return 0, 0

    print("Watching folders:")
    for path in watch_dirs:
        print(f"  - {path}")

    accepted_count = 0
    scanned_count = 0
    recheck_seconds = float(args.stability_seconds)
    for path in iter_candidate_files(watch_dirs):
        scanned_count += 1
        record = mark_file_seen(path, state, now_ts)

        if str(record.get("last_processed_signature") or "") == str(record.get("signature") or ""):
            continue

        if not confirm_file_settled(path, record, recheck_seconds):
            continue

        try:
            accepted, result = process_file(path, record, state, args, client)
            print(f"[{record.get('status','?')}] {path.name} -> {result}")
            if accepted:
                accepted_count += 1
        except Exception as exc:
            record["status"] = "failed"
            record["last_error"] = str(exc)
            record["last_processed_signature"] = record.get("signature", "")
            record["last_processed_at"] = iso_now()
            print(f"[failed] {path.name} -> {exc}")

    save_state(args.state_file, state)
    return scanned_count, accepted_count


def main() -> None:
    configure_console_encoding()
    args = parse_args()
    args.source_root = args.source_root.resolve()
    args.update_dir = args.update_dir.resolve()
    args.manifest = args.manifest.resolve()
    args.archive_root = args.archive_root.resolve()
    args.state_file = args.state_file.resolve()

    threshold = max(0.0, min(1.0, float(args.llm_confidence_threshold)))
    args.llm_confidence_threshold = threshold
    client = build_llm_client(args.llm_api_key)
    pending_pipeline = False
    next_pipeline_at: float | None = None

    while True:
        scanned_count, accepted_count = scan_once(args, client)
        print(f"Scan complete: scanned={scanned_count}, accepted={accepted_count}")

        if accepted_count > 0 and args.run_pipeline:
            if args.once:
                run_publish_pipeline(args)
            else:
                pending_pipeline = True
                next_pipeline_at = time.time() + float(args.debounce_seconds)
                print(
                    "Pipeline run scheduled "
                    f"in {float(args.debounce_seconds):.0f}s after quiet period."
                )

        if args.once:
            break

        now_ts = time.time()
        if pending_pipeline and next_pipeline_at is not None and now_ts >= next_pipeline_at:
            try:
                run_publish_pipeline(args)
                pending_pipeline = False
                next_pipeline_at = None
            except Exception as exc:
                print(f"Pipeline trigger failed: {exc}")
                next_pipeline_at = time.time() + float(args.debounce_seconds)

        time.sleep(max(1.0, float(args.poll_seconds)))


def run() -> None:
    main()


if __name__ == "__main__":
    main()
