from __future__ import annotations

import argparse
import uuid
from pathlib import Path

from psycopg import connect

from logistics_ingest.infra.db.raw_repo import ensure_tables, find_sheet_bundles, insert_sheet, read_grid_rows
from logistics_ingest.shared.settings import default_out_dir, load_settings


def parse_args() -> argparse.Namespace:
    settings = load_settings()
    parser = argparse.ArgumentParser(
        description="Import grid_filled.csv and merged_ranges.json into PostgreSQL raw tables"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=default_out_dir(),
        help="Root directory produced by export step (default: ./data/out)",
    )
    parser.add_argument(
        "--dsn",
        default=settings.pg_dsn,
        help="PostgreSQL DSN string. Defaults to PG_DSN from env/.env.",
    )
    parser.add_argument(
        "--batch-id",
        default="",
        help="Optional import batch UUID. If omitted, a new UUID is generated",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.dsn:
        raise SystemExit("Missing DSN. Provide --dsn or set PG_DSN.")

    output_dir: Path = args.output_dir.resolve()
    if not output_dir.exists():
        raise SystemExit(f"Output dir not found: {output_dir}")

    batch_id = uuid.UUID(args.batch_id) if args.batch_id else uuid.uuid4()

    bundles = list(find_sheet_bundles(output_dir))
    if not bundles:
        print(f"No sheet bundles found under: {output_dir}")
        return

    print(f"Import batch : {batch_id}")
    print(f"Output dir   : {output_dir}")
    print(f"Sheet bundles: {len(bundles)}")

    with connect(args.dsn) as conn:
        ensure_tables(conn)

        for idx, bundle in enumerate(bundles, start=1):
            rows, col_count = read_grid_rows(bundle.grid_filled_path)
            print(
                f"[{idx}/{len(bundles)}] {bundle.workbook_name} / {bundle.sheet_title} "
                f"rows={len(rows)} cols={col_count}"
            )
            insert_sheet(conn, batch_id, bundle, rows, col_count)

    print("Import complete.")


def run() -> None:
    main()


if __name__ == "__main__":
    main()
