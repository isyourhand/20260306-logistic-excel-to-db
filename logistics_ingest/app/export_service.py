from __future__ import annotations

import argparse
from pathlib import Path

from logistics_ingest.infra.excel.grid_exporter import EXCEL_PATTERNS, iter_excel_files, process_workbook, safe_name
from logistics_ingest.shared.logging import configure_console_encoding
from logistics_ingest.shared.settings import default_out_dir, default_update_dir

__all__ = ["EXCEL_PATTERNS", "iter_excel_files", "process_workbook", "safe_name", "run", "main"]


def main():
    configure_console_encoding()
    parser = argparse.ArgumentParser(
        description=(
            "Export each sheet in every Excel workbook to structured grids: "
            "grid.csv, merged_ranges.json, and grid_filled.csv"
        )
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=default_update_dir(),
        help="Directory containing Excel files (default: ./data/update_excel)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=default_out_dir(),
        help="Output directory root (default: ./data/out)",
    )
    parser.add_argument(
        "--data-only",
        action="store_true",
        help="Read calculated values for formula cells instead of formula text",
    )
    parser.add_argument(
        "--bounds-mode",
        choices=("effective", "strict"),
        default="effective",
        help=(
            "effective: infer practical bounds from meaningful cells (default); "
            "strict: use worksheet max_row/max_column exactly"
        ),
    )

    args = parser.parse_args()
    input_dir: Path = args.input_dir.resolve()
    output_dir: Path = args.output_dir.resolve()

    excel_files = sorted(set(iter_excel_files(input_dir)), key=lambda p: p.name.lower())
    if not excel_files:
        print(f"No Excel files found in: {input_dir}")
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Input directory : {input_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Found {len(excel_files)} workbook(s).")

    for idx, excel_file in enumerate(excel_files, start=1):
        print(f"[{idx}/{len(excel_files)}] Processing: {excel_file.name}")
        try:
            sheets = process_workbook(
                excel_file,
                output_dir,
                data_only=args.data_only,
                bounds_mode=args.bounds_mode,
            )
            print(f"    Exported sheets: {', '.join(sheets)}")
        except Exception as exc:
            print(f"    Skipped due to error: {exc}")

    print("Done.")


def run() -> None:
    main()


if __name__ == "__main__":
    main()
