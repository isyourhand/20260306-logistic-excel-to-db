from __future__ import annotations

import csv
import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


@dataclass
class SheetBundle:
    workbook_name: str
    sheet_dir_name: str
    sheet_title: str
    merged_ranges: list[str]
    grid_filled_path: Path
    relative_dir: str


def find_sheet_bundles(output_dir: Path) -> Iterator[SheetBundle]:
    for meta_path in output_dir.rglob("merged_ranges.json"):
        sheet_dir = meta_path.parent
        workbook_dir = sheet_dir.parent
        grid_filled_path = sheet_dir / "grid_filled.csv"
        if not grid_filled_path.exists():
            print(f"Skip missing grid_filled.csv: {sheet_dir}")
            continue

        payload = json.loads(meta_path.read_text(encoding="utf-8"))
        sheet_title = str(payload.get("sheet") or sheet_dir.name)
        merged_ranges = payload.get("merged_ranges") or []
        if not isinstance(merged_ranges, list):
            merged_ranges = []

        yield SheetBundle(
            workbook_name=workbook_dir.name,
            sheet_dir_name=sheet_dir.name,
            sheet_title=sheet_title,
            merged_ranges=[str(x) for x in merged_ranges],
            grid_filled_path=grid_filled_path,
            relative_dir=str(sheet_dir.relative_to(output_dir)),
        )


def read_grid_rows(path: Path) -> tuple[list[list[str]], int]:
    rows: list[list[str]] = []
    max_cols = 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            rows.append(row)
            if len(row) > max_cols:
                max_cols = len(row)
    return rows, max_cols


def ensure_tables(conn) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS raw_sheet_meta (
        id BIGSERIAL PRIMARY KEY,
        import_batch_id UUID NOT NULL,
        workbook_name TEXT NOT NULL,
        sheet_name TEXT NOT NULL,
        sheet_dir_name TEXT NOT NULL,
        relative_dir TEXT NOT NULL,
        source_grid_path TEXT NOT NULL,
        merged_ranges JSONB NOT NULL DEFAULT '[]'::jsonb,
        row_count INTEGER NOT NULL,
        col_count INTEGER NOT NULL,
        imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(import_batch_id, workbook_name, sheet_name)
    );

    CREATE TABLE IF NOT EXISTS raw_sheet_rows (
        id BIGSERIAL PRIMARY KEY,
        meta_id BIGINT NOT NULL REFERENCES raw_sheet_meta(id) ON DELETE CASCADE,
        row_index INTEGER NOT NULL,
        row_data JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(meta_id, row_index)
    );

    CREATE INDEX IF NOT EXISTS idx_raw_sheet_meta_batch
        ON raw_sheet_meta(import_batch_id);

    CREATE INDEX IF NOT EXISTS idx_raw_sheet_meta_workbook_sheet
        ON raw_sheet_meta(workbook_name, sheet_name);

    CREATE INDEX IF NOT EXISTS idx_raw_sheet_rows_meta_row
        ON raw_sheet_rows(meta_id, row_index);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def insert_sheet(conn, batch_id: uuid.UUID, bundle: SheetBundle, rows: list[list[str]], col_count: int) -> None:
    row_count = len(rows)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw_sheet_meta (
                import_batch_id,
                workbook_name,
                sheet_name,
                sheet_dir_name,
                relative_dir,
                source_grid_path,
                merged_ranges,
                row_count,
                col_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
            RETURNING id
            """,
            (
                str(batch_id),
                bundle.workbook_name,
                bundle.sheet_title,
                bundle.sheet_dir_name,
                bundle.relative_dir,
                str(bundle.grid_filled_path),
                json.dumps(bundle.merged_ranges, ensure_ascii=False),
                row_count,
                col_count,
            ),
        )
        meta_id = cur.fetchone()[0]

        if rows:
            cur.executemany(
                """
                INSERT INTO raw_sheet_rows (meta_id, row_index, row_data)
                VALUES (%s, %s, %s::jsonb)
                """,
                [
                    (
                        meta_id,
                        idx,
                        json.dumps(row, ensure_ascii=False),
                    )
                    for idx, row in enumerate(rows, start=1)
                ],
            )

    conn.commit()


__all__ = [
    "SheetBundle",
    "find_sheet_bundles",
    "read_grid_rows",
    "ensure_tables",
    "insert_sheet",
    "list_sheet_metas",
    "load_rows",
    "latest_batch_id",
]


def list_sheet_metas(conn, batch_id: str | None):
    where = ""
    params: tuple[str, ...] = ()
    if batch_id:
        where = "WHERE import_batch_id = %s"
        params = (batch_id,)

    sql = f"""
    SELECT id, workbook_name, sheet_name
    FROM raw_sheet_meta
    {where}
    ORDER BY id
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def load_rows(conn, meta_id: int) -> list[list[str]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT row_data
            FROM raw_sheet_rows
            WHERE meta_id = %s
            ORDER BY row_index
            """,
            (meta_id,),
        )
        return [list(r[0]) for r in cur.fetchall()]


def latest_batch_id(conn) -> str | None:
    with conn.cursor() as cur:
        cur.execute("SELECT import_batch_id::text FROM raw_sheet_meta ORDER BY imported_at DESC LIMIT 1")
        row = cur.fetchone()
        return row[0] if row else None
