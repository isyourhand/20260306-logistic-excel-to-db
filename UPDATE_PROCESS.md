# Standard Update Process

This project should run as a full-snapshot ETL pipeline for logistics pricing data.

## Directory Convention

1. `update_excel/`
   - Keep only the current active workbook per feed/company.
   - Move replaced files to an external archive location.
2. `archive_excel/root_legacy/`
   - Keep legacy workbooks moved out from project root.
3. Project root (`logistics_excel/`)
   - Keep **no `.xlsx` files** here to avoid accidental mixed imports from default commands.
4. `out/update_runs/`
   - Pipeline run artifacts (logs, audit, summary) are generated here.

## Optional Manifest

Use a manifest CSV to control which files are included in a run.

1. Copy `update_manifest.template.csv` to `update_excel/manifest.csv`.
2. Fill columns:
   - `filename`: exact workbook file name under `update_excel/`
   - `enabled`: `1/0` (include or skip)
   - `expect_channels`: `1/0` (whether this workbook should produce channels)
   - `notes`: optional

## One-Command Pipeline

Run:

```bash
python run_update_pipeline.py --manifest update_excel/manifest.csv --manifest-required
```

This executes:

1. `export_excel_grids.py`
2. `import_to_pg.py`
3. `normalize_rates_to_pg.py`
4. quality gate checks

## Optional Auto-Ingest Watcher

Use the watcher when incoming files first land in the external monthly WXWork cache and need LLM-based filename triage before entering `update_excel/`.

Run once for a single scan:

```bash
python watch_incoming_logistics.py --once
```

Run continuously:

```bash
python watch_incoming_logistics.py
```

What it does:

1. Watches the current month folder under `D:\wx_lj\WXWork\1688854674811621\Cache\File\YYYY-MM`
2. Also watches the previous month folder by default if it exists
3. Waits for files to become stable before processing
4. Uses an LLM to accept/ignore/review candidate Excel files
5. Replaces or adds files in `update_excel/`
6. Rewrites `update_excel/manifest.csv`
7. Optionally triggers `run_update_pipeline.py` after a debounce window

State is stored in `out/auto_ingest_state.json`.

## Quality Gate (Default)

1. Minimum channels for batch workbooks: `>= 1`
2. Minimum tiers for batch workbooks: `>= 1`
3. Workbooks marked `expect_channels=1` must produce channels
4. Optional parser flag limit via `--max-parser-flags`

If quality gate fails, process exits with code `2` and writes details to summary JSON.

## Important Rule

Use a full snapshot of active files for each publish run.
Do not run partial updates with `--truncate` unless `update_excel/` already contains the full active set.
