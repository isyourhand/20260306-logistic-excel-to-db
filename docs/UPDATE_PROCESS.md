# Standard Update Process

This project is a full-snapshot ETL pipeline for logistics pricing data.

## Directory Convention

1. `data/update_excel/`
   - Keep only the current active workbook per feed/company.
   - Replaced files are archived to `data/archive_excel/`.
2. `data/out/update_runs/`
   - Pipeline run artifacts (logs, audit, summary).
3. Project root
   - Keep no `.xlsx` files in root.

## Optional Manifest

Use `data/update_excel/manifest.csv` to control inclusion.

Template: `templates/update_manifest.template.csv`

Columns:
- `filename`: exact workbook filename under `data/update_excel/`
- `enabled`: `1/0`
- `expect_channels`: `1/0`
- `notes`: optional

## One-Command Pipeline

```bash
python -m logistics_ingest.cli.run_pipeline --manifest data/update_excel/manifest.csv --manifest-required
```

This runs:
1. export grids
2. import raw
3. normalize to pricing tables
4. quality gate checks

## Auto-Ingest Watcher

Single scan:

```bash
python -m logistics_ingest.cli.watch_incoming --once
```

Continuous:

```bash
python -m logistics_ingest.cli.watch_incoming
```

Watcher behavior:
1. scans current month folder under `LOGISTICS_SOURCE_ROOT` (or built-in default path)
2. optionally scans previous month folder
3. LLM triages filenames
4. updates `data/update_excel/` and rewrites manifest
5. optionally triggers publish pipeline

State file: `data/out/auto_ingest_state.json`

## Quality Gate (Default)

1. minimum channels: `>= 1`
2. minimum tiers: `>= 1`
3. `expect_channels=1` files must produce channels
4. optional parser-flag threshold via `--max-parser-flags`

On failure, process exits with code `2`.

