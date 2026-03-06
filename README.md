# logistics_ingest

Pipeline for ingesting logistics pricing Excel workbooks into PostgreSQL.

## Configuration

This project auto-loads settings from `.env`, `.env.local`, then `.env.example`.

Required keys:
- `PG_DSN`
- `DEEPSEEK_API_KEY` (needed for LLM checks)

## Command Order (and why)

Run commands in this order when doing manual execution:

1. `export_grids`
   - Reads Excel files and exports structured grid artifacts.
2. `import_raw`
   - Imports exported grid artifacts into raw DB tables.
3. `normalize_rates`
   - Converts raw tables into final pricing tables.
4. `run_pipeline`
   - Orchestrator that runs `1 -> 2 -> 3` plus quality gate.
5. `watch_incoming`
   - Outer automation: watches source folders, triages files with LLM, updates snapshots, then optionally triggers `run_pipeline`.

## Recommended Full Flow (one command)

Put active workbooks under `data/update_excel/`, then run:

```bash
python -m logistics_ingest.cli.run_pipeline --manifest data/update_excel/manifest.csv --manifest-required
```

## Manual Full Flow (step-by-step)

```bash
python -m logistics_ingest.cli.export_grids --input-dir data/update_excel --output-dir data/out/tmp_export
python -m logistics_ingest.cli.import_raw --output-dir data/out/tmp_export --batch-id <batch_id>
python -m logistics_ingest.cli.normalize_rates --batch-id <batch_id> --truncate --llm-divisor-check
```

Use the same `<batch_id>` for `import_raw` and `normalize_rates`.

## Watcher Flow

Run once (ingest only):

```bash
python -m logistics_ingest.cli.watch_incoming --once --no-run-pipeline
```

Run once (ingest + pipeline):

```bash
python -m logistics_ingest.cli.watch_incoming --once
```

Run continuously:

```bash
python -m logistics_ingest.cli.watch_incoming
```

## Help Commands

```bash
python -m logistics_ingest.cli.export_grids --help
python -m logistics_ingest.cli.import_raw --help
python -m logistics_ingest.cli.normalize_rates --help
python -m logistics_ingest.cli.run_pipeline --help
python -m logistics_ingest.cli.watch_incoming --help
```

## Project Layout

- `logistics_ingest/cli`: command entrypoints
- `logistics_ingest/app`: orchestration services
- `logistics_ingest/domain`: models and rules
- `logistics_ingest/infra`: db/excel/fs/manifest/llm adapters
- `logistics_ingest/shared`: settings and shared helpers
- `data/`: runtime data (`update_excel`, `archive_excel`, `out`)
- `docs/`: operation docs
- `templates/`: CSV templates
