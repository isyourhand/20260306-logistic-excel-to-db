## logistics_ingest package

Primary source tree for the logistics ETL pipeline.

### Run commands

From project root:

```bash
python -m logistics_ingest.cli.run_pipeline --manifest data/update_excel/manifest.csv --manifest-required
python -m logistics_ingest.cli.watch_incoming --once --no-run-pipeline
python -m logistics_ingest.cli.export_grids --input-dir data/update_excel --output-dir data/out/tmp_export
python -m logistics_ingest.cli.import_raw --output-dir data/out/tmp_export
python -m logistics_ingest.cli.normalize_rates --batch-id <batch_id>
```

### Notes

- Package CLI entrypoints are the only supported run path.
- DB schema source of truth is `logistics_ingest/infra/db/schema.py`.

