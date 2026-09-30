# Deployment

## Dashboard (static hosting)

`frontend/dist/index.html` is a single, self-contained file — no build
step, no `npm install`, no server required to view it. Any static host
works identically:

- **Vercel / Netlify**: drag-and-drop the `frontend/dist/` folder, or
  connect the repo and set the output directory to `frontend/dist`.
- **GitHub Pages**: copy `frontend/dist/index.html` to the repo root or a
  `docs/` branch and enable Pages.
- **Anywhere else**: it's one HTML file — any static file server, S3
  bucket + CloudFront, or even opening it directly from disk works, since
  it has no server-side dependency.

To refresh the embedded data after re-running the pipeline:
```
python scripts/run_pipeline.py
python scripts/export_dashboard_data.py
python scripts/build_dashboard.py
```
This regenerates `frontend/dist/index.html` with fresh numbers.

## Databricks

1. Create a Repos folder from this GitHub repository (or upload the
   `databricks/` folder as workspace files).
2. Open `databricks/00_setup/00_setup.py`, set `MY_ID` to a short unique
   identifier, and run it. This creates a namespaced catalog/schema/Volume
   so multiple students can share one workspace.
3. Run `databricks/01_generator/01_generate_data.py` **once** — it's a
   setup step, not part of the recurring pipeline.
4. Create a Databricks Job with tasks, in order:
   `02_bronze → 03_silver → 04_gold → 05_validation → 06_export`, each
   depending on the previous. Set a schedule (e.g. daily) — "a pipeline
   you have to start by hand is not a pipeline."
5. Confirm `05_validation` writes `validation_report.json` to your Volume
   with all checks `PASS`. If any check fails, the notebook raises rather
   than continuing — that's intentional; debug before re-running.

## Snowflake

1. Run `sql/snowflake_setup.sql` (creates database, schema, file format,
   stage).
2. Run `sql/tables.sql` (creates the two Gold tables + `SILVER_REJECTS`).
3. Upload the CSVs written by `06_export` to `@SKILLPULSE_STAGE`, either
   via SnowSQL `PUT` or the Snowsight UI.
4. Run `sql/copy_into.sql`. Run it a second time without truncating first
   — the accompanying `COPY_HISTORY` query proves the second run loaded
   zero new rows (idempotency).
5. Run `sql/analytical_queries.sql` to answer the three faculty questions
   directly in Snowflake.

## Credentials

Never commit `.env`, passwords, or API keys. `.env.example` lists every
variable the (optional) `backend/` service or a Snowflake connector
script would need:

```
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=SKILLPULSE
SNOWFLAKE_SCHEMA=ANALYTICS
```

Databricks Volume paths and catalog names are namespaced by `MY_ID`, set
once per student/workspace — no secret material there, so no
Databricks-side `.env` is required beyond your personal access token,
which should also never be committed.

## Optional backend

`backend/app/` contains a minimal FastAPI service that serves the same
Gold-layer JSON the dashboard already embeds at build time, useful if you
later want live querying instead of a static snapshot. It is **not**
required for the dashboard described in this README — see
`docs/architecture.md` → "Frontend architecture decision" for why.
