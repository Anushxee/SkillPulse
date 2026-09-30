# Architecture

## Flow

```mermaid
flowchart LR
    A[Generator — seed 26] --> B[Raw CSV: companies, postings]
    B --> C[Databricks Bronze]
    C --> D[Databricks Silver]
    D --> E[Databricks Gold]
    E --> F[CSV Export]
    F --> G[Snowflake Stage]
    G --> H[Snowflake Tables]
    H --> I[SkillPulse Dashboard]
```

## Two implementations, one source of truth

This project ships **two** implementations of the exact same pipeline
logic:

1. **`scripts/generator_core.py` + `scripts/pipeline_core.py`** — pandas,
   runs locally, no external dependencies beyond `pandas`/`numpy`. This is
   what was actually executed to produce `data/validation_report.json` and
   the sample exports under `databricks/06_export/output/`. It's also what
   `tests/test_pipeline.py` exercises.

2. **`databricks/01_generator` through `05_validation`** — real PySpark,
   written to run on an actual Databricks workspace. Each notebook's logic
   is a direct, deliberate translation of the corresponding pandas
   function (e.g. `03_silver.py`'s dedup/split/normalize/reject steps
   mirror `pipeline_core.py::build_silver` line for line in intent). These
   notebooks were **not executed** — no Databricks workspace is available
   in the environment this repo was built in — and are clearly marked as
   such at the top of each file.

Why two implementations instead of one? Because the brief requires real,
runnable PySpark/Databricks code, but also requires that every claimed
number be genuinely computed and verified, not asserted on faith. Pandas
gave a fast, fully-verifiable proving ground; the PySpark notebooks are
the production artifact for when you have a workspace. If you run the
Databricks notebooks and get different numbers, that's a real finding —
open an issue, don't paper over it.

## Layers

**Bronze** — raw, unmodified, string-typed. Every column arrives exactly
as written in the source file. Three provenance columns are added:
`_source_file`, `_ingested_at`, `_row_hash`. No trimming, no casting, no
deduplication, no defect repair happens here — Bronze exists to preserve
evidence of what arrived.

**Silver** — every cleaning judgement call happens here, exactly once:
1. Deduplicate on `posting_id` (before exploding skills).
2. Split `skills_raw` on comma or semicolon (delimiter-agnostic).
3. Normalize: trim + lowercase, once.
4. Reject rows with missing/invalid skills (`no_skills_listed` /
   `not_specified`), unknown company (`unknown_company`, via anti-join
   against `silver_companies`), or unparseable date (`unparseable_date`,
   via `try_cast` — never `cast`, since Databricks' ANSI mode throws on a
   bad `cast` and kills the notebook rather than giving you a `NULL` you
   can count).

**Gold** — two tables that directly answer the three faculty questions:
- `gold_skill_month`: postings-with-skill / postings-that-month, ranked.
  `skill_share` is a **posting-count ratio**, not a row-count ratio — a
  posting that (structurally, by construction) can't contain the same
  skill twice still gets defended against with an explicit
  `distinct(posting_id, skill)` before counting.
- `gold_skill_pair_month`: built from a self-join on
  `(posting_id, month)`, excluding `A = A`, de-duplicated so a posting
  contributes each ordered pair at most once. `support`, `confidence`,
  and `lift` are computed as ratios of posting counts, never row counts.

## Idempotency

- **Databricks**: Bronze/Silver/Gold tables are written with
  `mode("overwrite")` against deterministic table names — re-running the
  pipeline replaces the table rather than appending to it, so running
  twice does not double the data.
- **Snowflake**: `COPY INTO` tracks loaded-file metadata per
  stage/table pair for 64 days by default. Re-running `sql/copy_into.sql`
  against the same file loads zero additional rows unless `FORCE=TRUE` is
  passed explicitly. `sql/copy_into.sql` includes a `COPY_HISTORY` query
  to prove this on your own account.

## Orchestration

Intended as a single Databricks Job with six tasks in sequence:
`02_bronze → 03_silver → 04_gold → 05_validation → 06_export`, on a daily
or on-demand schedule. `01_generator` is intentionally **not** part of
that job — it's a one-time setup step (see the note at the top of
`databricks/01_generator/01_generate_data.py`).

## Frontend architecture decision

The brief allows a "robust static-data/API serving architecture" when a
separate backend would add unnecessary deployment complexity. SkillPulse
takes that path: `frontend/index.html` is a single, self-contained
HTML/CSS/JS file (no build step, no npm dependency tree to install on a
grader's machine) that reads a build-time JSON snapshot of the Gold layer
(`databricks/06_export/output/dashboard_data.json`, embedded via
`scripts/build_dashboard.py`). Every chart is hand-rolled inline SVG — no
charting library dependency, no external network calls beyond Google
Fonts. `backend/` contains an optional FastAPI service exposing the same
data over HTTP, for a future move to live querying instead of a
build-time snapshot.
