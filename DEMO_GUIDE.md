# Demo Guide — 8-10 minutes

A straight-line sequence for demonstrating SkillPulse to a professor.

**STEP 1 — Open the dashboard.**
Open the hosted SkillPulse link (or `frontend/dist/index.html` locally).
Point out the light/dark toggle in the nav rail; switch it once to show
dark mode is a warm grey, not black.

**STEP 2 — Overview.**
Walk the KPI row: 54,000 postings, 400 companies, 20 canonical skills, 18
months. Point out "Top skill this month" (Snowflake, 31%) and "Fastest
growing" (Databricks, +8.5% QoQ) — say these numbers come straight out of
`data/validation_report.json`, not typed in.

**STEP 3 — Select Snowflake on Skill Trends.**
Go to Skill Trends, use the skill chips to isolate Snowflake, and note
its share climbing from 18.1% to 31.0% across the date range — matching
the brief's stated trend exactly.

**STEP 4 — Show its demand trend.**
Switch the "Posting count / Market share %" toggle to show both views.
Show the rank-movement chart underneath.

**STEP 5 — Show QoQ growth.**
Go to Skill Growth. Point at the ranked table — Databricks, dbt, and
Snowflake are the three fastest growers this quarter, all documented as
ratio-of-sums, not average-of-ratios (the trap the brief specifically
warns about).

**STEP 6 — Show related skills.**
Go to Skill Relationships, select Snowflake. Point out dbt as the
strongest companion (50% confidence, ~4.2× lift) — explain that's because
dbt only ever appears on Snowflake postings, by construction, and the
dashboard is reflecting a real structural relationship, not a coincidence.

**STEP 7 — Open Data Quality.**
Show the funnel: 55,350 raw postings → 1,350 duplicates removed → 54,000
clean → 2,160 more rejected (missing skills / unknown company / invalid
date) → 51,840 valid. Point at "57 raw skill variants → 20 canonical" and
explain the SQL/AWS/GCP already-uppercase quirk documented in
`docs/data-dictionary.md`.

**STEP 8 — Show the 55,350 → 54,000 transformation in code.**
Open `databricks/03_silver/03_silver.py`, scroll to the `assert n_before -
n_after == 1350` line — show that the pipeline asserts and stops loudly on
any mismatch, rather than silently continuing.

**STEP 9 — Open Databricks (if available).**
Show `bronze_postings`, `silver_posting_skill`, `gold_skill_month` as
Delta tables. If no live workspace is available for this demo, show the
notebook source instead and explain it mirrors the locally-verified
pandas run.

**STEP 10 — Open Snowflake (if available).**
Show `GOLD_SKILL_MONTH`, run `sql/analytical_queries.sql`'s Q1 query live.

**STEP 11 — Run one analytical SQL query.**
`sql/analytical_queries.sql` → Q3 (Snowflake's top companions) — show the
result matches the dashboard's Relationships page exactly.

**STEP 12 — Show the GitHub repository.**
Point out `tests/test_pipeline.py` (28 tests), `data/validation_report.json`
(17/17 PASS), and `PROJECT_STATUS.md` for an honest per-component status.

**STEP 13 — Show the hosted application.**
Return to the live dashboard link, resize the browser to show mobile
responsiveness (nav rail collapses to icons-only top bar).
