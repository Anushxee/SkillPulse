# Testing

## Running the tests

```
pip install -r backend/requirements.txt
pytest tests/ -v
```

Expected: **28 passed**, in roughly 20 seconds (the run generates the full
55,350-row dataset and runs it through Bronze/Silver/Gold for real —
nothing is mocked or stubbed).

## What's actually tested

`tests/test_pipeline.py` is organized into four classes, each exercising
real behavior rather than "does the function exist":

**`TestGenerator`** — company count/IDs/sectors/cities, raw posting row
count (55,350), posting-ID uniqueness, all four defect counts (900/450/
540/270), duplicate-injection count (1,350), all four tracked-skill
totals (12,726/9,954/9,666/6,363) computed by string search — independent
of the pipeline's own skill-splitting logic — and determinism (running
the generator twice with the same seed produces identical output).

**`TestSkillSplitting`** — semicolon splitting, comma splitting, that no
unprocessed comma survives a split, that space-padded tokens normalize
correctly, and the empty-string edge case.

**`TestBronze`** — row counts match the generator's output exactly,
provenance columns exist, and defects are still present (unrepaired) in
Bronze.

**`TestSilver`** — dedup count is exactly 1,350, all three reject reasons
have exactly the right counts, no invalid company ID survives into
`silver_postings`, the 57→20 skill normalization holds, every month has
exactly 2,880 valid postings, and every stored skill is genuinely
trimmed+lowercased (not just claimed to be).

**`TestGold`** — `gold_skill_month` is 360 rows (20 skills × 18 months),
`skill_share` never exceeds 1.0 (i.e. computed from distinct posting
counts, not skill-row counts — the exact trap the brief warns against),
`gold_skill_pair_month` has exactly 6,660 of 6,840 possible pairs
populated, no `A→A` self-pairs exist, tracked-skill totals reconciled
independently from Gold, and `rank_in_month` is a dense 1–20 ranking
every month.

## Validation report

`data/validation_report.json` is produced by `scripts/run_pipeline.py`
and contains the 17 checks the brief explicitly lists (Bronze/Silver row
counts, all four defect counts, 57→20 skill normalization, Gold row
counts, per-month posting counts, and all four tracked-skill totals). The
pipeline **stops and exits non-zero** if any check fails — it does not
write partial output or silently continue. As of the last run: **17/17
PASS**.

## What is not covered by automated tests here

- The PySpark/Databricks notebooks under `databricks/` have not been
  executed (no workspace available) and therefore have no automated test
  coverage beyond visual/logical review against the validated pandas
  equivalents. If you run them on your own workspace and get a
  discrepancy, that's real signal — please report it rather than editing
  around it.
- The Snowflake SQL has not been executed against a live account.
- `backend/tests/` covers the optional FastAPI service (see
  `backend/README.md` if present); it is not required for the static
  dashboard to work.
- Frontend interaction testing: `frontend/index.html`'s JavaScript was
  syntax-checked (`node --check`) and exercised end-to-end with a jsdom
  harness (every nav page rendered, every control — month-range selects,
  skill chips, mode toggle, relationship-skill select, theme toggle —
  clicked/changed with no runtime errors). This is not a substitute for
  manual testing in a real browser, which is recommended before final
  submission.
