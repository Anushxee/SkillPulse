# data/

- `validation_report.json` — produced by `scripts/run_pipeline.py`. 17/17
  checks PASS against real, computed pipeline output.
- `sample/companies.csv` — the full, real 400-row companies table.
- `sample/postings_sample.csv` — first 2,000 rows of the real 55,350-row
  raw postings file (the full file is regenerable in seconds via
  `python scripts/run_pipeline.py` and isn't committed, to keep the repo
  small — it's deterministic from `seed=26`, so regenerating it produces
  byte-identical output).
