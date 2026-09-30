# Data Dictionary

## Important note on the generator

The uploaded faculty brief for **Project 26 — Job Postings Skill Demand**
specifies exact target counts and formulas (row counts, tracked-skill
totals, defect counts) but — unlike the network-analysis briefs (Topics
31–34), which ship literal, runnable PySpark generator code — it does
**not** supply a row-by-row generation algorithm for Project 26.

`scripts/generator_core.py` is therefore an original, seeded (`seed=26`),
fully reproducible generator, deliberately designed to satisfy every
explicit numeric target in the brief. This page documents every design
decision so the generator is fully auditable. Nothing downstream
(Bronze/Silver/Gold/validation) is hardcoded to "look right" — the
pipeline computes its numbers from whatever this generator actually
emits, and `data/validation_report.json` reports the real, computed
values (17/17 PASS).

## companies.csv

| Column | Type | Notes |
|---|---|---|
| `company_id` | string | `CMP000`–`CMP399` |
| `company_name` | string | `"{sector} Solutions {index}"` |
| `sector` | string | one of 8: Technology, Finance, Healthcare, Retail, Manufacturing, Energy, Education, Media (50 companies each, in index order) |
| `city` | string | one of 10: New York, San Francisco, Chicago, Austin, Seattle, Boston, Denver, Atlanta, Miami, Portland (cycled by `index % 10`) |

## postings.csv (raw / Bronze)

| Column | Type | Notes |
|---|---|---|
| `posting_id` | string | `JP000000` onward, unique within the original 54,000 |
| `company_id` | string | drawn uniformly at random (seeded) from the 400 companies; 540 rows deliberately overwritten to `CMP999` |
| `posted_date` | string | `first day of month + (global_id % 28) days`, where `global_id = month_index*3000 + row_in_month` — **documented assumption**, since the brief specifies the formula in terms of "row position" without pinning down whether that's the month-local `k` or a global row id; a global id was chosen for date diversity within a month |
| `skills_raw` | string | 2–4 filler skills + 0–2 tracked skills, semicolon-joined in the clean case; subject to defects below |

### Filler skills (16)
Python, SQL, Spark, Airflow, Kafka, Hive, AWS, Azure, GCP, Docker,
Kubernetes, Terraform, Scala, Java, PowerBI, Tableau — 2–4 drawn without
replacement per posting.

### Tracked skills (4) and their exact formulas

For month index `m` (0–17) and row-within-month `k` (0–2999):

| Skill | Condition | Total (Σ over m=0..17) | Verified |
|---|---|---|---|
| Snowflake | `k < 520 + 22m` | `18×520 + 22×Σm` = 12,726 | ✅ |
| Databricks | `1000 ≤ k < 1400 + 18m` | `18×400 + 18×Σm` = 9,954 | ✅ |
| Hadoop | `1800 ≤ k < 2660 - 38m` | `18×860 - 38×Σm` = 9,666 | ✅ |
| dbt | on even-`k` Snowflake rows | `18×260 + 11×Σm` = 6,363 | ✅ |

(`Σm` for `m=0..17` = 153.) The Snowflake, Databricks, and Hadoop
`k`-ranges are **mutually disjoint** in every month (894 < 1000, and
1706 < 1800 at the widest), so a posting never carries more than one
tracked skill from these three, by construction — verified, not assumed.

## Intentional defects (applied to the original 54,000 rows)

| Defect | Count | Placement rule |
|---|---|---|
| Empty `skills_raw` | 900 | 50/month, sampled only from rows with **no** tracked skill (so tracked-skill totals survive rejection intact) |
| `skills_raw = "Not specified"` | 450 | 25/month, same pool |
| `company_id = "CMP999"` | 540 | 30/month, same pool |
| `posted_date = "0000-00-00"` | 270 | 15/month, same pool |
| Comma-delimited (`,` instead of `;`) | 1,000 | any row not already blanked above |
| Uppercased `skills_raw` | 2,200 | any row not already blanked; explicitly topped up so all 20 canonical skills appear at least once in uppercase form |
| Space-padded (`" ; "` + leading/trailing space) | 1,700 | same pool; explicitly topped up so all 20 canonical skills appear at least once in space-padded form |
| Duplicated whole rows | 1,350 | sampled from the full defected 54,000, appended |

**Why the empty/"Not specified"/CMP999/invalid-date defects avoid
tracked-skill rows, and why 120/month:** Silver rejects the whole posting
for these four defect types. If they landed on tracked-skill rows, the
tracked-skill totals (12,726 / 9,954 / 9,666 / 6,363) would come out
*lower* than the brief's target after Silver — so those four defects are
restricted to postings with no tracked skill. They're also stratified at
exactly 120/month (50+25+30+15) so that every month ends up with exactly
`3000 - 120 = 2880` valid postings, matching the brief's Gold-layer
target exactly.

**Why 57 raw skill strings normalize to 20 canonical skills:** 3 of the
20 canonical skills (**SQL, AWS, GCP**) are already fully uppercase, so
uppercasing them produces no *new* distinct string — the uppercase defect
therefore contributes at most 17 new variants, not 20. Space-padding has
no such collision (padding `"SQL"` into `" SQL "` is still a new string)
and contributes a full 20. `20 canonical + 17 uppercase variants + 20
space-padded variants = 57`, which is exactly the brief's target — this
was a deliberate design choice (see `_ensure_full_coverage` in
`generator_core.py`), not luck.

## Silver tables

**`silver_companies`**: `company_id, company_name, sector, city`

**`silver_postings`**: `posting_id, company_id, posted_date, month` — one
row per valid posting (51,840 rows).

**`silver_posting_skill`**: `posting_id, company_id, company_name,
sector, city, posted_date, month, skill, _source_file` — one row per
posting-skill relationship, `skill` always trimmed + lowercased.

**`silver_rejects`**: `posting_id, company_id, rejection_reason,
rejected_at, _source_file, rejection_details` — 2,160 rows total
(1,350 `no_skills_listed`/`not_specified` + 540 `unknown_company` + 270
`unparseable_date`).

## Gold tables

**`gold_skill_month`**: `skill, month, postings_with_skill,
postings_that_month, skill_share, rank_in_month` — 360 rows (20×18).

**`gold_skill_pair_month`**: `skill_a, skill_b, month, pair_postings,
postings_a, confidence_a_to_b, support, lift` — 6,660 of 6,840 possible
ordered pairs (20×19×18) have `pair_postings > 0`.

**`gold_skill_quarter`** (derived, for QoQ growth): `skill, quarter,
quarter_postings, prev_quarter_postings, qoq_abs_change,
qoq_pct_change`.

## Metric definitions

- **support(A, B)** = postings containing both A and B ÷ postings that
  month
- **confidence(A → B)** = postings containing both A and B ÷ postings
  containing A
- **lift(A → B)** = confidence(A → B) ÷ (postings containing B ÷
  postings that month)
