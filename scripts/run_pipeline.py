import json
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from generator_core import generate_all, TRACKED_SKILLS
from pipeline_core import build_bronze, build_silver, build_gold

OUT = os.path.join(os.path.dirname(__file__), "..", "data")
SAMPLE = os.path.join(OUT, "sample")
EXPORT = os.path.join(os.path.dirname(__file__), "..", "databricks", "06_export", "output")
os.makedirs(SAMPLE, exist_ok=True)
os.makedirs(EXPORT, exist_ok=True)


def main():
    t0 = time.time()
    print("=== PHASE: GENERATE ===")
    companies, postings, gen_meta = generate_all()
    print(f"  companies={len(companies)} postings(raw)={len(postings)}  [{time.time()-t0:.1f}s]")

    print("=== PHASE: BRONZE ===")
    bronze_companies, bronze_postings = build_bronze(companies, postings)
    print(f"  bronze_companies={len(bronze_companies)} bronze_postings={len(bronze_postings)}  [{time.time()-t0:.1f}s]")

    print("=== PHASE: SILVER ===")
    silver, slog = build_silver(bronze_companies, bronze_postings)
    print(f"  {slog}  [{time.time()-t0:.1f}s]")

    print("=== PHASE: GOLD ===")
    gold, glog = build_gold(silver)
    print(f"  {glog}  [{time.time()-t0:.1f}s]")

    # tracked-skill totals, computed the same way the dashboard will read them:
    # sum of postings_with_skill across all months, from Gold.
    skill_month = gold["gold_skill_month"]
    tracked_totals = {}
    for skill in TRACKED_SKILLS:
        tracked_totals[skill] = int(skill_month[skill_month["skill"] == skill.lower()]["postings_with_skill"].sum())

    checks = []

    def check(name, expected, actual, details=""):
        status = "PASS" if expected == actual else "FAIL"
        checks.append({"check_name": name, "expected": expected, "actual": actual,
                        "status": status, "details": details})
        if status == "FAIL":
            print(f"  !!! FAIL: {name} expected={expected} actual={actual}")

    check("bronze_companies", 400, len(bronze_companies))
    check("bronze_postings", 55350, len(bronze_postings))
    check("silver_postings_after_dedup", 54000, slog["postings_after_dedup"])
    check("duplicates_removed", 1350, slog["dedup_removed"])
    check("missing_or_notspecified_skills", 1350, slog["reject_no_skills_or_notspecified"])
    check("unknown_company_rejects", 540, slog["reject_unknown_company"])
    check("invalid_date_rejects", 270, slog["reject_invalid_date"])
    check("raw_distinct_skill_strings", 57, slog["raw_distinct_skill_strings"])
    check("normalized_distinct_skills", 20, slog["normalized_distinct_skills"])
    check("gold_skill_month_rows", 360, glog["gold_skill_month_rows"])
    check("gold_pair_month_possible_rows", 6840, glog["gold_pair_month_possible"])
    check("gold_pair_month_populated_rows", 6660, glog["gold_pair_month_populated"])
    check("postings_per_month_in_gold", 2880, slog["postings_per_month"])
    check("tracked_skill_total_snowflake", 12726, tracked_totals["Snowflake"])
    check("tracked_skill_total_databricks", 9954, tracked_totals["Databricks"])
    check("tracked_skill_total_hadoop", 9666, tracked_totals["Hadoop"])
    check("tracked_skill_total_dbt", 6363, tracked_totals["dbt"])

    n_pass = sum(1 for c in checks if c["status"] == "PASS")
    n_fail = len(checks) - n_pass
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "seed": 26,
        "summary": {"total_checks": len(checks), "passed": n_pass, "failed": n_fail},
        "checks": checks,
    }

    if n_fail > 0:
        print(f"\n!!! VALIDATION FAILED: {n_fail} check(s) did not match spec. Stopping — not writing outputs.")
        with open(os.path.join(OUT, "validation_report.json"), "w") as f:
            json.dump(report, f, indent=2)
        sys.exit(1)

    print(f"\n=== ALL {n_pass} CHECKS PASSED ===")

    # --- write outputs ---
    with open(os.path.join(OUT, "validation_report.json"), "w") as f:
        json.dump(report, f, indent=2)

    # sample raw data (first 2000 rows of each, for repo/demo purposes — full
    # 55,350-row file is regenerable any time via `python scripts/run_pipeline.py`)
    companies.to_csv(os.path.join(SAMPLE, "companies.csv"), index=False)
    postings.head(2000).to_csv(os.path.join(SAMPLE, "postings_sample.csv"), index=False)

    silver["silver_rejects"].to_csv(os.path.join(EXPORT, "silver_rejects.csv"), index=False)

    gold["gold_skill_month"].to_csv(os.path.join(EXPORT, "gold_skill_month.csv"), index=False)
    gold["gold_skill_pair_month"].to_csv(os.path.join(EXPORT, "gold_skill_pair_month.csv"), index=False)
    gold["gold_skill_quarter"].to_csv(os.path.join(EXPORT, "gold_skill_quarter.csv"), index=False)

    # data-quality funnel numbers, for the dashboard
    dq = {
        "raw_postings": 55350,
        "duplicates_removed": 1350,
        "clean_postings": 54000,
        "missing_or_notspecified": 1350,
        "unknown_company": 540,
        "invalid_date": 270,
        "valid_postings": 51840,
        "raw_skill_variants": 57,
        "canonical_skills": 20,
        "quality_score_pct": round(51840 / 54000 * 100, 2),
    }
    with open(os.path.join(EXPORT, "data_quality.json"), "w") as f:
        json.dump(dq, f, indent=2)

    print(f"\nTotal runtime: {time.time()-t0:.1f}s")
    print("Outputs written to data/ and databricks/06_export/output/")

    return companies, postings, bronze_companies, bronze_postings, silver, gold, report


if __name__ == "__main__":
    main()
