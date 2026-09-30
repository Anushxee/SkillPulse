# Databricks notebook: 05_validation
# ---------------------------------------------------------------------------
# PRODUCTION CODE — real PySpark, NOT executed here. Mirrors the checks that
# WERE run (locally, in pandas) in scripts/run_pipeline.py, whose output is
# data/validation_report.json (17/17 PASS). This notebook re-derives the
# same checks against the actual Delta tables on a real workspace, and fails
# loudly (raises) rather than "fixing" any mismatch.
# ---------------------------------------------------------------------------

import json
import pyspark.sql.functions as F

MY_ID = "AnushreeBhattacharya"
VOL = "/Volumes/workspace/skillpulse_AnushreeBhattacharya/raw_data"
SCHEMA = "workspace.skillpulse_AnushreeBhattacharya"

checks = []

def check(name, expected, actual, details=""):
    status = "PASS" if expected == actual else "FAIL"
    checks.append({"check_name": name, "expected": expected, "actual": actual,
                    "status": status, "details": details})
    print(f"[{status}] {name}: expected={expected} actual={actual}")

bronze_companies = spark.table(f"{SCHEMA}.bronze_companies")
bronze_postings = spark.table(f"{SCHEMA}.bronze_postings")
silver_postings = spark.table(f"{SCHEMA}.silver_postings")
silver_rejects = spark.table(f"{SCHEMA}.silver_rejects")
silver_posting_skill = spark.table(f"{SCHEMA}.silver_posting_skill")
gold_skill_month = spark.table(f"{SCHEMA}.gold_skill_month")
gold_skill_pair_month = spark.table(f"{SCHEMA}.gold_skill_pair_month")

check("bronze_companies", 400, bronze_companies.count())
check("bronze_postings", 55350, bronze_postings.count())
check("silver_postings", 54000 - 2160, silver_postings.count())
check("duplicates_removed", 1350, bronze_postings.count() - (bronze_postings.select("posting_id").distinct().count()))
check("missing_skills_rejects", 1350,
      silver_rejects.where(F.col("rejection_reason").isin("no_skills_listed", "not_specified")).count())
check("unknown_company_rejects", 540, silver_rejects.where(F.col("rejection_reason") == "unknown_company").count())
check("invalid_date_rejects", 270, silver_rejects.where(F.col("rejection_reason") == "unparseable_date").count())
check("gold_skill_month_rows", 360, gold_skill_month.count())
check("gold_pair_month_populated", 6660, gold_skill_pair_month.count())
per_month = silver_postings.groupBy("month").count().collect()
check("postings_per_month_uniform_2880", True, all(r["count"] == 2880 for r in per_month))

for skill, expected in [("snowflake", 12726), ("databricks", 9954), ("hadoop", 9666), ("dbt", 6363)]:
    total = (gold_skill_month.where(F.col("skill") == skill)
             .agg(F.sum("postings_with_skill")).collect()[0][0])
    check(f"tracked_skill_total_{skill}", expected, total)

n_fail = sum(1 for c in checks if c["status"] == "FAIL")
report = {"checks": checks, "summary": {"total": len(checks), "passed": len(checks) - n_fail, "failed": n_fail}}

report_json = json.dumps(report, indent=2, default=str)
dbutils.fs.put(f"{VOL}/validation_report.json", report_json, overwrite=True)

if n_fail > 0:
    raise AssertionError(f"VALIDATION FAILED: {n_fail} check(s) did not match spec — see validation_report.json")
print(f"ALL {len(checks)} CHECKS PASSED")
