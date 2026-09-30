# Databricks notebook: 01_generator
# ---------------------------------------------------------------------------
#
# It reuses the exact generation logic in scripts/generator_core.py (which
# WAS run locally and validated — see data/validation_report.json and
# tests/test_pipeline.py::TestGenerator). Reproducing it here as pandas ->
# Spark DataFrame keeps a single source of truth for the generation
# algorithm instead of a second, hand-translated copy that could drift.
# ---------------------------------------------------------------------------

MY_ID = "AnushreeBhattacharya"
VOL = f"/Volumes/workspace/skillpulse_AnushreeBhattacharya/raw_data"

import sys
sys.path.append("/Workspace/Users/anushree0001@gmail.com/SkillPulse/skillpulse-project26/scripts")
from generator_core import generate_all 

companies_pd, postings_pd, meta = generate_all()
print("companies:", len(companies_pd), " postings (raw incl. duplicates):", len(postings_pd))
print(meta)

companies_sdf = spark.createDataFrame(companies_pd)
postings_sdf = spark.createDataFrame(postings_pd)

companies_sdf.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .option("ignoreLeadingWhiteSpace", False) \
    .option("ignoreTrailingWhiteSpace", False) \
    .csv(f"{VOL}/companies")

postings_sdf.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .option("ignoreLeadingWhiteSpace", False) \
    .option("ignoreTrailingWhiteSpace", False) \
    .csv(f"{VOL}/postings")

written_companies = spark.read.option("header", True).csv(f"{VOL}/companies").count()
written_postings = spark.read.option("header", True).csv(f"{VOL}/postings").count()
assert written_companies == 400, written_companies
assert written_postings == 55350, written_postings
print("Generation verified on disk: companies=400, postings=55350")
