# Databricks notebook: 00_setup
# ---------------------------------------------------------------------------
# PRODUCTION CODE — this notebook targets a real Databricks workspace and has
# NOT been executed by Claude (no Databricks workspace is available in this
# environment). It is a direct PySpark translation of the logic proven out
# locally in scripts/generator_core.py + scripts/pipeline_core.py, which WAS
# executed and validated (see data/validation_report.json).
#
# Run this notebook first. It creates a student-namespaced catalog/schema and
# a Volume for raw + exported data, so multiple students can share one
# workspace safely.
# ---------------------------------------------------------------------------

MY_ID = "yourname"  # <-- CHANGE THIS to a short, unique, lowercase identifier

CATALOG = "workspace"
SCHEMA = f"skillpulse_{MY_ID}"
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

dbutils.fs.mkdirs(f"{VOL}/postings")
dbutils.fs.mkdirs(f"{VOL}/companies")

print(f"Catalog.Schema : {CATALOG}.{SCHEMA}")
print(f"Volume path    : {VOL}")
print("Every table created by later notebooks lives in this schema, e.g.")
print(f"  {CATALOG}.{SCHEMA}.bronze_postings")
print(f"  {CATALOG}.{SCHEMA}.silver_posting_skill")
print(f"  {CATALOG}.{SCHEMA}.gold_skill_month")

# Persist MY_ID / VOL for the rest of the notebooks via a widget or a small
# config table — simplest is to redefine these two lines at the top of every
# subsequent notebook (00_setup is not auto-%run'd on Databricks Free Edition
# serverless in all configurations).
