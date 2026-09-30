# Databricks notebook: 06_export
# ---------------------------------------------------------------------------
# PRODUCTION CODE — real PySpark, NOT executed here. Exports the two Gold
# tables to single CSV files in the Volume, ready for a Snowflake stage
# upload + COPY INTO (see sql/copy_into.sql).
#
# The equivalent local exports (produced by scripts/run_pipeline.py against
# the pandas pipeline) are at databricks/06_export/output/*.csv and ARE real,
# validated files — use them for local dashboard development or as a demo
# dataset while you don't yet have a Databricks/Snowflake account handy.
# ---------------------------------------------------------------------------

MY_ID = "AnushreeBhattacharya"
VOL = "/Volumes/workspace/skillpulse_AnushreeBhattacharya/raw_data"
SCHEMA = "workspace.skillpulse_AnushreeBhattacharya"

gold_skill_month = spark.table(f"{SCHEMA}.gold_skill_month")
gold_skill_pair_month = spark.table(f"{SCHEMA}.gold_skill_pair_month")

(gold_skill_month.coalesce(1).write.mode("overwrite").option("header", True)
 .csv(f"{VOL}/export/gold_skill_month"))
(gold_skill_pair_month.coalesce(1).write.mode("overwrite").option("header", True)
 .csv(f"{VOL}/export/gold_skill_pair_month"))

print("Exported to:")
print(f"  {VOL}/export/gold_skill_month")
print(f"  {VOL}/export/gold_skill_pair_month")
print("Next: upload these to your Snowflake stage and run sql/copy_into.sql")
