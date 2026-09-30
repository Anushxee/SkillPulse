# Databricks notebook: 04_gold
# (executed and validated locally: 360 skill-month rows, 6,840 possible /
# 6,660 populated skill pairs, 2,880 valid postings every month).
# ---------------------------------------------------------------------------

import pyspark.sql.functions as F
from pyspark.sql import Window

MY_ID = "AnushreeBhattacharya"
SCHEMA = "workspace.skillpulse_AnushreeBhattacharya"

sps = spark.table(f"{SCHEMA}.silver_posting_skill")
silver_postings = spark.table(f"{SCHEMA}.silver_postings")

# Defensive de-dup: a posting must not count the same skill twice.
sps_unique = sps.select("posting_id", "month", "skill").distinct()

valid_postings_per_month = (silver_postings.groupBy("month").count()
                             .withColumnRenamed("count", "postings_that_month"))

skill_month = (sps_unique.groupBy("skill", "month")
               .agg(F.countDistinct("posting_id").alias("postings_with_skill"))
               .join(valid_postings_per_month, on="month", how="left")
               .withColumn("skill_share", F.col("postings_with_skill") / F.col("postings_that_month")))

w_rank = Window.partitionBy("month").orderBy(F.desc("postings_with_skill"))
skill_month = skill_month.withColumn("rank_in_month", F.row_number().over(w_rank))

n_skill_month = skill_month.count()
print(f"gold_skill_month rows = {n_skill_month} (expect 360)")
assert n_skill_month == 360, "STOP"

# --- Skill pairs: self-join on (posting_id, month), excluding A=A, and
# de-duplicated so a posting contributes each ordered pair at most once. ---
left = sps_unique.withColumnRenamed("skill", "skill_a")
right = sps_unique.withColumnRenamed("skill", "skill_b")
pairs = (left.join(right, on=["posting_id", "month"], how="inner")
         .where(F.col("skill_a") != F.col("skill_b"))
         .select("skill_a", "skill_b", "month", "posting_id").distinct())

pair_month = (pairs.groupBy("skill_a", "skill_b", "month")
              .agg(F.countDistinct("posting_id").alias("pair_postings")))

postings_a = (skill_month.select(F.col("skill").alias("skill_a"), "month",
                                  F.col("postings_with_skill").alias("postings_a")))
postings_b = (skill_month.select(F.col("skill").alias("skill_b"), "month",
                                  F.col("skill_share").alias("prob_b")))

pair_month = (pair_month
              .join(postings_a, on=["skill_a", "month"], how="left")
              .join(postings_b, on=["skill_b", "month"], how="left")
              .withColumn("confidence_a_to_b", F.col("pair_postings") / F.col("postings_a"))
              .withColumn("lift", F.col("confidence_a_to_b") / F.col("prob_b"))
              .drop("prob_b"))

n_possible = 20 * 19 * 18
n_populated = pair_month.count()
print(f"gold_skill_pair_month possible={n_possible} (expect 6840), populated={n_populated} (expect 6660)")
assert n_possible == 6840, "STOP"
assert n_populated == 6660, "STOP: populated pair-month combinations drifted from spec"

skill_month.write.mode("overwrite").format("delta").saveAsTable(f"{SCHEMA}.gold_skill_month")
pair_month.write.mode("overwrite").format("delta").saveAsTable(f"{SCHEMA}.gold_skill_pair_month")
print("Gold tables written.")
