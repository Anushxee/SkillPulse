# Databricks notebook: 03_silver
# Logic mirrors scripts/pipeline_core.py::build_silver
#
# Expected results:
#   55,350 raw postings
#         deduplicate
#   54,000 postings
#         reject invalid/missing data
#   51,840 valid postings
#         explode skills
#   57 distinct raw skill strings
#         trim + lower
#   20 canonical skills
#
# Every judgement call is made ONCE and validated before continuing.

import pyspark.sql.functions as F
from pyspark.sql import Window


MY_ID = "AnushreeBhattacharya"

SCHEMA = "workspace.skillpulse_AnushreeBhattacharya"

bronze_postings = spark.table(
    f"{SCHEMA}.bronze_postings"
)

bronze_companies = spark.table(
    f"{SCHEMA}.bronze_companies"
)


silver_companies = bronze_companies.select(
    "company_id",
    "company_name",
    "sector",
    "city"
)


w_dedup = (
    Window
    .partitionBy("posting_id")
    .orderBy("_ingested_at")
)

deduped = (
    bronze_postings
    .withColumn(
        "_rn",
        F.row_number().over(w_dedup)
    )
    .where(F.col("_rn") == 1)
    .drop("_rn")
)


n_before = bronze_postings.count()
n_after = deduped.count()

print(
    f"postings: {n_before} -> {n_after} "
    f"({n_before - n_after} duplicates removed)"
)

assert (
    n_before - n_after == 1350
), "STOP: dedup did not remove exactly 1,350 rows"

assert (
    n_after == 54000
), "STOP: expected 54,000 postings after dedup"


deduped = deduped.withColumn(
    "_skill_reject_reason",
    F.when(
        F.col("skills_raw") == "",
        F.lit("no_skills_listed")
    )
    .when(
        F.col("skills_raw") == "Not specified",
        F.lit("not_specified")
    )
    .otherwise(
        F.lit(None)
    )
)



deduped = (
    deduped.join(
        F.broadcast(
            silver_companies
            .select("company_id")
            .withColumn("_known", F.lit(True))
        ),
        on="company_id",
        how="left"
    )
)

deduped = deduped.withColumn(
    "_company_reject_reason",
    F.when(
        F.col("_known").isNull(),
        F.lit("unknown_company")
    )
    .otherwise(
        F.lit(None)
    )
)


deduped = deduped.withColumn(
    "_posted_date_ts",
    F.expr("try_cast(posted_date as date)")
)

deduped = deduped.withColumn(
    "_date_reject_reason",
    F.when(
        F.col("_posted_date_ts").isNull(),
        F.lit("unparseable_date")
    )
    .otherwise(
        F.lit(None)
    )
)


n_skill_reject = (
    deduped
    .where(F.col("_skill_reject_reason").isNotNull())
    .count()
)

n_company_reject = (
    deduped
    .where(F.col("_company_reject_reason").isNotNull())
    .count()
)

n_date_reject = (
    deduped
    .where(F.col("_date_reject_reason").isNotNull())
    .count()
)

print(
    f"reject: "
    f"no_skills/not_specified={n_skill_reject}, "
    f"unknown_company={n_company_reject}, "
    f"unparseable_date={n_date_reject}"
)

assert (
    n_skill_reject == 1350
), "STOP: expected exactly 1,350 skill rejects"

assert (
    n_company_reject == 540
), "STOP: expected exactly 540 unknown-company rejects"

assert (
    n_date_reject == 270
), "STOP: expected exactly 270 unparseable-date rejects"


reject_condition = (
    F.col("_skill_reject_reason").isNotNull()
    |
    F.col("_company_reject_reason").isNotNull()
    |
    F.col("_date_reject_reason").isNotNull()
)


silver_rejects = (
    deduped
    .where(reject_condition)
    .withColumn(
        "rejection_reason",
        F.coalesce(
            "_skill_reject_reason",
            "_company_reject_reason",
            "_date_reject_reason"
        )
    )
    .withColumn(
        "rejected_at",
        F.current_timestamp()
    )
    .withColumn(
        "rejection_details",
        F.lit(None).cast("string")
    )
    .select(
        "posting_id",
        "company_id",
        "rejection_reason",
        "rejected_at",
        "_source_file",
        "rejection_details"
    )
)

valid = (
    deduped
    .where(~reject_condition)
    .withColumn(
        "posted_date",
        F.col("_posted_date_ts")
    )
    .withColumn(
        "month",
        F.date_format(
            "posted_date",
            "yyyy-MM"
        )
    )
    .drop(
        "_skill_reject_reason",
        "_company_reject_reason",
        "_date_reject_reason",
        "_posted_date_ts",
        "_known"
    )
)


n_valid = valid.count()

print(
    f"silver valid postings = {n_valid} "
    f"(expect 51840)"
)

assert (
    n_valid == 51840
), "STOP: expected 51,840 valid postings"


per_month = (
    valid
    .groupBy("month")
    .count()
    .collect()
)

assert all(
    r["count"] == 2880
    for r in per_month
), (
    "STOP: not every month has 2880 valid postings: "
    f"{per_month}"
)


silver_postings = valid.select(
    "posting_id",
    "company_id",
    "posted_date",
    "month"
)



tokenized = valid.withColumn(
    "skill_raw_token",
    F.explode(
        F.split(
            F.col("skills_raw"),
            r"[;,]"
        )
    )
)

# Remove tokens that are completely empty after splitting.
tokenized = tokenized.where(
    F.trim(
        F.col("skill_raw_token")
    ) != ""
)


tokenized = tokenized.withColumn(
    "skill",
    F.lower(
        F.trim(
            F.col("skill_raw_token")
        )
    )
)


n_raw_variants = (
    tokenized
    .select("skill_raw_token")
    .distinct()
    .count()
)

n_canonical = (
    tokenized
    .select("skill")
    .distinct()
    .count()
)

print(
    f"raw distinct skill strings = "
    f"{n_raw_variants} (expect 57)"
)

print(
    f"normalized distinct skills = "
    f"{n_canonical} (expect 20)"
)

assert (
    n_raw_variants == 57
), "STOP: raw variant count drifted from spec"

assert (
    n_canonical == 20
), "STOP: canonical skill count drifted from spec"


silver_posting_skill = (
    tokenized
    .join(
        silver_companies,
        on="company_id",
        how="left"
    )
    .select(
        "posting_id",
        "company_id",
        "company_name",
        "sector",
        "city",
        "posted_date",
        "month",
        "skill",
        "_source_file"
    )
)

silver_tables = [
    ("silver_companies", silver_companies),
    ("silver_postings", silver_postings),
    ("silver_posting_skill", silver_posting_skill),
    ("silver_rejects", silver_rejects),
]

for name, df in silver_tables:

    (
        df
        .write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(
            f"{SCHEMA}.{name}"
        )
    )

    print(
        f"wrote {SCHEMA}.{name}: "
        f"{df.count()} rows"
    )



assert (
    spark.table(
        f"{SCHEMA}.silver_companies"
    ).count() == 400
)

assert (
    spark.table(
        f"{SCHEMA}.silver_postings"
    ).count() == 51840
)

assert (
    spark.table(
        f"{SCHEMA}.silver_rejects"
    ).count() == 2160
)

print("==========================================")
print("SILVER PIPELINE COMPLETED SUCCESSFULLY")
print("==========================================")
print("silver_companies      = 400")
print("silver_postings       = 51,840")
print("silver_rejects        = 2,160")
print(f"silver_posting_skill  = {silver_posting_skill.count()}")
print("raw skill variants    = 57")
print("canonical skills      = 20")
print("==========================================")