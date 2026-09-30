import pyspark.sql.functions as F

MY_ID = "AnushreeBhattacharya"

VOL = "/Volumes/workspace/skillpulse_AnushreeBhattacharya/raw_data"

SCHEMA = "workspace.skillpulse_AnushreeBhattacharya"


companies_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .option("nullValue", "__NULL_SENTINEL__")
    .option("ignoreLeadingWhiteSpace", False)
    .option("ignoreTrailingWhiteSpace", False)
    .csv(f"{VOL}/companies")
)

postings_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .option("nullValue", "__NULL_SENTINEL__")
    .option("ignoreLeadingWhiteSpace", False)
    .option("ignoreTrailingWhiteSpace", False)
    .csv(f"{VOL}/postings")
)


def add_provenance(df, source_file: str, key_cols: list):

    return (
        df
        .withColumn(
            "_source_file",
            F.lit(source_file)
        )
        .withColumn(
            "_ingested_at",
            F.current_timestamp()
        )
        .withColumn(
            "_row_hash",
            F.sha2(
                F.concat_ws(
                    "|",
                    *[F.col(c) for c in key_cols]
                ),
                256
            )
        )
    )



bronze_companies = add_provenance(
    companies_raw,
    "companies.csv",
    [
        "company_id",
        "company_name",
        "sector",
        "city"
    ]
)


bronze_postings = add_provenance(
    postings_raw,
    "postings.csv",
    [
        "posting_id",
        "company_id",
        "posted_date",
        "skills_raw"
    ]
)

(
    bronze_companies
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(
        f"{SCHEMA}.bronze_companies"
    )
)

(
    bronze_postings
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(
        f"{SCHEMA}.bronze_postings"
    )
)


n_companies = (
    spark.table(
        f"{SCHEMA}.bronze_companies"
    ).count()
)

n_postings = (
    spark.table(
        f"{SCHEMA}.bronze_postings"
    ).count()
)


print(
    f"bronze_companies = {n_companies} "
    f"(expect 400)"
)

print(
    f"bronze_postings  = {n_postings} "
    f"(expect 55350)"
)


assert n_companies == 400, (
    f"STOP: expected 400 bronze companies, "
    f"got {n_companies}"
)

assert n_postings == 55350, (
    f"STOP: expected 55350 bronze postings, "
    f"got {n_postings}"
)

n_empty = (
    spark.table(
        f"{SCHEMA}.bronze_postings"
    )
    .where(
        F.col("skills_raw") == ""
    )
    .count()
)


n_cmp999 = (
    spark.table(
        f"{SCHEMA}.bronze_postings"
    )
    .where(
        F.col("company_id") == "CMP999"
    )
    .count()
)


n_baddate = (
    spark.table(
        f"{SCHEMA}.bronze_postings"
    )
    .where(
        F.col("posted_date") == "0000-00-00"
    )
    .count()
)


print(
    f"evidence check -> "
    f"empty skills_raw>={n_empty}, "
    f"CMP999>={n_cmp999}, "
    f"bad date>={n_baddate}"
)


assert n_empty >= 900, (
    "STOP: Bronze empty skills defect was altered"
)

assert n_cmp999 >= 540, (
    "STOP: Bronze CMP999 defect was altered"
)

assert n_baddate >= 270, (
    "STOP: Bronze invalid-date defect was altered"
)


print("==========================================")
print("BRONZE PIPELINE COMPLETED SUCCESSFULLY")
print("==========================================")
print(f"bronze_companies = {n_companies}")
print(f"bronze_postings  = {n_postings}")
print(f"empty skills_raw = {n_empty}")
print(f"CMP999 rows      = {n_cmp999}")
print(f"bad date rows    = {n_baddate}")
print("==========================================")