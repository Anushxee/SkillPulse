"""
SkillPulse pipeline — Bronze -> Silver -> Gold, implemented in pandas as a
local, verifiable stand-in for the PySpark/Databricks production code
(databricks/02_bronze, 03_silver, 04_gold). Logic is 1:1 equivalent; see
those notebooks for the Spark version meant to run on an actual workspace.
"""
import hashlib
import re
from datetime import datetime, timezone

import numpy as np
import pandas as pd

FILLER_SKILLS = ["Python", "SQL", "Spark", "Airflow", "Kafka", "Hive", "AWS",
                  "Azure", "GCP", "Docker", "Kubernetes", "Terraform",
                  "Scala", "Java", "PowerBI", "Tableau"]
TRACKED_SKILLS = ["Snowflake", "Databricks", "Hadoop", "dbt"]
ALL_20_SKILLS_LOWER = sorted(s.lower() for s in FILLER_SKILLS + TRACKED_SKILLS)


def row_hash(row_values):
    return hashlib.sha256("|".join(str(v) for v in row_values).encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# BRONZE — raw, untouched, provenance-only
# ---------------------------------------------------------------------------
def build_bronze(companies_raw: pd.DataFrame, postings_raw: pd.DataFrame):
    now = datetime.now(timezone.utc).isoformat()

    bronze_companies = companies_raw.copy()
    bronze_companies["_source_file"] = "companies.csv"
    bronze_companies["_ingested_at"] = now
    bronze_companies["_row_hash"] = bronze_companies.apply(
        lambda r: row_hash(r[["company_id", "company_name", "sector", "city"]]), axis=1)

    bronze_postings = postings_raw.copy()
    bronze_postings["_source_file"] = "postings.csv"
    # stagger ingestion timestamps slightly so ROW_NUMBER ordering for dedup is deterministic,
    # exactly mirroring "keep the first-ingested copy" semantics used in the Silver dedupe step.
    bronze_postings["_ingested_at"] = [
        f"{now}#{i:06d}" for i in range(len(bronze_postings))
    ]
    bronze_postings["_row_hash"] = bronze_postings.apply(
        lambda r: row_hash(r[["posting_id", "company_id", "posted_date", "skills_raw"]]), axis=1)

    return bronze_companies, bronze_postings


# ---------------------------------------------------------------------------
# SILVER — dedupe, split, normalize, reject
# ---------------------------------------------------------------------------
def split_skills(raw: str):
    """Robust delimiter handling: semicolon or comma. Returns RAW (untrimmed)
    tokens so callers can inspect pre-normalization variance; callers should
    .strip().lower() each token for the canonical form. No unprocessed comma
    should survive a split (regex splits on either delimiter)."""
    if raw is None or raw == "":
        return []
    parts = re.split(r"[;,]", raw)
    return [p for p in parts if p.strip() != ""]


def build_silver(bronze_companies: pd.DataFrame, bronze_postings: pd.DataFrame):
    log = {}

    # --- Silver companies: passthrough with clean typing ---
    silver_companies = bronze_companies[["company_id", "company_name", "sector", "city"]].copy()

    # --- STEP 1: Dedup on posting_id (before exploding skills) ---
    n_before = len(bronze_postings)
    bp = bronze_postings.sort_values("_ingested_at")
    deduped = bp.drop_duplicates(subset=["posting_id"], keep="first").copy()
    n_after = len(deduped)
    log["bronze_postings"] = n_before
    log["dedup_removed"] = n_before - n_after
    log["postings_after_dedup"] = n_after
    assert n_before - n_after == 1350, f"expected 1350 duplicates removed, got {n_before - n_after}"
    assert n_after == 54000, f"expected 54000 after dedup, got {n_after}"

    rejects = []

    # --- STEP 4: invalid skills (empty / "Not specified") ---
    is_empty = deduped["skills_raw"].fillna("") == ""
    is_notspec = deduped["skills_raw"] == "Not specified"
    invalid_skills_mask = is_empty | is_notspec
    for _, r in deduped[is_empty].iterrows():
        rejects.append({"posting_id": r["posting_id"], "company_id": r["company_id"],
                         "rejection_reason": "no_skills_listed", "rejected_at": datetime.now(timezone.utc).isoformat(),
                         "_source_file": r["_source_file"], "rejection_details": "skills_raw was empty"})
    for _, r in deduped[is_notspec].iterrows():
        rejects.append({"posting_id": r["posting_id"], "company_id": r["company_id"],
                         "rejection_reason": "not_specified", "rejected_at": datetime.now(timezone.utc).isoformat(),
                         "_source_file": r["_source_file"], "rejection_details": "skills_raw was 'Not specified'"})
    log["reject_no_skills_or_notspecified"] = int(invalid_skills_mask.sum())
    assert log["reject_no_skills_or_notspecified"] == 1350

    # --- STEP 5: invalid company (referential integrity, LEFT JOIN style) ---
    valid_company_ids = set(silver_companies["company_id"])
    unknown_company_mask = ~deduped["company_id"].isin(valid_company_ids)
    for _, r in deduped[unknown_company_mask].iterrows():
        rejects.append({"posting_id": r["posting_id"], "company_id": r["company_id"],
                         "rejection_reason": "unknown_company", "rejected_at": datetime.now(timezone.utc).isoformat(),
                         "_source_file": r["_source_file"], "rejection_details": f"company_id {r['company_id']} not found in companies"})
    log["reject_unknown_company"] = int(unknown_company_mask.sum())
    assert log["reject_unknown_company"] == 540

    # --- STEP 6: invalid date (try_cast semantics -> NULL, not error) ---
    def try_parse_date(s):
        try:
            return pd.Timestamp(s)
        except Exception:
            return pd.NaT
    parsed_dates = deduped["posted_date"].apply(try_parse_date)
    invalid_date_mask = parsed_dates.isna()
    for _, r in deduped[invalid_date_mask].iterrows():
        rejects.append({"posting_id": r["posting_id"], "company_id": r["company_id"],
                         "rejection_reason": "unparseable_date", "rejected_at": datetime.now(timezone.utc).isoformat(),
                         "_source_file": r["_source_file"], "rejection_details": f"posted_date {r['posted_date']} failed try_cast"})
    log["reject_invalid_date"] = int(invalid_date_mask.sum())
    assert log["reject_invalid_date"] == 270

    # A posting is valid only if it fails NONE of the three checks.
    reject_mask = invalid_skills_mask | unknown_company_mask | invalid_date_mask
    valid_postings = deduped[~reject_mask].copy()
    valid_postings["posted_date"] = parsed_dates[~reject_mask]
    valid_postings["month"] = valid_postings["posted_date"].dt.strftime("%Y-%m")
    log["silver_postings_valid"] = len(valid_postings)
    assert len(valid_postings) == 54000 - 2160 == 51840

    # per-month check: exactly 2,880 valid postings per month
    per_month = valid_postings.groupby("month").size()
    assert (per_month == 2880).all(), f"per-month valid counts not all 2880: {per_month.to_dict()}"
    log["postings_per_month"] = int(per_month.iloc[0])

    silver_rejects = pd.DataFrame(rejects)
    log["silver_rejects_total"] = len(silver_rejects)

    # --- STEP 2 + 3: split (comma/semicolon aware) then normalize once ---
    raw_variants = set()
    exploded_rows = []
    company_lookup = silver_companies.set_index("company_id")[["company_name", "sector", "city"]]
    for _, r in valid_postings.iterrows():
        tokens = split_skills(r["skills_raw"])
        for t in tokens:
            raw_variants.add(t)
            canon = t.strip().lower()
            exploded_rows.append({
                "posting_id": r["posting_id"],
                "company_id": r["company_id"],
                "posted_date": r["posted_date"].strftime("%Y-%m-%d"),
                "month": r["month"],
                "skill": canon,
                "_source_file": r["_source_file"],
            })
    silver_posting_skill = pd.DataFrame(exploded_rows)
    # attach company_name/sector/city
    silver_posting_skill = silver_posting_skill.join(company_lookup, on="company_id")

    log["raw_distinct_skill_strings"] = len(raw_variants)
    log["normalized_distinct_skills"] = silver_posting_skill["skill"].nunique()
    assert log["raw_distinct_skill_strings"] == 57
    assert log["normalized_distinct_skills"] == 20
    assert set(silver_posting_skill["skill"].unique()) == set(ALL_20_SKILLS_LOWER)

    silver_postings_clean = valid_postings[["posting_id", "company_id", "posted_date", "month"]].copy()

    return {
        "silver_companies": silver_companies,
        "silver_postings": silver_postings_clean,
        "silver_posting_skill": silver_posting_skill,
        "silver_rejects": silver_rejects,
    }, log


# ---------------------------------------------------------------------------
# GOLD — skill-month and skill-pair-month analytics
# ---------------------------------------------------------------------------
def build_gold(silver: dict):
    sps = silver["silver_posting_skill"]
    log = {}

    # A posting containing the same skill twice cannot happen post-generation
    # (filler drawn without replacement, tracked skills disjoint from filler),
    # but we defensively dedupe posting+skill pairs before counting anyway.
    sps_unique = sps.drop_duplicates(subset=["posting_id", "skill"])

    postings_per_month = sps_unique.groupby("month")["posting_id"].nunique()
    # postings_that_month must be the count of VALID postings that month (2,880),
    # not the count of postings that merely have >=1 skill row (same thing here
    # since every valid posting has 2-6 skills, but computed independently for clarity).
    valid_postings_per_month = silver["silver_postings"].groupby("month").size()

    skill_month = (
        sps_unique.groupby(["skill", "month"])["posting_id"].nunique()
        .rename("postings_with_skill").reset_index()
    )
    skill_month["postings_that_month"] = skill_month["month"].map(valid_postings_per_month)
    skill_month["skill_share"] = skill_month["postings_with_skill"] / skill_month["postings_that_month"]
    skill_month["rank_in_month"] = (
        skill_month.groupby("month")["postings_with_skill"].rank(method="first", ascending=False).astype(int)
    )
    skill_month = skill_month.sort_values(["month", "rank_in_month"]).reset_index(drop=True)

    log["gold_skill_month_rows"] = len(skill_month)
    assert len(skill_month) == 20 * 18 == 360

    # --- Skill pairs ---
    pair_rows = []
    for (posting_id, month), grp in sps_unique.groupby(["posting_id", "month"]):
        skills = sorted(grp["skill"].unique())
        for i in range(len(skills)):
            for j in range(len(skills)):
                if i == j:
                    continue
                pair_rows.append((skills[i], skills[j], month, posting_id))
    pairs_df = pd.DataFrame(pair_rows, columns=["skill_a", "skill_b", "month", "posting_id"])

    pair_month = (
        pairs_df.groupby(["skill_a", "skill_b", "month"])["posting_id"].nunique()
        .rename("pair_postings").reset_index()
    )
    postings_a = skill_month.set_index(["skill", "month"])["postings_with_skill"]
    pair_month["postings_a"] = pair_month.apply(
        lambda r: postings_a.get((r["skill_a"], r["month"]), 0), axis=1)
    pair_month["confidence_a_to_b"] = pair_month["pair_postings"] / pair_month["postings_a"]
    pair_month["support"] = pair_month["pair_postings"] / pair_month["month"].map(valid_postings_per_month)
    postings_b = skill_month.set_index(["skill", "month"])["postings_with_skill"]
    pair_month["postings_b"] = pair_month.apply(
        lambda r: postings_b.get((r["skill_b"], r["month"]), 0), axis=1)
    pair_month["prob_b"] = pair_month["postings_b"] / pair_month["month"].map(valid_postings_per_month)
    pair_month["lift"] = pair_month["confidence_a_to_b"] / pair_month["prob_b"]
    pair_month = pair_month.drop(columns=["postings_b", "prob_b"])

    n_possible = 20 * 19 * 18
    log["gold_pair_month_possible"] = n_possible
    log["gold_pair_month_populated"] = len(pair_month)
    assert n_possible == 6840
    assert len(pair_month) == 6660, f"expected 6660 populated pairs, got {len(pair_month)}"

    # --- QoQ growth (skill x quarter) ---
    skill_month2 = skill_month.copy()
    skill_month2["quarter"] = pd.PeriodIndex(skill_month2["month"], freq="M").asfreq("Q").astype(str)
    skill_quarter = (
        skill_month2.groupby(["skill", "quarter"])["postings_with_skill"].sum()
        .rename("quarter_postings").reset_index()
    )
    skill_quarter = skill_quarter.sort_values(["skill", "quarter"])
    skill_quarter["prev_quarter_postings"] = skill_quarter.groupby("skill")["quarter_postings"].shift(1)
    skill_quarter["qoq_abs_change"] = skill_quarter["quarter_postings"] - skill_quarter["prev_quarter_postings"]
    skill_quarter["qoq_pct_change"] = np.where(
        (skill_quarter["prev_quarter_postings"].isna()) | (skill_quarter["prev_quarter_postings"] == 0),
        np.nan,
        skill_quarter["qoq_abs_change"] / skill_quarter["prev_quarter_postings"] * 100,
    )

    return {
        "gold_skill_month": skill_month,
        "gold_skill_pair_month": pair_month,
        "gold_skill_quarter": skill_quarter,
    }, log
