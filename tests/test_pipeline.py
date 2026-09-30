"""
Real behavioral tests for the SkillPulse pipeline. Run with:
    pytest tests/ -v
These exercise the SAME code path as scripts/run_pipeline.py — no numbers
are hardcoded into the pipeline to make these pass; the pipeline computes
them and the tests check the computed values against the brief's spec.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from generator_core import generate_all, FILLER_SKILLS, TRACKED_SKILLS  # noqa: E402
from pipeline_core import build_bronze, build_silver, build_gold, split_skills  # noqa: E402


@pytest.fixture(scope="module")
def generated():
    companies, postings, meta = generate_all()
    return companies, postings, meta


@pytest.fixture(scope="module")
def bronze(generated):
    companies, postings, _ = generated
    return build_bronze(companies, postings)


@pytest.fixture(scope="module")
def silver(bronze):
    bc, bp = bronze
    silver, log = build_silver(bc, bp)
    return silver, log


@pytest.fixture(scope="module")
def gold(silver):
    silver_tables, _ = silver
    gold_tables, log = build_gold(silver_tables)
    return gold_tables, log


# ---------------------------------------------------------------------------
# GENERATOR
# ---------------------------------------------------------------------------
class TestGenerator:
    def test_company_count_and_ids(self, generated):
        companies, _, _ = generated
        assert len(companies) == 400
        assert set(companies["company_id"]) == {f"CMP{i:03d}" for i in range(400)}

    def test_company_sectors_and_cities(self, generated):
        companies, _, _ = generated
        assert companies["sector"].nunique() == 8
        assert (companies.groupby("sector").size() == 50).all()
        assert companies["city"].nunique() == 10

    def test_postings_raw_row_count(self, generated):
        _, postings, _ = generated
        assert len(postings) == 55350

    def test_posting_ids_unique_within_original_54000(self, generated):
        _, postings, _ = generated
        orig = postings.iloc[:54000]
        assert orig["posting_id"].is_unique
        assert orig["posting_id"].nunique() == 54000

    def test_defect_counts(self, generated):
        _, postings, _ = generated
        orig = postings.iloc[:54000]
        assert (orig["skills_raw"] == "").sum() == 900
        assert (orig["skills_raw"] == "Not specified").sum() == 450
        assert (orig["company_id"] == "CMP999").sum() == 540
        assert (orig["posted_date"] == "0000-00-00").sum() == 270

    def test_duplicate_injection(self, generated):
        _, postings, _ = generated
        assert len(postings) - 54000 == 1350

    def test_tracked_skill_totals(self, generated):
        _, postings, _ = generated
        orig = postings.iloc[:54000]
        expected = {"Snowflake": 12726, "Databricks": 9954, "Hadoop": 9666, "dbt": 6363}
        for skill, exp in expected.items():
            actual = orig["skills_raw"].str.contains(skill, case=False, regex=False).sum()
            assert actual == exp, f"{skill}: expected {exp}, got {actual}"

    def test_seed_is_deterministic(self):
        c1, p1, _ = generate_all()
        c2, p2, _ = generate_all()
        assert c1.equals(c2)
        assert p1.equals(p2)


# ---------------------------------------------------------------------------
# SKILL SPLITTING (used by Silver)
# ---------------------------------------------------------------------------
class TestSkillSplitting:
    def test_semicolon_split(self):
        assert split_skills("Python;SQL;Spark") == ["Python", "SQL", "Spark"]

    def test_comma_split(self):
        assert split_skills("Python,SQL,Spark") == ["Python", "SQL", "Spark"]

    def test_no_unprocessed_comma_survives(self):
        tokens = split_skills("Python,SQL,Snowflake")
        assert all("," not in t for t in tokens)

    def test_spaced_semicolon_variant(self):
        tokens = split_skills(" Snowflake ;  Databricks ")
        canon = [t.strip().lower() for t in tokens]
        assert canon == ["snowflake", "databricks"]

    def test_empty_string(self):
        assert split_skills("") == []


# ---------------------------------------------------------------------------
# BRONZE
# ---------------------------------------------------------------------------
class TestBronze:
    def test_row_counts_match_generator(self, bronze, generated):
        bc, bp = bronze
        companies, postings, _ = generated
        assert len(bc) == len(companies) == 400
        assert len(bp) == len(postings) == 55350

    def test_provenance_columns_exist(self, bronze):
        bc, bp = bronze
        for col in ("_source_file", "_ingested_at", "_row_hash"):
            assert col in bc.columns
            assert col in bp.columns

    def test_bronze_preserves_raw_defects(self, bronze):
        _, bp = bronze
        assert (bp["skills_raw"] == "").sum() >= 900
        assert (bp["company_id"] == "CMP999").sum() >= 540
        assert (bp["posted_date"] == "0000-00-00").sum() >= 270


# ---------------------------------------------------------------------------
# SILVER
# ---------------------------------------------------------------------------
class TestSilver:
    def test_dedup_exact(self, silver):
        _, log = silver
        assert log["dedup_removed"] == 1350
        assert log["postings_after_dedup"] == 54000

    def test_reject_reasons(self, silver):
        tables, log = silver
        rejects = tables["silver_rejects"]
        assert log["reject_no_skills_or_notspecified"] == 1350
        assert log["reject_unknown_company"] == 540
        assert log["reject_invalid_date"] == 270
        assert (rejects["rejection_reason"] == "unknown_company").sum() == 540
        assert (rejects["rejection_reason"] == "unparseable_date").sum() == 270
        assert (rejects["rejection_reason"].isin(["no_skills_listed", "not_specified"])).sum() == 1350

    def test_no_invalid_company_survives_to_silver_postings(self, silver):
        tables, _ = silver
        valid_ids = set(tables["silver_companies"]["company_id"])
        assert set(tables["silver_postings"]["company_id"]).issubset(valid_ids)
        assert "CMP999" not in set(tables["silver_postings"]["company_id"])

    def test_normalization_57_to_20(self, silver):
        _, log = silver
        assert log["raw_distinct_skill_strings"] == 57
        assert log["normalized_distinct_skills"] == 20

    def test_postings_per_month_2880(self, silver):
        _, log = silver
        assert log["postings_per_month"] == 2880

    def test_canonical_skills_are_lowercase_trimmed(self, silver):
        tables, _ = silver
        skills = tables["silver_posting_skill"]["skill"]
        assert (skills == skills.str.strip().str.lower()).all()


# ---------------------------------------------------------------------------
# GOLD
# ---------------------------------------------------------------------------
class TestGold:
    def test_skill_month_shape(self, gold):
        tables, log = gold
        assert log["gold_skill_month_rows"] == 360
        assert tables["gold_skill_month"]["skill"].nunique() == 20
        assert tables["gold_skill_month"]["month"].nunique() == 18

    def test_skill_share_is_posting_based_not_row_based(self, gold):
        tables, _ = gold
        sm = tables["gold_skill_month"]
        # every skill_share must be <= 1 (a posting cannot count itself twice)
        assert (sm["skill_share"] <= 1.0).all()
        assert (sm["skill_share"] >= 0.0).all()

    def test_pair_month_shape(self, gold):
        _, log = gold
        assert log["gold_pair_month_possible"] == 6840
        assert log["gold_pair_month_populated"] == 6660

    def test_no_self_pairs(self, gold):
        tables, _ = gold
        pm = tables["gold_skill_pair_month"]
        assert (pm["skill_a"] != pm["skill_b"]).all()

    def test_tracked_skill_totals_from_gold(self, gold):
        tables, _ = gold
        sm = tables["gold_skill_month"]
        expected = {"snowflake": 12726, "databricks": 9954, "hadoop": 9666, "dbt": 6363}
        for skill, exp in expected.items():
            total = int(sm[sm["skill"] == skill]["postings_with_skill"].sum())
            assert total == exp, f"{skill}: expected {exp}, got {total}"

    def test_rank_in_month_is_dense_1_to_20(self, gold):
        tables, _ = gold
        sm = tables["gold_skill_month"]
        for month, grp in sm.groupby("month"):
            assert sorted(grp["rank_in_month"].tolist()) == list(range(1, 21))
