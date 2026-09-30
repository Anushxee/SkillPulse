"""
SkillPulse — Project 26 (Job Postings Skill Demand) data generator.

"""
import numpy as np
import pandas as pd

SEED = 26
N_MONTHS = 18
POSTINGS_PER_MONTH = 3000
N_COMPANIES = 400
N_SECTORS = 8
N_CITIES = 10

SECTORS = ["Technology", "Finance", "Healthcare", "Retail",
           "Manufacturing", "Energy", "Education", "Media"]
CITIES = ["New York", "San Francisco", "Chicago", "Austin", "Seattle",
          "Boston", "Denver", "Atlanta", "Miami", "Portland"]

FILLER_SKILLS = ["Python", "SQL", "Spark", "Airflow", "Kafka", "Hive", "AWS",
                  "Azure", "GCP", "Docker", "Kubernetes", "Terraform",
                  "Scala", "Java", "PowerBI", "Tableau"]
TRACKED_SKILLS = ["Snowflake", "Databricks", "Hadoop", "dbt"]
ALL_20_SKILLS = FILLER_SKILLS + TRACKED_SKILLS

# NOTE on the 57-distinct-raw-strings target: 3 of the 20 canonical skills
# (SQL, AWS, GCP) are already all-uppercase, so upper-casing them produces no
# NEW distinct string. Upper-casing therefore contributes at most 17 new
# variants (not 20). Space-padding has no such collision and can contribute
# a full 20 new variants. 20 canonical + 17 upper variants + 20 spaced
# variants = 57, which is the target this generator is designed to hit.
ALWAYS_UPPERCASE_SKILLS = {"SQL", "AWS", "GCP"}


def _ensure_full_coverage(rng, chosen_idx, pool, df, target_skills, target_size):
    """Swap rows in/out of `chosen_idx` (keeping len == target_size) until every
    skill in `target_skills` appears in at least one chosen row's skill list."""
    chosen_idx = list(chosen_idx)
    pool = list(pool)

    def covered():
        c = set()
        for i in chosen_idx:
            c.update(df.loc[i, "_skills_list"])
        return c & target_skills

    missing = target_skills - covered()
    for skill in list(missing):
        if covered() >= target_skills:
            break
        # find a donor row in pool containing the missing skill
        donor_pos = None
        for j, i in enumerate(pool):
            if skill in df.loc[i, "_skills_list"]:
                donor_pos = j
                break
        if donor_pos is None:
            continue  # no donor available anywhere; leave as-is (documented, non-fatal)
        donor = pool.pop(donor_pos)
        # find a row in chosen_idx that is "safe" to evict (doesn't uniquely cover any skill)
        evict_pos = None
        for j, i in enumerate(chosen_idx):
            trial = set(chosen_idx) - {i}
            trial_cov = set()
            for t in trial:
                trial_cov.update(df.loc[t, "_skills_list"])
            if target_skills.issubset(trial_cov | {skill}):
                evict_pos = j
                break
        if evict_pos is None:
            continue
        evicted = chosen_idx.pop(evict_pos)
        pool.append(evicted)
        chosen_idx.append(donor)
    assert len(chosen_idx) == target_size
    return chosen_idx, pool


def month_start(m: int) -> pd.Timestamp:
    year = 2024 + (m // 12)
    month = (m % 12) + 1
    return pd.Timestamp(year=year, month=month, day=1)


def generate_companies(rng: np.random.RandomState) -> pd.DataFrame:
    rows = []
    for i in range(N_COMPANIES):
        sector = SECTORS[i // 50]
        city = CITIES[i % N_CITIES]
        rows.append({
            "company_id": f"CMP{i:03d}",
            "company_name": f"{sector} Solutions {i:03d}",
            "sector": sector,
            "city": city,
        })
    return pd.DataFrame(rows)


def generate_clean_postings(rng: np.random.RandomState, companies: pd.DataFrame) -> pd.DataFrame:
    """Generates the 54,000 ORIGINAL, defect-free postings."""
    company_ids = companies["company_id"].values
    rows = []
    for m in range(N_MONTHS):
        m_start = month_start(m)
        snowflake_cut = 520 + 22 * m
        databricks_lo, databricks_hi = 1000, 1400 + 18 * m
        hadoop_lo, hadoop_hi = 1800, 2660 - 38 * m
        for k in range(POSTINGS_PER_MONTH):
            gid = m * POSTINGS_PER_MONTH + k
            posting_id = f"JP{gid:06d}"
            posted_date = m_start + pd.Timedelta(days=int(gid % 28))
            company_id = company_ids[rng.randint(0, N_COMPANIES)]

            n_filler = rng.randint(2, 5)  # 2-4 inclusive
            filler = list(rng.choice(FILLER_SKILLS, size=n_filler, replace=False))

            tracked = []
            has_snowflake = k < snowflake_cut
            if has_snowflake:
                tracked.append("Snowflake")
            if databricks_lo <= k < databricks_hi:
                tracked.append("Databricks")
            if hadoop_lo <= k < hadoop_hi:
                tracked.append("Hadoop")
            if has_snowflake and k % 2 == 0:
                tracked.append("dbt")

            skills = filler + tracked
            rows.append({
                "posting_id": posting_id,
                "company_id": company_id,
                "posted_date": posted_date.strftime("%Y-%m-%d"),
                "month_index": m,
                "row_in_month": k,
                "skills_raw": ";".join(skills),
                "_skills_list": skills,  # kept for defect-targeting, dropped before CSV export
            })
    return pd.DataFrame(rows)


def apply_defects(rng: np.random.RandomState, postings: pd.DataFrame) -> pd.DataFrame:
    """Applies every intentional defect described in the brief, in place, on a copy."""
    df = postings.copy()
    n = len(df)
    assert n == 54000

    has_tracked = df["_skills_list"].apply(
        lambda s: any(t in s for t in TRACKED_SKILLS)
    )

    # --- Content-destroying defects (900 empty / 450 not-specified / 540
    # unknown-company / 270 invalid-date). Restricted to postings with NO
    # tracked skill, and stratified evenly per month (120/month = 50+25+30+15)
    # so Silver ends up with exactly 2,880 valid postings in every month.
    empty_idx, notspec_idx, badcmp_idx, baddate_idx = [], [], [], []
    for m in range(N_MONTHS):
        month_mask = (df["month_index"] == m) & (~has_tracked)
        pool = df.index[month_mask].to_numpy().copy()
        rng.shuffle(pool)
        assert len(pool) >= 120, f"month {m} has insufficient unprotected rows"
        empty_idx.extend(pool[0:50])
        notspec_idx.extend(pool[50:75])
        badcmp_idx.extend(pool[75:105])
        baddate_idx.extend(pool[105:120])

    df["skills_raw"] = df["skills_raw"].astype(object)
    df.loc[empty_idx, "skills_raw"] = ""
    df.loc[notspec_idx, "skills_raw"] = "Not specified"
    df.loc[badcmp_idx, "company_id"] = "CMP999"
    df.loc[baddate_idx, "posted_date"] = "0000-00-00"

    content_destroyed = set(empty_idx) | set(notspec_idx)

    # --- Skill-format defects (delimiter / case / spacing). These preserve
    # the underlying skill content, so Silver must recover it. Eligible pool
    # excludes rows whose skills_raw was already blanked out above.
    eligible = df.index[~df.index.isin(content_destroyed)].to_numpy().copy()
    rng.shuffle(eligible)

    comma_idx = eligible[0:1000]
    remaining = eligible[1000:].copy()
    rng.shuffle(remaining)

    upper_idx = list(remaining[0:2200])
    after_upper_pool = list(remaining[2200:4200])
    spaced_idx = list(remaining[4200:5900])
    leftover_pool = list(remaining[5900:])

    # Guarantee coverage: uppercase defect covers all skills whose upper()
    # differs from the original (17 of them); spaced defect covers all 20.
    case_changing_skills = {s for s in ALL_20_SKILLS if s.upper() != s}
    upper_idx, after_upper_pool = _ensure_full_coverage(
        rng, upper_idx, after_upper_pool + leftover_pool, df, case_changing_skills, 2200
    )
    spaced_idx, _ = _ensure_full_coverage(
        rng, spaced_idx, after_upper_pool, df, set(ALL_20_SKILLS), 1700
    )

    df.loc[comma_idx, "skills_raw"] = df.loc[comma_idx, "skills_raw"].str.replace(";", ",", regex=False)
    df.loc[upper_idx, "skills_raw"] = df.loc[upper_idx, "skills_raw"].str.upper()
    df.loc[spaced_idx, "skills_raw"] = df.loc[spaced_idx, "skills_raw"].apply(
        lambda s: " " + " ; ".join(s.split(";")) + " "
    )

    df = df.drop(columns=["_skills_list", "month_index", "row_in_month"])

    # --- Duplicate 1,350 whole rows (sampled from the full defected 54,000),
    # appended to the end -> 55,350 rows in the raw file.
    dup_positions = rng.choice(n, size=1350, replace=False)
    dup_rows = df.iloc[dup_positions].copy()
    df_final = pd.concat([df, dup_rows], ignore_index=True)

    return df_final, upper_idx, spaced_idx, comma_idx


def generate_all():
    rng = np.random.RandomState(SEED)
    companies = generate_companies(rng)
    clean = generate_clean_postings(rng, companies)
    postings, upper_idx, spaced_idx, comma_idx = apply_defects(rng, clean)
    return companies, postings, {
        "n_upper_defect_rows": len(upper_idx),
        "n_spaced_defect_rows": len(spaced_idx),
        "n_comma_defect_rows": len(comma_idx),
    }


if __name__ == "__main__":
    companies, postings, meta = generate_all()
    print("companies:", len(companies))
    print("postings (raw/bronze):", len(postings))
    print(meta)
