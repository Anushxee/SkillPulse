import json
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from generator_core import generate_all, TRACKED_SKILLS
from pipeline_core import build_bronze, build_silver, build_gold

EXPORT = os.path.join(os.path.dirname(__file__), "..", "databricks", "06_export", "output")


def main():
    companies, postings, _ = generate_all()
    bc, bp = build_bronze(companies, postings)
    silver, slog = build_silver(bc, bp)
    gold, glog = build_gold(silver)

    skill_month = gold["gold_skill_month"].copy()
    skill_quarter = gold["gold_skill_quarter"].copy()
    pair_month = gold["gold_skill_pair_month"].copy()

    months = sorted(skill_month["month"].unique().tolist())
    skills = sorted(skill_month["skill"].unique().tolist())

    # skill_month -> nested dict for compact JSON: {skill: [{month,postings,share,rank}...]}
    skill_trends = {}
    for skill in skills:
        rows = skill_month[skill_month["skill"] == skill].sort_values("month")
        skill_trends[skill] = [
            {"month": r["month"], "postings": int(r["postings_with_skill"]),
             "share": round(float(r["skill_share"]) * 100, 2), "rank": int(r["rank_in_month"])}
            for _, r in rows.iterrows()
        ]

    # aggregate skill pairs across all months -> {skill: [{companion, pair_postings, confidence, lift}]}
    pair_agg = (
        pair_month.groupby(["skill_a", "skill_b"])
        .agg(pair_postings=("pair_postings", "sum"),
             confidence=("confidence_a_to_b", "mean"),
             lift=("lift", "mean"))
        .reset_index()
    )
    relationships = {}
    for skill in skills:
        rows = pair_agg[pair_agg["skill_a"] == skill].sort_values("pair_postings", ascending=False).head(10)
        relationships[skill] = [
            {"companion": r["skill_b"], "pair_postings": int(r["pair_postings"]),
             "confidence": round(float(r["confidence"]) * 100, 2), "lift": round(float(r["lift"]), 2)}
            for _, r in rows.iterrows()
        ]

    # QoQ growth table, most recent quarter available with a prior quarter
    sq = skill_quarter.dropna(subset=["prev_quarter_postings"]).copy()
    latest_q = sq["quarter"].max()
    growth_latest = sq[sq["quarter"] == latest_q].sort_values("qoq_pct_change", ascending=False)
    growth_table = [
        {"skill": r["skill"], "quarter": r["quarter"],
         "previous_quarter": int(r["prev_quarter_postings"]),
         "current_quarter": int(r["quarter_postings"]),
         "change": int(r["qoq_abs_change"]),
         "growth_pct": round(float(r["qoq_pct_change"]), 1)}
        for _, r in growth_latest.iterrows()
    ]
    # full quarter-over-quarter history per skill, for charting
    quarter_history = {}
    for skill in skills:
        rows = sq[sq["skill"] == skill].sort_values("quarter") if False else skill_quarter[skill_quarter["skill"] == skill].sort_values("quarter")
        quarter_history[skill] = [
            {"quarter": r["quarter"], "postings": int(r["quarter_postings"])}
            for _, r in rows.iterrows()
        ]

    latest_month = months[-1]
    top_current = skill_month[(skill_month["month"] == latest_month)].sort_values(
        "postings_with_skill", ascending=False).iloc[0]
    fastest_growing = growth_table[0] if growth_table else None

    top10_by_month = {}
    for m in months:
        rows = skill_month[skill_month["month"] == m].sort_values("rank_in_month").head(10)
        top10_by_month[m] = [
            {"skill": r["skill"], "postings": int(r["postings_with_skill"]),
             "share": round(float(r["skill_share"]) * 100, 2), "rank": int(r["rank_in_month"])}
            for _, r in rows.iterrows()
        ]

    with open(os.path.join(EXPORT, "data_quality.json")) as f:
        dq = json.load(f)

    import datetime as _dt
    generated_at = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    dashboard = {
        "meta": {
            "project_no": 26,
            "project_name": "SkillPulse",
            "seed": 26,
            "generated_at": generated_at,
            "months": months,
            "skills": skills,
            "kpi": {
                "total_postings": 54000,
                "valid_postings": 51840,
                "companies": 400,
                "canonical_skills": 20,
                "months_analyzed": 18,
                "top_current_skill": top_current["skill"],
                "top_current_skill_share": round(float(top_current["skill_share"]) * 100, 1),
                "fastest_growing_skill": fastest_growing["skill"] if fastest_growing else None,
                "fastest_growing_skill_pct": fastest_growing["growth_pct"] if fastest_growing else None,
            },
        },
        "skill_trends": skill_trends,
        "top10_by_month": top10_by_month,
        "growth_latest_quarter": growth_table,
        "quarter_history": quarter_history,
        "relationships": relationships,
        "data_quality": dq,
        "tracked_skills": {
            s: int(skill_month[skill_month["skill"] == s.lower()]["postings_with_skill"].sum())
            for s in TRACKED_SKILLS
        },
    }

    out_path = os.path.join(EXPORT, "dashboard_data.json")
    with open(out_path, "w") as f:
        json.dump(dashboard, f)
    print("wrote", out_path, os.path.getsize(out_path), "bytes")


if __name__ == "__main__":
    main()
