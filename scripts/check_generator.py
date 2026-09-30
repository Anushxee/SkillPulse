import re
from generator_core import generate_all, FILLER_SKILLS, TRACKED_SKILLS

companies, postings, meta = generate_all()

print("=== BASIC COUNTS ===")
print("companies:", len(companies), "(expect 400)")
print("postings raw file:", len(postings), "(expect 55350)")

# Original (pre-duplicate) 54000 rows are the first 54000 by construction
orig = postings.iloc[:54000]

print("\n=== DEFECT COUNTS (on original 54,000) ===")
print("empty skills_raw:", (orig["skills_raw"] == "").sum(), "(expect 900)")
print("Not specified:", (orig["skills_raw"] == "Not specified").sum(), "(expect 450)")
print("CMP999:", (orig["company_id"] == "CMP999").sum(), "(expect 540)")
print("bad date:", (orig["posted_date"] == "0000-00-00").sum(), "(expect 270)")

dup_count = len(postings) - orig["posting_id"].nunique()
print("duplicate rows appended:", len(postings) - 54000, "(expect 1350)")

print("\n=== TRACKED SKILL TOTALS (on ORIGINAL 54,000, case-insensitive substring) ===")
for skill in TRACKED_SKILLS:
    cnt = orig["skills_raw"].str.contains(skill, case=False, regex=False).sum()
    print(f"{skill}: {cnt}")

print("\n=== RAW SKILL VARIANT ANALYSIS ===")
variants = set()
for s in orig["skills_raw"]:
    if s in ("", "Not specified"):
        continue
    # naive split on both delimiters, keep raw (unnormalized) tokens
    parts = re.split(r"[;,]", s)
    for p in parts:
        variants.add(p)
print("distinct raw skill token strings:", len(variants), "(expect 57)")
canon = set(v.strip().lower() for v in variants)
print("distinct canonical (trim+lower) skills:", len(canon), "(expect 20)")
print("canonical set == 20 known skills?", canon == set(x.lower() for x in FILLER_SKILLS + TRACKED_SKILLS))
