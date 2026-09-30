"""Embeds databricks/06_export/output/dashboard_data.json into
frontend/index.html (replacing the __DASHBOARD_DATA__ placeholder) to
produce a single self-contained, deployable file. This is the same file
that gets published as the hosted dashboard and shipped in the repo as
frontend/dist/index.html.

Run scripts/run_pipeline.py and scripts/export_dashboard_data.py first.
"""
import json
import os

ROOT = os.path.join(os.path.dirname(__file__), "..")
TEMPLATE = os.path.join(ROOT, "frontend", "index.html")
DATA_FILE = os.path.join(ROOT, "databricks", "06_export", "output", "dashboard_data.json")
OUT_DIR = os.path.join(ROOT, "frontend", "dist")
OUT_FILE = os.path.join(OUT_DIR, "index.html")


def main():
    with open(TEMPLATE, "r", encoding="utf-8") as f:
        html = f.read()
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        data_json = f.read()
    # sanity: valid JSON
    json.loads(data_json)

    built = html.replace("__DASHBOARD_DATA__", data_json)
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        f.write(built)
    print(f"Built {OUT_FILE} ({os.path.getsize(OUT_FILE)/1024:.1f} KB)")


if __name__ == "__main__":
    main()
