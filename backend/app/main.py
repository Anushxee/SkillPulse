"""
Optional FastAPI data-serving layer for SkillPulse.

NOT required for the dashboard shipped in frontend/dist/index.html 
Run locally:
    uvicorn backend.app.main:app --reload --port 8000
"""
import json
import os

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
DATA_FILE = os.path.join(ROOT, "databricks", "06_export", "output", "dashboard_data.json")
VALIDATION_FILE = os.path.join(ROOT, "data", "validation_report.json")

app = FastAPI(title="SkillPulse API", version="1.0.0",
              description="Read-only data-serving layer over the SkillPulse Gold layer.")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def _load(path):
    if not os.path.exists(path):
        raise HTTPException(status_code=503,
                             detail=f"{os.path.basename(path)} not found — run scripts/run_pipeline.py "
                                    "and scripts/export_dashboard_data.py first.")
    with open(path) as f:
        return json.load(f)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/dashboard")
def dashboard():
    """Full dashboard payload — same shape the static frontend embeds."""
    return _load(DATA_FILE)


@app.get("/api/skill-trends/{skill}")
def skill_trend(skill: str):
    data = _load(DATA_FILE)
    skill = skill.lower()
    if skill not in data["skill_trends"]:
        raise HTTPException(status_code=404, detail=f"Unknown skill '{skill}'")
    return {"skill": skill, "months": data["skill_trends"][skill]}


@app.get("/api/relationships/{skill}")
def relationships(skill: str):
    data = _load(DATA_FILE)
    skill = skill.lower()
    if skill not in data["relationships"]:
        raise HTTPException(status_code=404, detail=f"Unknown skill '{skill}'")
    return {"skill": skill, "companions": data["relationships"][skill]}


@app.get("/api/growth")
def growth():
    data = _load(DATA_FILE)
    return {"quarter_growth": data["growth_latest_quarter"]}


@app.get("/api/data-quality")
def data_quality():
    data = _load(DATA_FILE)
    return data["data_quality"]


@app.get("/api/validation")
def validation():
    return _load(VALIDATION_FILE)
