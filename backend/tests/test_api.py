
import os
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from backend.app.main import app  # noqa: E402

client = TestClient(app)

DATA_PRESENT = os.path.exists(
    os.path.join(os.path.dirname(__file__), "..", "..", "databricks", "06_export", "output", "dashboard_data.json")
)


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


@pytest.mark.skipif(not DATA_PRESENT, reason="run scripts/export_dashboard_data.py first")
def test_dashboard_payload_shape():
    r = client.get("/api/dashboard")
    assert r.status_code == 200
    body = r.json()
    assert body["meta"]["kpi"]["companies"] == 400
    assert body["meta"]["kpi"]["canonical_skills"] == 20
    assert len(body["meta"]["months"]) == 18


@pytest.mark.skipif(not DATA_PRESENT, reason="run scripts/export_dashboard_data.py first")
def test_skill_trend_known_skill():
    r = client.get("/api/skill-trends/snowflake")
    assert r.status_code == 200
    body = r.json()
    assert body["skill"] == "snowflake"
    assert len(body["months"]) == 18
    total = sum(m["postings"] for m in body["months"])
    assert total == 12726


@pytest.mark.skipif(not DATA_PRESENT, reason="run scripts/export_dashboard_data.py first")
def test_skill_trend_unknown_skill_404():
    r = client.get("/api/skill-trends/cobol")
    assert r.status_code == 404


@pytest.mark.skipif(not DATA_PRESENT, reason="run scripts/export_dashboard_data.py first")
def test_relationships_endpoint():
    r = client.get("/api/relationships/dbt")
    assert r.status_code == 200
    companions = r.json()["companions"]
    assert any(c["companion"] == "snowflake" for c in companions)


@pytest.mark.skipif(not DATA_PRESENT, reason="run scripts/export_dashboard_data.py first")
def test_data_quality_endpoint():
    r = client.get("/api/data-quality")
    assert r.status_code == 200
    body = r.json()
    assert body["raw_postings"] == 55350
    assert body["clean_postings"] == 54000
