# tests/test_api.py
from starlette.testclient import TestClient


def test_health(client: TestClient):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["ok"] is True


def test_upload_pdf(client: TestClient, sample_pdf_content):
    r = client.post("/upload", files={"file": ("test.pdf", sample_pdf_content, "application/pdf")})
    assert r.status_code == 200
    data = r.json()
    assert "doc_id" in data
    assert data["filename"] == "test.pdf"
    assert "sha256" in data
    assert "ingestion_run_id" in data


def test_upload_non_pdf(client: TestClient):
    r = client.post("/upload", files={"file": ("test.txt", b"nope", "text/plain")})
    assert r.status_code == 400


def test_get_observations(client: TestClient, sample_pdf_content):
    upload = client.post("/upload", files={"file": ("test.pdf", sample_pdf_content, "application/pdf")})
    doc_id = upload.json()["doc_id"]

    r = client.get(f"/documents/{doc_id}/observations")
    assert r.status_code == 200
    payload = r.json()

    assert payload["doc_id"] == doc_id
    assert "ingestion_runs" in payload
    assert "observations" in payload

    obs = payload["observations"]
    assert isinstance(obs, list)
    assert len(obs) >= 6  # doc meta + per-page + required identity missing/extracted

    for o in obs:
        assert "obs_id" in o
        assert "ingestion_run_id" in o
        assert "field_key" in o
        assert "raw_value" in o
        assert "method" in o
        assert "confidence" in o
        assert isinstance(o["confidence"], (int, float))
        assert 0.0 <= float(o["confidence"]) <= 1.0
        assert "page_number" in o
        assert "anchor_violation" in o
        assert "status" in o

    meta = [o for o in obs if o["field_key"].startswith("doc.meta.")]
    assert len(meta) >= 5
    for o in meta:
        assert o["page_number"] is None

    page_obs = [o for o in obs if o["field_key"] == "doc.page.text_length"]
    assert len(page_obs) >= 1
    for o in page_obs:
        assert isinstance(o["page_number"], int)
        assert o["page_number"] >= 1
        assert o["anchor_violation"] is False


def test_get_resolved(client: TestClient, sample_pdf_content):
    upload = client.post("/upload", files={"file": ("test.pdf", sample_pdf_content, "application/pdf")})
    doc_id = upload.json()["doc_id"]

    r = client.get(f"/documents/{doc_id}/resolved")
    assert r.status_code == 200
    data = r.json()
    assert data["doc_id"] == doc_id
    assert "resolved_profile" in data
    rp = data["resolved_profile"]
    assert "doc.meta.page_count" in rp or any(k.endswith("doc.meta.page_count") for k in rp.keys())


def test_get_quality(client: TestClient, sample_pdf_content):
    upload = client.post("/upload", files={"file": ("test.pdf", sample_pdf_content, "application/pdf")})
    doc_id = upload.json()["doc_id"]

    r = client.get(f"/documents/{doc_id}/quality")
    assert r.status_code == 200
    q = r.json()["quality"]
    assert "total_observations" in q
    assert "anchor_violations" in q
    assert q["quality_status"] in ("ok", "needs_review")
