from fastapi.testclient import TestClient


def test_health(client: TestClient):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["ok"] is True


def test_upload_pdf(client: TestClient, sample_pdf_content):
    response = client.post(
        "/upload",
        files={"file": ("test.pdf", sample_pdf_content, "application/pdf")}
    )
    assert response.status_code == 200
    data = response.json()
    assert "doc_id" in data
    assert data["filename"] == "test.pdf"
    assert "sha256" in data


def test_upload_non_pdf(client: TestClient):
    response = client.post(
        "/upload",
        files={"file": ("test.txt", b"not a pdf", "text/plain")}
    )
    assert response.status_code == 400


def test_get_observations(client: TestClient, sample_pdf_content):
    upload_response = client.post(
        "/upload",
        files={"file": ("test.pdf", sample_pdf_content, "application/pdf")}
    )
    assert upload_response.status_code == 200
    doc_id = upload_response.json()["doc_id"]

    response = client.get(f"/documents/{doc_id}/observations")
    assert response.status_code == 200
    data = response.json()

    assert data["doc_id"] == doc_id
    assert "observations" in data

    observations = data["observations"]
    assert isinstance(observations, list)
    assert len(observations) >= 4

    # verify observation structure (document-level OR page-level)
    for obs in observations:
        assert "field_key" in obs
        assert "raw_value" in obs
        assert "method" in obs
        assert obs["method"] == "text"
        assert "confidence" in obs
        assert isinstance(obs["confidence"], (int, float))
        assert "page_number" in obs

        pn = obs["page_number"]
        assert (pn is None) or (isinstance(pn, int) and pn >= 1)


def test_get_observations_not_found(client: TestClient):
    response = client.get("/documents/not-a-real-id/observations")
    assert response.status_code == 404


def test_get_resolved(client: TestClient, sample_pdf_content):
    upload_response = client.post(
        "/upload",
        files={"file": ("test.pdf", sample_pdf_content, "application/pdf")}
    )
    assert upload_response.status_code == 200
    doc_id = upload_response.json()["doc_id"]

    response = client.get(f"/documents/{doc_id}/resolved")
    assert response.status_code == 200
    data = response.json()

    assert data["doc_id"] == doc_id
    assert "resolved_profile" in data
    rp = data["resolved_profile"]
    assert "doc.meta.sha256" in rp
    assert "doc.meta.page_count" in rp


def test_get_resolved_not_found(client: TestClient):
    response = client.get("/documents/not-a-real-id/resolved")
    assert response.status_code == 404
