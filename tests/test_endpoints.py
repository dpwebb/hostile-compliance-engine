import json
import os
from io import BytesIO

from fastapi.testclient import TestClient
from pypdf import PdfWriter

from app.main import app


client = TestClient(app)


def build_blank_pdf() -> bytes:
    writer = PdfWriter()
    writer.add_blank_page(width=612, height=792)
    buffer = BytesIO()
    writer.write(buffer)
    return buffer.getvalue()


def upload_sample_pdf() -> str:
    response = client.post(
        "/upload",
        files={"file": ("sample.pdf", build_blank_pdf(), "application/pdf")},
    )
    assert response.status_code == 200, response.text
    return response.json()["doc_id"]


def test_upload_creates_observations() -> None:
    doc_id = upload_sample_pdf()

    document_resp = client.get(f"/documents/{doc_id}")
    assert document_resp.status_code == 200
    upload_path = document_resp.json()["upload_path"]
    assert os.path.exists(upload_path)

    observations_resp = client.get(f"/documents/{doc_id}/observations")
    assert observations_resp.status_code == 200
    observations = observations_resp.json()

    with open("tests/fixtures/golden_fields.json", "r", encoding="utf-8") as handle:
        golden = json.load(handle)

    expected_fields = set(golden["expected_fields"])
    observed_fields = {item["field_key"] for item in observations}
    assert expected_fields.issubset(observed_fields)

    for field_key in expected_fields:
        assert any(item["field_key"] == field_key for item in observations)


def test_resolved_is_deterministic() -> None:
    doc_id = upload_sample_pdf()

    first = client.get(f"/documents/{doc_id}/resolved")
    second = client.get(f"/documents/{doc_id}/resolved")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()
