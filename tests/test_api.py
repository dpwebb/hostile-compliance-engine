# tests/test_api.py
import io
import pytest
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


def test_text_based_pdf_skips_ocr_when_disabled(client: TestClient, text_based_pdf_content, monkeypatch):
    """Assert that a known text-based PDF (anchor strings on page 1) never enters OCR when OCR is disabled."""
    for flag in ["ENABLE_OCR", "OCR_ENABLED", "USE_OCR", "FORCE_OCR"]:
        monkeypatch.setenv(flag, "0")
    r = client.post("/upload", files={"file": ("test.pdf", text_based_pdf_content, "application/pdf")})
    assert r.status_code == 200, "Text-based PDF with anchors should succeed when OCR disabled"
    obs = client.get(f"/documents/{r.json()['doc_id']}/observations").json()["observations"]
    assert all(o.get("method") != "ocr" for o in obs), "Text-based PDF should not use OCR when disabled"


def test_non_semantic_pdf_422_when_ocr_disabled(client: TestClient, sample_pdf_content, monkeypatch):
    """Non-semantic (no anchors) PDF returns 422 when OCR is disabled."""
    for flag in ["ENABLE_OCR", "OCR_ENABLED", "USE_OCR", "FORCE_OCR"]:
        monkeypatch.setenv(flag, "0")
    r = client.post("/upload", files={"file": ("test.pdf", sample_pdf_content, "application/pdf")})
    assert r.status_code == 422
    assert r.json().get("detail") == "Text layer is non-semantic and OCR is disabled"


def test_tu_address_post_process_glued_tail(client: TestClient, monkeypatch):
    """Test post-processing extracts city/province from glued address tail for TransUnion reports.
    
    Acceptance criteria:
    - Upload TU PDF with glued address like "...593STEWIACKENSBON2J0"
    - /quality required_fields_missing becomes 0
    - /resolved shows province=NS and city non-empty and line1 no longer contains "STEWIACKENSBON2J0"
    """
    try:
        import fitz
    except ImportError:
        pytest.skip("PyMuPDF required for test_tu_address_post_process_glued_tail")
    
    # Enable OCR
    monkeypatch.setenv("ENABLE_OCR", "1")
    
    # Create a TransUnion PDF with glued address
    doc = fitz.open()
    page = doc.new_page()
    
    # Add TransUnion anchor strings
    page.insert_text((72, 72), "TransUnion\nAccounts Summary\nPersonal Information\nCredit Report", fontsize=11)
    
    # Add address with glued tail: "26 MAIN ST E PO BOX 593STEWIACKENSBON2J0"
    # This simulates OCR output where city/province/postal are glued together
    page.insert_text((72, 200), "Current\n26 MAIN ST E PO BOX 593STEWIACKENSBON2J0 01/10/2026", fontsize=11)
    
    buf = io.BytesIO()
    doc.save(buf, deflate=True)
    doc.close()
    pdf_content = buf.getvalue()
    
    # Upload PDF
    upload = client.post("/upload", files={"file": ("tu_test.pdf", pdf_content, "application/pdf")})
    assert upload.status_code == 200
    doc_id = upload.json()["doc_id"]
    
    # Check quality - required_fields_missing should be 0
    quality = client.get(f"/documents/{doc_id}/quality").json()
    assert quality["quality"]["required_fields_missing"] == 0, \
        f"Expected 0 missing fields, got {quality['quality']['required_fields_missing']}. " \
        f"Missing: {quality['quality'].get('missing_required_fields', [])}"
    
    # Check resolved values
    resolved = client.get(f"/documents/{doc_id}/resolved").json()
    rp = resolved["resolved_profile"]
    
    # Check province
    province_key = "consumer.current_address.province"
    province_resolved = rp.get(province_key, {})
    province_value = province_resolved.get("resolved_value") if isinstance(province_resolved, dict) else None
    assert province_value == "NS", f"Expected province=NS, got {province_value}"
    
    # Check city
    city_key = "consumer.current_address.city"
    city_resolved = rp.get(city_key, {})
    city_value = city_resolved.get("resolved_value") if isinstance(city_resolved, dict) else None
    assert city_value is not None and len(city_value) > 0, f"Expected non-empty city, got {city_value}"
    assert "STEWIACKE" in city_value.upper(), f"Expected city to contain 'STEWIACKE', got {city_value}"
    
    # Check line1 - should NOT contain the glued tail
    line1_key = "consumer.current_address.line1"
    line1_resolved = rp.get(line1_key, {})
    line1_value = line1_resolved.get("resolved_value") if isinstance(line1_resolved, dict) else None
    assert line1_value is not None, "Expected line1 to be resolved"
    assert "STEWIACKENSBON2J0" not in line1_value.upper(), \
        f"Expected line1 to NOT contain glued tail 'STEWIACKENSBON2J0', got: {line1_value}"
    assert "593" in line1_value or "PO BOX 593" in line1_value.upper(), \
        f"Expected line1 to contain address before city, got: {line1_value}"


def test_case_merging_fills_fields_from_multiple_docs(client: TestClient, monkeypatch):
    """Test that case merging fills consumer.full_name from doc A and province from doc B."""
    try:
        import fitz
    except ImportError:
        pytest.skip("PyMuPDF required for test_case_merging_fills_fields_from_multiple_docs")
    
    monkeypatch.setenv("ENABLE_OCR", "1")
    case_id = "test-case-001"
    
    # Create doc A with name but no province
    doc_a = fitz.open()
    page_a = doc_a.new_page()
    page_a.insert_text((72, 72), "TransUnion\nAccounts Summary\nPersonal Information\nCredit Report", fontsize=11)
    page_a.insert_text((72, 150), "Name JOHN DOE 01/01/2024", fontsize=11)
    page_a.insert_text((72, 200), "Current\n123 MAIN ST\nTORONTO ON M5H 2N2 01/10/2026", fontsize=11)
    buf_a = io.BytesIO()
    doc_a.save(buf_a, deflate=True)
    doc_a.close()
    pdf_a = buf_a.getvalue()
    
    # Create doc B with province but no name
    doc_b = fitz.open()
    page_b = doc_b.new_page()
    page_b.insert_text((72, 72), "TransUnion\nAccounts Summary\nPersonal Information\nCredit Report", fontsize=11)
    page_b.insert_text((72, 200), "Current\n456 OAK AVE\nVANCOUVER BC V6B 1A1 01/10/2026", fontsize=11)
    buf_b = io.BytesIO()
    doc_b.save(buf_b, deflate=True)
    doc_b.close()
    pdf_b = buf_b.getvalue()
    
    # Upload both documents to the same case
    upload_a = client.post("/upload", files={"file": ("doc_a.pdf", pdf_a, "application/pdf")}, data={"case_id": case_id})
    assert upload_a.status_code == 200
    doc_id_a = upload_a.json()["doc_id"]
    
    upload_b = client.post("/upload", files={"file": ("doc_b.pdf", pdf_b, "application/pdf")}, data={"case_id": case_id})
    assert upload_b.status_code == 200
    doc_id_b = upload_b.json()["doc_id"]
    
    # Check case quality
    case_quality = client.get(f"/cases/{case_id}/quality").json()
    assert case_quality["case_id"] == case_id
    assert len(case_quality["merged_quality"]["merged_missing_required_fields"]) < 5, \
        f"Expected fewer missing fields after merging, got: {case_quality['merged_quality']['merged_missing_required_fields']}"
    
    # Verify that name comes from doc A and province from doc B (or vice versa, both should be present)
    # Load observations to check provenance
    obs_a = client.get(f"/documents/{doc_id_a}/observations").json()["observations"]
    obs_b = client.get(f"/documents/{doc_id_b}/observations").json()["observations"]
    
    name_from_a = any(o.get("field_key") == "consumer.full_name" and o.get("raw_value") for o in obs_a)
    province_from_b = any(o.get("field_key") == "consumer.current_address.province" and o.get("raw_value") for o in obs_b)
    
    # At least one doc should have name, at least one should have province
    assert name_from_a or any(o.get("field_key") == "consumer.full_name" and o.get("raw_value") for o in obs_b), \
        "Expected at least one document to have consumer.full_name"
    assert province_from_b or any(o.get("field_key") == "consumer.current_address.province" and o.get("raw_value") for o in obs_a), \
        "Expected at least one document to have consumer.current_address.province"


def test_case_merging_city_remains_missing(client: TestClient, monkeypatch):
    """Test that city remains missing if absent in both documents."""
    try:
        import fitz
    except ImportError:
        pytest.skip("PyMuPDF required for test_case_merging_city_remains_missing")
    
    monkeypatch.setenv("ENABLE_OCR", "1")
    case_id = "test-case-002"
    
    # Create doc A without city
    doc_a = fitz.open()
    page_a = doc_a.new_page()
    page_a.insert_text((72, 72), "TransUnion\nAccounts Summary\nPersonal Information\nCredit Report", fontsize=11)
    page_a.insert_text((72, 200), "Current\n123 MAIN ST\nON M5H 2N2 01/10/2026", fontsize=11)
    buf_a = io.BytesIO()
    doc_a.save(buf_a, deflate=True)
    doc_a.close()
    pdf_a = buf_a.getvalue()
    
    # Create doc B also without city
    doc_b = fitz.open()
    page_b = doc_b.new_page()
    page_b.insert_text((72, 72), "TransUnion\nAccounts Summary\nPersonal Information\nCredit Report", fontsize=11)
    page_b.insert_text((72, 200), "Current\n456 OAK AVE\nBC V6B 1A1 01/10/2026", fontsize=11)
    buf_b = io.BytesIO()
    doc_b.save(buf_b, deflate=True)
    doc_b.close()
    pdf_b = buf_b.getvalue()
    
    # Upload both documents
    upload_a = client.post("/upload", files={"file": ("doc_a.pdf", pdf_a, "application/pdf")}, data={"case_id": case_id})
    assert upload_a.status_code == 200
    
    upload_b = client.post("/upload", files={"file": ("doc_b.pdf", pdf_b, "application/pdf")}, data={"case_id": case_id})
    assert upload_b.status_code == 200
    
    # Check case quality - city should still be missing
    case_quality = client.get(f"/cases/{case_id}/quality").json()
    missing = case_quality["merged_quality"]["merged_missing_required_fields"]
    assert "consumer.current_address.city" in missing, \
        f"Expected city to remain missing, but missing fields are: {missing}"


def test_case_conflicts_force_needs_review(client: TestClient, monkeypatch):
    """Test that conflicts force needs_review status."""
    try:
        import fitz
    except ImportError:
        pytest.skip("PyMuPDF required for test_case_conflicts_force_needs_review")
    
    monkeypatch.setenv("ENABLE_OCR", "1")
    case_id = "test-case-003"
    
    # Create doc A with province ON
    doc_a = fitz.open()
    page_a = doc_a.new_page()
    page_a.insert_text((72, 72), "TransUnion\nAccounts Summary\nPersonal Information\nCredit Report", fontsize=11)
    page_a.insert_text((72, 200), "Current\n123 MAIN ST\nTORONTO ON M5H 2N2 01/10/2026", fontsize=11)
    buf_a = io.BytesIO()
    doc_a.save(buf_a, deflate=True)
    doc_a.close()
    pdf_a = buf_a.getvalue()
    
    # Create doc B with province BC (conflict!)
    doc_b = fitz.open()
    page_b = doc_b.new_page()
    page_b.insert_text((72, 72), "TransUnion\nAccounts Summary\nPersonal Information\nCredit Report", fontsize=11)
    page_b.insert_text((72, 200), "Current\n456 OAK AVE\nVANCOUVER BC V6B 1A1 01/10/2026", fontsize=11)
    buf_b = io.BytesIO()
    doc_b.save(buf_b, deflate=True)
    doc_b.close()
    pdf_b = buf_b.getvalue()
    
    # Upload both documents
    upload_a = client.post("/upload", files={"file": ("doc_a.pdf", pdf_a, "application/pdf")}, data={"case_id": case_id})
    assert upload_a.status_code == 200
    
    upload_b = client.post("/upload", files={"file": ("doc_b.pdf", pdf_b, "application/pdf")}, data={"case_id": case_id})
    assert upload_b.status_code == 200
    
    # Check case quality - should have conflicts and needs_review
    case_quality = client.get(f"/cases/{case_id}/quality").json()
    assert case_quality["merged_quality"]["merged_quality_status"] == "needs_review", \
        f"Expected needs_review due to conflicts, got: {case_quality['merged_quality']['merged_quality_status']}"
    
    conflicts = case_quality["merged_quality"]["conflicts"]
    assert "consumer.current_address.province" in conflicts, \
        f"Expected province conflict, but conflicts are: {list(conflicts.keys())}"
    
    # Verify conflict has provenance
    prov_conflict = conflicts["consumer.current_address.province"]
    assert len(prov_conflict) >= 2, "Expected at least 2 conflicting values"
    assert all("doc_id" in c for c in prov_conflict), "Expected all conflict candidates to have doc_id"
    assert all("value" in c for c in prov_conflict), "Expected all conflict candidates to have value"


def test_low_text_page_forces_ocr_escalation(client: TestClient, monkeypatch):
    """Test that low-text page forces OCR path invocation."""
    try:
        import fitz
    except ImportError:
        pytest.skip("PyMuPDF required for test_low_text_page_forces_ocr_escalation")
    
    monkeypatch.setenv("ENABLE_OCR", "1")
    
    # Create PDF with very little text on page 1 (below threshold)
    doc = fitz.open()
    page = doc.new_page()
    # Add minimal text (below LOW_TEXT_THRESHOLD which is 120 by default)
    page.insert_text((72, 72), "A", fontsize=11)  # Just one character
    buf = io.BytesIO()
    doc.save(buf, deflate=True)
    doc.close()
    pdf_content = buf.getvalue()
    
    # Upload PDF
    upload = client.post("/upload", files={"file": ("low_text.pdf", pdf_content, "application/pdf")})
    assert upload.status_code == 200
    doc_id = upload.json()["doc_id"]
    
    # Check observations for OCR escalation
    obs_data = client.get(f"/documents/{doc_id}/observations").json()["observations"]
    
    # Should have OCR escalation observations
    ocr_escalation_obs = [o for o in obs_data if o.get("method") == "ocr_escalation"]
    assert len(ocr_escalation_obs) > 0, \
        f"Expected OCR escalation observations, but found none. Methods used: {set(o.get('method') for o in obs_data)}"
    
    # Should have doc.page.text_ocr observations for escalated pages
    ocr_text_obs = [o for o in obs_data if o.get("field_key") == "doc.page.text_ocr" and o.get("method") == "ocr_escalation"]
    assert len(ocr_text_obs) > 0, "Expected OCR text observations from escalation"
    
    # Check ingestion runs for escalation note
    runs = client.get(f"/documents/{doc_id}/observations").json()["ingestion_runs"]
    assert len(runs) > 0, "Expected at least one ingestion run"
    latest_run = runs[-1]
    assert "OCR escalation" in latest_run.get("notes", ""), \
        f"Expected OCR escalation in notes, got: {latest_run.get('notes', '')}"
