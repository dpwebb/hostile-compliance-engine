import io
import os
import tempfile
import shutil
import pytest
from fastapi.testclient import TestClient

from app.main import app  # Import the FastAPI instance


@pytest.fixture(scope="function")
def client():
    test_base_dir = tempfile.mkdtemp()
    test_upload_dir = os.path.join(test_base_dir, "uploads")
    test_observations_dir = os.path.join(test_base_dir, "observations")
    os.makedirs(test_upload_dir, exist_ok=True)
    os.makedirs(test_observations_dir, exist_ok=True)

    # Point app to test directories
    original_upload_dir = os.getenv("UPLOAD_DIR")
    os.environ["UPLOAD_DIR"] = test_upload_dir

    original_obs_dir = os.getenv("OBSERVATIONS_DIR")
    os.environ["OBSERVATIONS_DIR"] = test_observations_dir

    # Enable OCR by default so minimal (no-anchor) PDFs get OCR fallback and return 200
    original_enable_ocr = os.getenv("ENABLE_OCR")
    os.environ["ENABLE_OCR"] = "1"

    # If app.main defines these module-level dirs, update them too
    try:
        import app.main as main_mod
        if hasattr(main_mod, "UPLOAD_DIR"):
            main_mod.UPLOAD_DIR = test_upload_dir
        if hasattr(main_mod, "OBSERVATIONS_DIR"):
            main_mod.OBSERVATIONS_DIR = test_observations_dir
    except Exception:
        pass

    with TestClient(app) as test_client:
        yield test_client

    # Restore env
    if original_upload_dir is None:
        os.environ.pop("UPLOAD_DIR", None)
    else:
        os.environ["UPLOAD_DIR"] = original_upload_dir

    if original_obs_dir is None:
        os.environ.pop("OBSERVATIONS_DIR", None)
    else:
        os.environ["OBSERVATIONS_DIR"] = original_obs_dir

    if original_enable_ocr is None:
        os.environ.pop("ENABLE_OCR", None)
    else:
        os.environ["ENABLE_OCR"] = original_enable_ocr

    shutil.rmtree(test_base_dir, ignore_errors=True)


@pytest.fixture(scope="function")
def sample_pdf_content():
    # Tiny valid-ish PDF bytes for testing upload (no text layer -> no anchors -> OCR path when OCR enabled)
    return (
        b"%PDF-1.4\n"
        b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n"
        b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n"
        b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 200] >>\nendobj\n"
        b"xref\n0 4\n0000000000 65535 f \n"
        b"0000000010 00000 n \n0000000060 00000 n \n0000000120 00000 n \n"
        b"trailer\n<< /Size 4 /Root 1 0 R >>\nstartxref\n180\n%%EOF"
    )


@pytest.fixture(scope="function")
def text_based_pdf_content():
    """PDF with embedded text containing anchor strings (TransUnion, etc.) so anchor_hit is true."""
    try:
        import fitz
    except ImportError:
        pytest.skip("PyMuPDF required for text_based_pdf_content fixture")
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((72, 72), "TransUnion\nAccounts Summary\nPersonal Information\nCredit Report", fontsize=11)
    buf = io.BytesIO()
    doc.save(buf, deflate=True)
    doc.close()
    return buf.getvalue()
