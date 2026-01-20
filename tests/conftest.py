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

    shutil.rmtree(test_base_dir, ignore_errors=True)


@pytest.fixture(scope="function")
def sample_pdf_content():
    # Tiny valid-ish PDF bytes for testing upload
    return (
        b"%PDF-1.4\n"
        b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n"
        b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n"
        b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 200] >>\nendobj\n"
        b"xref\n0 4\n0000000000 65535 f \n"
        b"0000000010 00000 n \n0000000060 00000 n \n0000000120 00000 n \n"
        b"trailer\n<< /Size 4 /Root 1 0 R >>\nstartxref\n180\n%%EOF"
    )
