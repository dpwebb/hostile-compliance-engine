import os
import uuid
import hashlib
import json
from datetime import datetime, timezone
from io import BytesIO

from fastapi import FastAPI, UploadFile, File, HTTPException
from pypdf import PdfReader

app = FastAPI(title="Hostile Compliance Engine (Ingestion v0)")

UPLOAD_DIR = os.getenv("UPLOAD_DIR", "./data/uploads")
OBSERVATIONS_DIR = os.getenv("OBSERVATIONS_DIR", "./data/observations")

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OBSERVATIONS_DIR, exist_ok=True)


@app.get("/health")
def health():
    return {"ok": True}


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _classify_bureau(all_text: str) -> tuple[str, float]:
    t = (all_text or "").lower()
    if "transunion" in t or "trans union" in t:
        return "transunion", 0.9
    if "equifax" in t:
        return "equifax", 0.9
    return "unknown", 0.5


def _section_keywords():
    # deterministic, tweakable, no AI guesses
    return {
        "identity": ["personal information", "consumer information", "identification", "date of birth"],
        "addresses": ["address", "addresses", "current address", "previous address"],
        "employment": ["employer", "employment"],
        "accounts": ["accounts", "credit accounts", "tradeline", "trade line", "creditor", "balance", "payment", "account"],
        "inquiries": ["inquiries", "who has accessed", "credit inquiries"],
        "public_records": ["public records", "bankruptcy", "judgment", "lien"],
    }


def _count_hits(text: str, keywords: list[str]) -> int:
    t = (text or "").lower()
    return sum(1 for kw in keywords if kw in t)


@app.post("/upload")
async def upload_pdf(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are allowed")

    doc_id = str(uuid.uuid4())
    stored_filename = f"{doc_id}.pdf"
    stored_path = os.path.join(UPLOAD_DIR, stored_filename)

    content = await file.read()

    # Save PDF
    with open(stored_path, "wb") as f:
        f.write(content)

    sha256 = hashlib.sha256(content).hexdigest()
    byte_size = len(content)

    # Parse PDF
    reader = PdfReader(BytesIO(content))
    page_count = len(reader.pages)

    text_parts: list[str] = [(p.extract_text() or "") for p in reader.pages]
    extracted_text = "\n".join(text_parts)
    text_length = len(extracted_text)

    created_at = _now_iso()

    # ---- observations (document-level + page-level) ----
    observations: list[dict] = [
        {"field_key": "doc.meta.original_filename", "raw_value": file.filename, "page_number": None, "method": "text", "confidence": 1.0, "created_at": created_at},
        {"field_key": "doc.meta.stored_filename", "raw_value": stored_filename, "page_number": None, "method": "text", "confidence": 1.0, "created_at": created_at},
        {"field_key": "doc.meta.sha256", "raw_value": sha256, "page_number": None, "method": "text", "confidence": 1.0, "created_at": created_at},
        {"field_key": "doc.meta.byte_size", "raw_value": byte_size, "page_number": None, "method": "text", "confidence": 1.0, "created_at": created_at},
        {"field_key": "doc.meta.page_count", "raw_value": page_count, "page_number": None, "method": "text", "confidence": 1.0, "created_at": created_at},
        {"field_key": "doc.meta.text_length", "raw_value": text_length, "page_number": None, "method": "text", "confidence": 1.0, "created_at": created_at},
    ]

    # Bureau classification (document-level)
    bureau, bureau_conf = _classify_bureau(extracted_text)
    observations.append(
        {"field_key": "doc.classification.bureau", "raw_value": bureau, "page_number": None, "method": "text", "confidence": bureau_conf, "created_at": created_at}
    )

    # Page-level: text length per page
    for idx, page_text in enumerate(text_parts, start=1):  # 1-based
        observations.append(
            {"field_key": "page.text_length", "raw_value": len(page_text), "page_number": idx, "method": "text", "confidence": 1.0, "created_at": created_at}
        )

    # Section mapping (document-level) from keyword hits per page
    sections = _section_keywords()
    for section_name, kws in sections.items():
        hit_pages: list[int] = []
        total_hits = 0
        for idx, page_text in enumerate(text_parts, start=1):
            hits = _count_hits(page_text, kws)
            if hits > 0:
                hit_pages.append(idx)
                total_hits += hits

        if hit_pages:
            start = min(hit_pages)
            end = max(hit_pages)
            conf = 0.8 if total_hits >= 3 else 0.7
            observations.append(
                {"field_key": f"section.{section_name}.page_range", "raw_value": f"{start}-{end}", "page_number": None, "method": "text", "confidence": conf, "created_at": created_at}
            )

    # Save observations JSON
    obs_path = os.path.join(OBSERVATIONS_DIR, f"{doc_id}.json")
    with open(obs_path, "w", encoding="utf-8") as f:
        json.dump({"doc_id": doc_id, "observations": observations}, f, ensure_ascii=False, indent=2)

    return {"doc_id": doc_id, "filename": file.filename, "stored_as": stored_path, "sha256": sha256}


@app.get("/documents/{doc_id}/observations")
def get_observations(doc_id: str):
    obs_path = os.path.join(OBSERVATIONS_DIR, f"{doc_id}.json")
    if not os.path.exists(obs_path):
        raise HTTPException(status_code=404, detail="Document observations not found")

    with open(obs_path, "r", encoding="utf-8") as f:
        return json.load(f)


@app.get("/documents/{doc_id}/resolved")
def get_resolved(doc_id: str):
    obs_path = os.path.join(OBSERVATIONS_DIR, f"{doc_id}.json")
    if not os.path.exists(obs_path):
        raise HTTPException(status_code=404, detail="Document observations not found")

    with open(obs_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    resolved = {}
    for obs in payload.get("observations", []):
        key = obs.get("field_key")
        if isinstance(key, str) and key.startswith("doc.meta."):
            resolved[key] = obs.get("raw_value")
        if key == "doc.classification.bureau":
            resolved[key] = obs.get("raw_value")

    return {"doc_id": doc_id, "resolved_profile": resolved}
