from __future__ import annotations

import hashlib
import os
import uuid
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, File, HTTPException, UploadFile
from pypdf import PdfReader

from app.observation_registry import canonical_fields, required_field_keys
from app.observation_store import (
    ensure_doc_dirs,
    load_ingestion_runs,
    load_observations,
    save_ingestion_runs,
    save_observations,
)
from app.pdf_extractor import extract_identity_from_pages

app = FastAPI(title="Hostile Compliance Engine (Ingestion v0)")

UPLOAD_DIR = os.getenv("UPLOAD_DIR", "./data/uploads")
OBSERVATIONS_DIR = os.getenv("OBSERVATIONS_DIR", "./data/observations")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OBSERVATIONS_DIR, exist_ok=True)

LOW_TEXT_THRESHOLD = int(os.getenv("LOW_TEXT_THRESHOLD", "120"))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def anchor_from_page_text(page_text: str, raw_value: str) -> Dict[str, Any]:
    if page_text is None:
        page_text = ""
    if raw_value is None:
        raw_value = ""

    idx = page_text.find(raw_value)
    if idx >= 0 and raw_value:
        before = page_text[max(0, idx - 30): idx]
        after = page_text[idx + len(raw_value): idx + len(raw_value) + 30]
        snippet = before + raw_value + after
        return {
            "anchor_text_before": before,
            "anchor_text_after": after,
            "anchor_hash": sha256_hex(snippet.encode("utf-8", errors="ignore")),
            "anchor_strength": "strong",
        }

    return {
        "anchor_text_before": "",
        "anchor_text_after": "",
        "anchor_hash": sha256_hex(raw_value.encode("utf-8", errors="ignore")),
        "anchor_strength": "weak" if raw_value else "none",
    }


def obs(
    *,
    doc_id: str,
    ingestion_run_id: str,
    field_key: str,
    raw_value: Any,
    method: str,
    confidence: float,
    status: str = "extracted",
    page_number: Optional[int] = None,
    entity_id: Optional[str] = None,
    scope: Optional[str] = None,
    reason: Optional[str] = None,
    anchor: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "obs_id": str(uuid.uuid4()),
        "doc_id": doc_id,
        "ingestion_run_id": ingestion_run_id,
        "field_key": field_key,
        "entity_id": entity_id,
        "raw_value": raw_value,
        "page_number": page_number,
        "method": method,
        "confidence": float(confidence),
        "created_at": utc_now_iso(),
        "status": status,
        "anchor_violation": False,
        "scope": scope,
        "reason": reason,
        "anchor": anchor or {
            "anchor_text_before": "",
            "anchor_text_after": "",
            "anchor_hash": "",
            "anchor_strength": "none",
        },
    }


def resolve_profile(observations: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_key: Dict[str, List[Dict[str, Any]]] = {}
    for o in observations:
        k = o["field_key"]
        if o.get("entity_id"):
            k = f"{o['entity_id']}.{o['field_key']}"
        by_key.setdefault(k, []).append(o)

    resolved: Dict[str, Any] = {}
    for k, items in by_key.items():
        items_sorted = sorted(
            items,
            key=lambda x: (float(x.get("confidence", 0.0)), x.get("created_at", "")),
            reverse=True,
        )
        best = items_sorted[0]
        candidates = []
        for i in items_sorted[1:]:
            candidates.append(
                {
                    "observation_id": i["obs_id"],
                    "raw_value": i["raw_value"],
                    "confidence": i.get("confidence", 0.0),
                    "method": i.get("method"),
                    "status": i.get("status"),
                }
            )

        resolved[k] = {
            "resolved_value": best["raw_value"],
            "resolution_status": "resolved",
            "best_observation_id": best["obs_id"],
            "candidates": candidates,
        }
    return resolved


def emit_missing_required_identity(
    *,
    doc_id: str,
    ingestion_run_id: str,
    observations: List[Dict[str, Any]],
) -> None:
    """
    Never silently omit required identity fields.
    If extraction yields nothing, we still emit missing observations so downstream can audit.
    """
    required = {
        "consumer.full_name",
        "consumer.current_address.line1",
        "consumer.current_address.city",
        "consumer.current_address.province",
        "consumer.current_address.postal_code",
    }

    present = {o["field_key"] for o in observations if o.get("status") == "extracted"}
    missing = sorted(list(required - present))

    for k in missing:
        observations.append(
            obs(
                doc_id=doc_id,
                ingestion_run_id=ingestion_run_id,
                field_key=k,
                raw_value="",
                method="missing",
                confidence=0.0,
                status="missing",
                reason="Required field not found in extracted text",
                page_number=None,
                anchor={
                    "anchor_text_before": "",
                    "anchor_text_after": "",
                    "anchor_hash": "",
                    "anchor_strength": "none",
                },
            )
        )


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/schema/fields")
def schema_fields():
    fields = []
    for f in canonical_fields():
        fields.append(
            {
                "field_key": f.field_key,
                "label": f.label,
                "scope": f.scope,
                "entity_type": f.entity_type,
                "value_type": f.value_type,
                "required": f.required,
                "description": f.description,
                "examples": f.examples or [],
            }
        )
    return {"fields": fields}


@app.post("/upload")
async def upload_pdf(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are supported")

    doc_id = str(uuid.uuid4())
    stored_filename = f"{doc_id}.pdf"
    stored_path = os.path.join(UPLOAD_DIR, stored_filename)

    content = await file.read()
    with open(stored_path, "wb") as f:
        f.write(content)

    ingestion_run_id = str(uuid.uuid4())
    created_at = utc_now_iso()

    reader = PdfReader(BytesIO(content))
    page_count = len(reader.pages)

    page_texts: List[str] = []
    for p in reader.pages:
        page_texts.append(p.extract_text() or "")

    total_text = "\n".join(page_texts)
    total_text_len = len(total_text)

    ensure_doc_dirs(OBSERVATIONS_DIR, doc_id)

    runs = load_ingestion_runs(OBSERVATIONS_DIR, doc_id)
    runs.append(
        {
            "ingestion_run_id": ingestion_run_id,
            "created_at": created_at,
            "source_filename": file.filename,
            "stored_filename": stored_filename,
            "method": "text-first",
            "notes": "Deterministic extraction only. No OCR/AI in v1 mechanics.",
        }
    )
    save_ingestion_runs(OBSERVATIONS_DIR, doc_id, runs)

    observations: List[Dict[str, Any]] = []

    # Document meta (derived)
    observations.append(
        obs(doc_id=doc_id, ingestion_run_id=ingestion_run_id, field_key="doc.meta.original_filename", raw_value=file.filename, method="derived", confidence=1.0)
    )
    observations.append(
        obs(doc_id=doc_id, ingestion_run_id=ingestion_run_id, field_key="doc.meta.stored_filename", raw_value=stored_filename, method="derived", confidence=1.0)
    )
    observations.append(
        obs(doc_id=doc_id, ingestion_run_id=ingestion_run_id, field_key="doc.meta.sha256", raw_value=sha256_hex(content), method="derived", confidence=1.0)
    )
    observations.append(
        obs(doc_id=doc_id, ingestion_run_id=ingestion_run_id, field_key="doc.meta.byte_size", raw_value=len(content), method="derived", confidence=1.0)
    )
    observations.append(
        obs(doc_id=doc_id, ingestion_run_id=ingestion_run_id, field_key="doc.meta.page_count", raw_value=page_count, method="derived", confidence=1.0)
    )
    observations.append(
        obs(doc_id=doc_id, ingestion_run_id=ingestion_run_id, field_key="doc.meta.text_length", raw_value=total_text_len, method="text", confidence=1.0)
    )

    # Page-level: text length per page (anchored)
    for i, t in enumerate(page_texts, start=1):
        observations.append(
            obs(
                doc_id=doc_id,
                ingestion_run_id=ingestion_run_id,
                field_key="doc.page.text_length",
                entity_id=f"page:{i}",
                page_number=i,
                raw_value=len(t),
                method="text",
                confidence=1.0,
                anchor=anchor_from_page_text(t, str(len(t))),
            )
        )

    # Bureau detection (deterministic)
    bureau = "Unknown"
    joined = total_text.lower()
    if "transunion" in joined:
        bureau = "TransUnion"
    elif "equifax" in joined:
        bureau = "Equifax"
    observations.append(
        obs(
            doc_id=doc_id,
            ingestion_run_id=ingestion_run_id,
            field_key="report.bureau",
            raw_value=bureau,
            method="derived",
            confidence=0.9,
        )
    )

    # Identity extraction v1 (deterministic)
    identity = extract_identity_from_pages(page_texts)
    consumer = identity.get("consumer", {}) or {}
    evidence = identity.get("evidence", {}) or {}

    # Name
    if consumer.get("full_name"):
        ev = evidence.get("full_name")
        page_num = ev[0] if ev else None
        page_text = page_texts[page_num - 1] if page_num and 1 <= page_num <= len(page_texts) else ""
        observations.append(
            obs(
                doc_id=doc_id,
                ingestion_run_id=ingestion_run_id,
                field_key="consumer.full_name",
                raw_value=consumer["full_name"],
                page_number=page_num,
                method="text",
                confidence=0.85,
                anchor=anchor_from_page_text(page_text, str(consumer["full_name"])),
            )
        )

    # Address block
    addr = consumer.get("current_address", {}) or {}
    addr_ev = evidence.get("address_block")
    addr_page = addr_ev[0] if addr_ev else None
    addr_page_text = page_texts[addr_page - 1] if addr_page and 1 <= addr_page <= len(page_texts) else ""

    def add_addr_obs(key: str, value: Any, conf: float):
        if value:
            observations.append(
                obs(
                    doc_id=doc_id,
                    ingestion_run_id=ingestion_run_id,
                    field_key=key,
                    raw_value=value,
                    page_number=addr_page,
                    method="text",
                    confidence=conf,
                    anchor=anchor_from_page_text(addr_page_text, str(value)),
                )
            )

    add_addr_obs("consumer.current_address.line1", addr.get("line1"), 0.80)
    add_addr_obs("consumer.current_address.city", addr.get("city"), 0.70)
    add_addr_obs("consumer.current_address.province", addr.get("province"), 0.90)
    add_addr_obs("consumer.current_address.postal_code", addr.get("postal_code"), 0.95)

    # Critical: emit missing required identity fields
    emit_missing_required_identity(doc_id=doc_id, ingestion_run_id=ingestion_run_id, observations=observations)

    save_observations(OBSERVATIONS_DIR, doc_id, observations)

    return {
        "doc_id": doc_id,
        "filename": file.filename,
        "stored_as": stored_path,
        "sha256": sha256_hex(content),
        "ingestion_run_id": ingestion_run_id,
    }


@app.get("/documents/{doc_id}/observations")
def get_observations(doc_id: str):
    runs = load_ingestion_runs(OBSERVATIONS_DIR, doc_id)
    observations = load_observations(OBSERVATIONS_DIR, doc_id)
    if not observations:
        raise HTTPException(status_code=404, detail="Document observations not found")
    return {"doc_id": doc_id, "ingestion_runs": runs, "observations": observations}


@app.get("/documents/{doc_id}/resolved")
def get_resolved(doc_id: str):
    observations = load_observations(OBSERVATIONS_DIR, doc_id)
    if not observations:
        raise HTTPException(status_code=404, detail="Document observations not found")
    return {"doc_id": doc_id, "resolved_profile": resolve_profile(observations)}


@app.get("/documents/{doc_id}/quality")
def quality(doc_id: str):
    observations = load_observations(OBSERVATIONS_DIR, doc_id)
    if not observations:
        raise HTTPException(status_code=404, detail="Document observations not found")

    present_keys = set()
    for o in observations:
        if o.get("field_key"):
            present_keys.add(o["field_key"])

    required = required_field_keys()
    missing_required = [k for k in required if k not in present_keys]

    low_text_pages = []
    for o in observations:
        if o["field_key"] == "doc.page.text_length" and isinstance(o.get("raw_value"), int):
            if o["raw_value"] < LOW_TEXT_THRESHOLD:
                low_text_pages.append(
                    {
                        "page_number": o.get("page_number"),
                        "text_length": o.get("raw_value"),
                        "obs_id": o.get("obs_id"),
                    }
                )

    anchor_violations = []
    for o in observations:
        a = o.get("anchor") or {}
        strength = a.get("anchor_strength", "none")
        h = a.get("anchor_hash", "")
        if strength == "none":
            continue
        if strength in ["weak", "strong"] and not h:
            anchor_violations.append(
                {
                    "obs_id": o.get("obs_id"),
                    "field_key": o.get("field_key"),
                    "page_number": o.get("page_number"),
                    "anchor_strength": strength,
                }
            )

    # quality_status is intentionally non-fatal in v1
    # missing required => needs_review (NOT fail), matching your tests + your “never omit” philosophy.
    status = "ok"
    if missing_required:
        status = "needs_review"
    if low_text_pages:
        status = "needs_review"
    if anchor_violations:
        status = "needs_review"

    return {
        "doc_id": doc_id,
        "quality": {
            "total_observations": len(observations),
            "required_fields_total": len(required),
            "required_fields_missing": len(missing_required),
            "missing_required_fields": missing_required,
            "low_text_threshold": LOW_TEXT_THRESHOLD,
            "low_text_pages": low_text_pages,
            "anchor_violations": len(anchor_violations),
            "anchor_violations_sample": anchor_violations[:10],
            "quality_status": status,
        },
    }
