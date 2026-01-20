import re
from datetime import datetime
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from app import models
from app.extract import extract_text_pages, ocr_pdf_pages, text_quality_score
from app.settings import settings

FIELD_KEYS = [
    "full_name",
    "ssn",
    "dob",
    "address",
    "report_date",
]

FIELD_PATTERNS = {
    "full_name": [r"Name[:\s]+([A-Z][A-Za-z ,.'-]+)"],
    "ssn": [r"SSN[:\s]+(\d{3}-\d{2}-\d{4})"],
    "dob": [
        r"DOB[:\s]+(\d{2}/\d{2}/\d{4})",
        r"Date of Birth[:\s]+(\d{2}/\d{2}/\d{4})",
    ],
    "address": [r"Address[:\s]+(.+)"],
    "report_date": [r"Report Date[:\s]+(\d{2}/\d{2}/\d{4})"],
}


def normalize_value(field_key: str, raw_value: Optional[str]) -> Optional[str]:
    if raw_value is None:
        return None
    raw_value = raw_value.strip()
    if not raw_value:
        return None
    if field_key == "ssn":
        digits = re.sub(r"[^\d]", "", raw_value)
        if len(digits) == 9:
            return f"{digits[0:3]}-{digits[3:5]}-{digits[5:9]}"
        return digits
    if field_key in {"dob", "report_date"}:
        for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(raw_value, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return raw_value
    return raw_value


def extract_candidates_from_page(text: str) -> Dict[str, List[str]]:
    candidates: Dict[str, List[str]] = {key: [] for key in FIELD_KEYS}
    for field_key, patterns in FIELD_PATTERNS.items():
        for pattern in patterns:
            for match in re.finditer(pattern, text, flags=re.IGNORECASE):
                value = match.group(1).strip()
                if value:
                    candidates[field_key].append(value)
    return candidates


def build_observations(pages: List[str], method: str) -> List[Dict]:
    observations: List[Dict] = []
    found: Dict[str, bool] = {key: False for key in FIELD_KEYS}

    for page_number, text in enumerate(pages, start=1):
        candidates = extract_candidates_from_page(text or "")
        for field_key, values in candidates.items():
            for value in values:
                normalized = normalize_value(field_key, value)
                observations.append(
                    {
                        "field_key": field_key,
                        "raw_value": value,
                        "normalized_value": normalized,
                        "page_number": page_number,
                        "method": method,
                        "confidence": 0.9,
                    }
                )
                found[field_key] = True

    for field_key in FIELD_KEYS:
        if not found[field_key]:
            observations.append(
                {
                    "field_key": field_key,
                    "raw_value": None,
                    "normalized_value": None,
                    "page_number": None,
                    "method": method,
                    "confidence": 0.0,
                }
            )

    return observations


def resolve_observations(observations: List[Dict]) -> Dict[str, Dict]:
    grouped: Dict[str, List[Dict]] = {}
    for obs in observations:
        grouped.setdefault(obs["field_key"], []).append(obs)

    method_rank = {"text": 0, "ocr": 1}
    resolved: Dict[str, Dict] = {}
    for field_key, items in grouped.items():
        items_sorted = sorted(
            items,
            key=lambda obs: (
                -obs["confidence"],
                method_rank.get(obs["method"], 99),
                obs["page_number"] or 9999,
                obs["normalized_value"] or "",
            ),
        )
        best = items_sorted[0]
        resolved[field_key] = {
            "field_key": field_key,
            "resolved_value": best["normalized_value"],
            "confidence": best["confidence"],
            "method": best["method"],
        }
    return resolved


def ingest_document(db: Session, document: models.Document, pdf_path: str) -> None:
    text_pages = extract_text_pages(pdf_path)
    quality = text_quality_score(text_pages)

    method = "text"
    pages_for_extraction = text_pages

    if quality < settings.text_quality_threshold and settings.ocr_enabled:
        ocr_pages = ocr_pdf_pages(pdf_path)
        if any(page.strip() for page in ocr_pages):
            method = "ocr"
            pages_for_extraction = ocr_pages

    observations_data = build_observations(pages_for_extraction, method)
    resolved_data = resolve_observations(observations_data)

    observation_models = [
        models.Observation(doc_id=document.id, **observation)
        for observation in observations_data
    ]

    db.add_all(observation_models)
    db.query(models.ResolvedProfile).filter(
        models.ResolvedProfile.doc_id == document.id
    ).delete()
    db.add_all(
        [
            models.ResolvedProfile(
                doc_id=document.id,
                field_key=field_key,
                resolved_value=payload["resolved_value"],
                confidence=payload["confidence"],
                method=payload["method"],
            )
            for field_key, payload in resolved_data.items()
        ]
    )
    db.commit()
