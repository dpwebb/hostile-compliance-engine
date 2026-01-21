from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel


class IngestionRun(BaseModel):
    ingestion_run_id: str
    created_at: str
    source_filename: str
    stored_filename: str
    method: str
    notes: Optional[str] = None


class Anchor(BaseModel):
    anchor_text_before: str = ""
    anchor_text_after: str = ""
    anchor_hash: str = ""
    anchor_strength: str = "none"  # none | weak | strong


class Observation(BaseModel):
    obs_id: str
    doc_id: str
    ingestion_run_id: str
    field_key: str
    entity_id: Optional[str] = None
    raw_value: Any
    page_number: Optional[int] = None

    # provenance + extraction mechanics
    method: str
    confidence: float
    created_at: str

    # status tracking (critical for provenance-first + “no silent omission”)
    status: str = "extracted"  # extracted | missing | ambiguous

    anchor_violation: bool = False
    scope: Optional[str] = None
    reason: Optional[str] = None
    anchor: Anchor = Anchor()


class ResolvedEntry(BaseModel):
    resolved_value: Any
    resolution_status: str
    best_observation_id: str
    candidates: List[Dict[str, Any]] = []


class ResolvedProfile(BaseModel):
    doc_id: str
    resolved_profile: Dict[str, ResolvedEntry]
