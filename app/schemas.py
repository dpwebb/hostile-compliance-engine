from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel
from uuid import UUID

class DocumentSchema(BaseModel):
    id: UUID
    original_filename: str
    stored_path: str
    sha256: str
    page_count: int
    text_length: int
    created_at: datetime

    class Config:
        from_attributes = True

class ObservationSchema(BaseModel):
    id: UUID
    doc_id: UUID
    field_key: str
    raw_value: str
    normalized_value: Optional[str]
    page_number: Optional[int]
    method: str
    confidence: float
    created_at: datetime

    class Config:
        from_attributes = True

class ResolvedProfileSchema(BaseModel):
    doc_id: UUID
    resolved_profile: dict
