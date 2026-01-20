from datetime import datetime
from typing import Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class ORMBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class DocumentOut(ORMBase):
    id: UUID
    original_filename: str
    stored_filename: str
    upload_path: str
    created_at: datetime


class ObservationOut(ORMBase):
    id: int
    doc_id: UUID
    field_key: str
    raw_value: Optional[str]
    normalized_value: Optional[str]
    page_number: Optional[int]
    method: str
    confidence: float
    created_at: datetime


class ResolvedFieldOut(ORMBase):
    doc_id: UUID
    field_key: str
    resolved_value: Optional[str]
    confidence: float
    method: str
    created_at: datetime


class ResolvedResponse(BaseModel):
    doc_id: UUID
    resolved: Dict[str, Optional[str]]
    fields: List[ResolvedFieldOut]


class UploadResponse(BaseModel):
    doc_id: UUID
