import uuid
from datetime import datetime
from sqlalchemy import Column, String, Integer, Float, ForeignKey, DateTime, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from app.database import Base

class Document(Base):
    __tablename__ = "documents"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    original_filename = Column(String, nullable=False)
    stored_path = Column(String, nullable=False)
    sha256 = Column(String, nullable=False)
    page_count = Column(Integer, nullable=False)
    text_length = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    observations = relationship("Observation", back_populates="document", cascade="all, delete-orphan")

class Observation(Base):
    __tablename__ = "observations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    doc_id = Column(UUID(as_uuid=True), ForeignKey("documents.id"), nullable=False)
    field_key = Column(String, nullable=False)
    raw_value = Column(Text, nullable=False)
    normalized_value = Column(Text, nullable=True)
    page_number = Column(Integer, nullable=True)
    method = Column(String, nullable=False)  # "text" or "ocr"
    confidence = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    document = relationship("Document", back_populates="observations")
