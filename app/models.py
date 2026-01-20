import uuid

from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import UUID

from app.db import Base


class Document(Base):
    __tablename__ = "documents"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    original_filename = Column(String, nullable=False)
    stored_filename = Column(String, nullable=False)
    upload_path = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class Observation(Base):
    __tablename__ = "observations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    doc_id = Column(
        UUID(as_uuid=True),
        ForeignKey("documents.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    field_key = Column(String, nullable=False, index=True)
    raw_value = Column(Text)
    normalized_value = Column(Text)
    page_number = Column(Integer)
    method = Column(String, nullable=False)
    confidence = Column(Float, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class ResolvedProfile(Base):
    __tablename__ = "resolved_profile"

    doc_id = Column(
        UUID(as_uuid=True),
        ForeignKey("documents.id", ondelete="CASCADE"),
        primary_key=True,
    )
    field_key = Column(String, primary_key=True)
    resolved_value = Column(Text)
    confidence = Column(Float, nullable=False)
    method = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
