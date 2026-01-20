import os
import shutil
import uuid
from uuid import UUID

from fastapi import Depends, FastAPI, File, HTTPException, UploadFile
from sqlalchemy.orm import Session

from app.db import get_db, init_db
from app.ingest import ingest_document
from app.models import Document, Observation, ResolvedProfile
from app.schemas import (
    DocumentOut,
    ObservationOut,
    ResolvedFieldOut,
    ResolvedResponse,
    UploadResponse,
)
from app.settings import settings

app = FastAPI(title="Document Ingestion Engine")


@app.on_event("startup")
def startup_event() -> None:
    os.makedirs(settings.uploads_dir, exist_ok=True)
    init_db()


@app.post("/upload", response_model=UploadResponse)
def upload_document(
    file: UploadFile = File(...), db: Session = Depends(get_db)
) -> UploadResponse:
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files supported")

    doc_id = uuid.uuid4()
    stored_filename = f"{doc_id}.pdf"
    upload_path = os.path.join(settings.uploads_dir, stored_filename)

    with open(upload_path, "wb") as out_file:
        shutil.copyfileobj(file.file, out_file)

    document = Document(
        id=doc_id,
        original_filename=file.filename,
        stored_filename=stored_filename,
        upload_path=upload_path,
    )
    db.add(document)
    db.commit()
    db.refresh(document)

    ingest_document(db, document, upload_path)

    return UploadResponse(doc_id=document.id)


@app.get("/documents/{doc_id}", response_model=DocumentOut)
def get_document(doc_id: UUID, db: Session = Depends(get_db)) -> DocumentOut:
    document = db.query(Document).filter(Document.id == doc_id).first()
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")
    return document


@app.get("/documents/{doc_id}/observations", response_model=list[ObservationOut])
def get_observations(doc_id: UUID, db: Session = Depends(get_db)) -> list[ObservationOut]:
    document = db.query(Document).filter(Document.id == doc_id).first()
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")
    observations = (
        db.query(Observation)
        .filter(Observation.doc_id == doc_id)
        .order_by(Observation.field_key, Observation.confidence.desc())
        .all()
    )
    return observations


@app.get("/documents/{doc_id}/resolved", response_model=ResolvedResponse)
def get_resolved(doc_id: UUID, db: Session = Depends(get_db)) -> ResolvedResponse:
    document = db.query(Document).filter(Document.id == doc_id).first()
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    rows = (
        db.query(ResolvedProfile)
        .filter(ResolvedProfile.doc_id == doc_id)
        .order_by(ResolvedProfile.field_key)
        .all()
    )
    resolved_map = {row.field_key: row.resolved_value for row in rows}
    return ResolvedResponse(doc_id=doc_id, resolved=resolved_map, fields=rows)
