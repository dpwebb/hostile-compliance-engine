# Document Ingestion Engine (v0)

Minimal FastAPI + Postgres service for ingesting credit bureau PDFs, extracting
fields into an observations table with provenance, and producing a resolved
profile deterministically.

## Requirements

- Docker + Docker Compose

## Run locally

```bash
docker compose up --build
```

The API will be available at `http://localhost:8000`.

## Upload a PDF

```bash
curl -F "file=@/path/to/your.pdf" http://localhost:8000/upload
```

Response:

```json
{ "doc_id": "..." }
```

## Fetch data

```bash
curl http://localhost:8000/documents/<doc_id>
curl http://localhost:8000/documents/<doc_id>/observations
curl http://localhost:8000/documents/<doc_id>/resolved
```

## Run tests

```bash
docker compose exec api pytest
```

## Notes

- Uploaded PDFs are stored at `./data/uploads/<uuid>.pdf` on the host.
- OCR is optional and disabled by default. To enable:
  - Install Tesseract + Poppler in the container/host.
  - Set `APP_OCR_ENABLED=1` in `docker-compose.yml`.
