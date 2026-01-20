# hostile-compliance-engine

Hostile Compliance Engine - PDF ingestion and observation storage system.

## Setup

### Using Docker Compose (Recommended)

1. Start services:
```bash
docker-compose up -d
```

2. The API will be available at `http://localhost:8000`
3. API documentation at `http://localhost:8000/docs`

### Local Development

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set environment variables (optional):
```bash
export UPLOAD_DIR="./data/uploads"
```

3. Run the application:
```bash
uvicorn app.main:app --reload
```

## API Endpoints

- `POST /upload` - Upload a PDF file. Computes metadata and creates observations JSON file.
- `GET /documents/{doc_id}/observations` - Retrieve all observations for a document (from JSON file)
- `GET /documents/{doc_id}/resolved` - Get resolved profile (deterministic resolution from observations)
- `GET /health` - Health check endpoint

## Usage Examples

### Upload a PDF

```bash
curl -X POST "http://localhost:8000/upload" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@/path/to/your/document.pdf"
```

Response:
```json
{
  "doc_id": "123e4567-e89b-12d3-a456-426614174000",
  "filename": "document.pdf",
  "stored_as": "./data/uploads/123e4567-e89b-12d3-a456-426614174000.pdf",
  "sha256": "a1b2c3d4e5f6..."
}
```

### Fetch Observations

```bash
curl "http://localhost:8000/documents/123e4567-e89b-12d3-a456-426614174000/observations"
```

Response:
```json
{
  "doc_id": "123e4567-e89b-12d3-a456-426614174000",
  "observations": [
    {
      "field_key": "doc.meta.sha256",
      "raw_value": "a1b2c3d4e5f6...",
      "page_number": null,
      "method": "text",
      "confidence": 1.0,
      "created_at": "2024-01-01T12:00:00Z"
    },
    {
      "field_key": "doc.meta.byte_size",
      "raw_value": "12345",
      "page_number": null,
      "method": "text",
      "confidence": 1.0,
      "created_at": "2024-01-01T12:00:00Z"
    },
    {
      "field_key": "doc.meta.page_count",
      "raw_value": "5",
      "page_number": null,
      "method": "text",
      "confidence": 1.0,
      "created_at": "2024-01-01T12:00:00Z"
    },
    {
      "field_key": "doc.meta.text_length",
      "raw_value": "1234",
      "page_number": null,
      "method": "text",
      "confidence": 1.0,
      "created_at": "2024-01-01T12:00:00Z"
    }
  ]
}
```

### Fetch Resolved Profile

```bash
curl "http://localhost:8000/documents/123e4567-e89b-12d3-a456-426614174000/resolved"
```

Response:
```json
{
  "doc_id": "123e4567-e89b-12d3-a456-426614174000",
  "resolved_profile": {
    "doc.meta.sha256": "a1b2c3d4e5f6...",
    "doc.meta.byte_size": 12345,
    "doc.meta.page_count": 5,
    "doc.meta.text_length": 1234
  }
}
```

## Storage

Observations are stored as JSON files in `./data/observations/{doc_id}.json`. Each file contains a list of observation objects with the following structure:

- `field_key` (string) - The field identifier (e.g., "doc.meta.sha256")
- `raw_value` (string or number) - The observed value
- `page_number` (null for metadata) - Page number if applicable
- `method` (string) - Extraction method ("text" or "ocr")
- `confidence` (float) - Confidence score (0.0 to 1.0)
- `created_at` (string) - ISO timestamp

## Testing

Run tests:
```bash
pytest
```

Tests verify:
- PDF upload creates observations JSON file
- Observations endpoint returns at least: doc.meta.sha256, doc.meta.page_count, doc.meta.text_length
- Resolved profile correctly extracts and types doc.meta.* fields
- Error handling for missing documents