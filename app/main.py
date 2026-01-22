from __future__ import annotations

import hashlib
import logging
import os
import uuid
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple

import re
import string
import unicodedata
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse

try:
    import fitz  # PyMuPDF
    FITZ_AVAILABLE = True
except ImportError:
    FITZ_AVAILABLE = False

try:
    import pytesseract
    from PIL import Image
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False
    Image = None

logger = logging.getLogger(__name__)

from app.observation_registry import canonical_fields, required_field_keys
from app.observation_store import (
    add_doc_to_case,
    delete_override,
    ensure_doc_dirs,
    get_case_doc_ids,
    load_ingestion_runs,
    load_observations,
    load_overrides,
    save_ingestion_runs,
    save_observations,
    set_override,
)
from app.pdf_extractor import extract_identity_from_pages, parse_canadian_city_province_from_glued_tail
from app.resolver import apply_tu_precedence

app = FastAPI(title="Hostile Compliance Engine (Ingestion v0)")

UPLOAD_DIR = os.getenv("UPLOAD_DIR", "./data/uploads")
OBSERVATIONS_DIR = os.getenv("OBSERVATIONS_DIR", "./data/observations")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OBSERVATIONS_DIR, exist_ok=True)

LOW_TEXT_THRESHOLD = int(os.getenv("LOW_TEXT_THRESHOLD", "120"))

# Minimal text threshold for OCR trigger (stripped text length)
OCR_TEXT_THRESHOLD = int(os.getenv("OCR_TEXT_THRESHOLD", "50"))

# Anchors used to detect semantic vs non-semantic native text layer (case-insensitive)
ANCHOR_STRINGS = ["transunion", "accounts summary", "personal information", "credit report"]


def is_ocr_enabled() -> Tuple[bool, bool]:
    """
    Check if OCR is enabled via environment flags.
    Checks: ENABLE_OCR, OCR_ENABLED, USE_OCR, FORCE_OCR
    Treats common false values as false (case-insensitive): "0", "false", "no", "", None
    Treats as true: "1", "true", "yes"
    
    Returns: (ocr_enabled: bool, force_ocr: bool)
    - If FORCE_OCR is truthy => ocr_enabled=True and force_ocr=True
    - Else ocr_enabled = any(ENABLE_OCR, OCR_ENABLED, USE_OCR) truthy, force_ocr=False
    - Default is (False, False) if none provided
    """
    false_values = {"0", "false", "False", "no", "No", "NO", ""}
    true_values = {"1", "true", "True", "yes", "Yes", "YES"}
    
    def is_truthy(value: Optional[str]) -> bool:
        if value is None:
            return False
        value_str = str(value).strip()
        if not value_str:
            return False
        if value_str.lower() in false_values:
            return False
        if value_str.lower() in true_values:
            return True
        # Any non-empty value not in false_values is considered truthy
        return True
    
    # Check FORCE_OCR first (highest precedence)
    force_ocr_val = os.getenv("FORCE_OCR")
    force_ocr = is_truthy(force_ocr_val)
    
    if force_ocr:
        return (True, True)
    
    # Check other flags
    enable_ocr = is_truthy(os.getenv("ENABLE_OCR"))
    ocr_enabled = is_truthy(os.getenv("OCR_ENABLED"))
    use_ocr = is_truthy(os.getenv("USE_OCR"))
    
    ocr_enabled_result = enable_ocr or ocr_enabled or use_ocr
    
    return (ocr_enabled_result, False)


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/ui")


@app.get("/ui", response_class=HTMLResponse, include_in_schema=False)
def ui():
    # Minimal, dependency-free upload UI (single file)
    return HTMLResponse(
        """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Hostile Compliance Engine — Test UI</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; }
    .row { display: flex; gap: 16px; flex-wrap: wrap; }
    .card { border: 1px solid #ddd; border-radius: 10px; padding: 16px; max-width: 860px; }
    .muted { color: #666; font-size: 14px; }
    button { padding: 10px 14px; border-radius: 8px; border: 1px solid #333; background: #111; color: #fff; cursor: pointer; }
    button:disabled { opacity: 0.5; cursor: not-allowed; }
    input[type="text"] { padding: 10px; border-radius: 8px; border: 1px solid #bbb; width: 520px; max-width: 100%; }
    pre { background: #0b0b0b; color: #eaeaea; padding: 12px; border-radius: 10px; overflow: auto; }
    a { color: #0b66c3; }
    .links a { display: inline-block; margin-right: 12px; margin-top: 8px; }
    .warn { color: #b45309; }
    .ok { color: #15803d; }
  </style>
</head>
<body>
  <h1>Test UI</h1>
  <p class="muted">Upload a PDF, get a <code>doc_id</code>, then click through to the API outputs.</p>

  <div class="row">
    <div class="card" style="flex: 1 1 360px;">
      <h2>1) Upload PDF</h2>
      <input id="file" type="file" accept="application/pdf" />
      <div style="height: 10px;"></div>
      <button id="uploadBtn">Upload</button>
      <p id="status" class="muted"></p>
      <p class="muted warn">If upload returns 422, your server expects multipart <code>file</code> field (this UI uses that).</p>
    </div>

    <div class="card" style="flex: 1 1 360px;">
      <h2>2) Use doc_id</h2>
      <input id="doc" type="text" placeholder="doc_id will appear here after upload" />
      <div class="links" id="links"></div>
      <div style="height: 10px;"></div>
      <button id="fetchEntitiesBtn">Fetch /entities</button>
      <button id="fetchResolvedBtn">Fetch /resolved</button>
      <button id="fetchObsBtn">Fetch /observations</button>
      <p class="muted">Optional debug: <a id="textLink" href="#" target="_blank">/text?page=1</a> (only if implemented)</p>
      <p class="muted" style="margin-top: 12px; font-weight: bold;">
        <a href="#" id="reviewLink" style="font-size: 16px;">📝 Review & Edit Document</a>
      </p>
    </div>
  </div>

  <div class="card" style="margin-top: 16px;">
    <h2>Output</h2>
    <pre id="out">{}</pre>
  </div>

  <script>
    const $ = (id) => document.getElementById(id);
    const base = window.location.origin;

    function setLinks(docId) {
      const links = [
        ["review", `${base}/documents/${docId}/review`, false],
        ["observations", `${base}/documents/${docId}/observations`, true],
        ["entities", `${base}/documents/${docId}/entities`, true],
        ["resolved", `${base}/documents/${docId}/resolved`, true],
        ["quality", `${base}/documents/${docId}/quality`, true],
      ];
      $("links").innerHTML = links.map(([name, url, newTab]) =>
        `<a href="${url}" ${newTab ? 'target="_blank"' : ''}>/${name}</a>`
      ).join("");
      $("textLink").href = `${base}/documents/${docId}/text?page=1`;
    }

    async function upload() {
      const f = $("file").files[0];
      if (!f) { $("status").textContent = "Pick a PDF first."; return; }

      $("uploadBtn").disabled = true;
      $("status").textContent = "Uploading…";

      try {
        const fd = new FormData();
        fd.append("file", f);
        const res = await fetch(`${base}/upload`, { method: "POST", body: fd });
        const json = await res.json();
        $("out").textContent = JSON.stringify(json, null, 2);
        if (!res.ok) {
          $("status").textContent = "Upload failed (see output).";
          $("status").className = "muted warn";
          return;
        }
        const docId = json.document_id || json.doc_id || json.id;
        if (!docId) {
          $("status").textContent = "Upload OK but doc_id not found in response (see output).";
          $("status").className = "muted warn";
          return;
        }
      $("doc").value = docId;
      setLinks(docId);
      $("reviewLink").href = `${base}/documents/${docId}/review`;
      $("status").textContent = `Upload OK. doc_id = ${docId}`;
      $("status").className = "muted ok";
      } catch (e) {
        $("status").textContent = `Upload error: ${e}`;
        $("status").className = "muted warn";
      } finally {
        $("uploadBtn").disabled = false;
      }
    }

    async function fetchJson(path) {
      const docId = $("doc").value.trim();
      if (!docId) { $("out").textContent = "Missing doc_id."; return; }
      try {
        const res = await fetch(`${base}/documents/${docId}/${path}`);
        const json = await res.json();
        $("out").textContent = JSON.stringify(json, null, 2);
      } catch (e) {
        $("out").textContent = `Fetch error: ${e}`;
      }
    }

    // Update review link when doc_id changes
    $("doc").addEventListener("input", function() {
      const docId = this.value.trim();
      if (docId) {
        $("reviewLink").href = `${base}/documents/${docId}/review`;
      }
    });

    $("uploadBtn").addEventListener("click", upload);
    $("fetchEntitiesBtn").addEventListener("click", () => fetchJson("entities"));
    $("fetchResolvedBtn").addEventListener("click", () => fetchJson("resolved"));
    $("fetchObsBtn").addEventListener("click", () => fetchJson("observations"));
  </script>
</body>
</html>
        """.strip()
    )


@app.get("/documents/{doc_id}/review", response_class=HTMLResponse, include_in_schema=False)
def review_ui(doc_id: str):
    """Human review UI for editing document fields and managing overrides."""
    # Verify document exists
    data = load_observations(OBSERVATIONS_DIR, doc_id)
    if not data:
        raise HTTPException(status_code=404, detail="Document observations not found")
    
    # Load resolved data with overrides
    if isinstance(data, list):
        observations = data
    else:
        observations = data.get("observations", [])
    
    if not observations:
        raise HTTPException(status_code=404, detail="Document observations not found")
    
    overrides = load_overrides(OBSERVATIONS_DIR, doc_id)
    resolved_with_overrides = get_resolved_with_overrides(doc_id, observations, overrides)
    
    # Get quality status - only check fields that are visible in review UI
    required = required_field_keys()
    # Filter out doc.meta.* and doc.page.* fields from missing check (not shown in UI)
    visible_required = [k for k in required if not k.startswith("doc.meta.") and not k.startswith("doc.page.")]
    missing_required = []
    for k in visible_required:
        resolved_entry = resolved_with_overrides.get(k)
        if not resolved_entry or resolved_entry.get("value") is None:
            missing_required.append(k)
    
    quality_status = "ok" if not missing_required else "needs_review"
    
    # Group fields by section
    field_index_dict = {f.field_key: f for f in canonical_fields()}
    sections: Dict[str, List[Dict[str, Any]]] = {}
    for field_key, resolved_entry in resolved_with_overrides.items():
        # Extract base field_key (remove entity_id prefix if present)
        # e.g., "inquiry:1:0.inquiry.date" -> "inquiry.date"
        base_field_key = field_key
        if "." in field_key and not field_key.startswith("doc.") and not field_key.startswith("report."):
            # Check if it's an entity-scoped field (entity_id.field_key format)
            parts = field_key.split(".", 1)
            if len(parts) == 2:
                # Check if first part looks like an entity_id (contains colon) and second part is a known field
                potential_entity_id, potential_field_key = parts
                if ":" in potential_entity_id and potential_field_key in field_index_dict:
                    base_field_key = potential_field_key
        
        field_def = field_index_dict.get(base_field_key)
        if not field_def:
            # Skip if we can't find the field definition
            continue
        
        # Skip Document Meta and Page Info sections (not user-editable)
        if base_field_key.startswith("doc.meta.") or base_field_key.startswith("doc.page."):
            continue
        
        # Determine section from base_field_key prefix
        section = "Other"
        if base_field_key.startswith("report."):
            section = "Report Info"
        elif base_field_key.startswith("consumer."):
            section = "Consumer Identity"
        elif base_field_key.startswith("tradeline."):
            section = "Tradelines"
        elif base_field_key.startswith("inquiry."):
            section = "Inquiries"
        elif base_field_key.startswith("collection."):
            section = "Collections"
        elif base_field_key.startswith("public_record."):
            section = "Public Records"
        elif base_field_key.startswith("fraud_alert.") or base_field_key.startswith("consumer_statement."):
            section = "Alerts & Statements"
        
        if section not in sections:
            sections[section] = []
        
        sections[section].append({
            "field_key": field_key,
            "field_def": field_def,
            "resolved_entry": resolved_entry,
        })
    
    # Sort sections and fields within sections
    section_order = [
        "Report Info", "Consumer Identity",
        "Tradelines", "Inquiries", "Collections", "Public Records",
        "Alerts & Statements", "Other"
    ]
    for section in sections:
        sections[section].sort(key=lambda x: x["field_key"])
    
    # Build HTML
    sections_html = ""
    for section_name in section_order:
        if section_name not in sections:
            continue
        fields = sections[section_name]
        
        fields_html = ""
        for field_data in fields:
            field_key = field_data["field_key"]
            field_def = field_data["field_def"]
            resolved_entry = field_data["resolved_entry"]
            
            value = resolved_entry.get("value")
            status = resolved_entry.get("status", "missing")
            provenance = resolved_entry.get("provenance", {})
            confidence = resolved_entry.get("confidence", 0.0)
            is_required = field_def.required
            is_override = status == "override"
            is_missing = status == "missing"
            
            # Format value for display
            display_value = str(value) if value is not None else ""
            
            # Status badge
            status_class = "status-extracted" if status == "extracted" else ("status-override" if status == "override" else "status-missing")
            status_text = "Extracted" if status == "extracted" else ("Override" if status == "override" else "Missing")
            
            # Provenance info
            provenance_text = ""
            if provenance.get("source") == "override":
                updated_at = provenance.get("updated_at", "")
                note = provenance.get("note", "")
                provenance_text = f"Override (updated: {updated_at[:19] if updated_at else 'N/A'})"
                if note:
                    provenance_text += f" — {note}"
            elif provenance.get("source") == "extracted":
                page_num = provenance.get("page_number")
                method = provenance.get("method", "")
                anchor_snippet = provenance.get("anchor_snippet", "")
                parts = []
                if page_num:
                    parts.append(f"Page {page_num}")
                if method:
                    parts.append(method)
                if anchor_snippet:
                    parts.append(f'"{anchor_snippet[:50]}..."')
                provenance_text = " | ".join(parts) if parts else "Extracted"
            
            # Required indicator
            required_badge = '<span class="required-badge">Required</span>' if is_required else ""
            
            fields_html += f"""
            <div class="field-row" data-field-key="{field_key}">
              <div class="field-label">
                <strong>{field_def.label}</strong>
                {required_badge}
                <span class="field-key">{field_key}</span>
              </div>
              <div class="field-controls">
                <input 
                  type="text" 
                  class="field-input" 
                  data-field-key="{field_key}"
                  value="{display_value.replace('"', '&quot;')}"
                  placeholder="(empty)"
                  {'data-is-override="true"' if is_override else ''}
                />
                <span class="status-badge {status_class}">{status_text}</span>
                <span class="confidence">Confidence: {confidence:.2f}</span>
                <button class="btn-save" data-field-key="{field_key}" {'style="display:none;"' if not is_override and not display_value else ''}>Save</button>
                <button class="btn-clear" data-field-key="{field_key}" {'style="display:none;"' if not is_override else ''}>Clear Override</button>
              </div>
              <div class="field-provenance">{provenance_text}</div>
            </div>
            """
        
        sections_html += f"""
        <div class="section">
          <h3>{section_name}</h3>
          {fields_html}
        </div>
        """
    
    missing_fields_html = ""
    if missing_required:
        missing_fields_html = f"""
        <div class="missing-fields">
          <h3>Missing Required Fields ({len(missing_required)})</h3>
          <ul>
            {''.join(f'<li>{field_index_dict.get(k, {}).label if k in field_index_dict else k} <code>{k}</code></li>' for k in missing_required)}
          </ul>
        </div>
        """
    
    html_content = f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Document Review — {doc_id[:8]}...</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; background: #f5f5f5; }}
    .container {{ max-width: 1200px; margin: 0 auto; }}
    .header {{ background: white; padding: 20px; border-radius: 10px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
    .header h1 {{ margin: 0 0 10px 0; }}
    .header .doc-id {{ font-family: monospace; color: #666; }}
    .quality-status {{ display: inline-block; padding: 6px 12px; border-radius: 6px; font-weight: bold; margin-top: 10px; }}
    .quality-status.ok {{ background: #15803d; color: white; }}
    .quality-status.needs_review {{ background: #b45309; color: white; }}
    .missing-fields {{ background: #fee; border: 2px solid #fcc; padding: 16px; border-radius: 10px; margin-bottom: 20px; }}
    .missing-fields h3 {{ margin-top: 0; color: #c00; }}
    .missing-fields ul {{ margin: 10px 0; }}
    .missing-fields code {{ background: #fcc; padding: 2px 6px; border-radius: 4px; }}
    .section {{ background: white; padding: 20px; border-radius: 10px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
    .section h3 {{ margin-top: 0; border-bottom: 2px solid #ddd; padding-bottom: 10px; }}
    .field-row {{ margin: 16px 0; padding: 16px; background: #fafafa; border-radius: 8px; border-left: 4px solid #ddd; }}
    .field-row:hover {{ background: #f5f5f5; }}
    .field-label {{ margin-bottom: 8px; }}
    .field-label strong {{ font-size: 16px; }}
    .field-key {{ font-family: monospace; font-size: 12px; color: #666; margin-left: 8px; }}
    .required-badge {{ background: #c00; color: white; padding: 2px 6px; border-radius: 4px; font-size: 11px; margin-left: 8px; }}
    .field-controls {{ display: flex; gap: 10px; align-items: center; flex-wrap: wrap; margin: 8px 0; }}
    .field-input {{ flex: 1; min-width: 300px; padding: 8px 12px; border: 2px solid #ddd; border-radius: 6px; font-size: 14px; }}
    .field-input:focus {{ outline: none; border-color: #0b66c3; }}
    .status-badge {{ padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: bold; }}
    .status-extracted {{ background: #dbeafe; color: #1e40af; }}
    .status-override {{ background: #fef3c7; color: #92400e; }}
    .status-missing {{ background: #fee2e2; color: #991b1b; }}
    .confidence {{ font-size: 12px; color: #666; }}
    button {{ padding: 8px 16px; border-radius: 6px; border: none; cursor: pointer; font-size: 14px; font-weight: 500; }}
    .btn-save {{ background: #15803d; color: white; }}
    .btn-save:hover {{ background: #16a34a; }}
    .btn-save:disabled {{ background: #ccc; cursor: not-allowed; }}
    .btn-clear {{ background: #dc2626; color: white; }}
    .btn-clear:hover {{ background: #ef4444; }}
    .field-provenance {{ font-size: 12px; color: #666; margin-top: 8px; font-style: italic; }}
    .saving {{ opacity: 0.6; pointer-events: none; }}
    .message {{ padding: 12px; border-radius: 6px; margin: 10px 0; }}
    .message.success {{ background: #d1fae5; color: #065f46; }}
    .message.error {{ background: #fee2e2; color: #991b1b; }}
    a {{ color: #0b66c3; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>Document Review</h1>
      <div class="doc-id">Document ID: {doc_id}</div>
      <div class="quality-status {quality_status}">
        Quality Status: {quality_status.upper().replace('_', ' ')}
      </div>
      <div style="margin-top: 10px;">
        <a href="/ui">← Back to Upload UI</a> | 
        <a href="/documents/{doc_id}/quality" target="_blank">Quality API</a> | 
        <a href="/documents/{doc_id}/resolved" target="_blank">Resolved API</a>
      </div>
    </div>
    
    {missing_fields_html}
    
    {sections_html}
  </div>
  
  <script>
    const base = window.location.origin;
    const docId = "{doc_id}";
    
    // Track original values to detect changes
    const originalValues = {{}};
    document.querySelectorAll('.field-input').forEach(input => {{
      originalValues[input.dataset.fieldKey] = input.value;
    }});
    
    // Show/hide save buttons based on changes
    document.querySelectorAll('.field-input').forEach(input => {{
      input.addEventListener('input', function() {{
        const fieldKey = this.dataset.fieldKey;
        const saveBtn = document.querySelector(`.btn-save[data-field-key="${{fieldKey}}"]`);
        const isChanged = this.value !== originalValues[fieldKey];
        const isOverride = this.dataset.isOverride === 'true';
        
        if (isChanged || isOverride) {{
          saveBtn.style.display = 'inline-block';
        }} else {{
          saveBtn.style.display = 'none';
        }}
      }});
    }});
    
    // Save override
    document.querySelectorAll('.btn-save').forEach(btn => {{
      btn.addEventListener('click', async function() {{
        const fieldKey = this.dataset.fieldKey;
        const input = document.querySelector(`.field-input[data-field-key="${{fieldKey}}"]`);
        const value = input.value.trim();
        const fieldRow = input.closest('.field-row');
        
        // Disable during save
        fieldRow.classList.add('saving');
        this.disabled = true;
        
        try {{
          const formData = new FormData();
          formData.append('field_key', fieldKey);
          formData.append('value', value);
          
          const res = await fetch(`${{base}}/documents/${{docId}}/overrides`, {{
            method: 'POST',
            body: formData
          }});
          
          if (!res.ok) {{
            const error = await res.json();
            throw new Error(error.detail || 'Save failed');
          }}
          
          // Update UI
          input.dataset.isOverride = 'true';
          originalValues[fieldKey] = value;
          this.style.display = 'none';
          
          // Update status badge
          const statusBadge = fieldRow.querySelector('.status-badge');
          statusBadge.className = 'status-badge status-override';
          statusBadge.textContent = 'Override';
          
          // Update provenance
          const provenance = fieldRow.querySelector('.field-provenance');
          const now = new Date().toISOString().slice(0, 19);
          provenance.textContent = `Override (updated: ${{now}})`;
          
          // Show clear button
          const clearBtn = fieldRow.querySelector('.btn-clear');
          clearBtn.style.display = 'inline-block';
          
          // Refresh quality status
          await refreshQuality();
          
          // Show success message
          showMessage('Override saved successfully', 'success');
        }} catch (e) {{
          showMessage(`Error: ${{e.message}}`, 'error');
        }} finally {{
          fieldRow.classList.remove('saving');
          this.disabled = false;
        }}
      }});
    }});
    
    // Clear override
    document.querySelectorAll('.btn-clear').forEach(btn => {{
      btn.addEventListener('click', async function() {{
        const fieldKey = this.dataset.fieldKey;
        const fieldRow = this.closest('.field-row');
        
        if (!confirm(`Clear override for ${{fieldKey}}?`)) {{
          return;
        }}
        
        // Disable during delete
        fieldRow.classList.add('saving');
        this.disabled = true;
        
        try {{
          const res = await fetch(`${{base}}/documents/${{docId}}/overrides/${{fieldKey}}`, {{
            method: 'DELETE'
          }});
          
          if (!res.ok) {{
            const error = await res.json();
            throw new Error(error.detail || 'Delete failed');
          }}
          
          // Reload page to show extracted value
          window.location.reload();
        }} catch (e) {{
          showMessage(`Error: ${{e.message}}`, 'error');
          fieldRow.classList.remove('saving');
          this.disabled = false;
        }}
      }});
    }});
    
    async function refreshQuality() {{
      try {{
        const res = await fetch(`${{base}}/documents/${{docId}}/quality`);
        const data = await res.json();
        const qualityStatus = data.quality.quality_status;
        
        // Update quality status badge
        const statusEl = document.querySelector('.quality-status');
        statusEl.className = `quality-status ${{qualityStatus}}`;
        statusEl.textContent = `Quality Status: ${{qualityStatus.toUpperCase().replace('_', ' ')}}`;
        
        // Update missing fields section if needed
        const missingFields = data.quality.missing_required_fields || [];
        if (missingFields.length === 0) {{
          const missingSection = document.querySelector('.missing-fields');
          if (missingSection) {{
            missingSection.remove();
          }}
        }}
      }} catch (e) {{
        console.error('Failed to refresh quality:', e);
      }}
    }}
    
    function showMessage(text, type) {{
      const message = document.createElement('div');
      message.className = `message ${{type}}`;
      message.textContent = text;
      const header = document.querySelector('.header');
      header.appendChild(message);
      setTimeout(() => message.remove(), 3000);
    }}
  </script>
</body>
</html>
    """
    
    return HTMLResponse(html_content.strip())


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


def anchor_hit_on_native(text: str) -> bool:
    """
    True iff any of ANCHOR_STRINGS appears in text (case-insensitive).
    Used to detect semantic vs non-semantic native text layer (page 1).
    """
    if not text:
        return False
    t = text.lower()
    return any(anchor in t for anchor in ANCHOR_STRINGS)


def compute_semantic_metrics(text: str) -> Tuple[float, int, bool]:
    """
    Compute semantic quality metrics for text.
    Returns: (alpha_ratio: float, word_count: int, contains_anchor: bool)
    - alpha_ratio: (# ASCII letters A-Z/a-z) / max(1, len(text))
    - word_count: count of tokens matching r"[A-Za-z]{3,}"
    - contains_anchor: anchor_hit_on_native(text)
    """
    if not text:
        return 0.0, 0, False
    
    # Count ASCII letters (A-Z, a-z)
    ascii_letter_count = sum(1 for c in text if c.isascii() and c.isalpha())
    text_len = len(text)
    alpha_ratio = ascii_letter_count / max(1, text_len)
    
    # Count words matching r"[A-Za-z]{3,}"
    word_pattern = re.compile(r"[A-Za-z]{3,}")
    words = word_pattern.findall(text)
    word_count = len(words)
    
    contains_anchor = anchor_hit_on_native(text)
    
    return alpha_ratio, word_count, contains_anchor


def assess_semantic_quality(page_text: str) -> Tuple[bool, float, str, Dict[str, Any]]:
    """
    Assess if page text is semantic or non-semantic.
    Returns: (is_non_semantic: bool, semantic_density: float, quality_reason: str, metrics: dict)
    """
    if not page_text:
        return True, 0.0, "empty_text", {
            "letter_count": 0,
            "slash_count": 0,
            "space_count": 0,
            "total_length": 0,
            "semantic_density": 0.0,
            "has_common_words": False,
        }

    letter_count = sum(1 for c in page_text if c.isalpha())
    slash_count = page_text.count("/")
    space_count = sum(1 for c in page_text if c.isspace())
    total_length = len(page_text)

    if total_length == 0:
        semantic_density = 0.0
    else:
        semantic_density = letter_count / total_length

    # Check for common English words
    common_words = ["the", "and", "of", "to", "in", "a", "is", "it", "you", "that", "he", "was"]
    page_lower = page_text.lower()
    has_common_words = any(word in page_lower for word in common_words)

    metrics = {
        "letter_count": letter_count,
        "slash_count": slash_count,
        "space_count": space_count,
        "total_length": total_length,
        "semantic_density": round(semantic_density, 4),
        "has_common_words": has_common_words,
    }

    # Non-semantic if ANY of these are true:
    if semantic_density < 0.15:
        return True, semantic_density, f"semantic_density_{semantic_density:.4f}_below_0.15", metrics
    if slash_count > letter_count:
        return True, semantic_density, f"slash_count_{slash_count}_exceeds_letter_count_{letter_count}", metrics
    if not has_common_words and total_length > 50:
        return True, semantic_density, "no_common_words_detected", metrics

    return False, semantic_density, "semantic", metrics


def extract_native_text_with_fallback(page) -> str:
    """
    Extract text from a PyMuPDF page using best-effort fallback strategy.
    1. Try get_text('text') mode first (preferred for canonical text)
    2. If that yields poor results, try get_text('blocks') and join in y/x order
    3. Apply normalization after extraction
    
    Returns: normalized canonical text
    """
    # Try 'text' mode first (preferred for canonical text)
    text_mode_result = page.get_text('text') or ""
    text_normalized = normalize_text(text_mode_result)
    
    # Check if 'text' mode has anchors (quick quality check)
    _, _, has_anchors_text = compute_semantic_metrics(text_normalized)
    
    if has_anchors_text:
        # 'text' mode has anchors - use it
        return text_normalized
    
    # 'text' mode didn't have anchors, try 'blocks' mode
    try:
        blocks = page.get_text('blocks')
        if blocks:
            # Sort blocks by y coordinate (top to bottom), then x (left to right)
            # Each block is (x0, y0, x1, y1, "text", block_no, block_type)
            text_blocks = []
            for block in blocks:
                if len(block) >= 5 and isinstance(block[4], str):
                    text_blocks.append((block[1], block[0], block[4]))  # (y, x, text)
            
            # Sort by y (top to bottom), then x (left to right)
            text_blocks.sort(key=lambda b: (b[0], b[1]))
            
            # Join block texts
            blocks_text = "\n".join(block[2] for block in text_blocks)
            blocks_normalized = normalize_text(blocks_text)
            
            # Check if 'blocks' mode has anchors
            _, _, has_anchors_blocks = compute_semantic_metrics(blocks_normalized)
            
            if has_anchors_blocks:
                # 'blocks' mode has anchors - use it
                return blocks_normalized
            else:
                # Neither mode has anchors, but 'blocks' might still be better quality
                # Prefer 'blocks' if it has more words, otherwise use 'text'
                _, word_count_text, _ = compute_semantic_metrics(text_normalized)
                _, word_count_blocks, _ = compute_semantic_metrics(blocks_normalized)
                
                if word_count_blocks > word_count_text:
                    return blocks_normalized
                else:
                    return text_normalized
    except Exception as e:
        # If 'blocks' extraction fails, fall back to 'text' mode result
        logger.warning(f"Failed to extract text using 'blocks' mode: {e}, using 'text' mode result")
        return text_normalized
    
    # Default to 'text' mode result
    return text_normalized


def extract_text_via_ocr(pdf_bytes: bytes, page_number: int) -> str:
    """
    Extract text from a specific PDF page using OCR.
    Uses PyMuPDF (fitz) to render page to image at 300 DPI, then pytesseract for OCR.
    Converts PyMuPDF pixmap to PIL Image before passing to pytesseract.
    Raises HTTPException (422) on failure (not 500).
    """
    if not OCR_AVAILABLE:
        raise HTTPException(
            status_code=422,
            detail="OCR dependencies not available. Please install PyMuPDF and pytesseract. Also ensure tesseract-ocr binary is installed on the system."
        )

    try:
        # Open PDF with PyMuPDF (fitz)
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        if page_number < 1 or page_number > len(doc):
            doc.close()
            return ""
        
        # Load page (0-indexed)
        page = doc.load_page(page_number - 1)
        
        # Render to pixmap at 300 DPI (deterministic: matrix zoom = 300/72)
        zoom = 300.0 / 72.0
        matrix = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=matrix)
        
        # Convert pixmap to PIL Image
        # Handle alpha channel: if pixmap has alpha, convert RGBA to RGB
        if pix.alpha:
            # Create RGB pixmap from RGBA
            pix_rgb = fitz.Pixmap(pix, 0)  # 0 = remove alpha channel
            img = Image.frombytes("RGB", (pix_rgb.width, pix_rgb.height), pix_rgb.samples)
            pix_rgb = None  # Free memory
        else:
            # Already RGB, no alpha channel
            img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
        
        pix = None  # Free memory
        
        doc.close()
        
        # Run OCR with pytesseract (will raise if tesseract binary not found)
        try:
            ocr_text = pytesseract.image_to_string(img, lang="eng", config="--psm 6")
            return ocr_text or ""
        except pytesseract.TesseractNotFoundError:
            raise HTTPException(
                status_code=422,
                detail="OCR extraction failed: Tesseract OCR binary not found. Please install tesseract-ocr on your system."
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=422,
            detail=f"OCR extraction failed: {str(e)}"
        )


def get_best_page_text(
    page_number: int,
    page_texts: List[str],
    page_texts_ocr: Dict[int, str],
    page_semantic_quality: Dict[int, bool],
) -> str:
    """
    Get the best available text for a page.
    Prefers OCR text when available (since OCR is only triggered when native text is insufficient).
    Falls back to native text extraction otherwise.
    """
    if page_number < 1 or page_number > len(page_texts):
        return ""
    
    # If OCR text exists, prefer it (OCR is only triggered when native text was insufficient)
    if page_number in page_texts_ocr:
        return page_texts_ocr[page_number]
    
    # Otherwise use native text extraction
    return page_texts[page_number - 1]


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
    # Get bureau from observations
    bureau = None
    for o in observations:
        if o.get("field_key") == "report.bureau" and o.get("raw_value"):
            bureau = o["raw_value"]
            break
    
    by_key: Dict[str, List[Dict[str, Any]]] = {}
    for o in observations:
        k = o["field_key"]
        if o.get("entity_id"):
            k = f"{o['entity_id']}.{o['field_key']}"
        by_key.setdefault(k, []).append(o)

    resolved: Dict[str, Any] = {}
    for k, items in by_key.items():
        # Apply TransUnion precedence rules if applicable (pass all observations for cross-field checks)
        items = apply_tu_precedence(k, items, bureau, observations)
        
        # Sort by TU priority (if set), then confidence, then created_at
        # Higher priority/confidence/earlier created_at = sorted first
        items_sorted = sorted(
            items,
            key=lambda x: (
                float(x.get("_tu_priority", 1)),  # TU priority (2 = highest, 1 = normal, 0 = lowest)
                float(x.get("confidence", 0.0)),
                x.get("created_at", ""),
            ),
            reverse=True,
        )
        # Remove temporary _tu_priority field before returning (cleanup)
        for item in items_sorted:
            item.pop("_tu_priority", None)
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

        # Handle missing observations: don't treat them as resolved with empty string
        if best.get("method") == "missing" or best.get("status") == "missing":
            resolved[k] = {
                "resolved_value": None,  # Use None, not empty string
                "resolution_status": "missing",
                "best_observation_id": best["obs_id"],
                "candidates": candidates,
            }
        else:
            resolved[k] = {
                "resolved_value": best["raw_value"],
                "resolution_status": "resolved",
                "best_observation_id": best["obs_id"],
                "candidates": candidates,
            }
    return resolved


def get_resolved_with_overrides(
    doc_id: str,
    observations: List[Dict[str, Any]],
    overrides: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """
    Get resolved values merging extracted observations with overrides.
    Resolution rules:
    - resolved_value = override if present else best extracted observation else missing
    - preserve extracted provenance; for overrides set provenance.source="override"
    """
    resolved = resolve_profile(observations)
    overrides_dict = overrides or {}
    
    # Build canonical field list for all possible field keys
    canonical = {f.field_key: f for f in canonical_fields()}
    
    # Merge overrides into resolved
    result: Dict[str, Dict[str, Any]] = {}
    
    for field_key, resolved_entry in resolved.items():
        # Check if there's an override for this field
        override = overrides_dict.get(field_key)
        
        if override:
            # Use override value
            result[field_key] = {
                "value": override["value"],
                "status": "override",
                "provenance": {
                    "source": "override",
                    "created_at": override.get("created_at"),
                    "updated_at": override.get("updated_at"),
                    "note": override.get("note", ""),
                },
                "confidence": 1.0,  # Overrides have full confidence
                "best_observation_id": resolved_entry.get("best_observation_id"),
                "candidates": resolved_entry.get("candidates", []),
            }
        else:
            # Use extracted value
            resolved_value = resolved_entry.get("resolved_value")
            resolution_status = resolved_entry.get("resolution_status", "missing")
            
            # Get provenance from best observation
            best_obs_id = resolved_entry.get("best_observation_id")
            best_obs = None
            for obs in observations:
                if obs.get("obs_id") == best_obs_id:
                    best_obs = obs
                    break
            
            provenance = {
                "source": "extracted",
                "page_number": best_obs.get("page_number") if best_obs else None,
                "method": best_obs.get("method") if best_obs else None,
                "anchor": best_obs.get("anchor", {}) if best_obs else {},
            }
            
            # Build anchor snippet from anchor data
            anchor = provenance.get("anchor", {})
            anchor_before = anchor.get("anchor_text_before", "")
            anchor_after = anchor.get("anchor_text_after", "")
            anchor_snippet = (anchor_before + anchor_after).strip()[:100] if (anchor_before or anchor_after) else ""
            if anchor_snippet:
                provenance["anchor_snippet"] = anchor_snippet
            
            status = "extracted" if resolution_status == "resolved" and resolved_value is not None else "missing"
            
            result[field_key] = {
                "value": resolved_value,
                "status": status,
                "provenance": provenance,
                "confidence": best_obs.get("confidence", 0.0) if best_obs else 0.0,
                "best_observation_id": best_obs_id,
                "candidates": resolved_entry.get("candidates", []),
            }
    
    return result


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
                raw_value=None,
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


def normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def normalize_text(text: str) -> str:
    """
    Normalize text by removing control characters, collapsing whitespace,
    and ensuring the result is readable and searchable.
    
    - Replace control characters (Unicode categories Cc/Cf) with space
    - Collapse repeated whitespace to single spaces
    - Preserve newlines, tabs, carriage returns
    - Keep printable characters (str.isprintable() OR in "\n\r\t")
    - Convert disallowed characters to space
    """
    if not text:
        return ""
    
    result = []
    for char in text:
        # Allow printable characters and common whitespace
        if char.isprintable() or char in "\n\r\t":
            # Check if it's a control character (Cc) or format character (Cf)
            category = unicodedata.category(char)
            if category in ("Cc", "Cf"):
                # Replace control/format chars with space, but preserve newlines/tabs/carriage returns
                if char in "\n\r\t":
                    result.append(char)
                else:
                    result.append(" ")
            else:
                result.append(char)
        else:
            # Non-printable and not in whitelist -> replace with space
            result.append(" ")
    
    # Collapse repeated whitespace (but preserve newlines)
    normalized = "".join(result)
    # Replace multiple spaces with single space, but keep newlines
    normalized = re.sub(r"[ \t]+", " ", normalized)  # Collapse spaces and tabs
    normalized = re.sub(r" +\n", "\n", normalized)  # Remove trailing spaces before newlines
    normalized = re.sub(r"\n +", "\n", normalized)  # Remove leading spaces after newlines
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)  # Collapse multiple newlines to max 2
    
    return normalized.strip()


def extract_inquiries_from_pages(
    page_texts: List[str],
    doc_id: str,
    ingestion_run_id: str,
) -> List[Dict[str, Any]]:
    """
    Minimal inquiry extraction (v1).

    Strategy:
    - Detect an "Inquiries" section on each page.
    - Extract date lines and nearby subscriber/member lines.
    - Emit:
        inquiry.date
        inquiry.subscriber_name
    - Stable entity_id: inquiry:{page_number}:{block_index}
    """
    observations: List[Dict[str, Any]] = []

    for page_idx, page_text in enumerate(page_texts):
        page_number = page_idx + 1

        # Must have "Inquiries" somewhere on the page
        if not re.search(r"\bInquiries\b", page_text, re.IGNORECASE):
            continue

        lines = page_text.splitlines()

        # Locate the Inquiries header line
        start_idx = None
        for i, line in enumerate(lines):
            if re.search(r"\bInquiries\b", line, re.IGNORECASE):
                start_idx = i
                break
        if start_idx is None:
            continue

        # Stop scanning when we hit another likely section header
        stop_header = re.compile(
            r"^\s*(Tradelines|Accounts|Collections|Public Records|Personal Information|Summary)\b",
            re.IGNORECASE
        )

        # Date patterns (flexible; keep simple & deterministic)
        date_patterns = [
            re.compile(r"\b(\d{4}-\d{2}-\d{2})\b"),          # YYYY-MM-DD
            re.compile(r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b"),   # M/D/YY(YY)
            re.compile(r"\b(\d{1,2}-\d{1,2}-\d{2,4})\b"),   # M-D-YY(YY)
        ]

        def find_anchor_pos(needle: str) -> int:
            if not needle:
                return -1
            return page_text.find(needle)

        block_index = 0
        i = start_idx + 1
        while i < len(lines):
            line = lines[i].strip()
            if stop_header.search(line):
                break
            if not line:
                i += 1
                continue

            # Date detection
            matched_date = None
            for pat in date_patterns:
                m = pat.search(line)
                if m:
                    matched_date = m.group(1)
                    break

            if matched_date:
                entity_id = f"inquiry:{page_number}:{block_index}"

                # anchor around the line where date was found
                pos = find_anchor_pos(line)
                if pos < 0:
                    pos = max(0, find_anchor_pos(matched_date))
                anchor_snippet = page_text[max(0, pos):min(len(page_text), pos + len(line) + 60)]
                anchor_dict = anchor_from_page_text(page_text, matched_date)

                observations.append(
                    obs(
                        doc_id=doc_id,
                        ingestion_run_id=ingestion_run_id,
                        field_key="inquiry.date",
                        raw_value=matched_date,
                        page_number=page_number,
                        method="pattern_match",
                        confidence=0.7,
                        status="extracted",
                        entity_id=entity_id,
                        anchor=anchor_dict,
                    )
                )

                # Try to capture subscriber name near the date (same line or next 3 lines)
                subscriber = None
                for j in range(0, 4):
                    if i + j >= len(lines):
                        break
                    cand = lines[i + j].strip()
                    if stop_header.search(cand):
                        break
                    if not cand:
                        continue

                    # Heuristic: remove date token, then use the remainder if it looks like a name/org
                    cand_clean = re.sub(r"\b\d{4}-\d{2}-\d{2}\b", "", cand).strip()
                    cand_clean = re.sub(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b", "", cand_clean).strip()
                    if not cand_clean:
                        continue

                    # Prefer explicit labels when present
                    if re.search(r"\b(Subscriber|Member|Company|Creditor)\b", cand, re.IGNORECASE):
                        # Extract text after the label
                        m = re.search(r"\b(Subscriber|Member|Company|Creditor)\s*[:\-]?\s*(.+)", cand, re.IGNORECASE)
                        if m:
                            subscriber = normalize_whitespace(m.group(2))[:120]
                        else:
                            subscriber = cand_clean[:120]
                        break

                    # Otherwise accept a plausible org-like string
                    if len(cand_clean) >= 4 and re.search(r"[A-Za-z]", cand_clean):
                        subscriber = cand_clean[:120]
                        break

                if subscriber:
                    s_anchor = anchor_from_page_text(page_text, subscriber)
                    observations.append(
                        obs(
                            doc_id=doc_id,
                            ingestion_run_id=ingestion_run_id,
                            field_key="inquiry.subscriber_name",
                            raw_value=subscriber,
                            page_number=page_number,
                            method="pattern_match",
                            confidence=0.7,
                            status="extracted",
                            entity_id=entity_id,
                            anchor=s_anchor,
                        )
                    )

                block_index += 1

            i += 1

    return observations


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


@app.post("/debug/assess-semantic-quality", include_in_schema=False)
def debug_assess_semantic_quality(text: str):
    """
    Debug endpoint to test semantic quality assessment.
    Accepts a text string and returns quality assessment results.
    """
    is_non_semantic, semantic_density, quality_reason, metrics = assess_semantic_quality(text)
    
    return {
        "text_preview": text[:200] + ("..." if len(text) > 200 else ""),
        "is_non_semantic": is_non_semantic,
        "semantic_density": semantic_density,
        "quality_reason": quality_reason,
        "metrics": metrics,
    }


@app.post("/upload")
async def upload_pdf(file: UploadFile = File(...), case_id: Optional[str] = Form(None)):
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

    # Use PyMuPDF (fitz) for native text extraction
    if not FITZ_AVAILABLE:
        raise HTTPException(
            status_code=500,
            detail="PyMuPDF (fitz) is required for PDF text extraction. Please install PyMuPDF."
        )
    
    doc = fitz.open(stream=content, filetype="pdf")
    page_count = len(doc)
    
    # 1) Anchor hit on NATIVE text (page 1 only): get_text('text') — no blocks fallback
    page0 = doc.load_page(0)
    native_page1_raw = page0.get_text("text") or ""
    native_page1 = normalize_text(native_page1_raw)
    anchor_hit = anchor_hit_on_native(native_page1)
    
    # 2) If anchor_hit == false: treat as non-semantic. OCR enabled -> fallback; OCR disabled -> 422
    ocr_enabled, force_ocr = is_ocr_enabled()
    if not anchor_hit and not ocr_enabled:
        doc.close()
        raise HTTPException(
            status_code=422,
            detail="Text layer is non-semantic and OCR is disabled",
        )
    
    if anchor_hit:
        ingestion_path = "native_text"
        logger.info(f"doc_id={doc_id}: native_text path (anchor_hit=true on page 1 native)")
    else:
        ingestion_path = "ocr_fallback"
        logger.info(f"doc_id={doc_id}: ocr_fallback path (anchor_hit=false on page 1 native, ocr_enabled=true)")
    
    # Extract native text from all pages: get_text('text') only, then normalize
    page_texts_canonical_native: List[str] = []
    page_texts_raw_native: List[str] = []
    for page_num in range(page_count):
        page = doc.load_page(page_num)
        raw_text = page.get_text("text") or ""
        page_texts_raw_native.append(raw_text)
        page_texts_canonical_native.append(normalize_text(raw_text))
    doc.close()
    
    page_texts_normalized_native = page_texts_canonical_native
    page_texts = page_texts_normalized_native

    ensure_doc_dirs(OBSERVATIONS_DIR, doc_id)

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
    # doc.meta.text_length added below after we have canonical page_texts_normalized

    # Page-level: text length per page (anchored) and semantic quality assessment
    page_semantic_quality: Dict[int, bool] = {}  # page_number -> is_non_semantic
    page_semantic_density: Dict[int, float] = {}  # page_number -> semantic_density
    page_quality_reasons: Dict[int, str] = {}  # page_number -> quality_reason
    page_quality_metrics: Dict[int, Dict[str, Any]] = {}
    page_texts_ocr: Dict[int, str] = {}  # Normalized OCR text
    page_texts_ocr_raw: Dict[int, str] = {}  # Raw OCR text
    ocr_triggered_pages: List[int] = []

    # Process pages based on ingestion path
    for i, t in enumerate(page_texts_normalized_native, start=1):
        # Assess semantic quality (for observation tracking, not OCR trigger)
        is_non_semantic, semantic_density, quality_reason, metrics = assess_semantic_quality(t)
        page_semantic_quality[i] = is_non_semantic
        page_semantic_density[i] = semantic_density
        page_quality_reasons[i] = quality_reason
        page_quality_metrics[i] = metrics

        # doc.page.text_length stored below after we have canonical page_texts_normalized

        # Store explicit quality observations
        observations.append(
            obs(
                doc_id=doc_id,
                ingestion_run_id=ingestion_run_id,
                field_key="doc.page.semantic_density",
                entity_id=f"page:{i}",
                page_number=i,
                raw_value=semantic_density,
                method="derived",
                confidence=1.0,
            )
        )

        observations.append(
            obs(
                doc_id=doc_id,
                ingestion_run_id=ingestion_run_id,
                field_key="doc.page.non_semantic",
                entity_id=f"page:{i}",
                page_number=i,
                raw_value=is_non_semantic,
                method="derived",
                confidence=1.0,
            )
        )

        observations.append(
            obs(
                doc_id=doc_id,
                ingestion_run_id=ingestion_run_id,
                field_key="doc.page.quality_reason",
                entity_id=f"page:{i}",
                page_number=i,
                raw_value=quality_reason,
                method="derived",
                confidence=1.0,
            )
        )

        # OCR processing: only if we're in ocr_fallback path
        if ingestion_path == "ocr_fallback":
            # For ocr_fallback path, run OCR on all pages (text is non-semantic at document level)
            try:
                ocr_text = extract_text_via_ocr(content, i)
                if ocr_text:
                    # Store raw OCR text, normalize separately
                    page_texts_ocr_raw[i] = ocr_text
                    page_texts_ocr[i] = normalize_text(ocr_text)  # Normalized OCR text
                    ocr_triggered_pages.append(i)

                    # Store OCR text observation (store normalized text)
                    ocr_normalized = page_texts_ocr[i]
                    observations.append(
                        obs(
                            doc_id=doc_id,
                            ingestion_run_id=ingestion_run_id,
                            field_key="doc.page.text_ocr",
                            entity_id=f"page:{i}",
                            page_number=i,
                            raw_value=ocr_normalized,  # Store normalized OCR text
                            method="ocr",
                            confidence=0.85,
                            reason="OCR invoked (ocr_fallback path, anchor_hit=false on page 1 native)",
                            anchor=anchor_from_page_text(ocr_normalized, ocr_normalized[:50] if ocr_normalized else ""),
                        )
                    )
            except HTTPException:
                # OCR failed - re-raise to surface the error
                raise

    # Build combined normalized page_texts (canonical: OCR when available, native when semantic)
    # When OCR fallback runs, OCR text becomes canonical for all downstream processing
    page_texts_normalized: List[str] = []
    page_texts_raw: List[str] = []  # Canonical raw (OCR raw when OCR used, native raw when native used)
    page_texts_native_raw: List[str] = []  # Native raw text (stored separately, not exposed as primary)
    for i in range(1, len(page_texts_normalized_native) + 1):
        # Use normalized versions for combined text (canonical: OCR when available)
        # get_best_page_text prefers OCR text when available (from ocr_fallback path)
        best_normalized = get_best_page_text(i, page_texts_normalized_native, page_texts_ocr, page_semantic_quality)
        page_texts_normalized.append(best_normalized)
        
        # Log which text source is being used for canonical text
        if i in page_texts_ocr:
            logger.debug(f"doc_id={doc_id}: page {i} using OCR text as canonical (len={len(best_normalized)})")
        else:
            logger.debug(f"doc_id={doc_id}: page {i} using native text as canonical (len={len(best_normalized)})")
        
        # Get corresponding raw text (canonical: OCR raw when OCR used, native raw when native used)
        if i in page_texts_ocr_raw:
            best_raw = page_texts_ocr_raw[i]
        else:
            best_raw = page_texts_raw_native[i - 1]
        page_texts_raw.append(best_raw)
        
        # Always store native raw separately
        page_texts_native_raw.append(page_texts_raw_native[i - 1])

    # Canonical total text length and per-page text length (use canonical text, not native when OCR ran)
    total_text_normalized = "\n".join(page_texts_normalized)
    total_text_len = len(total_text_normalized)
    observations.append(
        obs(doc_id=doc_id, ingestion_run_id=ingestion_run_id, field_key="doc.meta.text_length", raw_value=total_text_len, method="text", confidence=1.0)
    )
    
    # Track low-text pages for OCR escalation
    low_text_pages_for_ocr: List[int] = []
    for i in range(1, len(page_texts_normalized) + 1):
        t = page_texts_normalized[i - 1]
        text_len = len(t)
        observations.append(
            obs(
                doc_id=doc_id,
                ingestion_run_id=ingestion_run_id,
                field_key="doc.page.text_length",
                entity_id=f"page:{i}",
                page_number=i,
                raw_value=text_len,
                method="text",
                confidence=1.0,
                anchor=anchor_from_page_text(t, str(text_len)),
            )
        )
        # OCR escalation: if page text_length < low_text_threshold and OCR not already done, force OCR
        if text_len < LOW_TEXT_THRESHOLD and i not in ocr_triggered_pages:
            low_text_pages_for_ocr.append(i)
    
    # OCR escalation: re-extract low-text pages with OCR
    ocr_escalation_pages: List[int] = []
    if low_text_pages_for_ocr and OCR_AVAILABLE:
        for page_num in low_text_pages_for_ocr:
            try:
                ocr_text = extract_text_via_ocr(content, page_num)
                if ocr_text:
                    ocr_normalized = normalize_text(ocr_text)
                    # Update page texts with OCR result
                    page_texts_ocr_raw[page_num] = ocr_text
                    page_texts_ocr[page_num] = ocr_normalized
                    page_texts_normalized[page_num - 1] = ocr_normalized  # Update normalized (0-indexed)
                    page_texts_raw[page_num - 1] = ocr_text  # Update raw (0-indexed)
                    ocr_triggered_pages.append(page_num)
                    ocr_escalation_pages.append(page_num)
                    
                    # Update page text_length observation
                    new_text_len = len(ocr_normalized)
                    # Find and update the observation
                    for obs_item in observations:
                        if (obs_item.get("field_key") == "doc.page.text_length" and 
                            obs_item.get("page_number") == page_num and
                            obs_item.get("entity_id") == f"page:{page_num}"):
                            obs_item["raw_value"] = new_text_len
                            obs_item["method"] = "ocr_escalation"
                            obs_item["confidence"] = 0.85
                            break
                    
                    # Store OCR text observation
                    observations.append(
                        obs(
                            doc_id=doc_id,
                            ingestion_run_id=ingestion_run_id,
                            field_key="doc.page.text_ocr",
                            entity_id=f"page:{page_num}",
                            page_number=page_num,
                            raw_value=ocr_normalized,
                            method="ocr_escalation",
                            confidence=0.85,
                            reason=f"OCR escalation triggered (text_length={text_len} < {LOW_TEXT_THRESHOLD})",
                            anchor=anchor_from_page_text(ocr_normalized, ocr_normalized[:50] if ocr_normalized else ""),
                        )
                    )
            except HTTPException:
                # OCR failed for this page - log but continue
                logger.warning(f"doc_id={doc_id}: OCR escalation failed for page {page_num}")
            except Exception as e:
                logger.warning(f"doc_id={doc_id}: OCR escalation error for page {page_num}: {e}")
    
    # Note: Identity extraction will happen later and will use the updated page_texts_normalized
    # from OCR escalation, so we don't need to re-run it here

    # Doc-level OCR audit observations
    if ocr_triggered_pages:
        observations.append(
            obs(
                doc_id=doc_id,
                ingestion_run_id=ingestion_run_id,
                field_key="doc.meta.ocr_pages_count",
                raw_value=len(ocr_triggered_pages),
                method="derived",
                confidence=1.0,
            )
        )
        
        total_ocr_length = sum(len(page_texts_ocr[p]) for p in ocr_triggered_pages)
        observations.append(
            obs(
                doc_id=doc_id,
                ingestion_run_id=ingestion_run_id,
                field_key="doc.meta.text_length_ocr_total",
                raw_value=total_ocr_length,
                method="derived",
                confidence=1.0,
            )
        )

    # Bureau detection (deterministic) - use canonical text (OCR when ocr_fallback, else native)
    bureau = "Unknown"
    normalized_lower = total_text_normalized.lower()
    
    # Robust TransUnion detection with exact keywords
    transunion_keywords = [
        "transunion",
        "credit report",
        "accounts summary",
        "personal information"
    ]
    if any(keyword in normalized_lower for keyword in transunion_keywords):
        bureau = "TransUnion"
    elif "equifax" in normalized_lower:
        bureau = "Equifax"
    
    # Calculate text quality metrics
    normalized_len = total_text_len
    printable_count = sum(1 for c in total_text_normalized if c.isprintable() or c in "\n\r\t")
    printable_ratio_norm = printable_count / max(1, normalized_len)
    
    # Add text quality observations
    observations.append(
        obs(
            doc_id=doc_id,
            ingestion_run_id=ingestion_run_id,
            field_key="doc.text.printable_ratio",
            raw_value=round(printable_ratio_norm, 4),
            method="derived",
            confidence=1.0,
        )
    )
    observations.append(
        obs(
            doc_id=doc_id,
            ingestion_run_id=ingestion_run_id,
            field_key="doc.text.normalized_length",
            raw_value=normalized_len,
            method="derived",
            confidence=1.0,
        )
    )
    
    logger.info(f"doc_id={doc_id}: bureau={bureau}, normalized_text_len={normalized_len}, printable_ratio={printable_ratio_norm:.2f}")
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

    # Identity extraction v1 (deterministic) - use normalized page_texts for better extraction
    # Pass bureau to prioritize Address(es) table for TransUnion reports
    identity = extract_identity_from_pages(page_texts_normalized, bureau=bureau)
    consumer = identity.get("consumer", {}) or {}
    evidence = identity.get("evidence", {}) or {}

    # Name
    if consumer.get("full_name"):
        ev = evidence.get("full_name")
        page_num = ev[0] if ev else None
        # Use normalized page_texts for anchor
        page_text = page_texts_normalized[page_num - 1] if page_num and 1 <= page_num <= len(page_texts_normalized) else ""
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
    # Use normalized page_texts for anchor
    addr_page_text = page_texts_normalized[addr_page - 1] if addr_page and 1 <= addr_page <= len(page_texts_normalized) else ""

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

    # Post-processing: Extract city/province from glued tail for TransUnion reports
    # Trigger condition:
    # - report.bureau == "TransUnion"
    # - consumer.current_address.city is missing OR province is missing
    # - consumer.current_address.line1 is resolved
    # - consumer.current_address.postal_code is resolved
    if bureau == "TransUnion":
        # Get resolved values from observations (check for any observation with non-None value)
        # Use the most recent observation for each field (last one wins)
        line1_obs = None
        postal_obs = None
        city_obs = None
        province_obs = None
        
        for o in reversed(observations):  # Check in reverse to get most recent
            if not line1_obs and o.get("field_key") == "consumer.current_address.line1" and o.get("raw_value"):
                line1_obs = o
            if not postal_obs and o.get("field_key") == "consumer.current_address.postal_code" and o.get("raw_value"):
                postal_obs = o
            if not city_obs and o.get("field_key") == "consumer.current_address.city" and o.get("raw_value"):
                city_obs = o
            if not province_obs and o.get("field_key") == "consumer.current_address.province" and o.get("raw_value"):
                province_obs = o
        
        line1_raw = line1_obs.get("raw_value") if line1_obs else None
        postal_code = postal_obs.get("raw_value") if postal_obs else None
        city_resolved = city_obs.get("raw_value") if city_obs else None
        province_resolved = province_obs.get("raw_value") if province_obs else None
        
        # Check trigger conditions
        if line1_raw and postal_code and (not city_resolved or not province_resolved):
            # Run post-processor
            line1_clean, city, province = parse_canadian_city_province_from_glued_tail(line1_raw, postal_code)
            
            # If parsing succeeded, update observations
            if city and province:
                # Update or create city observation
                if not city_resolved:
                    observations.append(
                        obs(
                            doc_id=doc_id,
                            ingestion_run_id=ingestion_run_id,
                            field_key="consumer.current_address.city",
                            raw_value=city,
                            page_number=addr_page,
                            method="post_process",
                            confidence=0.85,
                            anchor=anchor_from_page_text(addr_page_text, str(city)),
                        )
                    )
                
                # Update or create province observation
                if not province_resolved:
                    observations.append(
                        obs(
                            doc_id=doc_id,
                            ingestion_run_id=ingestion_run_id,
                            field_key="consumer.current_address.province",
                            raw_value=province,
                            page_number=addr_page,
                            method="post_process",
                            confidence=0.90,
                            anchor=anchor_from_page_text(addr_page_text, str(province)),
                        )
                    )
                
                # Update line1 if it was cleaned (removed glued tail)
                if line1_clean and line1_clean != line1_raw and line1_obs:
                    # Update existing observation
                    line1_obs["raw_value"] = normalize_whitespace(line1_clean)
                    line1_obs["method"] = "post_process"
                    line1_obs["confidence"] = 0.85

    # Critical: emit missing required identity fields
    emit_missing_required_identity(doc_id=doc_id, ingestion_run_id=ingestion_run_id, observations=observations)

    # Update total text length if OCR escalation occurred
    if ocr_escalation_pages:
        total_text_normalized = "\n".join(page_texts_normalized)
        total_text_len = len(total_text_normalized)
        # Update the doc.meta.text_length observation
        for obs_item in observations:
            if obs_item.get("field_key") == "doc.meta.text_length":
                obs_item["raw_value"] = total_text_len
                break
    
    # Update OCR audit to include escalation pages
    if ocr_escalation_pages:
        # Update ocr_pages_count if it exists
        for obs_item in observations:
            if obs_item.get("field_key") == "doc.meta.ocr_pages_count":
                obs_item["raw_value"] = len(ocr_triggered_pages)
                break

    # Inquiry extraction v1 (deterministic) - use normalized page_texts for better extraction
    inquiry_observations = extract_inquiries_from_pages(page_texts_normalized, doc_id, ingestion_run_id)
    observations.extend(inquiry_observations)

    # Store observations with page_texts for text endpoint
    # page_texts_normalized is canonical: OCR text when available (ocr_fallback path), native text when semantic (native_text path)
    # Store both raw (for debugging) and canonical normalized (for display/parsing)
    page_texts_raw_dict = {str(i + 1): text for i, text in enumerate(page_texts_raw)}  # Canonical raw (OCR raw when OCR used, native raw when native used)
    page_texts_native_raw_dict = {str(i + 1): text for i, text in enumerate(page_texts_native_raw)}  # Native raw text (stored separately, not exposed as primary)
    page_texts_normalized_dict = {str(i + 1): text for i, text in enumerate(page_texts_normalized)}  # Canonical text (OCR when available, native when semantic)
    page_texts_ocr_dict = {str(page_num): ocr_text for page_num, ocr_text in page_texts_ocr.items()}
    # Track which pages used OCR (for endpoint labeling)
    page_texts_source_dict = {str(i + 1): "ocr-fallback" if (i + 1) in ocr_triggered_pages else "text-first" for i in range(len(page_texts_normalized))}
    
    payload: Dict[str, Any] = {
        "observations": observations,
        "page_texts": page_texts_normalized_dict,  # Canonical normalized text (OCR when available, native when semantic) - PRIMARY OUTPUT
        "page_texts_raw": page_texts_raw_dict,  # Canonical raw text (OCR raw when OCR used, native raw when native used)
        "page_texts_native_raw": page_texts_native_raw_dict,  # Native raw text (stored separately, not exposed as primary)
        "page_texts_source": page_texts_source_dict,  # Source label per page: "ocr-fallback" or "text-first"
    }
    # Only add page_texts_ocr if OCR was used
    if page_texts_ocr_dict:
        payload["page_texts_ocr"] = page_texts_ocr_dict
    
    save_observations(OBSERVATIONS_DIR, doc_id, payload)

    # Save ingestion run metadata AFTER OCR processing (with audit trail)
    runs = load_ingestion_runs(OBSERVATIONS_DIR, doc_id)
    page1_native_len = len(native_page1)
    notes_parts = [f"Ingestion path: {ingestion_path}"]
    notes_parts.append(f"Anchor hit on page 1 native: {anchor_hit}")
    notes_parts.append(f"Page 1 native text length: {page1_native_len}")
    if ingestion_path == "native_text":
        notes_parts.append("Native text contains anchor strings (page 1). OCR skipped.")
    elif ingestion_path == "ocr_fallback":
        if ocr_triggered_pages:
            notes_parts.append(f"OCR invoked for pages: {sorted(ocr_triggered_pages)} (anchor_hit=false on page 1 native)")
            if force_ocr:
                notes_parts.append("(FORCE_OCR=true)")
        else:
            notes_parts.append("OCR path chosen but no pages required OCR.")
    
    # Add OCR escalation info
    if ocr_escalation_pages:
        notes_parts.append(f"OCR escalation triggered for pages: {sorted(ocr_escalation_pages)} (low-text threshold: {LOW_TEXT_THRESHOLD})")
    
    # Link document to case if provided
    if case_id:
        add_doc_to_case(OBSERVATIONS_DIR, case_id, doc_id)
        notes_parts.append(f"Linked to case_id: {case_id}")
    
    notes = " ".join(notes_parts)
    
    runs.append(
        {
            "ingestion_run_id": ingestion_run_id,
            "created_at": created_at,
            "source_filename": file.filename,
            "stored_filename": stored_filename,
            "method": ingestion_path,
            "notes": notes,
        }
    )
    save_ingestion_runs(OBSERVATIONS_DIR, doc_id, runs)

    result = {
        "doc_id": doc_id,
        "filename": file.filename,
        "stored_as": stored_path,
        "sha256": sha256_hex(content),
        "ingestion_run_id": ingestion_run_id,
    }
    if case_id:
        result["case_id"] = case_id
    return result


@app.get("/documents/{doc_id}/observations")
def get_observations(doc_id: str):
    runs = load_ingestion_runs(OBSERVATIONS_DIR, doc_id)
    data = load_observations(OBSERVATIONS_DIR, doc_id)
    if not data:
        raise HTTPException(status_code=404, detail="Document observations not found")
    
    # Handle backward compatibility: data might be a list (old format) or dict (new format)
    if isinstance(data, list):
        observations = data
    else:
        observations = data.get("observations", [])
    
    if not observations:
        raise HTTPException(status_code=404, detail="Document observations not found")
    return {"doc_id": doc_id, "ingestion_runs": runs, "observations": observations}



@app.get("/documents/{doc_id}/entities")
def get_entities(doc_id: str):
    data = load_observations(OBSERVATIONS_DIR, doc_id)
    if not data:
        raise HTTPException(status_code=404, detail="Document observations not found")
    
    # Handle backward compatibility: data might be a list (old format) or dict (new format)
    if isinstance(data, list):
        observations = data
    else:
        observations = data.get("observations", [])
    
    if not observations:
        raise HTTPException(status_code=404, detail="Document observations not found")

    entities: Dict[str, Dict[str, Any]] = {}

    for o in observations:
        eid = o.get("entity_id")
        if not eid:
            continue

        field_key = o.get("field_key", "")
        entity_type = field_key.split(".", 1)[0] if "." in field_key else "unknown"

        if eid not in entities:
            entities[eid] = {
                "entity_id": eid,
                "entity_type": entity_type,
                "field_count": 0,
                "fields": set(),
                "page_numbers": set(),
            }

        entities[eid]["field_count"] += 1
        entities[eid]["fields"].add(field_key)

        pn = o.get("page_number")
        if pn is not None:
            entities[eid]["page_numbers"].add(pn)

    for e in entities.values():
        e["fields"] = sorted(e["fields"])
        e["page_numbers"] = sorted(e["page_numbers"])

    entity_counts: Dict[str, int] = {}
    for e in entities.values():
        t = e["entity_type"]
        entity_counts[t] = entity_counts.get(t, 0) + 1

    return {
        "doc_id": doc_id,
        "entity_count": len(entities),
        "entity_counts": entity_counts,
        "entities": entities,
    }

@app.get("/documents/{doc_id}/text")
def get_document_text(doc_id: str, page: Optional[int] = None):
    """
    Debug endpoint: return extracted page_texts preview if present in stored observation JSON.
    - Uses canonical text (OCR when available via ocr-fallback, native when semantic via text-first).
    - If ?page=N provided (1-indexed), returns that page only.
    - Caps preview to 4000 chars.
    """
    try:
        data = load_observations(OBSERVATIONS_DIR, doc_id)
        if not data:
            raise HTTPException(status_code=404, detail="Document observations not found")

        # Handle backward compatibility: data might be a list (old format) or dict (new format)
        if isinstance(data, list):
            raise HTTPException(status_code=404, detail="Page texts not available for this document (pre-inquiry format)")

        # page_texts contains canonical normalized text (OCR when available, native when semantic)
        page_texts = data.get("page_texts") or {}
        page_texts_source = data.get("page_texts_source") or {}  # Source labels: "ocr-fallback" or "text-first"
        if not page_texts:
            raise HTTPException(status_code=404, detail="Page texts not available for this document")

        # page_texts keys may be strings; normalize
        norm = {}
        sources = {}
        for k, v in page_texts.items():
            try:
                page_num = int(k)
                norm[page_num] = v  # Already normalized canonical text
                sources[page_num] = page_texts_source.get(k, "text-first")  # Default to text-first for backward compatibility
            except Exception:
                continue

        if not norm:
            raise HTTPException(status_code=404, detail="Page texts not available for this document")

        total_pages = len(norm)
        cap = 4000

        if page is not None:
            if page not in norm:
                raise HTTPException(status_code=404, detail=f"Page {page} not found")
            text = (norm[page] or "")[:cap]
            source = sources.get(page, "text-first")
            return {
                "doc_id": doc_id,
                "page": page,
                "total_pages": total_pages,
                "source": source,
                "preview_chars": len(text),
                "text_preview": text,
            }

        combined = ""
        for p in sorted(norm.keys()):
            source_label = f"[{sources.get(p, 'text-first')}]"
            part = f"[Page {p}] {source_label}\n{norm[p]}\n\n"
            if len(combined) + len(part) > cap:
                remaining = max(0, cap - len(combined))
                combined += part[:remaining]
                break
            combined += part

        return {
            "doc_id": doc_id,
            "page": None,
            "total_pages": total_pages,
            "preview_chars": len(combined),
            "text_preview": combined,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/documents/{doc_id}/resolved")
def get_resolved(doc_id: str):
    data = load_observations(OBSERVATIONS_DIR, doc_id)
    if not data:
        raise HTTPException(status_code=404, detail="Document observations not found")
    
    # Handle backward compatibility: data might be a list (old format) or dict (new format)
    if isinstance(data, list):
        observations = data
    else:
        observations = data.get("observations", [])
    
    if not observations:
        raise HTTPException(status_code=404, detail="Document observations not found")
    
    # Load overrides and merge with resolved
    overrides = load_overrides(OBSERVATIONS_DIR, doc_id)
    resolved_with_overrides = get_resolved_with_overrides(doc_id, observations, overrides)
    
    # Also compute old format for backward compatibility
    resolved_profile_old = resolve_profile(observations)
    
    return {
        "doc_id": doc_id,
        "resolved": resolved_with_overrides,  # New format with overrides
        "resolved_profile": resolved_profile_old,  # Old format for backward compatibility
    }


@app.get("/documents/{doc_id}/quality")
def quality(doc_id: str):
    data = load_observations(OBSERVATIONS_DIR, doc_id)
    if not data:
        raise HTTPException(status_code=404, detail="Document observations not found")
    
    # Handle backward compatibility: data might be a list (old format) or dict (new format)
    if isinstance(data, list):
        observations = data
    else:
        observations = data.get("observations", [])
    
    if not observations:
        raise HTTPException(status_code=404, detail="Document observations not found")

    # Load overrides and get resolved view (extracted merged with overrides)
    overrides = load_overrides(OBSERVATIONS_DIR, doc_id)
    resolved_with_overrides = get_resolved_with_overrides(doc_id, observations, overrides)
    
    # Check required fields using RESOLVED view (extracted + overrides)
    required = required_field_keys()
    missing_required = []
    for k in required:
        resolved_entry = resolved_with_overrides.get(k)
        if not resolved_entry or resolved_entry.get("value") is None:
            missing_required.append(k)

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

    # Bureau from report.bureau observation (canonical text; TransUnion for TU PDF when OCR used)
    bureau = "Unknown"
    for o in observations:
        if o.get("field_key") == "report.bureau" and o.get("raw_value"):
            bureau = o["raw_value"]
            break

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
            "bureau": bureau,
        },
    }


@app.get("/documents/{doc_id}/overrides")
def get_overrides(doc_id: str):
    """Get all overrides for a document."""
    # Verify document exists
    data = load_observations(OBSERVATIONS_DIR, doc_id)
    if not data:
        raise HTTPException(status_code=404, detail="Document observations not found")
    
    overrides = load_overrides(OBSERVATIONS_DIR, doc_id)
    return {
        "doc_id": doc_id,
        "overrides": overrides,
    }


@app.post("/documents/{doc_id}/overrides")
def upsert_override(
    doc_id: str,
    field_key: str = Form(...),
    value: str = Form(...),
    note: Optional[str] = Form(None),
):
    """Upsert an override for a field. Creates or updates."""
    # Verify document exists
    data = load_observations(OBSERVATIONS_DIR, doc_id)
    if not data:
        raise HTTPException(status_code=404, detail="Document observations not found")
    
    # Validate field_key exists in canonical fields
    field_index_dict = {f.field_key: f for f in canonical_fields()}
    if field_key not in field_index_dict:
        raise HTTPException(status_code=400, detail=f"Unknown field_key: {field_key}")
    
    # Set override (handles empty string as None for missing fields)
    override_value = value if value else None
    override = set_override(OBSERVATIONS_DIR, doc_id, field_key, override_value, note)
    
    return {
        "doc_id": doc_id,
        "field_key": field_key,
        "override": override,
    }


@app.delete("/documents/{doc_id}/overrides/{field_key}")
def delete_override_endpoint(doc_id: str, field_key: str):
    """Delete an override for a field."""
    # Verify document exists
    data = load_observations(OBSERVATIONS_DIR, doc_id)
    if not data:
        raise HTTPException(status_code=404, detail="Document observations not found")
    
    deleted = delete_override(OBSERVATIONS_DIR, doc_id, field_key)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Override not found for field_key: {field_key}")
    
    return {
        "doc_id": doc_id,
        "field_key": field_key,
        "deleted": True,
    }


@app.get("/cases/{case_id}/quality")
def case_quality(case_id: str):
    """
    Get merged quality metrics for a case (multiple documents).
    Merges observations across all documents in the case, preserving provenance.
    Detects conflicts when multiple documents have different values for the same required field.
    """
    # Get all document IDs for this case
    doc_ids = get_case_doc_ids(OBSERVATIONS_DIR, case_id)
    if not doc_ids:
        raise HTTPException(status_code=404, detail=f"Case {case_id} not found or has no documents")
    
    # Load observations from all documents
    all_observations: List[Dict[str, Any]] = []
    doc_observations_map: Dict[str, List[Dict[str, Any]]] = {}
    
    for doc_id in doc_ids:
        data = load_observations(OBSERVATIONS_DIR, doc_id)
        if not data:
            continue
        
        # Handle backward compatibility
        if isinstance(data, list):
            observations = data
        else:
            observations = data.get("observations", [])
        
        if observations:
            all_observations.extend(observations)
            doc_observations_map[doc_id] = observations
    
    if not all_observations:
        raise HTTPException(status_code=404, detail=f"No observations found for case {case_id}")
    
    # Group observations by field_key (including entity_id for entity-scoped fields)
    field_observations: Dict[str, List[Dict[str, Any]]] = {}
    for obs in all_observations:
        field_key = obs.get("field_key", "")
        entity_id = obs.get("entity_id")
        
        # For entity-scoped fields, include entity_id in the key
        if entity_id:
            full_key = f"{entity_id}.{field_key}"
        else:
            full_key = field_key
        
        if full_key not in field_observations:
            field_observations[full_key] = []
        field_observations[full_key].append(obs)
    
    # Merge observations: for each field, pick the best observation (highest confidence, most recent)
    # Track conflicts for required fields
    merged_fields: Dict[str, Dict[str, Any]] = {}
    conflicts: Dict[str, List[Dict[str, Any]]] = {}
    required = required_field_keys()
    
    for full_key, obs_list in field_observations.items():
        # Extract base field_key (without entity_id prefix for required field check)
        base_field_key = full_key.split(".", 1)[-1] if "." in full_key else full_key
        
        # Filter to only extracted observations (not missing)
        extracted_obs = [o for o in obs_list if o.get("status") == "extracted" and o.get("raw_value") is not None]
        
        if not extracted_obs:
            continue
        
        # Sort by confidence (desc) then created_at (desc)
        extracted_obs.sort(key=lambda x: (
            float(x.get("confidence", 0.0)),
            x.get("created_at", "")
        ), reverse=True)
        
        best_obs = extracted_obs[0]
        best_value = best_obs.get("raw_value")
        
        # Check for conflicts: if this is a required field and we have multiple different values
        if base_field_key in required:
            unique_values = set()
            for o in extracted_obs:
                val = o.get("raw_value")
                if val is not None:
                    unique_values.add(str(val))
            
            if len(unique_values) > 1:
                # Conflict detected - include all candidates with provenance
                conflicts[base_field_key] = []
                for o in extracted_obs[:5]:  # Limit to top 5 candidates
                    conflicts[base_field_key].append({
                        "value": o.get("raw_value"),
                        "confidence": o.get("confidence", 0.0),
                        "method": o.get("method", "unknown"),
                        "doc_id": o.get("doc_id"),
                        "page_number": o.get("page_number"),
                        "obs_id": o.get("obs_id"),
                        "anchor": o.get("anchor", {}),
                    })
        
        # Store merged field with provenance
        merged_fields[full_key] = {
            "value": best_value,
            "confidence": best_obs.get("confidence", 0.0),
            "method": best_obs.get("method", "unknown"),
            "doc_id": best_obs.get("doc_id"),
            "page_number": best_obs.get("page_number"),
            "obs_id": best_obs.get("obs_id"),
            "anchor": best_obs.get("anchor", {}),
        }
    
    # Calculate merged required fields missing
    present_required_keys = set()
    for full_key in merged_fields.keys():
        base_field_key = full_key.split(".", 1)[-1] if "." in full_key else full_key
        if base_field_key in required:
            present_required_keys.add(base_field_key)
    
    merged_missing_required = [k for k in required if k not in present_required_keys]
    
    # Determine merged quality status
    merged_quality_status = "ok"
    if merged_missing_required:
        merged_quality_status = "needs_review"
    if conflicts:
        merged_quality_status = "needs_review"
    
    return {
        "case_id": case_id,
        "doc_ids": doc_ids,
        "merged_quality": {
            "total_observations": len(all_observations),
            "merged_required_fields_total": len(required),
            "merged_required_fields_missing": len(merged_missing_required),
            "merged_missing_required_fields": merged_missing_required,
            "conflicts": conflicts,
            "merged_quality_status": merged_quality_status,
            "documents_count": len(doc_ids),
        },
    }

