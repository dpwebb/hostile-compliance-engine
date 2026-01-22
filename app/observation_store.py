from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional


def _doc_dir(base_dir: str, doc_id: str) -> str:
    return os.path.join(base_dir, doc_id)


def ensure_doc_dirs(base_dir: str, doc_id: str) -> Dict[str, str]:
    """
    Layout:
      data/observations/<doc_id>/
        ingestion_runs.json
        observations.json
        resolved.json   (optional cache)
        overrides.json  (field overrides)
    """
    d = _doc_dir(base_dir, doc_id)
    os.makedirs(d, exist_ok=True)
    return {
        "doc_dir": d,
        "ingestion_runs": os.path.join(d, "ingestion_runs.json"),
        "observations": os.path.join(d, "observations.json"),
        "resolved": os.path.join(d, "resolved.json"),
        "overrides": os.path.join(d, "overrides.json"),
    }


def write_json(path: str, payload: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_ingestion_runs(obs_base_dir: str, doc_id: str) -> List[Dict[str, Any]]:
    paths = ensure_doc_dirs(obs_base_dir, doc_id)
    if not os.path.exists(paths["ingestion_runs"]):
        return []
    return read_json(paths["ingestion_runs"])


def save_ingestion_runs(obs_base_dir: str, doc_id: str, runs: List[Dict[str, Any]]) -> None:
    paths = ensure_doc_dirs(obs_base_dir, doc_id)
    write_json(paths["ingestion_runs"], runs)


def load_observations(obs_base_dir: str, doc_id: str) -> List[Dict[str, Any]]:
    paths = ensure_doc_dirs(obs_base_dir, doc_id)
    if not os.path.exists(paths["observations"]):
        return []
    return read_json(paths["observations"])


def save_observations(obs_base_dir: str, doc_id: str, observations: Any) -> None:
    """
    Save observations. Accepts either a list (legacy) or dict (new format with page_texts).
    """
    paths = ensure_doc_dirs(obs_base_dir, doc_id)
    write_json(paths["observations"], observations)


def _cases_file(obs_base_dir: str) -> str:
    """Path to cases.json mapping file."""
    return os.path.join(obs_base_dir, "cases.json")


def load_cases(obs_base_dir: str) -> Dict[str, List[str]]:
    """
    Load case_id -> [doc_id, ...] mapping.
    Returns: {case_id: [doc_id, ...]}
    """
    cases_file = _cases_file(obs_base_dir)
    if not os.path.exists(cases_file):
        return {}
    return read_json(cases_file)


def save_cases(obs_base_dir: str, cases: Dict[str, List[str]]) -> None:
    """Save case_id -> [doc_id, ...] mapping."""
    cases_file = _cases_file(obs_base_dir)
    write_json(cases_file, cases)


def add_doc_to_case(obs_base_dir: str, case_id: str, doc_id: str) -> None:
    """Add a document to a case. Creates case if it doesn't exist."""
    cases = load_cases(obs_base_dir)
    if case_id not in cases:
        cases[case_id] = []
    if doc_id not in cases[case_id]:
        cases[case_id].append(doc_id)
    save_cases(obs_base_dir, cases)


def get_case_doc_ids(obs_base_dir: str, case_id: str) -> List[str]:
    """Get all document IDs for a case."""
    cases = load_cases(obs_base_dir)
    return cases.get(case_id, [])


def load_overrides(obs_base_dir: str, doc_id: str) -> Dict[str, Dict[str, Any]]:
    """
    Load overrides for a document.
    Returns: {field_key: {value, note, created_at, updated_at}}
    """
    paths = ensure_doc_dirs(obs_base_dir, doc_id)
    if not os.path.exists(paths["overrides"]):
        return {}
    return read_json(paths["overrides"])


def save_overrides(obs_base_dir: str, doc_id: str, overrides: Dict[str, Dict[str, Any]]) -> None:
    """Save overrides for a document."""
    paths = ensure_doc_dirs(obs_base_dir, doc_id)
    write_json(paths["overrides"], overrides)


def set_override(
    obs_base_dir: str,
    doc_id: str,
    field_key: str,
    value: Any,
    note: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Set an override for a field. Upserts (creates or updates).
    Returns the override dict with created_at/updated_at.
    """
    from datetime import datetime, timezone
    
    overrides = load_overrides(obs_base_dir, doc_id)
    now = datetime.now(timezone.utc).isoformat()
    
    if field_key in overrides:
        # Update existing
        overrides[field_key]["value"] = value
        overrides[field_key]["updated_at"] = now
        if note is not None:
            overrides[field_key]["note"] = note
    else:
        # Create new
        overrides[field_key] = {
            "value": value,
            "note": note or "",
            "created_at": now,
            "updated_at": now,
        }
    
    save_overrides(obs_base_dir, doc_id, overrides)
    return overrides[field_key]


def delete_override(obs_base_dir: str, doc_id: str, field_key: str) -> bool:
    """
    Delete an override for a field.
    Returns True if override existed and was deleted, False otherwise.
    """
    overrides = load_overrides(obs_base_dir, doc_id)
    if field_key in overrides:
        del overrides[field_key]
        save_overrides(obs_base_dir, doc_id, overrides)
        return True
    return False
