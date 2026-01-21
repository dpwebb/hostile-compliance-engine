from __future__ import annotations

import json
import os
from typing import Any, Dict, List


def _doc_dir(base_dir: str, doc_id: str) -> str:
    return os.path.join(base_dir, doc_id)


def ensure_doc_dirs(base_dir: str, doc_id: str) -> Dict[str, str]:
    """
    Layout:
      data/observations/<doc_id>/
        ingestion_runs.json
        observations.json
        resolved.json   (optional cache)
    """
    d = _doc_dir(base_dir, doc_id)
    os.makedirs(d, exist_ok=True)
    return {
        "doc_dir": d,
        "ingestion_runs": os.path.join(d, "ingestion_runs.json"),
        "observations": os.path.join(d, "observations.json"),
        "resolved": os.path.join(d, "resolved.json"),
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


def save_observations(obs_base_dir: str, doc_id: str, observations: List[Dict[str, Any]]) -> None:
    paths = ensure_doc_dirs(obs_base_dir, doc_id)
    write_json(paths["observations"], observations)
