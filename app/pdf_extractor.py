from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

CAN_POSTAL_RE = re.compile(r"\b([A-Z]\d[A-Z])\s?(\d[A-Z]\d)\b", re.IGNORECASE)
PROVINCE_RE = re.compile(r"\b(AB|BC|MB|NB|NL|NS|NT|NU|ON|PE|QC|SK|YT)\b", re.IGNORECASE)

NAME_CLEAN_RE = re.compile(r"[^A-Za-z \-'.]", re.IGNORECASE)


def normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def find_can_postal(text: str) -> Optional[str]:
    m = CAN_POSTAL_RE.search(text or "")
    if not m:
        return None
    return f"{m.group(1).upper()} {m.group(2).upper()}"


def find_province(text: str) -> Optional[str]:
    m = PROVINCE_RE.search(text or "")
    if not m:
        return None
    return m.group(1).upper()


def looks_like_name(s: str) -> bool:
    s = normalize_whitespace(s)
    if not s:
        return False
    cleaned = NAME_CLEAN_RE.sub("", s).strip()
    if len(cleaned) < 4:
        return False
    if sum(ch.isalpha() for ch in cleaned) < 3:
        return False
    bad = ["transunion", "equifax", "report", "consumer", "address", "credit", "monitoring"]
    low = cleaned.lower()
    if any(b in low for b in bad):
        return False
    if len(cleaned.split()) < 2:
        return False
    return True


def score_name_candidate(s: str) -> int:
    s = normalize_whitespace(s)
    words = s.split()
    score = 0
    if 2 <= len(words) <= 4:
        score += 4
    alpha = sum(c.isalpha() for c in s)
    score += min(6, alpha // 3)
    if len(s) <= 40:
        score += 2
    return score


def extract_identity_from_pages(page_texts: List[str]) -> Dict[str, Any]:
    """
    Deterministic best-effort extraction of:
      - consumer.full_name
      - consumer.current_address.line1
      - consumer.current_address.city
      - consumer.current_address.province
      - consumer.current_address.postal_code
    """
    name_candidates: List[Tuple[int, str, int]] = []

    name_patterns = [
        re.compile(r"(?:\bconsumer\s+name\b|\bname\b)\s*[:\-]\s*(.+)", re.IGNORECASE),
    ]

    address_header_patterns = [
        re.compile(r"\bcurrent\s+address\b", re.IGNORECASE),
        re.compile(r"\baddress\b", re.IGNORECASE),
        re.compile(r"\bresidential\s+address\b", re.IGNORECASE),
    ]

    address_blocks: List[Tuple[int, str]] = []

    for idx, raw in enumerate(page_texts):
        page_num = idx + 1
        text = raw or ""

        for pat in name_patterns:
            for m in pat.finditer(text):
                cand = normalize_whitespace(m.group(1))
                cand = cand.split("  ")[0].strip()
                cand = cand.split(" | ")[0].strip()
                cand = cand[:80].strip()
                if looks_like_name(cand):
                    name_candidates.append((page_num, cand, score_name_candidate(cand)))

        if re.search(r"\bconsumer\s+information\b", text, re.IGNORECASE):
            lines = [normalize_whitespace(x) for x in text.splitlines() if normalize_whitespace(x)]
            for i, line in enumerate(lines[:40]):
                if re.search(r"\bconsumer\s+information\b", line, re.IGNORECASE):
                    for nxt in lines[i + 1:i + 6]:
                        if looks_like_name(nxt):
                            name_candidates.append((page_num, nxt, score_name_candidate(nxt) + 2))

        lines = [normalize_whitespace(x) for x in text.splitlines()]
        for i, line in enumerate(lines):
            if not line:
                continue
            if any(h.search(line) for h in address_header_patterns):
                block_lines: List[str] = []
                for j in range(i, min(i + 8, len(lines))):
                    if lines[j]:
                        block_lines.append(lines[j])
                block = "\n".join(block_lines).strip()
                if find_can_postal(block) or re.search(r"\b\d{1,6}\b", block):
                    address_blocks.append((page_num, block))

    best_name = None
    best_name_page = None
    if name_candidates:
        name_candidates.sort(key=lambda x: (x[2], -x[0]), reverse=True)
        best_name_page, best_name, _ = name_candidates[0]

    best_block = None
    best_block_page = None
    best_block_score = -1
    for page_num, block in address_blocks:
        score = 0
        if find_can_postal(block):
            score += 5
        if find_province(block):
            score += 3
        if re.search(r"\b\d{1,6}\b", block):
            score += 1
        if page_num == 1:
            score += 2
        if score > best_block_score:
            best_block_score = score
            best_block = block
            best_block_page = page_num

    addr_line1 = None
    addr_city = None
    addr_prov = None
    addr_postal = None

    if best_block:
        postal = find_can_postal(best_block)
        prov = find_province(best_block)

        city_line = None
        for line in best_block.splitlines():
            if postal and postal.replace(" ", "") in line.replace(" ", "").upper():
                city_line = line
                break
        if city_line:
            city_line_norm = normalize_whitespace(city_line).replace(",", " ")
            if postal:
                city_line_norm = re.sub(re.escape(postal), " ", city_line_norm, flags=re.IGNORECASE)
                city_line_norm = re.sub(re.escape(postal.replace(" ", "")), " ", city_line_norm, flags=re.IGNORECASE)
            if prov:
                city_line_norm = re.sub(rf"\b{prov}\b", " ", city_line_norm, flags=re.IGNORECASE)
            city_line_norm = normalize_whitespace(city_line_norm)
            if city_line_norm:
                addr_city = city_line_norm

        addr_postal = postal
        addr_prov = prov

        block_lines = [normalize_whitespace(x) for x in best_block.splitlines() if normalize_whitespace(x)]
        for line in block_lines:
            if re.search(r"\b\d{1,6}\b", line) and sum(c.isalpha() for c in line) >= 4:
                if re.search(r"\baddress\b", line, re.IGNORECASE):
                    m = re.search(r"\baddress\b\s*[:\-]?\s*(.+)", line, re.IGNORECASE)
                    if m:
                        candidate = normalize_whitespace(m.group(1))
                        if candidate:
                            addr_line1 = candidate
                            break
                    continue
                addr_line1 = line
                break

        if not addr_line1 and len(block_lines) >= 2:
            addr_line1 = block_lines[1]

    consumer = {
        "full_name": best_name,
        "current_address": {
            "line1": addr_line1,
            "city": addr_city,
            "province": addr_prov,
            "postal_code": addr_postal,
        },
    }

    evidence = {
        "full_name": (best_name_page, best_name) if best_name else None,
        "address_block": (best_block_page, best_block) if best_block else None,
    }

    return {"consumer": consumer, "evidence": evidence}
