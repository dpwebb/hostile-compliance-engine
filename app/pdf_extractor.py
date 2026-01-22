from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

CAN_POSTAL_RE = re.compile(r"\b([A-Z]\d[A-Z])\s?(\d[A-Z]\d)\b", re.IGNORECASE)
PROVINCE_RE = re.compile(r"\b(AB|BC|MB|NB|NL|NS|NT|NU|ON|PE|QC|SK|YT)\b", re.IGNORECASE)

NAME_CLEAN_RE = re.compile(r"[^A-Za-z \-'.]", re.IGNORECASE)

# TransUnion-specific patterns
# Pattern: "Current" on one line, then address + date on next line(s)
TU_CURRENT_ADDRESS_RE = re.compile(
    r"(?im)^\s*Current\s*$[\r\n]+^\s*(?P<addr>.+?)\s+(?P<date>\d{2}/\d{2}/\d{4})",
    re.MULTILINE | re.DOTALL
)
TU_NAME_RE = re.compile(r"(?i)^\s*Name\s+(?P<name>[A-Z\s]+?)\s+(\d{2}/\d{2}/\d{4})", re.MULTILINE)

# OCR-tolerant Canadian postal code: allow O where digit expected
TU_POSTAL_CANDIDATE_RE = re.compile(r"[A-Z][0-9O][A-Z][0-9O][A-Z][0-9O]", re.IGNORECASE)


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


def extract_tu_current_address(text: str) -> Optional[str]:
    """
    Extract current address from TransUnion OCR text.
    Anchored on "Current" label (not "Previous").
    Pattern: "Current" newline "<address_line> <date>"
    Returns the address string (without date) or None.
    """
    # Try multiline pattern first
    m = TU_CURRENT_ADDRESS_RE.search(text)
    if m:
        addr = m.group("addr").strip()
        # Never return if it starts with "Previous"
        if addr.lower().startswith("previous"):
            return None
        return addr
    
    # Fallback: try simpler pattern if multiline didn't match
    # "Current" followed by address on same or next line
    simple_pattern = re.compile(
        r"(?i)Current\s+(?P<addr>[^\r\n]+?)\s+(\d{2}/\d{2}/\d{4})",
        re.MULTILINE
    )
    m = simple_pattern.search(text)
    if m:
        addr = m.group("addr").strip()
        if addr.lower().startswith("previous"):
            return None
        return addr
    
    return None


def find_tu_postal_code_ocr_tolerant(text: str) -> Optional[str]:
    """
    Find Canadian postal code with OCR tolerance (O/0 confusion).
    Returns formatted as "A1A 1A1" or None.
    """
    # Find candidate near the end of the string (postal codes are usually at the end)
    # Search from end backwards
    text_upper = text.upper()
    for i in range(len(text_upper) - 5, -1, -1):
        candidate = text_upper[i:i+6]
        m = TU_POSTAL_CANDIDATE_RE.match(candidate)
        if m:
            # Normalize: replace O with 0 in digit positions (2, 4, 6)
            normalized = list(candidate)
            normalized[1] = '0' if normalized[1] == 'O' else normalized[1]
            normalized[3] = '0' if normalized[3] == 'O' else normalized[3]
            normalized[5] = '0' if normalized[5] == 'O' else normalized[5]
            # Format as "A1A 1A1"
            return f"{normalized[0]}{normalized[1]}{normalized[2]} {normalized[3]}{normalized[4]}{normalized[5]}"
    return None


def parse_canadian_city_province_from_glued_tail(addr_raw: str, postal: str) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Deterministic post-processor to split city/province from glued tail in Canadian addresses.
    
    Algorithm:
    1) Normalize postal code (uppercase, remove spaces) => e.g. "B0N2J0"
    2) Normalize address (uppercase, remove spaces)
    3) Find postal code position from end (rfind). If not found, return unchanged.
    4) Extract province candidate (2 chars before postal code). Validate against Canadian provinces.
    5) Walk backwards from province start to collect trailing letters as city (min length 2).
    6) Clean line1 by removing the glued city/province/postal tail.
    
    Returns: (line1_clean, city, province)
    - If parsing fails, returns (addr_raw, None, None)
    """
    if not addr_raw or not postal:
        return (addr_raw, None, None)
    
    # Step 1: Normalize postal code
    pc = postal.upper().replace(" ", "")  # e.g. B0N2J0
    
    # Step 2: Normalize address (for searching)
    s = addr_raw.upper().replace(" ", "")
    
    # Step 3: Find postal code position from end
    idx = s.rfind(pc)
    if idx == -1:
        # Try OCR variant: O instead of 0
        pc_ocr = pc.replace("0", "O")
        idx = s.rfind(pc_ocr)
        if idx == -1:
            return (addr_raw, None, None)
    
    # Step 4: Extract and validate province candidate
    if idx < 2:
        return (addr_raw, None, None)
    
    prov_candidate = s[idx - 2:idx]
    valid_provinces = {"AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT"}
    if prov_candidate not in valid_provinces:
        return (addr_raw, None, None)
    
    # Step 5: Walk backwards from idx-2 to collect trailing letters as city
    city_end_pos_normalized = idx - 2
    city_start_pos_normalized = city_end_pos_normalized
    
    # Walk backwards to find start of city (letters only)
    while city_start_pos_normalized > 0 and s[city_start_pos_normalized - 1].isalpha():
        city_start_pos_normalized -= 1
    
    # Extract city from normalized string
    city_normalized = s[city_start_pos_normalized:city_end_pos_normalized]
    
    # City must be at least 2 characters
    if len(city_normalized) < 2:
        return (addr_raw, None, None)
    
    # Step 6: Find positions in original string
    # Find postal code in original (try multiple formats, case-insensitive)
    addr_upper = addr_raw.upper()
    postal_no_space = postal.replace(" ", "").upper()
    postal_pos = addr_upper.rfind(postal_no_space)
    if postal_pos == -1:
        postal_pos = addr_upper.rfind(postal.upper())
    if postal_pos == -1:
        postal_ocr = postal_no_space.replace("0", "O")
        postal_pos = addr_upper.rfind(postal_ocr)
    
    if postal_pos == -1 or postal_pos < 2:
        return (addr_raw, None, None)
    
    # Province is 2 chars before postal in original
    prov_start_original = postal_pos - 2
    
    # Walk backwards from province to find city start (letters only)
    city_start_original = prov_start_original
    while city_start_original > 0 and addr_raw[city_start_original - 1].isalpha():
        city_start_original -= 1
    
    # Extract city from original (preserve original case)
    if city_start_original < prov_start_original:
        city_raw = addr_raw[city_start_original:prov_start_original]
        city_clean = re.sub(r'[^A-Za-z]', '', city_raw)
        
        # Validate city length
        if len(city_clean) >= 2:
            # Step 7: Clean line1 by removing glued tail
            line1_clean = addr_raw[:city_start_original].strip()
            # Remove trailing non-alphanumeric characters
            line1_clean = re.sub(r'[^\w\s]+$', '', line1_clean).strip()
            
            # Step 8: Return results
            province = prov_candidate
            city = city_clean.title() if city_clean else None
            return (line1_clean, city, province)
    
    return (addr_raw, None, None)


def parse_tu_address(addr_str: str) -> Dict[str, Optional[str]]:
    """
    Parse TransUnion address string into line1, city, province, postal_code.
    Handles smashed OCR text like "26 MAIN ST E PO BOX 593STEWIACKENSBON2J0"
    """
    result = {
        "line1": None,
        "city": None,
        "province": None,
        "postal_code": None,
    }
    
    if not addr_str:
        return result
    
    original_addr = addr_str
    
    # Find postal code first (OCR-tolerant)
    postal = find_tu_postal_code_ocr_tolerant(addr_str)
    if postal:
        result["postal_code"] = postal
        # Remove postal code from string for further parsing
        postal_no_space = postal.replace(" ", "")
        # Remove from end of string (postal codes are usually at the end)
        addr_str = re.sub(re.escape(postal_no_space) + r'[^\w]*$', '', addr_str, flags=re.IGNORECASE)
        addr_str = re.sub(re.escape(postal) + r'[^\w]*$', '', addr_str, flags=re.IGNORECASE)
    
    # Find province code
    prov_match = PROVINCE_RE.search(addr_str)
    if prov_match:
        result["province"] = prov_match.group(1).upper()
        prov_pos = prov_match.start()
        
        # Everything before province is line1 + city (possibly smashed together)
        before_prov = addr_str[:prov_pos].strip()
        
        # For smashed text like "26 MAIN ST E PO BOX 593STEWIACKE", we need to split:
        # - line1: "26 MAIN ST E PO BOX 593"
        # - city: "STEWIACKE"
        # Look for the boundary: last number/digit sequence before province
        
        # Find the last sequence of digits/numbers before province
        # This often marks the end of line1 (e.g., "PO BOX 593" or "APT 123")
        digit_pattern = re.compile(r'\d+')
        digit_matches = list(digit_pattern.finditer(before_prov))
        
        if digit_matches:
            # Use the last digit sequence as potential boundary
            last_digit_end = digit_matches[-1].end()
            # City candidate is everything after last digit sequence
            city_candidate = before_prov[last_digit_end:].strip()
            # Remove non-alphabetic characters from start/end
            city_candidate = re.sub(r'^[^\w]+', '', city_candidate)
            city_candidate = re.sub(r'[^\w]+$', '', city_candidate)
            
            # Extract city (should be alphabetic, 4+ chars)
            if city_candidate and len(re.sub(r'[^\w]', '', city_candidate)) >= 4:
                # Take the first word-like sequence as city
                city_match = re.search(r'([A-Za-z]{4,})', city_candidate)
                if city_match:
                    result["city"] = city_match.group(1).title()
                    # Remove city from before_prov
                    city_start_in_before = before_prov.find(city_match.group(1))
                    if city_start_in_before >= 0:
                        before_prov = before_prov[:city_start_in_before].strip()
            
            # line1 is everything before city
            if before_prov:
                result["line1"] = normalize_whitespace(before_prov)
        else:
            # No digits found - try to find city as last substantial word before province
            words = before_prov.split()
            if words:
                # Look for last word that's 4+ chars, alphabetic
                for word in reversed(words):
                    word_clean = re.sub(r'[^\w]', '', word)
                    if len(word_clean) >= 4 and word_clean.isalpha():
                        result["city"] = word_clean.title()
                        # Remove city from line1
                        city_pos = before_prov.rfind(word)
                        if city_pos >= 0:
                            before_prov = before_prov[:city_pos].strip()
                        break
                if before_prov:
                    result["line1"] = normalize_whitespace(before_prov)
    
    # Fallback: if we have postal but no province, try to extract from original string
    if postal and not result["province"]:
        prov_match = PROVINCE_RE.search(original_addr)
        if prov_match:
            result["province"] = prov_match.group(1).upper()
    
    # If we still don't have line1, use the address string up to postal/province
    if not result["line1"] and original_addr:
        remaining = original_addr
        if postal:
            remaining = re.sub(re.escape(postal.replace(" ", "")), "", remaining, flags=re.IGNORECASE)
        if result["province"]:
            remaining = re.sub(rf"\b{result['province']}\b", "", remaining, flags=re.IGNORECASE)
        remaining = normalize_whitespace(remaining)
        if remaining:
            result["line1"] = remaining
    
    # Post-processing: if we have postal_code and line1 but missing city/province, 
    # try deterministic tail parsing from glued text (e.g. "593STEWIACKENSBON2J0")
    if result["postal_code"] and (not result["city"] or not result["province"]):
        # Use original_addr which has the full glued string, or line1 if original_addr not available
        search_text = original_addr if original_addr else (result["line1"] or "")
        if search_text:
            line1_clean, city, province = parse_canadian_city_province_from_glued_tail(search_text, result["postal_code"])
            
            # If parsing succeeded (city and province found)
            if city and province:
                # Update results
                if not result["city"]:
                    result["city"] = city
                if not result["province"]:
                    result["province"] = province
                
                # Update line1 if we got a cleaned version
                if line1_clean and line1_clean != search_text:
                    result["line1"] = normalize_whitespace(line1_clean)
    
    return result


def extract_tu_full_name(text: str) -> Optional[str]:
    """
    Extract full name from TransUnion OCR text.
    Pattern: "Name DAVIDPHILIPWEBB 01/10/2026"
    Returns the name token (may be all-caps, no spaces).
    """
    # Try the standard pattern first
    m = TU_NAME_RE.search(text)
    if m:
        name = m.group("name").strip()
        if name:
            # Keep as-is (may be "DAVIDPHILIPWEBB" or already spaced)
            # Don't overfit with name splitting heuristics
            return name.upper() if name.isupper() else name
    
    # Fallback: simpler pattern for "Name" followed by all-caps text then date
    simple_pattern = re.compile(
        r"(?i)^\s*Name\s+(?P<name>[A-Z]+(?:[A-Z\s]+[A-Z]+)?)\s+(\d{2}/\d{2}/\d{4})",
        re.MULTILINE
    )
    m = simple_pattern.search(text)
    if m:
        name = m.group("name").strip()
        if name and len(name) >= 4:  # Minimum reasonable name length
            return name.upper() if name.isupper() else name
    
    return None


def extract_tu_personal_information_name(page_texts: List[str]) -> Optional[Tuple[int, str]]:
    """
    Extract full name from TransUnion Personal Information section.
    Looks for "Personal Information" section and extracts:
    - Given Name(s)
    - Middle Name (if present)
    - Surname
    Concatenates them into full name.
    
    Returns: (page_number, full_name) or None
    """
    for idx, text in enumerate(page_texts):
        page_num = idx + 1
        
        # Look for "Personal Information" section
        if not re.search(r"\bPersonal\s+Information\b", text, re.IGNORECASE):
            continue
        
        lines = text.splitlines()
        given_name = None
        middle_name = None
        surname = None
        
        # Find the Personal Information section
        in_section = False
        for i, line in enumerate(lines):
            line_lower = line.lower()
            
            # Start of section
            if re.search(r"\bPersonal\s+Information\b", line, re.IGNORECASE):
                in_section = True
                continue
            
            # End of section (next major section)
            if in_section and re.search(r"^\s*(Address|Tradelines|Accounts|Collections|Public Records|Summary)\b", line, re.IGNORECASE):
                break
            
            if not in_section:
                continue
            
            # Extract Given Name(s)
            if re.search(r"\bGiven\s+Name", line, re.IGNORECASE):
                # Look for pattern like "Given Name(s): DAVID PHILIP" or "Given Name(s) DAVID PHILIP"
                match = re.search(r"Given\s+Name(?:\(s\))?\s*:?\s*([A-Z][A-Z\s]+)", line, re.IGNORECASE)
                if match:
                    given_name = match.group(1).strip()
                # Also check next line if current line is just the label
                elif i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if next_line and re.match(r"^[A-Z][A-Z\s]+$", next_line):
                        given_name = next_line.strip()
            
            # Extract Middle Name
            if re.search(r"\bMiddle\s+Name", line, re.IGNORECASE):
                match = re.search(r"Middle\s+Name\s*:?\s*([A-Z][A-Z\s]*)", line, re.IGNORECASE)
                if match:
                    middle_name = match.group(1).strip()
                elif i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if next_line and re.match(r"^[A-Z][A-Z\s]*$", next_line):
                        middle_name = next_line.strip()
            
            # Extract Surname
            if re.search(r"\bSurname", line, re.IGNORECASE):
                match = re.search(r"Surname\s*:?\s*([A-Z][A-Z\s]+)", line, re.IGNORECASE)
                if match:
                    surname = match.group(1).strip()
                elif i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if next_line and re.match(r"^[A-Z][A-Z\s]+$", next_line):
                        surname = next_line.strip()
        
        # Build full name from components
        if surname:  # Surname is required
            name_parts = []
            if given_name:
                name_parts.append(given_name)
            if middle_name:
                name_parts.append(middle_name)
            name_parts.append(surname)
            
            if name_parts:
                full_name = " ".join(name_parts)
                return (page_num, full_name)
    
    return None


def extract_tu_addresses_table(page_texts: List[str]) -> Optional[Tuple[int, Dict[str, Optional[str]]]]:
    """
    Extract current address from TransUnion "Address(es):" table section.
    
    The section states: "Your most current Since date address is listed first."
    This is the authoritative source for current address.
    
    Parses the table into structured rows with:
    - address_line
    - city
    - province
    - postal_code
    - since_date (if available)
    
    Returns the first row (most current) as the current address.
    
    Returns: (page_number, address_dict) or None
    """
    for idx, text in enumerate(page_texts):
        page_num = idx + 1
        
        # Look for "Address(es):" section header
        addresses_match = re.search(r"\bAddress(?:\(es\))?\s*:", text, re.IGNORECASE)
        if not addresses_match:
            continue
        
        # Verify this is the authoritative section (should mention "most current")
        section_start = addresses_match.start()
        section_text = text[section_start:section_start + 500]  # Look ahead 500 chars
        if not re.search(r"most\s+current|listed\s+first", section_text, re.IGNORECASE):
            continue
        
        lines = text.splitlines()
        
        # Find where the Address(es) section starts in the lines
        section_line_idx = None
        for i, line in enumerate(lines):
            if addresses_match.start() <= text.find(line) <= addresses_match.end() + 50:
                section_line_idx = i
                break
        
        if section_line_idx is None:
            continue
        
        # Look for table data rows after the section header
        # Table might have headers like "Address", "City", "Province", "Postal Code", "Since"
        # Or might be a simple list format
        address_rows = []
        
        # Scan lines after the section header
        for i in range(section_line_idx + 1, min(section_line_idx + 30, len(lines))):
            line = lines[i].strip()
            if not line:
                continue
            
            # Stop if we hit another major section
            if re.search(r"^\s*(Previous\s+Address|Investigation|Tradelines|Accounts|Collections|Public Records|Summary|Personal Information)\b", line, re.IGNORECASE):
                break
            
            # Skip header lines
            if re.search(r"^\s*(Address|City|Province|Postal|Since)\s*$", line, re.IGNORECASE):
                continue
            
            # Try to parse as address row - look for postal code or province
            postal = find_can_postal(line)
            province = find_province(line)
            
            if not postal and not province:
                # Might be a multi-line address - check if next line has postal/province
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    postal = find_can_postal(next_line)
                    province = find_province(next_line)
                    if postal or province:
                        # Combine current and next line
                        line = line + " " + next_line
                        i += 1  # Skip next line in outer loop
            
            if postal or province:
                # This looks like an address row - parse it
                addr_dict = {
                    "address_line": None,
                    "city": None,
                    "province": province,
                    "postal_code": postal,
                    "since_date": None,
                }
                
                # Extract since_date if present (format: MM/YYYY or MM/DD/YYYY)
                since_match = re.search(r"(\d{1,2}/\d{2,4})", line)
                if since_match:
                    addr_dict["since_date"] = since_match.group(1)
                
                # Extract city - look for word sequences before province/postal
                if province:
                    prov_pos = line.upper().find(province.upper())
                    if prov_pos > 0:
                        before_prov = line[:prov_pos].strip()
                        # City is typically the last substantial word sequence before province
                        # Remove address line parts first
                        city_candidate = before_prov
                        # Remove common address patterns
                        city_candidate = re.sub(r"^\d+[A-Z]?\s+[A-Z\s]+(?:ST|STREET|AVE|AVENUE|RD|ROAD|BLVD|BOULEVARD|DR|DRIVE|CT|COURT|LN|LANE|PL|PLACE|WAY|CIRCLE|CIR)\s*", "", city_candidate, flags=re.IGNORECASE)
                        city_candidate = re.sub(r"PO\s+BOX\s+\d+\s*", "", city_candidate, flags=re.IGNORECASE)
                        city_candidate = re.sub(r"APT\s+\d+\s*", "", city_candidate, flags=re.IGNORECASE)
                        city_candidate = re.sub(r"UNIT\s+\d+\s*", "", city_candidate, flags=re.IGNORECASE)
                        city_candidate = city_candidate.strip()
                        
                        # Extract the last word sequence as city (should be 2+ chars, alphabetic)
                        city_words = city_candidate.split()
                        if city_words:
                            # Take the last word or last few words as city
                            city = city_words[-1] if len(city_words) == 1 else " ".join(city_words[-2:])
                            city = re.sub(r"[^A-Za-z\s]", "", city).strip()
                            if city and len(city) >= 2:
                                addr_dict["city"] = city
                                
                                # Address line is everything before city
                                city_start = before_prov.upper().rfind(city.upper())
                                if city_start >= 0:
                                    addr_dict["address_line"] = before_prov[:city_start].strip()
                
                # If no city found but we have province, try different approach
                if not addr_dict["city"] and province:
                    prov_pos = line.upper().find(province.upper())
                    if prov_pos > 0:
                        before_prov = line[:prov_pos].strip()
                        # Try to find city as last capitalized word before province
                        words = before_prov.split()
                        for word in reversed(words):
                            word_clean = re.sub(r"[^A-Za-z]", "", word)
                            if len(word_clean) >= 2 and word_clean[0].isupper():
                                addr_dict["city"] = word_clean
                                # Address line is before city
                                city_start = before_prov.upper().rfind(word_clean.upper())
                                if city_start >= 0:
                                    addr_dict["address_line"] = before_prov[:city_start].strip()
                                break
                
                # Extract address_line if not already set
                if not addr_dict["address_line"]:
                    if province:
                        prov_pos = line.upper().find(province.upper())
                        if prov_pos > 0:
                            addr_dict["address_line"] = line[:prov_pos].strip()
                    elif postal:
                        postal_pos = line.upper().find(postal.replace(" ", "").upper())
                        if postal_pos > 0:
                            addr_dict["address_line"] = line[:postal_pos].strip()
                
                # Only add if we have at least some address components
                if addr_dict["address_line"] or addr_dict["city"] or addr_dict["province"] or addr_dict["postal_code"]:
                    address_rows.append(addr_dict)
        
        # Select first row (most current) as authoritative
        if address_rows:
            current_addr = address_rows[0]
            return (page_num, {
                "line1": current_addr.get("address_line"),
                "city": current_addr.get("city"),
                "province": current_addr.get("province"),
                "postal_code": current_addr.get("postal_code"),
            })
    
    return None


def extract_identity_from_pages(page_texts: List[str], bureau: Optional[str] = None) -> Dict[str, Any]:
    """
    Deterministic best-effort extraction of:
      - consumer.full_name
      - consumer.current_address.line1
      - consumer.current_address.city
      - consumer.current_address.province
      - consumer.current_address.postal_code
    
    For TransUnion reports, prioritizes:
    1. Address(es) table (authoritative source: "Your most current Since date address is listed first.")
    2. Personal Information section for full name
    
    Then falls back to generic extraction.
    """
    # Check if this is a TransUnion report
    is_transunion = bureau == "TransUnion"
    if not is_transunion:
        # Try to detect from text patterns
        combined_text = "\n".join(page_texts)
        is_transunion = bool(re.search(r"\btransunion\b", combined_text, re.IGNORECASE))
    
    # For TransUnion: prioritize Address(es) table and Personal Information section
    if is_transunion:
        # Extract from Address(es) table (authoritative source)
        addresses_result = extract_tu_addresses_table(page_texts)
        addr_from_table = addresses_result[1] if addresses_result else None
        addr_table_page = addresses_result[0] if addresses_result else None
        
        # Extract from Personal Information section
        name_result = extract_tu_personal_information_name(page_texts)
        name_from_pi = name_result[1] if name_result else None
        name_pi_page = name_result[0] if name_result else None
        
        # If we found Address(es) table, use it as authoritative (ignore later sections)
        if addr_from_table:
            consumer = {
                "full_name": name_from_pi,  # From Personal Information section
                "current_address": {
                    "line1": addr_from_table.get("line1"),
                    "city": addr_from_table.get("city"),
                    "province": addr_from_table.get("province"),
                    "postal_code": addr_from_table.get("postal_code"),
                },
            }
            
            evidence = {
                "full_name": (name_pi_page, name_from_pi) if name_from_pi else None,
                "address_block": (addr_table_page, f"Address(es) table row: {addr_from_table}") if addr_table_page else None,
            }
            
            return {"consumer": consumer, "evidence": evidence}
    
    # Fallback: Try TransUnion extraction with legacy patterns (if Address(es) table not found)
    combined_text = "\n".join(page_texts)
    tu_current_addr_str = extract_tu_current_address(combined_text)
    tu_name = extract_tu_full_name(combined_text)
    
    # If we found TransUnion patterns, use TU extraction
    if tu_current_addr_str or tu_name:
        tu_addr_parsed = parse_tu_address(tu_current_addr_str) if tu_current_addr_str else {}
        
        # Find which page had the TU data
        tu_page = None
        for idx, text in enumerate(page_texts):
            if extract_tu_current_address(text) or extract_tu_full_name(text):
                tu_page = idx + 1
                break
        
        consumer = {
            "full_name": tu_name,
            "current_address": {
                "line1": tu_addr_parsed.get("line1"),
                "city": tu_addr_parsed.get("city"),
                "province": tu_addr_parsed.get("province"),
                "postal_code": tu_addr_parsed.get("postal_code"),
            },
        }
        
        evidence = {
            "full_name": (tu_page, tu_name) if tu_name else None,
            "address_block": (tu_page, tu_current_addr_str) if tu_current_addr_str else None,
        }
        
        return {"consumer": consumer, "evidence": evidence}
    
    # Fall back to generic extraction
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
            # Never use a line that starts with "Previous"
            if line.lower().strip().startswith("previous"):
                continue
            if re.search(r"\b\d{1,6}\b", line) and sum(c.isalpha() for c in line) >= 4:
                if re.search(r"\baddress\b", line, re.IGNORECASE):
                    m = re.search(r"\baddress\b\s*[:\-]?\s*(.+)", line, re.IGNORECASE)
                    if m:
                        candidate = normalize_whitespace(m.group(1))
                        if candidate and not candidate.lower().startswith("previous"):
                            addr_line1 = candidate
                            break
                    continue
                addr_line1 = line
                break

        if not addr_line1 and len(block_lines) >= 2:
            # Skip lines that start with "Previous"
            for line in block_lines[1:]:
                if not line.lower().strip().startswith("previous"):
                    addr_line1 = line
                    break

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
