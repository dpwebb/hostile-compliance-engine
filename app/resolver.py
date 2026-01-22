"""
Resolver logic for prioritizing authoritative sections in TransUnion reports.
"""
from typing import Any, Dict, List, Optional, Set


def is_from_addresses_table(obs: Dict[str, Any]) -> bool:
    """Check if observation is from Address(es) table (authoritative source)."""
    anchor = obs.get("anchor", {})
    anchor_before = anchor.get("anchor_text_before", "").upper()
    return "ADDRESS(ES)" in anchor_before or "ADDRESSES:" in anchor_before


def is_from_previous_address(obs: Dict[str, Any]) -> bool:
    """Check if observation is from Previous Address section (should be ignored)."""
    anchor = obs.get("anchor", {})
    anchor_before = anchor.get("anchor_text_before", "").upper()
    return "PREVIOUS ADDRESS" in anchor_before


def is_from_personal_information(obs: Dict[str, Any]) -> bool:
    """Check if observation is from Personal Information section (authoritative source)."""
    anchor = obs.get("anchor", {})
    anchor_before = anchor.get("anchor_text_before", "").upper()
    return "PERSONAL INFORMATION" in anchor_before


def get_addresses_table_pages(all_observations: List[Dict[str, Any]]) -> Set[int]:
    """Get page numbers where Address(es) table observations exist."""
    pages = set()
    for obs in all_observations:
        if obs.get("field_key", "").startswith("consumer.current_address."):
            if is_from_addresses_table(obs) and obs.get("page_number"):
                pages.add(obs.get("page_number"))
    return pages


def apply_tu_precedence(
    field_key: str,
    items: List[Dict[str, Any]],
    bureau: Optional[str],
    all_observations: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Apply TransUnion precedence rules to observation items.
    
    For TransUnion reports:
    - Address fields: prioritize Address(es) table, ignore Previous Address
    - Full name: prioritize Personal Information section
    
    Adds a _tu_priority field to items to ensure correct sorting order.
    Higher priority = sorted first.
    """
    if bureau != "TransUnion":
        return items
    
    # Address fields: prioritize Address(es) table, filter out Previous Address
    if field_key.startswith("consumer.current_address."):
        # Check if ANY address field observation (across all fields) is from Address(es) table
        addresses_table_pages = get_addresses_table_pages(all_observations)
        
        if addresses_table_pages:
            # Mark items with priority: Address(es) table page = priority 2, others = priority 1, Previous Address = 0
            for o in items:
                if o.get("page_number") in addresses_table_pages and not is_from_previous_address(o):
                    o["_tu_priority"] = 2
                elif is_from_previous_address(o):
                    o["_tu_priority"] = 0  # Will be filtered out by sorting
                else:
                    o["_tu_priority"] = 1
        else:
            # No Address(es) table found - mark Previous Address as lower priority
            for o in items:
                if is_from_previous_address(o):
                    o["_tu_priority"] = 0
                else:
                    o["_tu_priority"] = 1
    
    # Full name: prioritize Personal Information section
    if field_key == "consumer.full_name":
        for o in items:
            if is_from_personal_information(o):
                o["_tu_priority"] = 2
            else:
                o["_tu_priority"] = 1
    
    # Set default priority for items that don't match any rule
    for o in items:
        if "_tu_priority" not in o:
            o["_tu_priority"] = 1
    
    return items
