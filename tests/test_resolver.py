"""
Tests for TransUnion resolver precedence logic.
Ensures Address(es) table and Personal Information section take precedence over later sections.
"""
import pytest
from app.main import resolve_profile


def test_tu_resolver_prioritizes_addresses_table_over_previous_address():
    """
    Test that when report.bureau == "TransUnion", the resolver prioritizes
    Address(es) table (page 2) over later Previous Address sections.
    """
    # Page 2 text with Address(es) table (authoritative source)
    page_2_text = """
Personal Information
Surname WEBB
Given Name(s) DAVID
Middle Name PHILIP

Address(es):
Your most current Since date address is listed first.

26 MAIN ST E PO BOX 593
STEWIACKE
NS
B0N2J0

WELLAND
ON
L3C0B7
"""
    
    # Page 10 text with Previous Address (should be ignored)
    page_10_text = """
Previous Address
Number & Street 44 CLARE AVE
City WELLAND
Prov/Postal ON L3C 0B7
"""
    
    # Create observations simulating extraction from both pages
    observations = [
        # Report bureau
        {
            "obs_id": "obs-1",
            "field_key": "report.bureau",
            "raw_value": "TransUnion",
            "status": "extracted",
            "method": "derived",
            "confidence": 0.9,
            "page_number": None,
            "created_at": "2024-01-01T00:00:00Z",
        },
        # Address(es) table observations (page 2) - should win
        {
            "obs_id": "obs-2",
            "field_key": "consumer.current_address.line1",
            "raw_value": "26 MAIN ST E PO BOX 593",
            "status": "extracted",
            "method": "text",
            "confidence": 0.80,
            "page_number": 2,
            "created_at": "2024-01-01T00:00:01Z",
            "anchor": {
                "anchor_text_before": "Address(es):",
                "anchor_text_after": "STEWIACKE",
                "anchor_hash": "abc123",
                "anchor_strength": "strong",
            },
        },
        {
            "obs_id": "obs-3",
            "field_key": "consumer.current_address.city",
            "raw_value": "STEWIACKE",
            "status": "extracted",
            "method": "text",
            "confidence": 0.70,
            "page_number": 2,
            "created_at": "2024-01-01T00:00:02Z",
            "anchor": {
                "anchor_text_before": "26 MAIN ST E PO BOX 593",
                "anchor_text_after": "NS",
                "anchor_hash": "def456",
                "anchor_strength": "strong",
            },
        },
        {
            "obs_id": "obs-4",
            "field_key": "consumer.current_address.province",
            "raw_value": "NS",
            "status": "extracted",
            "method": "text",
            "confidence": 0.90,
            "page_number": 2,
            "created_at": "2024-01-01T00:00:03Z",
            "anchor": {
                "anchor_text_before": "STEWIACKE",
                "anchor_text_after": "B0N2J0",
                "anchor_hash": "ghi789",
                "anchor_strength": "strong",
            },
        },
        {
            "obs_id": "obs-5",
            "field_key": "consumer.current_address.postal_code",
            "raw_value": "B0N 2J0",
            "status": "extracted",
            "method": "text",
            "confidence": 0.95,
            "page_number": 2,
            "created_at": "2024-01-01T00:00:04Z",
            "anchor": {
                "anchor_text_before": "NS",
                "anchor_text_after": "",
                "anchor_hash": "jkl012",
                "anchor_strength": "strong",
            },
        },
        # Previous Address observations (page 10) - should be ignored
        {
            "obs_id": "obs-6",
            "field_key": "consumer.current_address.line1",
            "raw_value": "44 CLARE AVE",
            "status": "extracted",
            "method": "text",
            "confidence": 0.85,  # Higher confidence but should still lose
            "page_number": 10,
            "created_at": "2024-01-01T00:00:05Z",
            "anchor": {
                "anchor_text_before": "Previous Address",
                "anchor_text_after": "WELLAND",
                "anchor_hash": "mno345",
                "anchor_strength": "strong",
            },
        },
        {
            "obs_id": "obs-7",
            "field_key": "consumer.current_address.city",
            "raw_value": "WELLAND",
            "status": "extracted",
            "method": "text",
            "confidence": 0.75,
            "page_number": 10,
            "created_at": "2024-01-01T00:00:06Z",
            "anchor": {
                "anchor_text_before": "44 CLARE AVE",
                "anchor_text_after": "ON",
                "anchor_hash": "pqr678",
                "anchor_strength": "strong",
            },
        },
        {
            "obs_id": "obs-8",
            "field_key": "consumer.current_address.province",
            "raw_value": "ON",
            "status": "extracted",
            "method": "text",
            "confidence": 0.95,  # Higher confidence but should still lose
            "page_number": 10,
            "created_at": "2024-01-01T00:00:07Z",
            "anchor": {
                "anchor_text_before": "WELLAND",
                "anchor_text_after": "L3C 0B7",
                "anchor_hash": "stu901",
                "anchor_strength": "strong",
            },
        },
    ]
    
    resolved = resolve_profile(observations)
    
    # Assert Address(es) table values win (page 2)
    assert resolved["consumer.current_address.line1"]["resolved_value"] == "26 MAIN ST E PO BOX 593"
    assert resolved["consumer.current_address.city"]["resolved_value"] == "STEWIACKE"
    assert resolved["consumer.current_address.province"]["resolved_value"] == "NS"
    assert resolved["consumer.current_address.postal_code"]["resolved_value"] == "B0N 2J0"
    
    # Assert page_number is 2 (from Address(es) table)
    line1_obs_id = resolved["consumer.current_address.line1"]["best_observation_id"]
    line1_obs = next(o for o in observations if o["obs_id"] == line1_obs_id)
    assert line1_obs["page_number"] == 2
    
    city_obs_id = resolved["consumer.current_address.city"]["best_observation_id"]
    city_obs = next(o for o in observations if o["obs_id"] == city_obs_id)
    assert city_obs["page_number"] == 2
    
    # Assert anchors are not empty
    assert line1_obs["anchor"]["anchor_strength"] != "none"
    assert city_obs["anchor"]["anchor_strength"] != "none"


def test_tu_resolver_prioritizes_personal_information_for_full_name():
    """
    Test that when report.bureau == "TransUnion", the resolver prioritizes
    Personal Information section for full_name over other name sources.
    """
    observations = [
        # Report bureau
        {
            "obs_id": "obs-1",
            "field_key": "report.bureau",
            "raw_value": "TransUnion",
            "status": "extracted",
            "method": "derived",
            "confidence": 0.9,
            "page_number": None,
            "created_at": "2024-01-01T00:00:00Z",
        },
        # Personal Information section (page 2) - should win
        {
            "obs_id": "obs-2",
            "field_key": "consumer.full_name",
            "raw_value": "DAVID PHILIP WEBB",
            "status": "extracted",
            "method": "text",
            "confidence": 0.85,
            "page_number": 2,
            "created_at": "2024-01-01T00:00:01Z",
            "anchor": {
                "anchor_text_before": "Personal Information",
                "anchor_text_after": "",
                "anchor_hash": "abc123",
                "anchor_strength": "strong",
            },
        },
        # Alternative name source (page 5) - should lose
        {
            "obs_id": "obs-3",
            "field_key": "consumer.full_name",
            "raw_value": "DAVID WEBB",
            "status": "extracted",
            "method": "text",
            "confidence": 0.90,  # Higher confidence but should still lose
            "page_number": 5,
            "created_at": "2024-01-01T00:00:02Z",
            "anchor": {
                "anchor_text_before": "Name",
                "anchor_text_after": "",
                "anchor_hash": "def456",
                "anchor_strength": "strong",
            },
        },
    ]
    
    resolved = resolve_profile(observations)
    
    # Assert Personal Information name wins
    assert resolved["consumer.full_name"]["resolved_value"] == "DAVID PHILIP WEBB"
    
    # Assert page_number is 2 (from Personal Information)
    name_obs_id = resolved["consumer.full_name"]["best_observation_id"]
    name_obs = next(o for o in observations if o["obs_id"] == name_obs_id)
    assert name_obs["page_number"] == 2
    
    # Assert anchor is not empty
    assert name_obs["anchor"]["anchor_strength"] != "none"


def test_tu_resolver_ignores_previous_address_when_addresses_table_exists():
    """
    Test that Previous Address observations are ignored when Address(es) table exists.
    """
    observations = [
        {
            "obs_id": "obs-1",
            "field_key": "report.bureau",
            "raw_value": "TransUnion",
            "status": "extracted",
            "method": "derived",
            "confidence": 0.9,
            "page_number": None,
            "created_at": "2024-01-01T00:00:00Z",
        },
        # Address(es) table - page 2
        {
            "obs_id": "obs-2",
            "field_key": "consumer.current_address.province",
            "raw_value": "NS",
            "status": "extracted",
            "method": "text",
            "confidence": 0.90,
            "page_number": 2,
            "created_at": "2024-01-01T00:00:01Z",
            "anchor": {
                "anchor_text_before": "Address(es):",
                "anchor_text_after": "",
                "anchor_hash": "abc123",
                "anchor_strength": "strong",
            },
        },
        # Previous Address - page 10 (should be ignored)
        {
            "obs_id": "obs-3",
            "field_key": "consumer.current_address.province",
            "raw_value": "ON",
            "status": "extracted",
            "method": "text",
            "confidence": 0.95,  # Higher confidence
            "page_number": 10,
            "created_at": "2024-01-01T00:00:02Z",
            "anchor": {
                "anchor_text_before": "Previous Address",
                "anchor_text_after": "",
                "anchor_hash": "def456",
                "anchor_strength": "strong",
            },
        },
    ]
    
    resolved = resolve_profile(observations)
    
    # Assert Address(es) table value wins (NS, not ON)
    assert resolved["consumer.current_address.province"]["resolved_value"] == "NS"
    
    # Assert page_number is 2, not 10
    prov_obs_id = resolved["consumer.current_address.province"]["best_observation_id"]
    prov_obs = next(o for o in observations if o["obs_id"] == prov_obs_id)
    assert prov_obs["page_number"] == 2


def test_resolver_falls_back_when_not_transunion():
    """
    Test that resolver uses normal confidence-based selection when bureau is not TransUnion.
    """
    observations = [
        {
            "obs_id": "obs-1",
            "field_key": "report.bureau",
            "raw_value": "Equifax",
            "status": "extracted",
            "method": "derived",
            "confidence": 0.9,
            "page_number": None,
            "created_at": "2024-01-01T00:00:00Z",
        },
        # Lower confidence, earlier page
        {
            "obs_id": "obs-2",
            "field_key": "consumer.current_address.province",
            "raw_value": "NS",
            "status": "extracted",
            "method": "text",
            "confidence": 0.80,
            "page_number": 2,
            "created_at": "2024-01-01T00:00:01Z",
        },
        # Higher confidence, later page - should win
        {
            "obs_id": "obs-3",
            "field_key": "consumer.current_address.province",
            "raw_value": "ON",
            "status": "extracted",
            "method": "text",
            "confidence": 0.95,
            "page_number": 10,
            "created_at": "2024-01-01T00:00:02Z",
        },
    ]
    
    resolved = resolve_profile(observations)
    
    # Assert higher confidence wins (normal behavior)
    assert resolved["consumer.current_address.province"]["resolved_value"] == "ON"
