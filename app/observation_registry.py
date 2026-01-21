from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class FieldDef:
    field_key: str
    label: str
    scope: str  # "document" | "page" | "entity"
    value_type: str  # "string" | "int" | "money" | "date" | "bool" | "json"
    required: bool = False
    entity_type: Optional[str] = None  # "tradeline" | "inquiry" | "collection" | etc.
    description: str = ""
    examples: Optional[List[Any]] = None


def canonical_fields() -> List[FieldDef]:
    """
    Canonical observation fields for Consumer Credit Reports (v1).
    This is a contract: stable keys, stable meaning.
    """
    fields: List[FieldDef] = [
        # --------------------
        # Document meta
        # --------------------
        FieldDef("doc.meta.original_filename", "Original filename", "document", "string", required=True),
        FieldDef("doc.meta.stored_filename", "Stored filename", "document", "string", required=True),
        FieldDef("doc.meta.sha256", "SHA-256 hash", "document", "string", required=True),
        FieldDef("doc.meta.byte_size", "File size (bytes)", "document", "int", required=True),
        FieldDef("doc.meta.page_count", "Page count", "document", "int", required=True),
        FieldDef("doc.meta.text_length", "Total extracted text length", "document", "int", required=True),

        FieldDef("doc.page.text_length", "Page extracted text length", "page", "int", required=True),

        # --------------------
        # Core report identifiers
        # --------------------
        FieldDef(
            "report.bureau",
            "Credit bureau",
            "document",
            "string",
            required=False,
            description="TransUnion / Equifax / Unknown",
            examples=["TransUnion", "Equifax", "Unknown"],
        ),
        FieldDef(
            "report.type",
            "Report type",
            "document",
            "string",
            required=False,
            description="consumer disclosure / file disclosure / credit report",
            examples=["consumer disclosure"],
        ),

        # --------------------
        # Consumer identity (minimum viable)
        # --------------------
        FieldDef("consumer.full_name", "Consumer full name", "document", "string", required=True),
        FieldDef("consumer.current_address.line1", "Current address line 1", "document", "string", required=True),
        FieldDef("consumer.current_address.city", "Current address city", "document", "string", required=True),
        FieldDef("consumer.current_address.province", "Current address province", "document", "string", required=True),
        FieldDef("consumer.current_address.postal_code", "Current address postal code", "document", "string", required=True),

        # --------------------
        # Tradelines (entity-scoped; id assigned later)
        # --------------------
        FieldDef("tradeline.creditor_name", "Creditor name", "entity", "string", entity_type="tradeline"),
        FieldDef("tradeline.account_type", "Account type", "entity", "string", entity_type="tradeline"),
        FieldDef("tradeline.account_status", "Account status", "entity", "string", entity_type="tradeline"),
        FieldDef("tradeline.opened_date", "Date opened", "entity", "date", entity_type="tradeline"),
        FieldDef("tradeline.reported_date", "Date reported", "entity", "date", entity_type="tradeline"),
        FieldDef("tradeline.balance", "Balance", "entity", "money", entity_type="tradeline"),
        FieldDef("tradeline.credit_limit", "Credit limit", "entity", "money", entity_type="tradeline"),
        FieldDef("tradeline.high_credit", "High credit", "entity", "money", entity_type="tradeline"),
        FieldDef("tradeline.past_due_amount", "Past due amount", "entity", "money", entity_type="tradeline"),
        FieldDef("tradeline.payment_status", "Payment status", "entity", "string", entity_type="tradeline"),
        FieldDef("tradeline.remarks", "Remarks", "entity", "json", entity_type="tradeline"),

        # --------------------
        # Inquiries
        # --------------------
        FieldDef("inquiry.subscriber_name", "Inquiry subscriber", "entity", "string", entity_type="inquiry"),
        FieldDef("inquiry.date", "Inquiry date", "entity", "date", entity_type="inquiry"),
        FieldDef(
            "inquiry.type",
            "Inquiry type",
            "entity",
            "string",
            entity_type="inquiry",
            description="hard/soft if detectable",
        ),

        # --------------------
        # Collections
        # --------------------
        FieldDef("collection.agency_name", "Collection agency", "entity", "string", entity_type="collection"),
        FieldDef("collection.original_creditor", "Original creditor", "entity", "string", entity_type="collection"),
        FieldDef("collection.balance", "Collection balance", "entity", "money", entity_type="collection"),
        FieldDef("collection.reported_date", "Collection reported date", "entity", "date", entity_type="collection"),
        FieldDef("collection.status", "Collection status", "entity", "string", entity_type="collection"),

        # --------------------
        # Public records
        # --------------------
        FieldDef("public_record.type", "Public record type", "entity", "string", entity_type="public_record"),
        FieldDef("public_record.filed_date", "Filed date", "entity", "date", entity_type="public_record"),
        FieldDef("public_record.status", "Public record status", "entity", "string", entity_type="public_record"),
        FieldDef("public_record.amount", "Public record amount", "entity", "money", entity_type="public_record"),

        # --------------------
        # Alerts / statements
        # --------------------
        FieldDef("fraud_alert.present", "Fraud alert present", "document", "bool"),
        FieldDef("fraud_alert.contact_phone", "Fraud alert phone", "document", "string"),
        FieldDef("consumer_statement.text", "Consumer statement", "document", "string"),
    ]
    return fields


def field_index() -> Dict[str, FieldDef]:
    return {f.field_key: f for f in canonical_fields()}


def required_field_keys() -> List[str]:
    return [f.field_key for f in canonical_fields() if f.required]
