"""
SentinelFlow — Unit Tests for Compliance Functions
These tests run automatically via GitHub Actions on every push.
They validate that the core compliance logic works correctly
WITHOUT needing Databricks or Spark.
"""

import pytest
import json
import uuid
import random
from datetime import datetime, timezone


# ── OFAC Screening Function (same logic as pipeline) ─────────

OFAC_SAMPLE = {
    "viktor bout", "semion mogilevich", "joaquin guzman loera",
    "ali khamenei", "kim jong un", "ramzan kadyrov"
}

def check_ofac(name):
    """Check if a name matches the OFAC sanctions list."""
    if not name:
        return "CLEAN"
    if name.lower().strip() in OFAC_SAMPLE:
        return "SANCTIONS_HIT"
    return "CLEAN"


# ── Travel Rule Validation Function ──────────────────────────

def check_travel_rule(address, sender, s_acct, receiver, r_acct):
    """Validate FATF Travel Rule compliance."""
    missing = [f for f, v in [
        ("sender_address",   address),
        ("sender_name",      sender),
        ("sender_account",   s_acct),
        ("receiver_name",    receiver),
        ("receiver_account", r_acct)
    ] if not v or not v.strip()]
    return f"TRAVEL_RULE_VIOLATION: missing {', '.join(missing)}" if missing else "COMPLIANT"


# ── USD Conversion Function ──────────────────────────────────

SAMPLE_RATES = {"USD": 1.0, "EUR": 0.85, "GBP": 0.73, "JPY": 156.76, "CHF": 0.78}

def to_usd(amount, currency):
    """Convert any amount to USD."""
    if not amount or not currency:
        return None
    return round(float(amount) / float(SAMPLE_RATES.get(currency, 1.0)), 2)


# ── Transaction Generator Function ───────────────────────────

def make_transaction(i):
    """Generate a single test transaction."""
    is_suspicious = (i % 20 == 0)
    txn_type = random.choice(["sanctioned", "missing_fields", "large_amount"]) if is_suspicious else "normal"
    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "message_type":   "pacs.008",
        "sender_name":    "Viktor Bout" if txn_type == "sanctioned" else "Alice Johnson",
        "sender_account": "DE123456789012345678",
        "sender_country": "IR" if txn_type == "large_amount" else "US",
        "sender_address": "" if txn_type == "missing_fields" else "123 Main St",
        "receiver_name":  "James Smith",
        "receiver_account": "GB123456789012345678",
        "receiver_country": "GB",
        "amount":         1000000.0 if txn_type == "large_amount" else 5000.0,
        "currency":       "USD",
        "purpose_code":   "TRAD",
        "transaction_type": txn_type,
    }


# ════════════════════════════════════════════════════════════
# TESTS — OFAC SCREENING
# ════════════════════════════════════════════════════════════

class TestOFACScreening:
    """Test OFAC sanctions screening logic."""

    def test_known_sanctioned_name(self):
        assert check_ofac("Viktor Bout") == "SANCTIONS_HIT"

    def test_known_sanctioned_name_lowercase(self):
        assert check_ofac("viktor bout") == "SANCTIONS_HIT"

    def test_known_sanctioned_name_whitespace(self):
        assert check_ofac("  Viktor Bout  ") == "SANCTIONS_HIT"

    def test_clean_name(self):
        assert check_ofac("Alice Johnson") == "CLEAN"

    def test_empty_name(self):
        assert check_ofac("") == "CLEAN"

    def test_none_name(self):
        assert check_ofac(None) == "CLEAN"

    def test_multiple_sanctioned_names(self):
        for name in OFAC_SAMPLE:
            assert check_ofac(name) == "SANCTIONS_HIT"

    def test_no_false_positives_on_common_names(self):
        common_names = ["Mohammed Al-Rashid", "James Smith",
                        "Maria Santos", "Wei Zhang"]
        for name in common_names:
            assert check_ofac(name) == "CLEAN"


# ════════════════════════════════════════════════════════════
# TESTS — TRAVEL RULE VALIDATION
# ════════════════════════════════════════════════════════════

class TestTravelRule:
    """Test FATF Travel Rule validation logic."""

    def test_all_fields_present(self):
        result = check_travel_rule("123 Main St", "Alice", "DE123", "Bob", "GB456")
        assert result == "COMPLIANT"

    def test_missing_address(self):
        result = check_travel_rule("", "Alice", "DE123", "Bob", "GB456")
        assert "TRAVEL_RULE_VIOLATION" in result
        assert "sender_address" in result

    def test_missing_sender_name(self):
        result = check_travel_rule("123 Main St", "", "DE123", "Bob", "GB456")
        assert "sender_name" in result

    def test_missing_multiple_fields(self):
        result = check_travel_rule("", "", "", "", "")
        assert "sender_address" in result
        assert "sender_name" in result
        assert "sender_account" in result
        assert "receiver_name" in result

    def test_whitespace_only_address(self):
        result = check_travel_rule("   ", "Alice", "DE123", "Bob", "GB456")
        assert "TRAVEL_RULE_VIOLATION" in result

    def test_none_address(self):
        result = check_travel_rule(None, "Alice", "DE123", "Bob", "GB456")
        assert "TRAVEL_RULE_VIOLATION" in result


# ════════════════════════════════════════════════════════════
# TESTS — USD CONVERSION
# ════════════════════════════════════════════════════════════

class TestUSDConversion:
    """Test currency conversion logic."""

    def test_usd_to_usd(self):
        assert to_usd(1000, "USD") == 1000.0

    def test_eur_to_usd(self):
        result = to_usd(850, "EUR")
        assert result == 1000.0

    def test_jpy_to_usd(self):
        result = to_usd(156760, "JPY")
        assert result == 1000.0

    def test_none_amount(self):
        assert to_usd(None, "USD") is None

    def test_none_currency(self):
        assert to_usd(1000, None) is None

    def test_unknown_currency_defaults_to_one(self):
        assert to_usd(1000, "XYZ") == 1000.0

    def test_positive_output(self):
        assert to_usd(500, "EUR") > 0


# ════════════════════════════════════════════════════════════
# TESTS — TRANSACTION GENERATOR
# ════════════════════════════════════════════════════════════

class TestTransactionGenerator:
    """Test transaction generator output."""

    def test_generates_valid_transaction(self):
        txn = make_transaction(1)
        assert "transaction_id" in txn
        assert "sender_name" in txn
        assert "amount" in txn
        assert txn["message_type"] == "pacs.008"

    def test_normal_transaction(self):
        txn = make_transaction(1)
        assert txn["transaction_type"] == "normal"
        assert txn["sender_name"] == "Alice Johnson"

    def test_suspicious_every_20th(self):
        txn = make_transaction(20)
        assert txn["transaction_type"] in ["sanctioned", "missing_fields", "large_amount"]

    def test_iso_format_timestamp(self):
        txn = make_transaction(1)
        datetime.fromisoformat(txn["timestamp"])

    def test_unique_ids(self):
        ids = [make_transaction(i)["transaction_id"] for i in range(100)]
        assert len(set(ids)) == 100

    def test_required_fields_present(self):
        required = ["transaction_id", "timestamp", "message_type",
                    "sender_name", "sender_account", "sender_country",
                    "receiver_name", "receiver_account", "amount",
                    "currency", "purpose_code"]
        txn = make_transaction(1)
        for field in required:
            assert field in txn, f"Missing field: {field}"

    def test_schema_field_count(self):
        txn = make_transaction(1)
        assert len(txn) == 14


# ════════════════════════════════════════════════════════════
# TESTS — DATA SCHEMA
# ════════════════════════════════════════════════════════════

class TestDataSchema:
    """Test data schema and types."""

    def test_amount_is_numeric(self):
        txn = make_transaction(1)
        assert isinstance(txn["amount"], (int, float))

    def test_transaction_id_is_uuid(self):
        txn = make_transaction(1)
        uuid.UUID(txn["transaction_id"])

    def test_country_code_length(self):
        txn = make_transaction(1)
        assert len(txn["sender_country"]) == 2
        assert len(txn["receiver_country"]) == 2

    def test_json_serializable(self):
        txn = make_transaction(1)
        json.dumps(txn)