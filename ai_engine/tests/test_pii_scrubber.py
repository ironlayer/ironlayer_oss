"""Tests for ai_engine.engines.pii_scrubber.

Validates that all PII categories are detected and scrubbed while
preserving SQL structure, keywords, and non-sensitive identifiers.
"""

from __future__ import annotations

from ai_engine.engines.pii_scrubber import (
    contains_pii,
    scrub_for_llm,
    scrub_sql_for_llm,
)

# Format-correct fake Databricks PAT: dapi + 32 lowercase hex chars.
# The real token regex is r"\bdapi[a-f0-9]{32}\b" — this string satisfies it.
# Do NOT use "dapi_FAKE_TOKEN_FOR_TESTING" — underscores fail the hex-only pattern.
_FAKE_DAPI = "dapi" + "a" * 32

# ================================================================== #
# Email scrubbing
# ================================================================== #


class TestEmailScrubbing:
    def test_email_replaced(self):
        text = "Contact alice@example.com for details"
        assert scrub_for_llm(text) == "Contact <EMAIL> for details"

    def test_multiple_emails(self):
        text = "cc: bob@corp.io and admin@sub.domain.org"
        result = scrub_for_llm(text)
        assert "bob@corp.io" not in result
        assert "admin@sub.domain.org" not in result
        assert result.count("<EMAIL>") == 2


# ================================================================== #
# Phone number scrubbing
# ================================================================== #


class TestPhoneScrubbing:
    def test_us_phone_with_dashes(self):
        assert "<PHONE>" in scrub_for_llm("Call 555-123-4567")

    def test_us_phone_with_country_code(self):
        assert "<PHONE>" in scrub_for_llm("Reach +1-800-555-0199")

    def test_us_phone_with_parens(self):
        assert "<PHONE>" in scrub_for_llm("Phone: (212) 555-1234")


# ================================================================== #
# SQL string literal scrubbing
# ================================================================== #


class TestSQLLiteralScrubbing:
    def test_single_quoted_literals_replaced(self):
        sql = "SELECT * FROM users WHERE name = 'Alice' AND city = 'NYC'"
        result = scrub_sql_for_llm(sql)
        assert "'Alice'" not in result
        assert "'NYC'" not in result
        assert result.count("<LITERAL>") == 2

    def test_escaped_quotes_handled(self):
        sql = "SELECT * FROM t WHERE col = 'it''s fine'"
        result = scrub_sql_for_llm(sql)
        assert "it''s fine" not in result
        assert "<LITERAL>" in result

    def test_sql_structure_preserved(self):
        sql = "SELECT customer_id, SUM(amount) AS total FROM orders WHERE status = 'active' GROUP BY customer_id"
        result = scrub_sql_for_llm(sql)
        assert "SELECT" in result
        assert "customer_id" in result
        assert "SUM(amount)" in result
        assert "FROM orders" in result
        assert "WHERE status = <LITERAL>" in result
        assert "GROUP BY customer_id" in result

    def test_large_numeric_ids_replaced(self):
        sql = "SELECT * FROM users WHERE user_id = 1234567890"
        result = scrub_sql_for_llm(sql)
        assert "1234567890" not in result
        assert "<ID>" in result

    def test_small_numbers_preserved(self):
        sql = "SELECT * FROM orders LIMIT 100"
        result = scrub_sql_for_llm(sql)
        assert "100" in result


# ================================================================== #
# Databricks token scrubbing
# ================================================================== #


class TestDatabricksTokenScrubbing:
    def test_dapi_token_scrubbed(self):
        text = f"token={_FAKE_DAPI} for auth"
        result = scrub_for_llm(text)
        assert _FAKE_DAPI not in result

    def test_dapi_in_sql_context(self):
        sql = f"-- token: {_FAKE_DAPI}\nSELECT 1"
        result = scrub_sql_for_llm(sql)
        assert _FAKE_DAPI not in result


# ================================================================== #
# Generic secret scrubbing
# ================================================================== #


class TestGenericSecretScrubbing:
    def test_password_equals(self):
        text = "config password=SuperSecret123"
        result = scrub_for_llm(text)
        assert "SuperSecret123" not in result
        assert "<SECRET>" in result

    def test_api_key_colon(self):
        text = "api_key: sk-abc123xyz"
        result = scrub_for_llm(text)
        assert "sk-abc123xyz" not in result
        assert "<SECRET>" in result

    def test_token_equals(self):
        text = "auth token=eyJhbGciOiJIUzI1NiJ9"
        result = scrub_for_llm(text)
        assert "eyJhbGciOiJIUzI1NiJ9" not in result

    def test_secret_equals(self):
        text = "secret=my_secret_value_42"
        result = scrub_for_llm(text)
        assert "my_secret_value_42" not in result


# ================================================================== #
# contains_pii detection
# ================================================================== #


class TestContainsPII:
    def test_clean_text_returns_false(self):
        assert contains_pii("SELECT id FROM orders") is False

    def test_email_detected(self):
        assert contains_pii("user@example.com") is True

    def test_token_detected(self):
        assert contains_pii(_FAKE_DAPI) is True

    def test_sql_literal_detected(self):
        assert contains_pii("WHERE name = 'Alice'") is True
