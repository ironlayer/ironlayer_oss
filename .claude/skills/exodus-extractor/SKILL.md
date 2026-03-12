---
name: exodus-extractor
description: >
  Create new data source extractors for Exodus Foundation. Generates Python extractor
  classes, YAML source configs, and unit tests following the Foundation framework.
  Use when adding a new API data source, creating extraction jobs, or onboarding a new data feed.
triggers:
  - "add a new extractor"
  - "create an extractor for"
  - "onboard {source} API"
  - "extract data from {source}"
  - "create extraction job"
outputs:
  - "config/extractors/{name}.yml"
  - "foundation/extractors/connectors/{name}.py"
  - "tests/unit/extractors/test_{name}_extractor.py"
---

# Extractor Builder

> **Core skill.** Follow every step precisely.
> Extractors are the entry point for all platform data — correctness matters.

---

## Before You Start — Read Existing Framework

```bash
# 1. Understand what extractors exist
ls foundation/extractors/connectors/
ls config/extractors/

# 2. Read the base class (do not modify)
cat foundation/extractors/base.py

# 3. Read the config loader (do not modify)
cat foundation/extractors/config.py

# 4. Read the closest existing connector (your pattern)
cat foundation/extractors/connectors/coinbase.py
cat config/extractors/coinbase.yml
```

**Never skip reading existing code. Never invent patterns.**

---

## Architecture

```
config/extractors/{name}.yml             Declarative config: endpoints, auth, schema, budget
foundation/extractors/connectors/{name}.py    Python class extending BaseExtractor
foundation/extractors/base.py            DO NOT MODIFY — HTTP, retry, rate limit, state
foundation/extractors/config.py          DO NOT MODIFY — YAML → typed dataclass loader
tests/unit/extractors/test_{name}_extractor.py    Unit tests (mocked HTTP)
```

---

## Step 1 — Config YAML (`config/extractors/{name}.yml`)

```yaml
name: {name}                     # lowercase, underscores: odds_api, coinbase
type: api
business_context: >
  {2-3 sentences: what data this provides, who uses it, business value.}

config:
  url: "{base_url}"
  format: json
  auth: api_key                  # api_key | bearer | oauth2 | none
  rate_limit: "{N}/minute"

  tables:
    {table_name}:
      description: "{one sentence}"
      columns:
        - name: {col}
          type: STRING            # STRING | INTEGER | DECIMAL | BOOLEAN | TIMESTAMP

extraction:
  auth:
    auth_type: api_key
    auth_param: apiKey
    auth_location: query          # query | header
    env_var: {NAME}_API_KEY       # e.g. ODDS_API_KEY

  landing:
    catalog: "${FOUNDATION_RAW_CATALOG:-foundation_raw}"  # always env var
    schema: {source_name}
    volume_prefix: landing

  endpoints:
    - name: {endpoint_name}
      path: /{version}/{resource}
      schedule: daily             # daily | hourly
      credit_cost: 1

  budget:
    max_credits_per_run: 500
    warn_at: 100
    monthly_limit: 5000
```

**YAML checklist — BLOCK if any missing:**
- [ ] `name` matches the Python class file name
- [ ] `business_context` is 2-3 meaningful sentences
- [ ] `env_var` follows `{NAME}_API_KEY` pattern
- [ ] `catalog` uses `${FOUNDATION_RAW_CATALOG:-foundation_raw}` — never hardcoded

---

## Step 2 — Python Extractor (`foundation/extractors/connectors/{name}.py`)

```python
"""
{Name} Extractor.

Extracts {what data} from {API name} ({api_docs_url}).
Rate limits: {N} requests per period.
Auth: API key via {query param / header}.
"""

from __future__ import annotations
from typing import Any

from foundation.extractors.base import BaseExtractor
from foundation.extractors.config import EndpointConfig, ExtractorConfig


class {Name}Extractor(BaseExtractor):
    """Extractor for {API Name}."""

    EXTRACTOR_NAME = "{name}"

    def __init__(self, config: ExtractorConfig | None = None, dry_run: bool = False) -> None:
        if config is None:
            config = ExtractorConfig.from_yaml(f"config/extractors/{self.EXTRACTOR_NAME}.yml")
        super().__init__(config=config, dry_run=dry_run)

    def extract_all(self) -> dict[str, Any]:
        """Extract all configured endpoints."""
        results: dict[str, Any] = {}
        for endpoint in self.config.endpoints:
            self._check_budget()                    # REQUIRED before each API call loop
            results[endpoint.name] = self._extract_endpoint(endpoint)
        return results

    def _extract_endpoint(self, endpoint: EndpointConfig) -> dict[str, Any]:
        """Extract a single endpoint."""
        url = f"{self.config.base_url}{endpoint.path}"
        params: dict[str, Any] = dict(endpoint.params or {})
        self._apply_auth(params)                    # REQUIRED before HTTP call

        response = self._get(url, params=params)    # use _get(), never requests directly
        records = self._normalize_response(response.json(), endpoint.name)
        self._write_landing(endpoint.name, records)
        self.console.print(f"[green]{endpoint.name}:[/green] {len(records)} records")

        return {"endpoint": endpoint.name, "records": len(records), "status": "success"}

    def _normalize_response(self, data: Any, endpoint_name: str) -> list[dict]:
        """Normalize API response to a list of records."""
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("data", "results", "items", endpoint_name):
                if key in data and isinstance(data[key], list):
                    return data[key]
        return [data]
```

**Python checklist — BLOCK if any missing:**
- [ ] `EXTRACTOR_NAME` matches config `name` exactly
- [ ] `self._check_budget()` called before each endpoint loop iteration
- [ ] `self._apply_auth(params)` called before each `self._get()` call
- [ ] `self._get()` used for HTTP — never `requests.get()` directly
- [ ] No hardcoded API keys or tokens
- [ ] No bare `except:` — catch specific exceptions
- [ ] `self.console.print()` used — never `print()`
- [ ] Type hints on all methods

---

## Step 3 — Unit Tests (`tests/unit/extractors/test_{name}_extractor.py`)

```python
from unittest.mock import MagicMock, patch
import pytest
from foundation.extractors.connectors.{name} import {Name}Extractor
from foundation.extractors.config import ExtractorConfig


@pytest.fixture
def extractor() -> {Name}Extractor:
    config = ExtractorConfig.from_yaml("config/extractors/{name}.yml")
    return {Name}Extractor(config=config, dry_run=True)


class TestConfig:
    def test_name_matches(self, extractor: {Name}Extractor) -> None:
        assert extractor.config.name == "{name}"

    def test_has_endpoints(self, extractor: {Name}Extractor) -> None:
        assert len(extractor.config.endpoints) >= 1

    def test_auth_env_var_set(self, extractor: {Name}Extractor) -> None:
        assert extractor.config.auth.env_var != ""


class TestNormalizeResponse:
    def test_list_response(self, extractor: {Name}Extractor) -> None:
        result = extractor._normalize_response([{"id": 1}], "ep")
        assert result == [{"id": 1}]

    def test_data_key(self, extractor: {Name}Extractor) -> None:
        result = extractor._normalize_response({"data": [{"id": 1}]}, "ep")
        assert result == [{"id": 1}]


class TestExtraction:
    @patch("foundation.extractors.base.BaseExtractor._get")
    @patch("foundation.extractors.base.BaseExtractor._write_landing")
    def test_extract_success(
        self, mock_write: MagicMock, mock_get: MagicMock, extractor: {Name}Extractor
    ) -> None:
        mock_get.return_value.json.return_value = [{"id": "1"}]
        result = extractor._extract_endpoint(extractor.config.endpoints[0])
        assert result["status"] == "success"
        mock_write.assert_called_once()
```

---

## Step 4 — Verify

```bash
# Config loads without error
python -c "from foundation.extractors.config import ExtractorConfig; c = ExtractorConfig.from_yaml('config/extractors/{name}.yml'); print('OK:', c.name)"

# Class imports without error
python -c "from foundation.extractors.connectors.{name} import {Name}Extractor; print('OK')"

# Tests pass
uv run pytest tests/unit/extractors/test_{name}_extractor.py -v

# Dry run (no real API calls)
uv run python -m foundation.extractors.cli run {name} --dry-run
```

---

## After Creating

1. Create staging models: use the `exodus-dbt-model` skill — `stg_{name}__*.sql`
2. Add to bundle config: enable in `config/client.yml` bundle toggles
3. Register in extractor CLI if needed: `foundation/extractors/cli.py`
4. Add source definition in `dbt/models/staging/{name}/__sources.yml`
