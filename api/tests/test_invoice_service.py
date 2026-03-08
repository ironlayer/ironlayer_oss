"""Tests for api/api/services/invoice_service.py

Covers:
- InvoiceService.generate_invoice: line item construction, totals, PDF render, DB persist
- InvoiceService.generate_invoice_from_stripe: Stripe event mapping, duplicate detection
- InvoiceService.get_invoice: returns None or formatted dict
- InvoiceService.list_invoices: paginated list
- InvoiceService.get_pdf: None cases and file read
- InvoiceService._render_pdf_fallback: text-based PDF content
- InvoiceService._store_pdf: filesystem write using tmp_path
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.services.invoice_service import InvoiceService

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_session() -> AsyncMock:
    """Return a mock AsyncSession for InvoiceService tests."""
    session = AsyncMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()

    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    result_mock.scalar_one.return_value = 0
    result_mock.scalars.return_value.all.return_value = []
    session.execute = AsyncMock(return_value=result_mock)

    return session


def _make_service(
    session: AsyncMock,
    tenant_id: str = "test-tenant",
    storage_path: str = "/tmp/invoices",
) -> InvoiceService:
    """Create an InvoiceService with mocked repository."""
    return InvoiceService(session, tenant_id, storage_path=storage_path)


# ---------------------------------------------------------------------------
# InvoiceService.generate_invoice
# ---------------------------------------------------------------------------


class TestGenerateInvoice:
    """Verify generate_invoice queries usage data, builds line items, and persists."""

    @pytest.mark.asyncio
    async def test_generates_invoice_with_all_line_items(self, mock_session: AsyncMock) -> None:
        """When all usage types have data, all line items appear in the invoice."""
        period_start = datetime(2024, 6, 1, tzinfo=UTC)
        period_end = datetime(2024, 7, 1, tzinfo=UTC)

        # 5 session.execute calls: plan_runs, ai_calls, run_cost, llm_cost, api_requests
        plan_runs_result = MagicMock()
        plan_runs_result.scalar_one.return_value = 42

        ai_calls_result = MagicMock()
        ai_calls_result.scalar_one.return_value = 15

        run_cost_result = MagicMock()
        run_cost_result.scalar_one.return_value = 12.50

        llm_cost_result = MagicMock()
        llm_cost_result.scalar_one.return_value = 3.75

        api_requests_result = MagicMock()
        api_requests_result.scalar_one.return_value = 200

        mock_session.execute = AsyncMock(
            side_effect=[
                plan_runs_result,
                ai_calls_result,
                run_cost_result,
                llm_cost_result,
                api_requests_result,
            ]
        )

        with patch("api.services.invoice_service.InvoiceRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get_next_invoice_number.return_value = "INV-2024-001"
            repo_instance.create.return_value = MagicMock()
            MockRepo.return_value = repo_instance

            service = _make_service(mock_session)

            with (
                patch.object(service, "_render_pdf", return_value=b"fake-pdf"),
                patch.object(service, "_store_pdf", return_value="/tmp/invoices/test-tenant/abc.pdf"),
            ):
                result = await service.generate_invoice(period_start, period_end)

        assert result["invoice_number"] == "INV-2024-001"
        assert result["tenant_id"] == "test-tenant"
        assert result["period_start"] == period_start.isoformat()
        assert result["period_end"] == period_end.isoformat()

        descriptions = [item["description"] for item in result["line_items"]]
        assert "Plan Runs" in descriptions
        assert "AI Advisory Calls" in descriptions
        assert "Compute Cost (Databricks)" in descriptions
        assert "LLM Usage (AI Advisory)" in descriptions
        assert "API Requests" in descriptions

        # Verify totals: compute cost 12.50 + LLM cost 3.75 = 16.25
        assert result["subtotal_usd"] == 16.25
        assert result["tax_usd"] == 0.0
        assert result["total_usd"] == 16.25

        repo_instance.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_generates_invoice_with_zero_usage(self, mock_session: AsyncMock) -> None:
        """When all usage counts are 0, no line items are created."""
        period_start = datetime(2024, 6, 1, tzinfo=UTC)
        period_end = datetime(2024, 7, 1, tzinfo=UTC)

        zero_result = MagicMock()
        zero_result.scalar_one.return_value = 0

        zero_float_result = MagicMock()
        zero_float_result.scalar_one.return_value = 0.0

        mock_session.execute = AsyncMock(
            side_effect=[zero_result, zero_result, zero_float_result, zero_float_result, zero_result]
        )

        with patch("api.services.invoice_service.InvoiceRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get_next_invoice_number.return_value = "INV-2024-002"
            repo_instance.create.return_value = MagicMock()
            MockRepo.return_value = repo_instance

            service = _make_service(mock_session)

            with (
                patch.object(service, "_render_pdf", return_value=b"fake-pdf"),
                patch.object(service, "_store_pdf", return_value="/tmp/invoices/test-tenant/def.pdf"),
            ):
                result = await service.generate_invoice(period_start, period_end)

        assert result["line_items"] == []
        assert result["subtotal_usd"] == 0.0
        assert result["total_usd"] == 0.0

    @pytest.mark.asyncio
    async def test_generates_invoice_with_only_compute_cost(self, mock_session: AsyncMock) -> None:
        """When only run cost is nonzero, only one line item appears."""
        period_start = datetime(2024, 6, 1, tzinfo=UTC)
        period_end = datetime(2024, 7, 1, tzinfo=UTC)

        zero_result = MagicMock()
        zero_result.scalar_one.return_value = 0

        zero_float_result = MagicMock()
        zero_float_result.scalar_one.return_value = 0.0

        run_cost_result = MagicMock()
        run_cost_result.scalar_one.return_value = 25.00

        mock_session.execute = AsyncMock(
            side_effect=[zero_result, zero_result, run_cost_result, zero_float_result, zero_result]
        )

        with patch("api.services.invoice_service.InvoiceRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get_next_invoice_number.return_value = "INV-2024-003"
            repo_instance.create.return_value = MagicMock()
            MockRepo.return_value = repo_instance

            service = _make_service(mock_session)

            with (
                patch.object(service, "_render_pdf", return_value=b"fake-pdf"),
                patch.object(service, "_store_pdf", return_value="/tmp/invoices/test-tenant/ghi.pdf"),
            ):
                result = await service.generate_invoice(period_start, period_end)

        assert len(result["line_items"]) == 1
        assert result["line_items"][0]["description"] == "Compute Cost (Databricks)"
        assert result["line_items"][0]["amount"] == 25.00
        assert result["subtotal_usd"] == 25.00
        assert result["total_usd"] == 25.00


# ---------------------------------------------------------------------------
# InvoiceService.generate_invoice_from_stripe
# ---------------------------------------------------------------------------


class TestGenerateInvoiceFromStripe:
    """Verify Stripe event mapping and duplicate detection."""

    @pytest.mark.asyncio
    async def test_valid_event_creates_invoice(self, mock_session: AsyncMock) -> None:
        """A valid Stripe invoice event creates and persists an internal invoice."""
        stripe_event = {
            "type": "invoice.payment_succeeded",
            "data": {
                "object": {
                    "id": "in_stripe_123",
                    "period_start": 1717200000,
                    "period_end": 1719792000,
                    "lines": {
                        "data": [
                            {
                                "description": "Team Plan",
                                "quantity": 1,
                                "unit_amount": 9900,
                                "amount": 9900,
                            }
                        ]
                    },
                    "subtotal": 9900,
                    "tax": 0,
                    "total": 9900,
                }
            },
        }

        with patch("api.services.invoice_service.InvoiceRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get_by_stripe_invoice.return_value = None  # No duplicate
            repo_instance.get_next_invoice_number.return_value = "INV-2024-010"
            repo_instance.create.return_value = MagicMock()
            repo_instance.update_status.return_value = None
            MockRepo.return_value = repo_instance

            service = _make_service(mock_session)

            with (
                patch.object(service, "_render_pdf", return_value=b"stripe-pdf"),
                patch.object(service, "_store_pdf", return_value="/tmp/invoices/test-tenant/stripe.pdf"),
            ):
                result = await service.generate_invoice_from_stripe(stripe_event)

        assert result is not None
        assert result["stripe_invoice_id"] == "in_stripe_123"
        assert result["invoice_number"] == "INV-2024-010"
        assert len(result["line_items"]) == 1
        assert result["line_items"][0]["description"] == "Team Plan"
        assert result["line_items"][0]["unit_price"] == 99.00
        assert result["line_items"][0]["amount"] == 99.00
        assert result["subtotal_usd"] == 99.00
        assert result["total_usd"] == 99.00
        repo_instance.create.assert_called_once()
        repo_instance.update_status.assert_called_once_with(result["invoice_id"], "paid")

    @pytest.mark.asyncio
    async def test_duplicate_stripe_invoice_returns_none(self, mock_session: AsyncMock) -> None:
        """When a Stripe invoice ID already exists in the DB, return None."""
        stripe_event = {
            "type": "invoice.payment_succeeded",
            "data": {
                "object": {
                    "id": "in_stripe_dup",
                    "period_start": 1717200000,
                    "period_end": 1719792000,
                    "lines": {"data": []},
                    "subtotal": 0,
                    "tax": 0,
                    "total": 0,
                }
            },
        }

        with patch("api.services.invoice_service.InvoiceRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get_by_stripe_invoice.return_value = MagicMock()  # Already exists
            MockRepo.return_value = repo_instance

            service = _make_service(mock_session)
            result = await service.generate_invoice_from_stripe(stripe_event)

        assert result is None

    @pytest.mark.asyncio
    async def test_missing_stripe_invoice_id_returns_none(self, mock_session: AsyncMock) -> None:
        """When the Stripe event has no invoice ID, return None."""
        stripe_event = {
            "type": "invoice.payment_succeeded",
            "data": {"object": {}},  # No "id" field
        }

        with patch("api.services.invoice_service.InvoiceRepository") as MockRepo:
            MockRepo.return_value = AsyncMock()

            service = _make_service(mock_session)
            result = await service.generate_invoice_from_stripe(stripe_event)

        assert result is None

    @pytest.mark.asyncio
    async def test_empty_data_object_returns_none(self, mock_session: AsyncMock) -> None:
        """When the Stripe event data.object is empty, return None."""
        stripe_event = {
            "type": "invoice.payment_succeeded",
            "data": {},
        }

        with patch("api.services.invoice_service.InvoiceRepository") as MockRepo:
            MockRepo.return_value = AsyncMock()

            service = _make_service(mock_session)
            result = await service.generate_invoice_from_stripe(stripe_event)

        assert result is None


# ---------------------------------------------------------------------------
# InvoiceService.get_invoice
# ---------------------------------------------------------------------------


class TestGetInvoice:
    """Verify get_invoice returns correct data or None."""

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, mock_session: AsyncMock) -> None:
        """When the repository returns None, get_invoice returns None."""
        with patch("api.services.invoice_service.InvoiceRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get.return_value = None
            MockRepo.return_value = repo_instance

            service = _make_service(mock_session)
            result = await service.get_invoice("nonexistent-id")

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_formatted_dict_when_found(self, mock_session: AsyncMock) -> None:
        """When an invoice record exists, returns a properly formatted dict."""
        now = datetime.now(UTC)
        row = MagicMock()
        row.invoice_id = "inv-001"
        row.invoice_number = "INV-2024-001"
        row.stripe_invoice_id = "in_stripe_abc"
        row.period_start = datetime(2024, 6, 1, tzinfo=UTC)
        row.period_end = datetime(2024, 7, 1, tzinfo=UTC)
        row.subtotal_usd = 99.00
        row.tax_usd = 0.0
        row.total_usd = 99.00
        row.line_items_json = [{"description": "Team Plan", "amount": 99.00}]
        row.status = "paid"
        row.created_at = now

        with patch("api.services.invoice_service.InvoiceRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get.return_value = row
            MockRepo.return_value = repo_instance

            service = _make_service(mock_session)
            result = await service.get_invoice("inv-001")

        assert result is not None
        assert result["invoice_id"] == "inv-001"
        assert result["invoice_number"] == "INV-2024-001"
        assert result["stripe_invoice_id"] == "in_stripe_abc"
        assert result["subtotal_usd"] == 99.00
        assert result["tax_usd"] == 0.0
        assert result["total_usd"] == 99.00
        assert result["line_items"] == [{"description": "Team Plan", "amount": 99.00}]
        assert result["status"] == "paid"
        assert result["period_start"] == datetime(2024, 6, 1, tzinfo=UTC).isoformat()
        assert result["period_end"] == datetime(2024, 7, 1, tzinfo=UTC).isoformat()
        assert result["created_at"] == now.isoformat()

    @pytest.mark.asyncio
    async def test_handles_none_datetime_fields(self, mock_session: AsyncMock) -> None:
        """Invoice with None period fields serializes them as None."""
        row = MagicMock()
        row.invoice_id = "inv-sparse"
        row.invoice_number = "INV-2024-999"
        row.stripe_invoice_id = None
        row.period_start = None
        row.period_end = None
        row.subtotal_usd = 0.0
        row.tax_usd = 0.0
        row.total_usd = 0.0
        row.line_items_json = []
        row.status = "draft"
        row.created_at = None

        with patch("api.services.invoice_service.InvoiceRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get.return_value = row
            MockRepo.return_value = repo_instance

            service = _make_service(mock_session)
            result = await service.get_invoice("inv-sparse")

        assert result["period_start"] is None
        assert result["period_end"] is None
        assert result["created_at"] is None


# ---------------------------------------------------------------------------
# InvoiceService.list_invoices
# ---------------------------------------------------------------------------


class TestListInvoices:
    """Verify list_invoices returns paginated list."""

    @pytest.mark.asyncio
    async def test_returns_paginated_list(self, mock_session: AsyncMock) -> None:
        """list_invoices returns a dict with invoices list and total count."""
        now = datetime.now(UTC)

        row1 = MagicMock()
        row1.invoice_id = "inv-a"
        row1.invoice_number = "INV-001"
        row1.period_start = datetime(2024, 6, 1, tzinfo=UTC)
        row1.period_end = datetime(2024, 7, 1, tzinfo=UTC)
        row1.total_usd = 50.00
        row1.status = "paid"
        row1.created_at = now

        row2 = MagicMock()
        row2.invoice_id = "inv-b"
        row2.invoice_number = "INV-002"
        row2.period_start = datetime(2024, 7, 1, tzinfo=UTC)
        row2.period_end = datetime(2024, 8, 1, tzinfo=UTC)
        row2.total_usd = 75.00
        row2.status = "paid"
        row2.created_at = now

        with patch("api.services.invoice_service.InvoiceRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.list_for_tenant.return_value = ([row1, row2], 5)
            MockRepo.return_value = repo_instance

            service = _make_service(mock_session)
            result = await service.list_invoices(limit=2, offset=0)

        assert result["total"] == 5
        assert len(result["invoices"]) == 2
        assert result["invoices"][0]["invoice_id"] == "inv-a"
        assert result["invoices"][0]["total_usd"] == 50.00
        assert result["invoices"][1]["invoice_id"] == "inv-b"
        assert result["invoices"][1]["total_usd"] == 75.00

    @pytest.mark.asyncio
    async def test_returns_empty_list(self, mock_session: AsyncMock) -> None:
        """list_invoices with no data returns an empty invoices list."""
        with patch("api.services.invoice_service.InvoiceRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.list_for_tenant.return_value = ([], 0)
            MockRepo.return_value = repo_instance

            service = _make_service(mock_session)
            result = await service.list_invoices()

        assert result["total"] == 0
        assert result["invoices"] == []


# ---------------------------------------------------------------------------
# InvoiceService.get_pdf
# ---------------------------------------------------------------------------


class TestGetPdf:
    """Verify get_pdf returns bytes or None."""

    @pytest.mark.asyncio
    async def test_returns_none_when_invoice_not_found(self, mock_session: AsyncMock) -> None:
        """When the invoice does not exist, returns None."""
        with patch("api.services.invoice_service.InvoiceRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get.return_value = None
            MockRepo.return_value = repo_instance

            service = _make_service(mock_session)
            result = await service.get_pdf("nonexistent")

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_no_pdf_storage_key(self, mock_session: AsyncMock) -> None:
        """When the invoice exists but has no pdf_storage_key, returns None."""
        row = MagicMock()
        row.pdf_storage_key = None

        with patch("api.services.invoice_service.InvoiceRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get.return_value = row
            MockRepo.return_value = repo_instance

            service = _make_service(mock_session)
            result = await service.get_pdf("inv-no-pdf")

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_empty_pdf_storage_key(self, mock_session: AsyncMock) -> None:
        """When pdf_storage_key is empty string (falsy), returns None."""
        row = MagicMock()
        row.pdf_storage_key = ""

        with patch("api.services.invoice_service.InvoiceRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get.return_value = row
            MockRepo.return_value = repo_instance

            service = _make_service(mock_session)
            result = await service.get_pdf("inv-empty-key")

        assert result is None

    @pytest.mark.asyncio
    async def test_reads_file_bytes_when_exists(self, mock_session: AsyncMock, tmp_path: Path) -> None:
        """When the PDF file exists on disk, returns its content."""
        pdf_content = b"%PDF-1.4 fake invoice content"
        pdf_file = tmp_path / "test-tenant" / "inv-real.pdf"
        pdf_file.parent.mkdir(parents=True, exist_ok=True)
        pdf_file.write_bytes(pdf_content)

        row = MagicMock()
        row.pdf_storage_key = str(pdf_file)

        with patch("api.services.invoice_service.InvoiceRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get.return_value = row
            MockRepo.return_value = repo_instance

            service = _make_service(mock_session, storage_path=str(tmp_path))
            result = await service.get_pdf("inv-real")

        assert result == pdf_content

    @pytest.mark.asyncio
    async def test_returns_none_when_file_missing(self, mock_session: AsyncMock, tmp_path: Path) -> None:
        """When pdf_storage_key points to a nonexistent file within the
        storage base, returns None."""
        storage_base = tmp_path / "invoices"
        storage_base.mkdir(parents=True, exist_ok=True)
        missing_file = storage_base / "test-tenant" / "inv-missing-file.pdf"

        row = MagicMock()
        row.pdf_storage_key = str(missing_file)

        with patch("api.services.invoice_service.InvoiceRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get.return_value = row
            MockRepo.return_value = repo_instance

            service = _make_service(mock_session, storage_path=str(storage_base))
            result = await service.get_pdf("inv-missing-file")

        assert result is None


# ---------------------------------------------------------------------------
# InvoiceService._render_pdf_fallback
# ---------------------------------------------------------------------------


class TestRenderPdfFallback:
    """Verify _render_pdf_fallback generates correct text-based PDF content."""

    def test_contains_invoice_header(self, mock_session: AsyncMock) -> None:
        """Fallback PDF starts with 'IRONLAYER INVOICE' header."""
        with patch("api.services.invoice_service.InvoiceRepository"):
            service = _make_service(mock_session)

        data = {
            "invoice_number": "INV-2024-050",
            "tenant_id": "test-tenant",
            "period_start": "2024-06-01T00:00:00+00:00",
            "period_end": "2024-07-01T00:00:00+00:00",
            "created_at": "2024-06-15T12:00:00+00:00",
            "line_items": [],
            "subtotal_usd": 0.0,
            "tax_usd": 0.0,
            "total_usd": 0.0,
        }

        result = service._render_pdf_fallback(data)
        text = result.decode("utf-8")

        assert "IRONLAYER INVOICE" in text

    def test_contains_invoice_number(self, mock_session: AsyncMock) -> None:
        """Fallback PDF includes the invoice number."""
        with patch("api.services.invoice_service.InvoiceRepository"):
            service = _make_service(mock_session)

        data = {
            "invoice_number": "INV-2024-050",
            "tenant_id": "test-tenant",
            "period_start": "2024-06-01T00:00:00+00:00",
            "period_end": "2024-07-01T00:00:00+00:00",
            "created_at": "2024-06-15T12:00:00+00:00",
            "line_items": [],
            "subtotal_usd": 0.0,
            "tax_usd": 0.0,
            "total_usd": 0.0,
        }

        result = service._render_pdf_fallback(data)
        text = result.decode("utf-8")

        assert "INV-2024-050" in text

    def test_contains_line_items(self, mock_session: AsyncMock) -> None:
        """Fallback PDF includes each line item description and amounts."""
        with patch("api.services.invoice_service.InvoiceRepository"):
            service = _make_service(mock_session)

        data = {
            "invoice_number": "INV-2024-051",
            "tenant_id": "test-tenant",
            "period_start": "2024-06-01T00:00:00+00:00",
            "period_end": "2024-07-01T00:00:00+00:00",
            "created_at": "2024-06-15T12:00:00+00:00",
            "line_items": [
                {"description": "Compute Cost (Databricks)", "quantity": 1, "unit_price": 25.00, "amount": 25.00},
                {"description": "LLM Usage (AI Advisory)", "quantity": 1, "unit_price": 5.50, "amount": 5.50},
            ],
            "subtotal_usd": 30.50,
            "tax_usd": 0.0,
            "total_usd": 30.50,
        }

        result = service._render_pdf_fallback(data)
        text = result.decode("utf-8")

        assert "Compute Cost (Databricks)" in text
        assert "LLM Usage (AI Advisory)" in text
        assert "$25.00" in text or "25.00" in text
        assert "$5.50" in text or "5.50" in text

    def test_contains_totals(self, mock_session: AsyncMock) -> None:
        """Fallback PDF includes subtotal, tax, and total amounts."""
        with patch("api.services.invoice_service.InvoiceRepository"):
            service = _make_service(mock_session)

        data = {
            "invoice_number": "INV-2024-052",
            "tenant_id": "test-tenant",
            "period_start": "2024-06-01T00:00:00+00:00",
            "period_end": "2024-07-01T00:00:00+00:00",
            "created_at": "2024-06-15T12:00:00+00:00",
            "line_items": [
                {"description": "Plan Runs", "quantity": 10, "unit_price": 0.0, "amount": 0.0},
            ],
            "subtotal_usd": 0.0,
            "tax_usd": 0.0,
            "total_usd": 0.0,
        }

        result = service._render_pdf_fallback(data)
        text = result.decode("utf-8")

        assert "Subtotal:" in text
        assert "Tax:" in text
        assert "Total:" in text

    def test_contains_footer(self, mock_session: AsyncMock) -> None:
        """Fallback PDF includes IronLayer footer."""
        with patch("api.services.invoice_service.InvoiceRepository"):
            service = _make_service(mock_session)

        data = {
            "invoice_number": "INV-2024-053",
            "tenant_id": "test-tenant",
            "period_start": "2024-06-01T00:00:00+00:00",
            "period_end": "2024-07-01T00:00:00+00:00",
            "created_at": "2024-06-15T12:00:00+00:00",
            "line_items": [],
            "subtotal_usd": 0.0,
            "tax_usd": 0.0,
            "total_usd": 0.0,
        }

        result = service._render_pdf_fallback(data)
        text = result.decode("utf-8")

        assert "Generated by IronLayer Platform" in text

    def test_returns_bytes(self, mock_session: AsyncMock) -> None:
        """Fallback PDF returns bytes, not str."""
        with patch("api.services.invoice_service.InvoiceRepository"):
            service = _make_service(mock_session)

        data = {
            "invoice_number": "INV-2024-054",
            "tenant_id": "test-tenant",
            "period_start": "2024-06-01T00:00:00+00:00",
            "period_end": "2024-07-01T00:00:00+00:00",
            "created_at": "2024-06-15T12:00:00+00:00",
            "line_items": [],
            "subtotal_usd": 0.0,
            "tax_usd": 0.0,
            "total_usd": 0.0,
        }

        result = service._render_pdf_fallback(data)
        assert isinstance(result, bytes)


# ---------------------------------------------------------------------------
# InvoiceService._store_pdf
# ---------------------------------------------------------------------------


class TestStorePdf:
    """Verify _store_pdf writes files to the correct path."""

    def test_creates_directory_and_writes_file(self, mock_session: AsyncMock, tmp_path: Path) -> None:
        """_store_pdf creates the tenant directory and writes PDF bytes."""
        storage_path = str(tmp_path / "invoices")

        with patch("api.services.invoice_service.InvoiceRepository"):
            service = _make_service(mock_session, storage_path=storage_path)

        pdf_bytes = b"%PDF-1.4 test content for store"
        result_path = service._store_pdf("inv-store-001", pdf_bytes)

        assert Path(result_path).exists()
        assert Path(result_path).read_bytes() == pdf_bytes
        assert "test-tenant" in result_path
        assert "inv-store-001.pdf" in result_path

    def test_creates_nested_parent_directories(self, mock_session: AsyncMock, tmp_path: Path) -> None:
        """_store_pdf creates parent directories that do not exist yet."""
        storage_path = str(tmp_path / "deep" / "nested" / "invoices")

        with patch("api.services.invoice_service.InvoiceRepository"):
            service = _make_service(mock_session, storage_path=storage_path)

        pdf_bytes = b"nested pdf content"
        result_path = service._store_pdf("inv-nested", pdf_bytes)

        assert Path(result_path).exists()
        assert Path(result_path).read_bytes() == pdf_bytes

    def test_overwrites_existing_file(self, mock_session: AsyncMock, tmp_path: Path) -> None:
        """_store_pdf overwrites an existing file with the same invoice_id."""
        storage_path = str(tmp_path / "invoices")
        tenant_dir = Path(storage_path) / "test-tenant"
        tenant_dir.mkdir(parents=True)
        existing_file = tenant_dir / "inv-overwrite.pdf"
        existing_file.write_bytes(b"old content")

        with patch("api.services.invoice_service.InvoiceRepository"):
            service = _make_service(mock_session, storage_path=storage_path)

        new_content = b"new content"
        result_path = service._store_pdf("inv-overwrite", new_content)

        assert Path(result_path).read_bytes() == new_content

    def test_returns_string_path(self, mock_session: AsyncMock, tmp_path: Path) -> None:
        """_store_pdf returns a string (not Path object)."""
        storage_path = str(tmp_path / "invoices")

        with patch("api.services.invoice_service.InvoiceRepository"):
            service = _make_service(mock_session, storage_path=storage_path)

        result_path = service._store_pdf("inv-type-check", b"content")
        assert isinstance(result_path, str)
