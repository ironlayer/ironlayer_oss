"""Unit tests for core_engine.executor.retry."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from core_engine.executor.retry import (
    RetryConfig,
    _compute_delay,
    retry_with_backoff,
)

# ---------------------------------------------------------------------------
# RetryConfig
# ---------------------------------------------------------------------------


class TestRetryConfig:
    def test_default_values(self):
        config = RetryConfig()
        assert config.max_retries == 3
        assert config.base_delay == 2.0
        assert config.max_delay == 60.0
        assert config.jitter is True

    def test_custom_values(self):
        config = RetryConfig(
            max_retries=5,
            base_delay=1.0,
            max_delay=30.0,
            jitter=False,
        )
        assert config.max_retries == 5
        assert config.base_delay == 1.0
        assert config.max_delay == 30.0
        assert config.jitter is False

    def test_zero_retries_allowed(self):
        config = RetryConfig(max_retries=0)
        assert config.max_retries == 0


# ---------------------------------------------------------------------------
# _compute_delay
# ---------------------------------------------------------------------------


class TestComputeDelay:
    def test_exponential_growth_no_jitter(self):
        config = RetryConfig(base_delay=1.0, max_delay=100.0, jitter=False)
        assert _compute_delay(0, config) == 1.0
        assert _compute_delay(1, config) == 2.0
        assert _compute_delay(2, config) == 4.0
        assert _compute_delay(3, config) == 8.0

    def test_max_delay_cap(self):
        config = RetryConfig(base_delay=1.0, max_delay=5.0, jitter=False)
        assert _compute_delay(10, config) == 5.0

    def test_jitter_stays_within_bounds(self):
        config = RetryConfig(base_delay=10.0, max_delay=100.0, jitter=True)
        for _ in range(100):
            delay = _compute_delay(0, config)
            # base_delay * 2^0 = 10.0, jitter [0.5x, 1.5x] => [5.0, 15.0]
            assert 5.0 <= delay <= 15.0


# ---------------------------------------------------------------------------
# retry_with_backoff
# ---------------------------------------------------------------------------


class TestRetryWithBackoff:
    def test_succeeds_first_try(self):
        fn = MagicMock(return_value=42)
        config = RetryConfig(max_retries=3, base_delay=0.01, jitter=False)

        result = retry_with_backoff(fn, config)
        assert result == 42
        assert fn.call_count == 1

    @patch("core_engine.executor.retry.time.sleep")
    def test_succeeds_after_retries(self, mock_sleep: MagicMock):
        call_count = 0

        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("temporary failure")
            return "success"

        config = RetryConfig(max_retries=3, base_delay=0.01, jitter=False)
        result = retry_with_backoff(flaky, config, retryable_exceptions=(ValueError,))
        assert result == "success"
        assert call_count == 3
        # Should have slept twice (after attempt 1 and attempt 2).
        assert mock_sleep.call_count == 2

    @patch("core_engine.executor.retry.time.sleep")
    def test_exhausts_retries_raises(self, mock_sleep: MagicMock):
        fn = MagicMock(side_effect=RuntimeError("always fails"))
        config = RetryConfig(max_retries=2, base_delay=0.01, jitter=False)

        with pytest.raises(RuntimeError, match="always fails"):
            retry_with_backoff(fn, config, retryable_exceptions=(RuntimeError,))

        # 1 initial call + 2 retries = 3 total.
        assert fn.call_count == 3

    @patch("core_engine.executor.retry.time.sleep")
    def test_only_retryable_exceptions_retried(self, mock_sleep: MagicMock):
        fn = MagicMock(side_effect=TypeError("not retryable"))
        config = RetryConfig(max_retries=3, base_delay=0.01, jitter=False)

        with pytest.raises(TypeError, match="not retryable"):
            retry_with_backoff(fn, config, retryable_exceptions=(ValueError,))

        # Should fail on first call -- TypeError is not retryable.
        assert fn.call_count == 1
        assert mock_sleep.call_count == 0

    def test_non_retryable_raises_immediately(self):
        fn = MagicMock(side_effect=KeyError("not retryable"))
        config = RetryConfig(max_retries=3, base_delay=0.01, jitter=False)

        with pytest.raises(KeyError):
            retry_with_backoff(fn, config, retryable_exceptions=(ValueError,))

        assert fn.call_count == 1

    @patch("core_engine.executor.retry.time.sleep")
    def test_zero_retries_fails_immediately(self, mock_sleep: MagicMock):
        fn = MagicMock(side_effect=ValueError("fail"))
        config = RetryConfig(max_retries=0, base_delay=0.01, jitter=False)

        with pytest.raises(ValueError, match="fail"):
            retry_with_backoff(fn, config, retryable_exceptions=(ValueError,))

        assert fn.call_count == 1
        assert mock_sleep.call_count == 0

    @patch("core_engine.executor.retry.time.sleep")
    def test_last_exception_is_raised(self, mock_sleep: MagicMock):
        errors = [ValueError("first"), ValueError("second"), ValueError("third")]
        call_idx = 0

        def raises():
            nonlocal call_idx
            exc = errors[call_idx]
            call_idx += 1
            raise exc

        config = RetryConfig(max_retries=2, base_delay=0.01, jitter=False)
        with pytest.raises(ValueError, match="third"):
            retry_with_backoff(raises, config, retryable_exceptions=(ValueError,))
