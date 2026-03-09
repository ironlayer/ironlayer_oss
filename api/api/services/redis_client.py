"""Async Redis connection manager with graceful in-process fallback.

Provides a singleton Redis client for rate limiting and token revocation
across multiple API replicas. When ``REDIS_URL`` is not configured, all
callers receive ``None`` and must fall back to in-process implementations.

Usage::

    client = await get_redis_client()
    if client is not None:
        await client.set("key", "value", ex=60)
    else:
        # use in-process fallback

Lifecycle
---------
Call :func:`init_redis_client` during application startup (optional; the
first call to :func:`get_redis_client` will also initialise the pool).
Call :func:`close_redis_client` during application shutdown to drain the
connection pool gracefully.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Module-level singleton — None if Redis is not configured or unavailable.
_redis_client: Any | None = None
_redis_url: str | None = None


async def init_redis_client(redis_url: str) -> Any | None:
    """Initialise the connection pool and test connectivity.

    Parameters
    ----------
    redis_url:
        Redis connection URL, e.g. ``redis://localhost:6379/0``.

    Returns
    -------
    redis.asyncio.Redis | None
        The connected client, or ``None`` if the connection fails.
    """
    global _redis_client, _redis_url  # noqa: PLW0603

    _redis_url = redis_url

    try:
        import redis.asyncio as aioredis  # type: ignore[import-untyped]
    except ImportError:
        logger.warning(
            "redis package not installed — rate limiting and token revocation "
            "will use in-process fallbacks. Install with: pip install redis[asyncio]"
        )
        return None

    try:
        client = aioredis.from_url(
            redis_url,
            encoding="utf-8",
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
            health_check_interval=30,
        )
        # Smoke-test connectivity.
        await client.ping()
        _redis_client = client
        logger.info("Redis client initialised: %s", redis_url.split("@")[-1])
        return _redis_client
    except Exception as exc:
        logger.warning(
            "Redis connection failed (%s) — rate limiting and token revocation "
            "will use in-process fallbacks: %s",
            redis_url.split("@")[-1],
            exc,
        )
        _redis_client = None
        return None


async def get_redis_client() -> Any | None:
    """Return the active Redis client, or ``None`` if unavailable."""
    return _redis_client


async def close_redis_client() -> None:
    """Drain the connection pool on application shutdown."""
    global _redis_client  # noqa: PLW0603
    if _redis_client is not None:
        try:
            await _redis_client.aclose()
            logger.info("Redis connection pool closed")
        except Exception as exc:
            logger.warning("Error closing Redis connection pool: %s", exc)
        finally:
            _redis_client = None
