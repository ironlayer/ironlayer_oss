"""BL-120: Keyset cursor pagination utilities.

Cursors are opaque, base64-encoded strings that encode a (timestamp, id)
pair.  This avoids ``OFFSET`` scans that are O(n) at large page positions.
"""

from __future__ import annotations

import base64
import json
import logging

logger = logging.getLogger(__name__)


def encode_cursor(timestamp: str, record_id: str) -> str:
    """Encode a (timestamp, id) pair into an opaque cursor string.

    Parameters
    ----------
    timestamp:
        ISO-8601 timestamp of the last item on the current page.
    record_id:
        Unique identifier of the last item on the current page.

    Returns
    -------
    str
        URL-safe base64-encoded cursor.
    """
    payload = json.dumps({"ts": timestamp, "id": record_id}, separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii")


def decode_cursor(cursor: str) -> tuple[str, str] | None:
    """Decode an opaque cursor back to ``(timestamp, id)``.

    Returns ``None`` if the cursor is malformed, expired, or otherwise
    unusable — callers should fall back to offset-based pagination.
    """
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii"))
        data = json.loads(raw)
        ts = data["ts"]
        record_id = data["id"]
        if not isinstance(ts, str) or not isinstance(record_id, str):
            return None
        return ts, record_id
    except Exception:
        logger.debug("Failed to decode cursor: %s", cursor[:32], exc_info=True)
        return None
