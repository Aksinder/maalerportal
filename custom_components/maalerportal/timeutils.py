"""Shared timestamp helpers for the Målerportal integration."""
from __future__ import annotations

from datetime import datetime, timezone


def parse_api_timestamp(timestamp: str | None) -> datetime | None:
    """Parse a Målerportal API timestamp into an aware datetime.

    Accepts both the ``...Z`` UTC form and explicit-offset ISO strings.
    Naive timestamps are assumed to be UTC. Returns ``None`` for empty or
    unparseable input so callers can simply skip the row.
    """
    if not timestamp:
        return None
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed
