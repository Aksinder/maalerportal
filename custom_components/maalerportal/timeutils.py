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


_UNPARSEABLE = datetime.min.replace(tzinfo=timezone.utc)


def reading_sort_key(reading: dict) -> datetime:
    """Chronological sort key for an API reading dict.

    ``/readings/historical`` answers in local time with an explicit offset
    (``...T02:00:00+02:00``). Sorting those strings is *not* chronological in
    the repeated hour when summer time ends: ``02:00+01:00`` (the later
    instant) sorts before ``02:00+02:00`` (the earlier one). Sort on the
    parsed instant instead. Unparseable rows sort first and are skipped later
    by the callers' own timestamp handling.
    """
    return parse_api_timestamp(reading.get("timestamp")) or _UNPARSEABLE


def is_older_reading(candidate_ts: str | None, reference_ts: str | None) -> bool:
    """Whether ``candidate_ts`` is strictly earlier than ``reference_ts``.

    Returns ``False`` unless both parse, so a missing or malformed timestamp
    never suppresses a reading.
    """
    candidate = parse_api_timestamp(candidate_ts)
    reference = parse_api_timestamp(reference_ts)
    if candidate is None or reference is None:
        return False
    return candidate < reference
