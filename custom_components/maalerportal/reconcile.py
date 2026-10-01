"""Pure logic for installation reconciliation and meter-swap offset math.

This module contains no Home Assistant imports so the logic can be unit
tested in isolation.
"""
from __future__ import annotations

import re
from typing import Any

# Installation IDs from the API are UUIDs. They are used to build the
# per-installation CSV log path (``<config>/maalerportal/<id>.csv``), so an
# id must never contain path separators or traversal sequences. Accept only a
# conservative charset and bounded length; anything else is rejected before it
# can reach a filesystem path.
_SAFE_INSTALLATION_ID = re.compile(r"\A[A-Za-z0-9_-]{1,64}\Z")

# Fields on an installation we want to keep in sync with the API.
TRACKED_INSTALLATION_FIELDS = (
    "address",
    "timezone",
    "installationType",
    "utilityName",
    "meterSerial",
    "nickname",
)

# Fields whose change strongly indicates a meter swap.
SWAP_TRIGGER_FIELDS = ("meterSerial",)


def reconcile_installations(
    saved: list[dict[str, Any]], fresh: list[dict[str, Any]]
) -> tuple[
    list[dict[str, Any]],
    set[str],
    dict[str, dict[str, tuple[Any, Any]]],
    bool,
]:
    """Merge fresh API data into the saved installation list.

    Args:
        saved: installations as currently stored in the config entry.
        fresh: installations as currently returned by ``GET /addresses``.

    Returns:
        merged: union list of installations with tracked fields refreshed.
        missing_ids: installation IDs that are no longer in ``fresh``.
        serial_changes: per-installation map of changed tracked fields,
            limited to installations whose ``meterSerial`` changed.
            Used by callers to trigger meter-swap offset recalculation.
        changed: True if any tracked field was updated. A missing
            installation does NOT set this flag because no field actually
            changed — it just becomes inaccessible. Callers detect that
            via ``missing_ids`` instead.
    """
    fresh_by_id = {i["installationId"]: i for i in fresh if i.get("installationId")}
    merged: list[dict[str, Any]] = []
    missing_ids: set[str] = set()
    serial_changes: dict[str, dict[str, tuple[Any, Any]]] = {}
    changed = False

    for installation in saved:
        installation_id = installation.get("installationId")
        upstream = fresh_by_id.get(installation_id)
        if upstream is None:
            merged.append(installation)
            missing_ids.add(installation_id)
            continue

        updated = dict(installation)
        installation_changes: dict[str, tuple[Any, Any]] = {}
        for field in TRACKED_INSTALLATION_FIELDS:
            new_value = upstream.get(field)
            old_value = installation.get(field)
            if new_value is not None and new_value != old_value:
                installation_changes[field] = (old_value, new_value)
                updated[field] = new_value

        if installation_changes:
            changed = True
            if any(field in installation_changes for field in SWAP_TRIGGER_FIELDS):
                serial_changes[installation_id] = installation_changes

        merged.append(updated)

    return merged, missing_ids, serial_changes, changed


def is_safe_installation_id(installation_id: Any) -> bool:
    """Whether an installation id is safe to use in a filesystem path.

    Guards the per-installation CSV log path against path traversal /
    absolute-path injection from a malicious or compromised upstream API
    response. Only a bounded alphanumeric/``_``/``-`` charset is allowed
    (UUIDs qualify); anything with separators, dots, or empty/oversized
    values is rejected.
    """
    return isinstance(installation_id, str) and bool(
        _SAFE_INSTALLATION_ID.match(installation_id)
    )


def find_new_installations(
    saved: list[dict[str, Any]], fresh: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Return installations present upstream but not configured."""
    saved_ids = {i.get("installationId") for i in saved}
    return [i for i in fresh if i.get("installationId") not in saved_ids]


def should_seed_previous_from_recorder(
    force_full_fetch: bool, has_existing_stats: bool
) -> bool:
    """Whether to seed ``prev_raw_value``/``prev_displayed_sum`` from recorder.

    The swap detector compares each incoming raw reading against the previous
    one. On a normal incremental update the previous value is the last value
    stored in the recorder, so seeding from recorder is correct.

    On a *full re-fetch* the cursor is reset and readings are reprocessed
    oldest-first. Seeding from the recorder would then compare the **newest**
    stored value against the **oldest** incoming reading — an apparent huge
    drop that looks exactly like a meter swap. That false positive re-anchors
    the offset and, because a full fetch runs on every startup, compounds the
    error on each restart. So only seed on incremental updates.
    """
    return has_existing_stats and not force_full_fetch


def is_meter_swap(
    prev_raw_value: float | None,
    prev_displayed_sum: float | None,
    value: float,
    swap_pending: bool,
    drop_threshold: float,
) -> bool:
    """Decide whether ``value`` represents a meter swap vs. the previous reading.

    A swap is recognised only when the utility has actually reported a new
    meter serial (``swap_pending``, raised by :func:`reconcile_installations`)
    **and** the raw value dropped below ``prev_raw_value * drop_threshold``.

    Requiring the serial-change flag is deliberate: an earlier data-only
    fallback (re-anchor whenever the value fell below 10% of the previous one)
    produced false positives — e.g. backfill ordering or counter rollover —
    that silently inflated the offset. The authoritative signal for a real
    swap is the serial change, so gate re-anchoring on it.
    """
    if prev_raw_value is None or prev_displayed_sum is None:
        return False
    if not swap_pending:
        return False
    return value < prev_raw_value * drop_threshold


def compute_swap_offset(
    last_displayed_sum: float, first_new_raw_value: float
) -> float:
    """Compute the cumulative offset to apply after a meter swap.

    A meter swap means a new physical meter is installed, whose raw counter
    starts near zero. To keep the user-facing accumulated total continuous,
    we apply an offset to all subsequent readings such that:

        displayed_sum = raw_value + new_offset

    For the first reading from the new meter, we want the displayed sum to
    equal what the previous meter ended at (``last_displayed_sum``)::

        last_displayed_sum = first_new_raw_value + new_offset
     => new_offset = last_displayed_sum - first_new_raw_value

    The returned offset replaces any previous offset; it is not additive.
    The previous offset is already baked into ``last_displayed_sum``
    (which comes from the recorder's stored statistics).

    Example:
        Old meter ended at 1973.969 m³ (this is also the displayed sum).
        New meter's first reading is 0.355 m³.
        new_offset = 1973.969 - 0.355 = 1973.614
        First displayed sum after swap = 0.355 + 1973.614 = 1973.969 ✓
        Second reading 0.446 → displayed = 0.446 + 1973.614 = 1974.060 ✓
    """
    return last_displayed_sum - first_new_raw_value


# ---------------------------------------------------------------------------
# Late-arriving hours
#
# The utility sometimes delivers an hour *after* a newer hour has already been
# imported. The periodic statistics update re-fetches a 7-day window, so the
# late hour is in the response, but a "newer than the last import" cursor
# discards it. The helpers below decide what to import for such an hour.
# ---------------------------------------------------------------------------


def rebase_consumption_rows(
    stored: list[tuple[Any, float | None, float | None]],
    late: dict[Any, float],
) -> tuple[list[tuple[Any, float, float]], float]:
    """Re-base cumulative sums after hours that arrived late.

    Consumption meters store one row per hour with ``state`` = that hour's
    interval and ``sum`` = running total. Inserting an earlier hour changes
    the ``sum`` of every row after it, so the rows from the earliest late hour
    onwards are recomputed.

    Args:
        stored: ``(start, state, sum)`` rows already in the recorder, ordered
            by ``start``. Rows with a ``None`` sum are ignored.
        late: ``{start: interval}`` for hours that are *not* stored yet but are
            older than the newest stored row. Starts compare with ``stored``'s.

    Returns:
        ``(rows, delta)``. ``rows`` are ``(start, interval, new_sum)`` for every
        late hour and every stored row whose sum changed, ordered by ``start``.
        ``delta`` is how much the newest total grew; add it to any cached
        running total. Both are empty/zero if ``late`` is empty.

    Only positive intervals add to the running total, matching the importer.
    """
    if not late:
        return [], 0.0

    rows = [(s, st, sm) for s, st, sm in stored if sm is not None]
    earliest = min(late)

    before = [r for r in rows if r[0] < earliest]
    after = [r for r in rows if r[0] > earliest]
    if before:
        base = before[-1][2]
    elif after:
        # Nothing stored before the late hour in the window: derive the total
        # that preceded the first stored row from that row itself.
        first_state = after[0][1] or 0.0
        base = after[0][2] - max(first_state, 0.0)
    else:
        base = 0.0

    items: dict[Any, tuple[float, float | None]] = {s: (v, None) for s, v in late.items()}
    for start, state, old_sum in after:
        items[start] = (state if state is not None else 0.0, old_sum)

    running = base
    out: list[tuple[Any, float, float]] = []
    for start in sorted(items):
        interval, old_sum = items[start]
        if interval > 0:
            running += interval
        if old_sum is None or abs(running - old_sum) > 1e-9:
            out.append((start, interval, running))

    newest_old = rows[-1][2] if rows else 0.0
    return out, running - newest_old


def late_counter_row_sum(
    value: float,
    prev: tuple[float | None, float | None] | None,
    next_: tuple[float | None, float | None] | None,
) -> float | None:
    """Sum to store for a cumulative-counter reading that arrived late.

    Counter rows carry an absolute raw ``state`` and a ``sum`` that is the raw
    value plus a constant (the meter-swap offset, or minus the first-install
    baseline). A late hour can therefore be added without touching other rows:
    take the constant from its stored neighbours and apply it.

    Args:
        value: the late raw reading.
        prev / next_: ``(state, sum)`` of the nearest stored rows before and
            after the late hour, or ``None`` if there is none in the window.

    Returns ``None`` -- meaning "do not add this row" -- when the reading does
    not fit: it is not monotone against a neighbour (e.g. a pre-swap reading
    next to a post-swap row), the neighbours disagree on the constant (a swap
    boundary), or there is no neighbour to anchor the constant on.
    """
    offsets: list[float] = []
    if prev is not None and prev[0] is not None and prev[1] is not None:
        if value < prev[0]:
            return None
        offsets.append(prev[1] - prev[0])
    if next_ is not None and next_[0] is not None and next_[1] is not None:
        if value > next_[0]:
            return None
        offsets.append(next_[1] - next_[0])
    if not offsets:
        return None
    if max(offsets) - min(offsets) > 1e-6:
        return None
    return value + offsets[0]
