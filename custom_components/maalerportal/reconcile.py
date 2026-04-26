"""Pure logic for installation reconciliation and meter-swap offset math.

This module contains no Home Assistant imports so the logic can be unit
tested in isolation.
"""
from __future__ import annotations

from typing import Any

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


def find_new_installations(
    saved: list[dict[str, Any]], fresh: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Return installations present upstream but not configured."""
    saved_ids = {i.get("installationId") for i in saved}
    return [i for i in fresh if i.get("installationId") not in saved_ids]


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
