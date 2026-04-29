"""Append-only CSV log of every meter reading we observe.

One file per installation under ``<config>/maalerportal/<installation_id>.csv``,
with a header row and columns:

    timestamp,counter_type,meter_counter_id,value,unit,source

* ``timestamp`` is the original upstream-reported timestamp from the API
  (ISO-8601 with timezone), preserved verbatim — not when we polled.
* ``source`` distinguishes ``latest`` (from /readings/latest), ``fallback``
  (filled in by the coordinator from /readings/historical when latest was
  null) and ``historical`` (from a deliberate historical fetch by the
  StatisticSensor or fetch-more-history button).

Writes are deduplicated on ``(meter_counter_id, timestamp)`` so re-fetches
of the same period don't grow the file. The dedup set is loaded once at
startup from the existing file so reloads don't re-write old rows.

The file lives on the user's filesystem and is meant for archival /
external analysis — tail it, grep it, import to a spreadsheet etc.
"""
from __future__ import annotations

import asyncio
import csv
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)

_SUBDIR = "maalerportal"
_HEADER = ["timestamp", "counter_type", "meter_counter_id", "value", "unit", "source"]


def _normalize_timestamp(ts: str) -> str:
    """Canonicalize any ISO-8601 timestamp to UTC with .000Z suffix.

    The Målerportal API returns the same physical moment in two
    different ISO formats depending on endpoint: ``/readings/latest``
    uses UTC (``...T17:00:00.000Z``) while ``/readings/historical``
    uses local time (``...T19:00:00.000+02:00``). Without normalization
    the dedup key sees them as different rows and both end up in the
    log — surfacing as visible duplicates in cards. Normalizing to a
    single canonical form fixes both dedup and sorting.
    """
    try:
        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, TypeError, AttributeError):
        return ts  # leave unparseable strings alone
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

# Last N rows kept in memory for cheap "recent readings" lookups (used by
# the LastReadingSensor's recent_readings attribute). The CSV is the
# canonical archive — this in-memory ring is just a fast view.
_RECENT_BUFFER_SIZE = 200


class ReadingsLog:
    """Per-installation CSV append-only log."""

    def __init__(self, hass: HomeAssistant, installation_id: str) -> None:
        self._dir = Path(hass.config.path(_SUBDIR))
        self._path = self._dir / f"{installation_id}.csv"
        self._known: set[tuple[str, str]] = set()
        # Last N rows in memory — populated from the CSV tail at load
        # and updated as new rows are written. Sorted oldest-first.
        self._recent: list[dict[str, Any]] = []
        self._lock = asyncio.Lock()
        self._loaded = False

    @property
    def path(self) -> Path:
        return self._path

    async def async_load(self) -> None:
        """Initialize file (creates header if missing) and load existing keys."""
        await asyncio.to_thread(self._init_file_if_missing)
        self._known, self._recent, needs_rewrite = await asyncio.to_thread(
            self._read_existing
        )
        if needs_rewrite:
            # Old CSV had timestamps in mixed timezone formats (e.g. one
            # row in UTC ...Z and another in CEST ...+02:00 for the same
            # moment). Normalize and dedup the file once.
            await asyncio.to_thread(self._rewrite_normalized)
            # Re-read the cleaned file so in-memory state matches disk.
            self._known, self._recent, _ = await asyncio.to_thread(self._read_existing)
            _LOGGER.info(
                "Migrated readings log %s to canonical UTC timestamps "
                "(now %d unique rows)",
                self._path,
                len(self._known),
            )
        self._loaded = True
        _LOGGER.debug(
            "Loaded readings log %s with %d existing rows (%d in recent buffer)",
            self._path,
            len(self._known),
            len(self._recent),
        )

    def _init_file_if_missing(self) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        if not self._path.exists():
            with self._path.open("w", encoding="utf-8", newline="") as f:
                csv.writer(f).writerow(_HEADER)

    def _read_existing(
        self,
    ) -> tuple[set[tuple[str, str]], list[dict[str, Any]], bool]:
        """Scan the CSV once to populate both the dedup key set and the
        recent-rows ring buffer.

        Returns ``(keys, recent, needs_rewrite)``.  ``needs_rewrite`` is
        True if any timestamp in the file isn't in canonical UTC ISO-Z
        form — the caller should call :py:meth:`_rewrite_normalized`
        to migrate the file.
        """
        keys: set[tuple[str, str]] = set()
        rows: list[dict[str, Any]] = []
        needs_rewrite = False
        if not self._path.exists():
            return keys, rows, needs_rewrite
        try:
            with self._path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cid = row.get("meter_counter_id")
                    ts = row.get("timestamp")
                    if not (cid and ts):
                        continue
                    norm_ts = _normalize_timestamp(ts)
                    if norm_ts != ts:
                        needs_rewrite = True
                    key = (cid, norm_ts)
                    if key in keys:
                        # Already seen the canonical version of this
                        # row in another (timezone-shifted) form.
                        needs_rewrite = True
                        continue
                    keys.add(key)
                    new_row = dict(row)
                    new_row["timestamp"] = norm_ts
                    rows.append(new_row)
        except OSError as err:
            _LOGGER.warning("Could not read readings log %s: %s", self._path, err)
        rows.sort(key=lambda r: r.get("timestamp", ""))
        return keys, rows[-_RECENT_BUFFER_SIZE:], needs_rewrite

    def _rewrite_normalized(self) -> None:
        """One-time migration: rewrite the CSV with canonical UTC
        timestamps and dedup any rows that map to the same canonical
        form (e.g. one row in CEST and one in UTC for the same moment).
        """
        if not self._path.exists():
            return
        try:
            with self._path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                source_rows = [dict(r) for r in reader]
        except OSError as err:
            _LOGGER.warning("Could not read %s for rewrite: %s", self._path, err)
            return
        seen: set[tuple[str, str]] = set()
        deduped: list[dict[str, Any]] = []
        for row in source_rows:
            cid = row.get("meter_counter_id")
            ts = row.get("timestamp")
            if not (cid and ts):
                continue
            norm_ts = _normalize_timestamp(ts)
            key = (cid, norm_ts)
            if key in seen:
                continue
            seen.add(key)
            row["timestamp"] = norm_ts
            deduped.append(row)
        deduped.sort(key=lambda r: r.get("timestamp", ""))
        try:
            with self._path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=_HEADER)
                writer.writeheader()
                for row in deduped:
                    writer.writerow({k: row.get(k, "") for k in _HEADER})
        except OSError as err:
            _LOGGER.warning("Could not rewrite %s: %s", self._path, err)

    async def async_record(
        self,
        *,
        timestamp: str,
        counter_type: str,
        meter_counter_id: str,
        value: Any,
        unit: str = "",
        source: str = "latest",
    ) -> bool:
        """Append one reading if it isn't already in the file.

        Returns True on a new write, False on duplicate / invalid input.
        """
        if not self._loaded:
            return False
        if not timestamp or not meter_counter_id or value is None:
            return False
        # Canonicalize the timestamp so the same physical moment received
        # via different endpoints (UTC vs local-tz) maps to one row.
        norm_ts = _normalize_timestamp(timestamp)
        key = (meter_counter_id, norm_ts)
        if key in self._known:
            return False
        async with self._lock:
            # Re-check inside the lock to avoid a race on concurrent records.
            if key in self._known:
                return False
            try:
                await asyncio.to_thread(
                    self._append_row,
                    [
                        norm_ts,
                        counter_type or "",
                        meter_counter_id,
                        str(value),
                        unit or "",
                        source,
                    ],
                )
            except OSError as err:
                _LOGGER.warning(
                    "Could not append to readings log %s: %s", self._path, err
                )
                return False
            self._known.add(key)
            # Mirror to in-memory buffer (kept sorted oldest-first, capped).
            self._recent.append({
                "timestamp": norm_ts,
                "counter_type": counter_type or "",
                "meter_counter_id": meter_counter_id,
                "value": value,
                "unit": unit or "",
                "source": source,
            })
            if len(self._recent) > _RECENT_BUFFER_SIZE:
                # Re-sort defensively in case out-of-order records arrived
                # (historical bulk imports often do).
                self._recent.sort(key=lambda r: r.get("timestamp", ""))
                del self._recent[: len(self._recent) - _RECENT_BUFFER_SIZE]
        return True

    def _append_row(self, row: list[str]) -> None:
        with self._path.open("a", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(row)

    def recent_readings(
        self,
        *,
        counter_id: str | None = None,
        n: int = 30,
    ) -> list[dict[str, Any]]:
        """Return the last ``n`` rows from the in-memory buffer.

        If ``counter_id`` is given, filter to that counter only —
        useful for cards that show one meter type at a time. Rows
        are returned sorted oldest-first to match the CSV order;
        callers can reverse if they want newest-first display.
        """
        sorted_recent = sorted(self._recent, key=lambda r: r.get("timestamp", ""))
        if counter_id:
            sorted_recent = [
                r for r in sorted_recent if r.get("meter_counter_id") == counter_id
            ]
        return sorted_recent[-n:]

    async def async_record_many(
        self,
        readings: list[dict[str, Any]],
        *,
        source: str = "historical",
    ) -> int:
        """Bulk record readings from a /readings/historical response.

        Each reading dict is expected to have keys ``timestamp``,
        ``meterCounterId``, ``value`` (and optionally ``unit``,
        ``counterType``). Returns the number of new rows actually written.
        """
        written = 0
        for r in readings:
            ok = await self.async_record(
                timestamp=r.get("timestamp", ""),
                counter_type=r.get("counterType", ""),
                meter_counter_id=r.get("meterCounterId", ""),
                value=r.get("value"),
                unit=r.get("unit", ""),
                source=source,
            )
            if ok:
                written += 1
        return written
