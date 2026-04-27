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
from pathlib import Path
from typing import Any

from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)

_SUBDIR = "maalerportal"
_HEADER = ["timestamp", "counter_type", "meter_counter_id", "value", "unit", "source"]


class ReadingsLog:
    """Per-installation CSV append-only log."""

    def __init__(self, hass: HomeAssistant, installation_id: str) -> None:
        self._dir = Path(hass.config.path(_SUBDIR))
        self._path = self._dir / f"{installation_id}.csv"
        self._known: set[tuple[str, str]] = set()
        self._lock = asyncio.Lock()
        self._loaded = False

    @property
    def path(self) -> Path:
        return self._path

    async def async_load(self) -> None:
        """Initialize file (creates header if missing) and load existing keys."""
        await asyncio.to_thread(self._init_file_if_missing)
        self._known = await asyncio.to_thread(self._read_existing_keys)
        self._loaded = True
        _LOGGER.debug(
            "Loaded readings log %s with %d existing rows",
            self._path,
            len(self._known),
        )

    def _init_file_if_missing(self) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        if not self._path.exists():
            with self._path.open("w", encoding="utf-8", newline="") as f:
                csv.writer(f).writerow(_HEADER)

    def _read_existing_keys(self) -> set[tuple[str, str]]:
        keys: set[tuple[str, str]] = set()
        if not self._path.exists():
            return keys
        try:
            with self._path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cid = row.get("meter_counter_id")
                    ts = row.get("timestamp")
                    if cid and ts:
                        keys.add((cid, ts))
        except OSError as err:
            _LOGGER.warning("Could not read readings log %s: %s", self._path, err)
        return keys

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
        key = (meter_counter_id, timestamp)
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
                        timestamp,
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
        return True

    def _append_row(self, row: list[str]) -> None:
        with self._path.open("a", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(row)

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
