"""The coordinator must never rewind a counter to an older reading."""
from __future__ import annotations

import logging

import pytest

from custom_components.maalerportal.coordinator import MaalerportalCoordinator

COUNTER = "counter-1"
T_OLD = "2026-05-11T08:00:00.000Z"
T_LIVE = "2026-05-11T10:00:00.000Z"
T_NEW = "2026-05-11T12:00:00.000Z"


def _coordinator(fallback: dict | None = None) -> MaalerportalCoordinator:
    """A coordinator without HA wiring; history lookup is replaced by `fallback`."""
    coord = MaalerportalCoordinator.__new__(MaalerportalCoordinator)
    coord.installation_id = "inst-1"
    coord.readings_log = None
    coord._fallback_values = {}
    coord._high_water = {}

    async def _fake_refresh(counter_ids):
        for cid in counter_ids:
            if fallback is not None:
                coord._fallback_values[cid] = dict(fallback)

    coord._refresh_fallback_from_history = _fake_refresh
    return coord


def _counter(value, ts):
    return {"meterCounterId": COUNTER, "latestValue": value, "latestTimestamp": ts}


async def test_stale_fallback_after_live_reading_is_not_applied():
    coord = _coordinator(fallback={"value": 100.0, "timestamp": T_OLD})
    await coord._backfill_null_latest_values([_counter(105.0, T_LIVE)])

    counters = [_counter(None, None)]
    await coord._backfill_null_latest_values(counters)

    assert counters[0]["latestValue"] is None  # sensors keep their state
    assert "isFallback" not in counters[0]


async def test_newer_fallback_than_live_reading_is_applied():
    coord = _coordinator(fallback={"value": 110.0, "timestamp": T_NEW})
    await coord._backfill_null_latest_values([_counter(105.0, T_LIVE)])

    counters = [_counter(None, None)]
    await coord._backfill_null_latest_values(counters)

    assert counters[0]["latestValue"] == 110.0
    assert counters[0]["isFallback"] is True


async def test_fallback_is_applied_when_no_live_reading_was_seen_yet():
    """Restart / meters that never answer /readings/latest keep working."""
    coord = _coordinator(fallback={"value": 100.0, "timestamp": T_OLD})

    counters = [_counter(None, None)]
    await coord._backfill_null_latest_values(counters)

    assert counters[0]["latestValue"] == 100.0
    assert counters[0]["isFallback"] is True


async def test_live_value_is_never_modified():
    coord = _coordinator()
    counters = [_counter(105.0, T_LIVE)]

    await coord._backfill_null_latest_values(counters)

    assert counters[0]["latestValue"] == 105.0
    assert coord._high_water[COUNTER] == (T_LIVE, 105.0)


async def test_genuine_regression_passes_through_and_is_logged(caplog):
    """Lower value + newer timestamp = meter swap/correction, not stale data."""
    coord = _coordinator()
    await coord._backfill_null_latest_values([_counter(2000.0, T_LIVE)])

    counters = [_counter(0.4, T_NEW)]
    with caplog.at_level(logging.WARNING):
        await coord._backfill_null_latest_values(counters)

    assert counters[0]["latestValue"] == 0.4
    assert "went backwards" in caplog.text
    assert coord._high_water[COUNTER] == (T_NEW, 0.4)


async def test_older_live_reading_does_not_lower_the_high_water_mark():
    coord = _coordinator()
    await coord._backfill_null_latest_values([_counter(105.0, T_LIVE)])

    await coord._backfill_null_latest_values([_counter(100.0, T_OLD)])

    assert coord._high_water[COUNTER] == (T_LIVE, 105.0)


async def test_applied_fallback_advances_the_high_water_mark():
    """live(T_OLD) -> fallback(T_NEW) applied -> a later older fallback is refused."""
    coord = _coordinator(fallback={"value": 110.0, "timestamp": T_NEW})
    await coord._backfill_null_latest_values([_counter(100.0, T_OLD)])
    counters = [_counter(None, None)]
    await coord._backfill_null_latest_values(counters)
    assert counters[0]["latestValue"] == 110.0
    assert coord._high_water[COUNTER] == (T_NEW, 110.0)

    # The history re-fetch now answers with something between the two.
    coord._fallback_values[COUNTER] = {"value": 105.0, "timestamp": T_LIVE}
    counters = [_counter(None, None)]
    await coord._backfill_null_latest_values(counters)

    assert counters[0]["latestValue"] is None
