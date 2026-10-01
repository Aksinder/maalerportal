"""Hours that the utility delivers late must still reach the statistics.

Runs the real ``_async_update_statistics`` against a real (in-memory) recorder:
a first import misses some hours, a later periodic update returns them, and the
statistics must end up complete and consistent.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.statistics import statistics_during_period
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_recording_done,
)

from custom_components.maalerportal.sensors.history import MaalerportalStatisticSensor

STAT_ID = "sensor.test_late_hours"
INSTALLATION = {
    "installationId": "inst-1",
    "installationType": "Electricity",
    "address": "Test 1",
    "meterSerial": "S1",
}


def _base() -> datetime:
    """A whole UTC hour a day ago, so every reading is inside the 7-day window."""
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return now - timedelta(hours=30)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _make_sensor(hass: HomeAssistant, reading_type: str) -> MaalerportalStatisticSensor:
    counter = {
        "meterCounterId": "c-1",
        "counterType": "ElectricityFromGrid",
        "readingType": reading_type,
        "isPrimary": True,
    }
    sensor = MaalerportalStatisticSensor(
        INSTALLATION, "key", "http://example.invalid", counter, timedelta(minutes=30)
    )
    sensor.hass = hass
    sensor.entity_id = STAT_ID
    sensor._statistic_id = STAT_ID
    sensor._attr_name = "Test late hours"  # Entity.name needs a platform otherwise
    sensor.async_write_ha_state = lambda *args, **kwargs: None
    return sensor


def _consumption(hours: list[int], interval: float = 1.0) -> list[dict]:
    base = _base()
    return [
        {
            "meterCounterId": "c-1",
            "timestamp": _iso(base + timedelta(hours=h)),
            "value": interval,
            "unit": "kWh",
        }
        for h in hours
    ]


def _counter(hours: list[int]) -> list[dict]:
    """Cumulative raw readings: 100 + 0.5 per hour."""
    base = _base()
    return [
        {
            "meterCounterId": "c-1",
            "timestamp": _iso(base + timedelta(hours=h)),
            "value": 100 + 0.5 * h,
            "unit": "kWh",
        }
        for h in hours
    ]


async def _run(sensor, hass, readings, *, full: bool) -> None:
    sensor._fetch_historical_chunked = AsyncMock(return_value=readings)
    await sensor._async_update_statistics(force_full_fetch=full)
    await async_wait_recording_done(hass)


async def _stored(hass: HomeAssistant) -> dict[int, tuple[float, float]]:
    """``{hour_offset_from_base: (state, sum)}`` as stored in the recorder."""
    base = _base()
    result = await get_instance(hass).async_add_executor_job(
        statistics_during_period,
        hass,
        base - timedelta(days=2),
        None,
        {STAT_ID},
        "hour",
        None,
        {"state", "sum"},
    )
    rows = {}
    for row in result.get(STAT_ID, []):
        start = datetime.fromtimestamp(row["start"], tz=timezone.utc)
        rows[int((start - base).total_seconds() // 3600)] = (row["state"], row["sum"])
    return rows


async def test_consumption_late_hours_are_imported_and_sums_rebased(
    recorder_mock, hass: HomeAssistant
) -> None:
    sensor = _make_sensor(hass, "consumption")

    # First import: hours 5 and 6 have not been delivered yet.
    await _run(sensor, hass, _consumption([0, 1, 2, 3, 4, 7, 8, 9]), full=True)
    first = await _stored(hass)
    assert sorted(first) == [0, 1, 2, 3, 4, 7, 8, 9]
    assert first[9][1] == pytest.approx(8.0)

    # Periodic update: the utility now returns 5 and 6, plus new hours 10, 11.
    await _run(sensor, hass, _consumption(list(range(12))), full=False)
    after = await _stored(hass)

    assert sorted(after) == list(range(12))
    # Every hour consumed 1.0, so the running total is hour+1 everywhere.
    assert {h: round(s, 6) for h, (_, s) in after.items()} == {h: float(h + 1) for h in range(12)}
    assert sensor._cumulative_sum == pytest.approx(12.0)


async def test_consumption_periodic_update_without_late_hours_changes_nothing(
    recorder_mock, hass: HomeAssistant
) -> None:
    sensor = _make_sensor(hass, "consumption")
    await _run(sensor, hass, _consumption(list(range(8))), full=True)
    before = await _stored(hass)

    await _run(sensor, hass, _consumption(list(range(8))), full=False)

    assert await _stored(hass) == before
    assert sensor._cumulative_sum == pytest.approx(8.0)


async def test_consumption_new_hours_after_a_late_one_continue_the_corrected_total(
    recorder_mock, hass: HomeAssistant
) -> None:
    sensor = _make_sensor(hass, "consumption")
    await _run(sensor, hass, _consumption([0, 1, 3, 4]), full=True)

    await _run(sensor, hass, _consumption([0, 1, 2, 3, 4]), full=False)  # late hour 2
    await _run(sensor, hass, _consumption([0, 1, 2, 3, 4, 5, 6]), full=False)  # new 5, 6
    after = await _stored(hass)

    assert {h: round(s, 6) for h, (_, s) in after.items()} == {h: float(h + 1) for h in range(7)}


async def test_counter_late_hour_is_filled_without_touching_other_rows(
    recorder_mock, hass: HomeAssistant
) -> None:
    sensor = _make_sensor(hass, "counter")
    await _run(sensor, hass, _counter([0, 1, 2, 4, 5, 6]), full=True)
    first = await _stored(hass)

    await _run(sensor, hass, _counter(list(range(8))), full=False)  # late 3, new 7
    after = await _stored(hass)

    # Counter rows are bucketed one hour earlier than the reading timestamp.
    assert sorted(after) == [-1, 0, 1, 2, 3, 4, 5, 6]
    for hour, row in first.items():
        assert after[hour] == row  # untouched
    late_state, late_sum = after[2]  # reading hour 3 -> bucket 2
    assert late_state == pytest.approx(101.5)
    # Same constant offset as its neighbours.
    neighbour_offset = first[1][1] - first[1][0]
    assert late_sum - late_state == pytest.approx(neighbour_offset)


async def test_counter_late_pre_swap_reading_is_not_added(
    recorder_mock, hass: HomeAssistant
) -> None:
    """A late reading that breaks monotonicity against its neighbours is dropped."""
    sensor = _make_sensor(hass, "counter")
    await _run(sensor, hass, _counter([0, 1, 3, 4]), full=True)
    before = await _stored(hass)

    bogus = _counter([2])
    bogus[0]["value"] = 9999.0  # far above the next stored reading
    await _run(sensor, hass, _counter([0, 1, 3, 4]) + bogus, full=False)

    assert await _stored(hass) == before
