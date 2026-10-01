"""A counter sensor's sums must not step between the first import and later polls.

Regression: the first full fetch on a fresh install stored ``sum = value -
first_reading`` (0.0, 0.5, ...), while the next periodic poll stored
``sum = value + offset`` (the raw ~100). The Energy dashboard read that ~100
jump between two adjacent hours as ~100 units of consumption in one hour, until
the next restart rewrote every row.
"""
from __future__ import annotations

import pytest
from homeassistant.core import HomeAssistant

from .test_late_hours import _counter, _make_sensor, _run, _stored


def _sums(rows: dict[int, tuple[float, float]]) -> list[float]:
    return [rows[h][1] for h in sorted(rows)]


async def test_counter_first_install_and_later_poll_use_the_same_convention(
    recorder_mock, hass: HomeAssistant
) -> None:
    sensor = _make_sensor(hass, "counter")

    await _run(sensor, hass, _counter([0, 1, 2, 3]), full=True)
    await _run(sensor, hass, _counter([0, 1, 2, 3, 4, 5]), full=False)
    rows = await _stored(hass)

    assert len(rows) == 6
    # sum - state is one constant (0.0 offset) across first-install and polled rows.
    assert {round(s - st, 6) for st, s in rows.values()} == {0.0}
    # No step between adjacent hours: the sum advances by the raw 0.5/hour.
    sums = _sums(rows)
    assert [round(b - a, 6) for a, b in zip(sums, sums[1:])] == [0.5] * 5


async def test_counter_first_install_matches_what_a_restart_would_store(
    recorder_mock, hass: HomeAssistant
) -> None:
    """The startup full fetch (existing stats present) must not change any row."""
    sensor = _make_sensor(hass, "counter")
    await _run(sensor, hass, _counter([0, 1, 2, 3]), full=True)
    first = await _stored(hass)

    await _run(sensor, hass, _counter([0, 1, 2, 3]), full=True)  # "restart"

    assert await _stored(hass) == first


async def test_counter_late_hour_after_first_install_fits_its_neighbours(
    recorder_mock, hass: HomeAssistant
) -> None:
    """A late hour takes its offset from stored neighbours, so it must agree
    with rows written by both the first import and a later poll."""
    sensor = _make_sensor(hass, "counter")
    await _run(sensor, hass, _counter([0, 1, 3, 4]), full=True)
    await _run(sensor, hass, _counter([0, 1, 3, 4, 5, 6]), full=False)
    await _run(sensor, hass, _counter(list(range(7))), full=False)  # late hour 2

    rows = await _stored(hass)
    assert len(rows) == 7
    assert {round(s - st, 6) for st, s in rows.values()} == {0.0}
    assert _sums(rows) == pytest.approx([100.0 + 0.5 * h for h in range(7)])


async def _fetch_older(sensor, hass, readings) -> int:
    from unittest.mock import AsyncMock

    from pytest_homeassistant_custom_component.components.recorder.common import (
        async_wait_recording_done,
    )

    sensor._fetch_historical_chunked = AsyncMock(return_value=readings)
    count = await sensor.async_fetch_older_history(3, 1)
    await async_wait_recording_done(hass)
    return count


async def test_counter_older_history_continues_the_stored_series(
    recorder_mock, hass: HomeAssistant
) -> None:
    """"Fetch more history" must not step the sum at the seam with stored rows."""
    sensor = _make_sensor(hass, "counter")
    await _run(sensor, hass, _counter([0, 1, 2, 3]), full=True)

    assert await _fetch_older(sensor, hass, _counter([-6, -5, -4, -3, -2, -1])) == 6

    rows = await _stored(hass)
    sums = _sums(rows)
    assert len(sums) == 10
    assert {round(s - st, 6) for st, s in rows.values()} == {0.0}
    assert [round(b - a, 6) for a, b in zip(sums, sums[1:])] == [0.5] * 9


async def test_counter_older_history_takes_the_offset_of_the_stored_neighbour(
    recorder_mock, hass: HomeAssistant
) -> None:
    sensor = _make_sensor(hass, "counter")
    await _run(sensor, hass, _counter([0, 1, 2, 3]), full=True)
    sensor._meter_offset = 7.0  # in-memory value must not override the neighbour

    await _fetch_older(sensor, hass, _counter([-3, -2, -1]))

    rows = await _stored(hass)
    assert {round(s - st, 6) for st, s in rows.values()} == {0.0}


async def test_counter_older_history_without_stored_rows_uses_the_meter_offset(
    recorder_mock, hass: HomeAssistant
) -> None:
    sensor = _make_sensor(hass, "counter")
    sensor._meter_offset = 7.0

    await _fetch_older(sensor, hass, _counter([-3, -2, -1]))

    rows = await _stored(hass)
    assert len(rows) == 3
    assert {round(s - st, 6) for st, s in rows.values()} == {7.0}


async def test_consumption_older_history_shifts_the_totals_of_stored_rows(
    recorder_mock, hass: HomeAssistant
) -> None:
    """Hours added before stored rows raise those rows' running totals."""
    from .test_late_hours import _consumption

    sensor = _make_sensor(hass, "consumption")
    await _run(sensor, hass, _consumption([0, 1, 2, 3, 4]), full=True)

    assert await _fetch_older(sensor, hass, _consumption([-3, -2, -1])) == 3

    rows = await _stored(hass)
    assert sorted(rows) == list(range(-3, 5))
    # One unit per hour throughout: the total is a straight line, no seam.
    assert {h: round(s, 6) for h, (_, s) in rows.items()} == {h: float(h + 4) for h in range(-3, 5)}
    assert sensor._cumulative_sum == pytest.approx(8.0)


async def test_consumption_older_history_is_idempotent_and_handles_overlap(
    recorder_mock, hass: HomeAssistant
) -> None:
    from .test_late_hours import _consumption

    sensor = _make_sensor(hass, "consumption")
    await _run(sensor, hass, _consumption([0, 1, 2, 3, 4]), full=True)

    # Overlaps hours 0..1 that are already stored; only -2, -1 are new.
    assert await _fetch_older(sensor, hass, _consumption([-2, -1, 0, 1])) == 2
    once = await _stored(hass)
    assert {h: round(s, 6) for h, (_, s) in once.items()} == {h: float(h + 3) for h in range(-2, 5)}

    assert await _fetch_older(sensor, hass, _consumption([-2, -1, 0, 1])) == 0
    assert await _stored(hass) == once
    assert sensor._cumulative_sum == pytest.approx(7.0)


async def test_consumption_older_history_then_periodic_poll_stays_continuous(
    recorder_mock, hass: HomeAssistant
) -> None:
    from .test_late_hours import _consumption

    sensor = _make_sensor(hass, "consumption")
    await _run(sensor, hass, _consumption([0, 1, 2]), full=True)
    await _fetch_older(sensor, hass, _consumption([-2, -1]))
    await _run(sensor, hass, _consumption([0, 1, 2, 3, 4]), full=False)

    rows = await _stored(hass)
    assert {h: round(s, 6) for h, (_, s) in rows.items()} == {h: float(h + 3) for h in range(-2, 5)}


async def test_consumption_older_history_without_stored_rows_starts_from_zero(
    recorder_mock, hass: HomeAssistant
) -> None:
    from .test_late_hours import _consumption

    sensor = _make_sensor(hass, "consumption")
    await _fetch_older(sensor, hass, _consumption([-3, -2, -1]))

    rows = await _stored(hass)
    assert {h: round(s, 6) for h, (_, s) in rows.items()} == {-3: 1.0, -2: 2.0, -1: 3.0}
