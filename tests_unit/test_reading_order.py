"""Unit tests for the pure timestamp-ordering helpers (no Home Assistant)."""
from __future__ import annotations

import importlib.util
from pathlib import Path

_PATH = (
    Path(__file__).resolve().parent.parent
    / "custom_components"
    / "maalerportal"
    / "timeutils.py"
)
_spec = importlib.util.spec_from_file_location("_maalerportal_timeutils", _PATH)
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)

reading_sort_key = _module.reading_sort_key
is_older_reading = _module.is_older_reading


def test_sort_key_is_chronological_across_mixed_offsets():
    later_but_sorts_first_as_text = {"timestamp": "2025-10-26T02:00:00.000+01:00"}
    earlier = {"timestamp": "2025-10-26T02:00:00.000+02:00"}
    # Sanity: the plain strings really do sort the wrong way round.
    assert sorted(
        [earlier, later_but_sorts_first_as_text], key=lambda r: r["timestamp"]
    )[0] is later_but_sorts_first_as_text

    ordered = sorted([later_but_sorts_first_as_text, earlier], key=reading_sort_key)

    assert ordered == [earlier, later_but_sorts_first_as_text]


def test_sort_key_mixes_z_and_offset_forms_correctly():
    rows = [
        {"timestamp": "2026-05-11T12:00:00.000+02:00"},  # 10:00Z
        {"timestamp": "2026-05-11T09:00:00.000Z"},
        {"timestamp": "2026-05-11T11:00:00.000Z"},
    ]

    ordered = sorted(rows, key=reading_sort_key)

    assert [r["timestamp"] for r in ordered] == [
        "2026-05-11T09:00:00.000Z",
        "2026-05-11T12:00:00.000+02:00",
        "2026-05-11T11:00:00.000Z",
    ]


def test_sort_key_puts_unparseable_rows_first_without_raising():
    rows = [{"timestamp": "2026-05-11T09:00:00Z"}, {"timestamp": "garbage"}, {}]

    ordered = sorted(rows, key=reading_sort_key)

    assert ordered[-1] == {"timestamp": "2026-05-11T09:00:00Z"}


def test_is_older_reading_compares_instants_not_strings():
    assert is_older_reading("2026-05-11T10:00:00.000Z", "2026-05-11T12:00:00.000+01:00")
    assert not is_older_reading("2026-05-11T12:00:00.000+02:00", "2026-05-11T10:00:00.000Z")


def test_is_older_reading_is_false_for_equal_missing_or_malformed():
    ts = "2026-05-11T10:00:00.000Z"
    assert not is_older_reading(ts, ts)
    assert not is_older_reading(None, ts)
    assert not is_older_reading(ts, None)
    assert not is_older_reading("garbage", ts)
