"""Unit tests for the pure reconciliation and offset logic.

These tests have no Home Assistant dependency and run with plain pytest.
Run with:
    pip install pytest
    pytest tests_unit/
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

# Load reconcile.py directly so we don't pull in custom_components/__init__.py
# (which imports homeassistant and voluptuous).
_RECONCILE_PATH = (
    Path(__file__).resolve().parent.parent
    / "custom_components"
    / "maalerportal"
    / "reconcile.py"
)
_spec = importlib.util.spec_from_file_location("_maalerportal_reconcile", _RECONCILE_PATH)
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)

compute_swap_offset = _module.compute_swap_offset
find_new_installations = _module.find_new_installations
reconcile_installations = _module.reconcile_installations


def _make_inst(
    installation_id: str,
    *,
    address: str = "Test Street 1, 123 45 Testtown",
    timezone: str = "Europe/Copenhagen",
    installation_type: str = "ColdWater",
    utility_name: str = "Test Utility",
    meter_serial: str = "TEST-SERIAL-A",
    nickname=None,
) -> dict:
    return {
        "installationId": installation_id,
        "address": address,
        "timezone": timezone,
        "installationType": installation_type,
        "utilityName": utility_name,
        "meterSerial": meter_serial,
        "nickname": nickname,
    }


# ---------------------------------------------------------------------------
# reconcile_installations
# ---------------------------------------------------------------------------


def test_no_changes_returns_unchanged_false():
    inst = _make_inst("inst-1")
    saved = [inst]
    fresh = [_make_inst("inst-1")]

    merged, missing, serial_changes, changed = reconcile_installations(saved, fresh)

    assert merged == [inst]
    assert missing == set()
    assert serial_changes == {}
    assert changed is False


def test_meter_serial_change_is_flagged_as_swap():
    saved = [_make_inst("inst-1", meter_serial="OLD-123")]
    fresh = [_make_inst("inst-1", meter_serial="NEW-456")]

    merged, missing, serial_changes, changed = reconcile_installations(saved, fresh)

    assert merged[0]["meterSerial"] == "NEW-456"
    assert "inst-1" in serial_changes
    assert serial_changes["inst-1"]["meterSerial"] == ("OLD-123", "NEW-456")
    assert missing == set()
    assert changed is True


def test_address_change_updates_but_does_not_flag_swap():
    saved = [_make_inst("inst-1", address="Old Street 1")]
    fresh = [_make_inst("inst-1", address="New Street 2")]

    merged, missing, serial_changes, changed = reconcile_installations(saved, fresh)

    assert merged[0]["address"] == "New Street 2"
    assert serial_changes == {}
    assert missing == set()
    assert changed is True


def test_missing_upstream_is_kept_and_marked():
    saved = [_make_inst("inst-1"), _make_inst("inst-2")]
    fresh = [_make_inst("inst-1")]

    merged, missing, serial_changes, changed = reconcile_installations(saved, fresh)

    assert {i["installationId"] for i in merged} == {"inst-1", "inst-2"}
    assert missing == {"inst-2"}
    assert serial_changes == {}
    # No tracked field actually changed — the missing-ness is signalled
    # via missing_ids, not via the changed flag.
    assert changed is False


def test_new_upstream_is_not_added_automatically():
    saved = [_make_inst("inst-1")]
    fresh = [_make_inst("inst-1"), _make_inst("inst-new")]

    merged, missing, serial_changes, changed = reconcile_installations(saved, fresh)

    # Only the configured installation is kept.
    assert [i["installationId"] for i in merged] == ["inst-1"]
    assert missing == set()
    assert serial_changes == {}
    assert changed is False


def test_null_nickname_does_not_overwrite_existing_nickname():
    saved = [_make_inst("inst-1", nickname="Stuga")]
    fresh = [_make_inst("inst-1", nickname=None)]

    merged, missing, serial_changes, changed = reconcile_installations(saved, fresh)

    # We never overwrite a saved field with None — protects against a
    # transient API quirk wiping out user-friendly data.
    assert merged[0]["nickname"] == "Stuga"
    assert changed is False


def test_setting_nickname_when_previously_none_updates():
    saved = [_make_inst("inst-1", nickname=None)]
    fresh = [_make_inst("inst-1", nickname="Stuga")]

    merged, missing, serial_changes, changed = reconcile_installations(saved, fresh)

    assert merged[0]["nickname"] == "Stuga"
    assert changed is True


def test_multiple_installations_only_changed_one_flagged():
    saved = [
        _make_inst("inst-1", meter_serial="A1"),
        _make_inst("inst-2", meter_serial="B1"),
    ]
    fresh = [
        _make_inst("inst-1", meter_serial="A1"),     # unchanged
        _make_inst("inst-2", meter_serial="B2"),     # swapped
    ]

    merged, missing, serial_changes, changed = reconcile_installations(saved, fresh)

    assert "inst-2" in serial_changes
    assert "inst-1" not in serial_changes
    assert {i["installationId"]: i["meterSerial"] for i in merged} == {
        "inst-1": "A1",
        "inst-2": "B2",
    }


def test_realistic_payload_shape_with_swap():
    """Two installations, one with a meter swap — modelled on the shape of
    a real /addresses payload (synthetic identifiers only)."""
    saved = [
        {
            "installationId": "00000000-0000-0000-0000-000000000001",
            "address": "Test Street 1, 123 45 Testtown",
            "timezone": "Europe/Copenhagen",
            "installationType": "ColdWater",
            "utilityName": "Test Utility",
            "meterSerial": "TEST-SERIAL-A",
            "nickname": None,
        },
        {
            "installationId": "00000000-0000-0000-0000-000000000002",
            "address": "Test Street 2, 123 45 Testtown",
            "timezone": "Europe/Copenhagen",
            "installationType": "ColdWater",
            "utilityName": "Test Utility",
            "meterSerial": "OLD-SERIAL-BEFORE-SWAP",
            "nickname": None,
        },
    ]
    fresh = [
        {
            "installationId": "00000000-0000-0000-0000-000000000001",
            "address": "Test Street 1, 123 45 Testtown",
            "timezone": "Europe/Copenhagen",
            "installationType": "ColdWater",
            "utilityName": "Test Utility",
            "meterSerial": "TEST-SERIAL-A",
            "nickname": None,
        },
        {
            "installationId": "00000000-0000-0000-0000-000000000002",
            "address": "Test Street 2, 123 45 Testtown",
            "timezone": "Europe/Copenhagen",
            "installationType": "ColdWater",
            "utilityName": "Test Utility",
            "meterSerial": "NEW-SERIAL-AFTER-SWAP",
            "nickname": None,
        },
    ]

    merged, missing, serial_changes, changed = reconcile_installations(saved, fresh)

    assert changed is True
    assert missing == set()
    swapped_id = "00000000-0000-0000-0000-000000000002"
    assert swapped_id in serial_changes
    swapped = next(i for i in merged if i["installationId"] == swapped_id)
    assert swapped["meterSerial"] == "NEW-SERIAL-AFTER-SWAP"


# ---------------------------------------------------------------------------
# find_new_installations
# ---------------------------------------------------------------------------


def test_find_new_installations_returns_unconfigured():
    saved = [_make_inst("inst-1")]
    fresh = [_make_inst("inst-1"), _make_inst("inst-2")]

    new = find_new_installations(saved, fresh)

    assert [i["installationId"] for i in new] == ["inst-2"]


def test_find_new_installations_empty_when_all_configured():
    saved = [_make_inst("inst-1"), _make_inst("inst-2")]
    fresh = [_make_inst("inst-1"), _make_inst("inst-2")]

    assert find_new_installations(saved, fresh) == []


# ---------------------------------------------------------------------------
# compute_swap_offset
# ---------------------------------------------------------------------------


def test_compute_swap_offset_preserves_continuity():
    """First reading after swap should display the same sum as the last
    reading before the swap."""
    last_displayed_sum = 1000.0   # what user saw right before swap
    first_new_raw_value = 0.5     # API's first reading from the new meter

    new_offset = compute_swap_offset(last_displayed_sum, first_new_raw_value)

    # Apply the offset to the new raw value — it must reproduce the
    # previously displayed sum.
    assert pytest.approx(first_new_raw_value + new_offset) == last_displayed_sum


def test_compute_swap_offset_subsequent_reading_consumption():
    """Deltas between subsequent readings remain correct after offset."""
    last_displayed_sum = 1000.0
    first_new_raw_value = 0.5
    second_new_raw_value = 0.6   # +0.1 m³ consumed since first new reading

    offset = compute_swap_offset(last_displayed_sum, first_new_raw_value)

    second_displayed_sum = second_new_raw_value + offset
    assert pytest.approx(second_displayed_sum - last_displayed_sum) == 0.1


def test_compute_swap_offset_is_replacement_not_additive():
    """The function returns the new absolute offset, not a delta to add to
    any previous offset. Previous offsets are already baked into
    ``last_displayed_sum``."""
    last_displayed_sum = 1000.0
    first_new_raw_value = 0.5

    new_offset = compute_swap_offset(last_displayed_sum, first_new_raw_value)

    # New offset is independent of any prior offset value.
    assert new_offset == last_displayed_sum - first_new_raw_value


def test_compute_swap_offset_zero_displayed_sum_negative_offset():
    """Edge case: if the user just installed the integration and the swap
    happens before any historical sum, we get a negative offset."""
    new_offset = compute_swap_offset(last_displayed_sum=0.0, first_new_raw_value=5.0)
    assert new_offset == -5.0
    # First reading after swap displays 0 (no history to anchor to).
    assert 5.0 + new_offset == 0.0
